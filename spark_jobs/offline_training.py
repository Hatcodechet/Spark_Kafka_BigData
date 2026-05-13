from __future__ import annotations

import argparse
import time
from pathlib import Path
from typing import Iterable

import joblib
import numpy as np
import pandas as pd
from sklearn.ensemble import IsolationForest


DEFAULT_SAMPLE_CANDIDATES = (
    Path("data/sample.csv"),
    Path("imputed_samples.csv"),
)
DEFAULT_DATA_DIR = Path("data")
DEFAULT_MODEL_PATH = Path("models/isolation_forest.pkl")


def find_column(columns: Iterable[str], candidates: tuple[str, ...]) -> str | None:
    normalized = {column.lower().strip(): column for column in columns}
    for candidate in candidates:
        key = candidate.lower().strip()
        if key in normalized:
            return normalized[key]
    return None


def resolve_sample_path(explicit_path: str | None) -> Path:
    if explicit_path:
        path = Path(explicit_path)
        if not path.exists():
            raise FileNotFoundError(f"Sample summary not found: {path}")
        return path

    for candidate in DEFAULT_SAMPLE_CANDIDATES:
        if candidate.exists():
            return candidate

    raise FileNotFoundError(
        "No sample summary file found. Expected one of: "
        f"{', '.join(str(path) for path in DEFAULT_SAMPLE_CANDIDATES)}"
    )


def discover_meter_files(data_dir: Path) -> list[Path]:
    if not data_dir.exists():
        return []
    return sorted(path for path in data_dir.glob("*.csv") if path.name != "sample.csv")


def choose_training_files(
    sample_df: pd.DataFrame,
    data_dir: Path,
    max_files: int,
    pct_threshold: float,
) -> list[Path]:
    if "fname" not in sample_df.columns or "pct" not in sample_df.columns:
        raise ValueError("Sample summary must contain 'fname' and 'pct' columns.")

    ranked = sample_df.sort_values(["pct", "total_samples"], ascending=[True, False])
    selected: list[Path] = []

    for row in ranked.itertuples(index=False):
        pct_value = float(getattr(row, "pct", 0.0))
        if pct_value > pct_threshold:
            continue

        file_path = data_dir / str(getattr(row, "fname"))
        if file_path.exists():
            selected.append(file_path)
        if len(selected) >= max_files:
            break

    if selected:
        return selected

    return discover_meter_files(data_dir)[:max_files]


def load_consumption_series(
    file_paths: Iterable[Path],
    max_rows_per_file: int | None = None,
    max_total_rows: int | None = None,
    seed: int = 42,
    progress_every: int = 50,
) -> pd.Series:
    series_chunks: list[pd.Series] = []
    rng = np.random.default_rng(seed)
    total_rows = 0
    paths = list(file_paths)
    total_files = len(paths)
    started_at = time.monotonic()

    for index, file_path in enumerate(paths, start=1):
        df = pd.read_csv(file_path, usecols=lambda col: col.lower().strip() in {"consumption_kwh", "kwh"})
        kwh_col = find_column(df.columns, ("consumption_kwh", "kwh"))
        if not kwh_col:
            continue

        chunk = pd.to_numeric(df[kwh_col], errors="coerce")
        chunk = chunk[(chunk >= 0) & (chunk < 100)].dropna()
        if max_rows_per_file and len(chunk) > max_rows_per_file:
            random_state = int(rng.integers(0, 2**32 - 1))
            chunk = chunk.sample(n=max_rows_per_file, random_state=random_state)
        if not chunk.empty:
            series_chunks.append(chunk)
            total_rows += len(chunk)
        if progress_every and (index == 1 or index % progress_every == 0 or index == total_files):
            elapsed = time.monotonic() - started_at
            files_per_second = index / elapsed if elapsed > 0 else 0
            remaining_files = total_files - index
            eta_seconds = remaining_files / files_per_second if files_per_second else 0
            print(
                "Load progress: "
                f"{index}/{total_files} files | "
                f"rows={total_rows:,} | "
                f"elapsed={elapsed / 60:.1f}m | "
                f"eta={eta_seconds / 60:.1f}m",
                flush=True,
            )
        if max_total_rows and total_rows >= max_total_rows:
            break

    if not series_chunks:
        raise ValueError(
            "No usable 'consumption_kwh' values found in selected training files."
        )

    values = pd.concat(series_chunks, ignore_index=True)
    if max_total_rows and len(values) > max_total_rows:
        values = values.sample(n=max_total_rows, random_state=seed)
    return values.reset_index(drop=True)


def identify_unusual_meters(file_paths: Iterable[Path], top_n: int = 5) -> list[tuple[str, float]]:
    scored: list[tuple[str, float]] = []

    for file_path in file_paths:
        df = pd.read_csv(file_path)
        kwh_col = find_column(df.columns, ("consumption_kwh", "kwh"))
        if not kwh_col:
            continue

        chunk = pd.to_numeric(df[kwh_col], errors="coerce").dropna()
        if chunk.empty:
            continue

        score = float(chunk.max())
        scored.append((file_path.name, score))

    scored.sort(key=lambda item: item[1], reverse=True)
    return scored[:top_n]


def train_model(kwh_values: pd.Series, contamination: float, seed: int) -> IsolationForest:
    model = IsolationForest(
        contamination=contamination,
        n_estimators=200,
        random_state=seed,
    )
    started_at = time.monotonic()
    print(f"Fitting Isolation Forest on {len(kwh_values):,} rows...", flush=True)
    model.fit(kwh_values.to_numpy().reshape(-1, 1))
    elapsed = time.monotonic() - started_at
    print(f"Model fit finished in {elapsed / 60:.1f}m", flush=True)
    return model


def summarize_training_data(kwh_values: pd.Series) -> str:
    quantiles = kwh_values.quantile([0.5, 0.9, 0.99]).to_dict()
    return (
        f"rows={len(kwh_values)} "
        f"min={kwh_values.min():.4f} "
        f"p50={quantiles[0.5]:.4f} "
        f"p90={quantiles[0.9]:.4f} "
        f"p99={quantiles[0.99]:.4f} "
        f"max={kwh_values.max():.4f}"
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Train the Isolation Forest model for smart meter anomaly detection."
    )
    parser.add_argument("--sample-file", default=None, help="Path to sample.csv summary")
    parser.add_argument("--data-dir", default=str(DEFAULT_DATA_DIR), help="Meter CSV directory")
    parser.add_argument("--model-path", default=str(DEFAULT_MODEL_PATH), help="Output model path")
    parser.add_argument("--max-files", type=int, default=20, help="Maximum meter files to use")
    parser.add_argument(
        "--all-files",
        action="store_true",
        help="Use every matching meter file from the sample summary",
    )
    parser.add_argument(
        "--max-rows-per-file",
        type=int,
        default=None,
        help="Optional per-file row sample limit for scalable full-dataset training",
    )
    parser.add_argument(
        "--max-total-rows",
        type=int,
        default=None,
        help="Optional global row sample limit for scalable full-dataset training",
    )
    parser.add_argument(
        "--pct-threshold",
        type=float,
        default=1.0,
        help="Maximum imputation percentage to allow for training selection",
    )
    parser.add_argument(
        "--contamination",
        type=float,
        default=0.02,
        help="Expected anomaly fraction for Isolation Forest",
    )
    parser.add_argument("--seed", type=int, default=42, help="Random seed")
    parser.add_argument(
        "--progress-every",
        type=int,
        default=50,
        help="Print load progress every N files; set 0 to disable",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    sample_path = resolve_sample_path(args.sample_file)
    data_dir = Path(args.data_dir)
    model_path = Path(args.model_path)
    model_path.parent.mkdir(parents=True, exist_ok=True)

    sample_df = pd.read_csv(sample_path)
    training_files = choose_training_files(
        sample_df=sample_df,
        data_dir=data_dir,
        max_files=len(sample_df) if args.all_files else args.max_files,
        pct_threshold=args.pct_threshold,
    )

    if not training_files:
        raise FileNotFoundError(
            f"No meter CSV files found in {data_dir}. Data can be added later and "
            "the training command rerun."
        )

    print("Selected training files:")
    for file_path in training_files:
        print(f" - {file_path}")

    unusual = identify_unusual_meters(training_files)
    if unusual:
        print("High-consumption candidates to reuse as anomaly test cases:")
        for file_name, score in unusual:
            print(f" - {file_name}: max_kwh={score:.4f}")

    kwh_values = load_consumption_series(
        training_files,
        max_rows_per_file=args.max_rows_per_file,
        max_total_rows=args.max_total_rows,
        seed=args.seed,
        progress_every=args.progress_every,
    )
    print("Training data summary:", summarize_training_data(kwh_values))
    print("Decision: use Z-score + Isolation Forest in the streaming pipeline.")

    model = train_model(
        kwh_values=kwh_values,
        contamination=args.contamination,
        seed=args.seed,
    )
    joblib.dump(model, model_path)
    print(f"Saved model to {model_path}")


if __name__ == "__main__":
    main()
