from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Prepare raw GoiEner CSV files for this pipeline.")
    parser.add_argument("--raw-dir", default="data/raw_goiener_v7", help="Directory with raw meter CSVs")
    parser.add_argument("--output-dir", default="data", help="Directory for generated contract files")
    parser.add_argument(
        "--metadata-file",
        default="data/metadata.csv",
        help="Existing real metadata file. It will be filtered, not overwritten.",
    )
    parser.add_argument("--sample-file", default="data/real_sample.csv", help="Output sample summary path")
    parser.add_argument(
        "--stream-file",
        default="data/real_stream_sample.csv",
        help="Output combined stream sample path",
    )
    parser.add_argument("--max-stream-meters", type=int, default=20, help="Meters to include in stream sample")
    parser.add_argument("--max-rows-per-meter", type=int, default=500, help="Rows per meter in stream sample")
    parser.add_argument(
        "--max-sample-files",
        type=int,
        default=200,
        help="Maximum raw files to scan for sample summary; use 0 for all files",
    )
    return parser.parse_args()


def read_meter(file_path: Path, max_rows: int | None = None) -> pd.DataFrame:
    df = pd.read_csv(file_path, nrows=max_rows)
    required = {"dt", "kWh"}
    missing = required.difference(df.columns)
    if missing:
        raise ValueError(f"{file_path} missing columns: {sorted(missing)}")

    output = pd.DataFrame(
        {
            "meter_id": file_path.stem,
            "timestamp": pd.to_datetime(df["dt"], utc=True, errors="coerce"),
            "kwh": pd.to_numeric(df["kWh"], errors="coerce"),
        }
    )
    output = output.dropna(subset=["timestamp", "kwh"])
    output = output[output["kwh"] >= 0]
    output["timestamp"] = output["timestamp"].dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    return output


def main() -> None:
    args = parse_args()
    raw_dir = Path(args.raw_dir)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    files = sorted(raw_dir.glob("*.csv"))
    if not files:
        raise FileNotFoundError(f"No CSV files found in {raw_dir}")

    sample_rows = []
    stream_frames = []
    sample_files = files if args.max_sample_files == 0 else files[: args.max_sample_files]
    stream_files = files[: args.max_stream_meters]

    for file_path in sample_files:
        df_head = pd.read_csv(file_path, usecols=["kWh"])
        total_samples = len(df_head)
        null_samples = int(pd.to_numeric(df_head["kWh"], errors="coerce").isna().sum())
        pct = round((null_samples / total_samples) * 100, 4) if total_samples else 0.0
        sample_rows.append(
            {
                "fname": file_path.name,
                "total_samples": total_samples,
                "imputed_samples": null_samples,
                "pct": pct,
            }
        )

    for file_path in stream_files:
        stream_frames.append(read_meter(file_path, max_rows=args.max_rows_per_meter))

    sample_path = Path(args.sample_file)
    stream_path = Path(args.stream_file)
    sample_path.parent.mkdir(parents=True, exist_ok=True)
    stream_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(sample_rows).to_csv(sample_path, index=False)

    if stream_frames:
        pd.concat(stream_frames, ignore_index=True).sort_values(["timestamp", "meter_id"]).to_csv(
            stream_path,
            index=False,
        )

    metadata_path = Path(args.metadata_file)
    if metadata_path.exists():
        metadata = pd.read_csv(metadata_path, usecols=["cups"])
        available = {file_path.stem for file_path in files}
        matched = metadata["cups"].astype(str).isin(available).sum()
        print(f"Metadata rows with matching raw CSV files: {matched}")

    print(f"Scanned {len(sample_files)} files for data quality summary")
    print(f"Wrote {sample_path}")
    print(f"Wrote {stream_path}")


if __name__ == "__main__":
    main()
