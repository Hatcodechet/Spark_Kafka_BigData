from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

import pandas as pd


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Replay smart meter readings to Kafka.")
    parser.add_argument("--bootstrap", default="localhost:9092", help="Kafka bootstrap server")
    parser.add_argument("--topic", default="electricity_meters", help="Kafka topic")
    parser.add_argument("--rate", type=float, default=100.0, help="Records per second")
    parser.add_argument("--meters", type=int, default=50, help="Number of meters to replay")
    parser.add_argument("--data-dir", default="data", help="Directory containing meter CSV files")
    parser.add_argument("--metadata-file", default="data/metadata.csv", help="Metadata CSV path")
    parser.add_argument("--seed", type=int, default=42, help="Random seed for meter sampling")
    parser.add_argument("--loop", action="store_true", help="Replay the input repeatedly")
    parser.add_argument(
        "--input-file",
        default=None,
        help="Optional combined CSV with meter_id,timestamp,kwh columns",
    )
    return parser.parse_args()


def _find_column(columns: list[str], candidates: tuple[str, ...]) -> str | None:
    normalized = {column.lower().strip(): column for column in columns}
    for candidate in candidates:
        key = candidate.lower().strip()
        if key in normalized:
            return normalized[key]
    return None


def _load_combined_input(input_path: Path) -> pd.DataFrame:
    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")

    df = pd.read_csv(input_path)
    required_columns = {"meter_id", "timestamp", "kwh"}
    missing = required_columns.difference(df.columns)
    if missing:
        raise ValueError(f"Input file missing columns: {sorted(missing)}")

    return df.loc[:, ["meter_id", "timestamp", "kwh"]]


def _load_meter_file(meter_id: str, file_path: Path) -> pd.DataFrame:
    if not file_path.exists():
        raise FileNotFoundError(f"Meter file not found for {meter_id}: {file_path}")

    df = pd.read_csv(file_path)
    columns = list(df.columns)
    timestamp_col = _find_column(columns, ("timestamp", "datetime", "date", "time", "dt"))
    kwh_col = _find_column(columns, ("consumption_kwh", "kwh"))

    if not timestamp_col or not kwh_col:
        raise ValueError(
            f"{file_path} must contain timestamp/date and consumption_kwh/kwh columns"
        )

    output = pd.DataFrame(
        {
            "meter_id": meter_id,
            "timestamp": pd.to_datetime(df[timestamp_col], utc=True, errors="coerce"),
            "kwh": pd.to_numeric(df[kwh_col], errors="coerce"),
        }
    )
    output = output.dropna(subset=["timestamp", "kwh"])
    output["timestamp"] = output["timestamp"].dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    return output


def _load_from_metadata(metadata_path: Path, data_dir: Path, meters: int, seed: int) -> pd.DataFrame:
    if not metadata_path.exists():
        meter_files = sorted(data_dir.glob("*.csv"))
        if not meter_files:
            raise FileNotFoundError(
                f"Metadata file not found: {metadata_path}; no meter CSV files in {data_dir}"
            )
        selected_files = pd.Series(meter_files).sample(
            n=min(meters, len(meter_files)),
            random_state=seed,
        )
        frames = [
            _load_meter_file(file_path.stem, file_path)
            for file_path in selected_files
            if file_path.name not in {"sample.csv", "metadata.csv", "fake_stream.csv"}
        ]
        if not frames:
            raise FileNotFoundError(f"No meter CSV files found in {data_dir}")
        return pd.concat(frames, ignore_index=True)

    metadata_header = pd.read_csv(metadata_path, nrows=0)
    cups_col = _find_column(list(metadata_header.columns), ("cups", "meter_id"))
    if not cups_col:
        raise ValueError(f"{metadata_path} must contain a cups or meter_id column")
    metadata = pd.read_csv(metadata_path, usecols=[cups_col])

    available_files = {
        file_path.stem: file_path
        for file_path in data_dir.glob("*.csv")
        if file_path.name not in {"sample.csv", "metadata.csv", "fake_stream.csv", "real_stream_sample.csv"}
    }
    if not available_files:
        raise FileNotFoundError(f"No meter CSV files found in {data_dir}")

    meter_ids = metadata[cups_col].dropna().astype(str).drop_duplicates()
    meter_ids = meter_ids[meter_ids.isin(available_files.keys())]
    if meter_ids.empty:
        raise ValueError(
            f"No metadata meter IDs from {metadata_path} have matching CSV files in {data_dir}"
        )

    selected = meter_ids.sample(
        n=min(meters, len(meter_ids)),
        random_state=seed,
    ).tolist()

    frames: list[pd.DataFrame] = []
    for meter_id in selected:
        file_path = available_files[meter_id]
        frames.append(_load_meter_file(meter_id, file_path))

    if not frames:
        raise FileNotFoundError(
            "No selected meter files were found. First missing examples: "
            f"{missing[:5]}"
        )

    return pd.concat(frames, ignore_index=True)


def _build_messages(args: argparse.Namespace) -> pd.DataFrame:
    if args.input_file:
        df = _load_combined_input(Path(args.input_file))
    else:
        df = _load_from_metadata(
            metadata_path=Path(args.metadata_file),
            data_dir=Path(args.data_dir),
            meters=args.meters,
            seed=args.seed,
        )

    df = df.copy()
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
    df["kwh"] = pd.to_numeric(df["kwh"], errors="coerce")
    df = df.dropna(subset=["meter_id", "timestamp", "kwh"])
    df = df[df["kwh"] >= 0]
    df["timestamp"] = df["timestamp"].dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    return df.sort_values(["timestamp", "meter_id"]).reset_index(drop=True)


def main() -> None:
    args = parse_args()
    df = _build_messages(args)
    if df.empty:
        raise ValueError("No valid readings to send")

    from kafka import KafkaProducer

    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )

    delay_seconds = 0.0 if args.rate <= 0 else 1.0 / args.rate
    sent = 0

    try:
        while True:
            for row in df.itertuples(index=False):
                message = {
                    "meter_id": str(row.meter_id),
                    "timestamp": str(row.timestamp),
                    "kwh": float(row.kwh),
                }
                producer.send(args.topic, value=message)
                sent += 1
                if sent % 50 == 0:
                    producer.flush()
                    print(f"Sent {sent} records")
                if delay_seconds:
                    time.sleep(delay_seconds)
            if not args.loop:
                break
    finally:
        producer.flush()
        producer.close()

    print(f"Finished sending {sent} records to topic '{args.topic}'")


if __name__ == "__main__":
    main()
