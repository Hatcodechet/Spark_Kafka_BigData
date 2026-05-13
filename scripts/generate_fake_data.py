from __future__ import annotations

import hashlib
from pathlib import Path

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"


def hashed_meter_id(seed_text: str) -> str:
    return hashlib.sha256(seed_text.encode("utf-8")).hexdigest()


def make_meter_frame(meter_id: str, timestamps: pd.DatetimeIndex, base: float, slope: float) -> pd.DataFrame:
    hours = np.arange(len(timestamps))
    daily_wave = 0.25 * np.sin(2 * np.pi * (hours % 24) / 24.0)
    noise = np.random.normal(loc=0.0, scale=0.05, size=len(timestamps))
    values = base + (hours * slope) + daily_wave + noise
    values = np.clip(values, 0.02, None)

    if len(values) > 72:
        values[18] *= 4.5
        values[57] *= 5.0
        values[89] *= 6.0

    meter_df = pd.DataFrame(
        {
            "timestamp": timestamps.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "consumption_kwh": np.round(values, 3),
        }
    )
    meter_df["meter_id"] = meter_id
    return meter_df


def main() -> None:
    np.random.seed(42)
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    timestamps = pd.date_range("2017-06-24T00:00:00Z", periods=96, freq="h")
    meter_specs = [
        ("CUPS_FAKE_001", 0.40, 0.0015),
        ("CUPS_FAKE_002", 0.65, 0.0005),
        ("CUPS_FAKE_003", 0.90, -0.0003),
    ]

    sample_rows = []
    metadata_rows = []
    stream_frames = []

    for seed_text, base, slope in meter_specs:
        meter_id = hashed_meter_id(seed_text)
        frame = make_meter_frame(meter_id, timestamps, base, slope)

        meter_file = DATA_DIR / f"{meter_id}.csv"
        frame.loc[:, ["timestamp", "consumption_kwh"]].to_csv(meter_file, index=False)

        sample_rows.append(
            {
                "fname": meter_file.name,
                "total_samples": len(frame),
                "imputed_samples": 0,
                "pct": 0.0,
            }
        )
        metadata_rows.append(
            {
                "cups": meter_id,
                "tariff": "2.0TD",
                "postal_code": "700000",
                "contract_start": "2017-01-01",
                "contract_end": "",
            }
        )
        stream_frames.append(
            frame.rename(columns={"consumption_kwh": "kwh"}).loc[:, ["meter_id", "timestamp", "kwh"]]
        )

    pd.DataFrame(sample_rows).to_csv(DATA_DIR / "sample.csv", index=False)
    pd.DataFrame(metadata_rows).to_csv(DATA_DIR / "metadata.csv", index=False)
    pd.concat(stream_frames, ignore_index=True).sort_values(["timestamp", "meter_id"]).to_csv(
        DATA_DIR / "fake_stream.csv",
        index=False,
    )

    print(f"Generated fake dataset in {DATA_DIR}")
    print("Files:")
    print(" - data/sample.csv")
    print(" - data/metadata.csv")
    print(" - data/fake_stream.csv")
    for row in sample_rows:
        print(f" - data/{row['fname']}")


if __name__ == "__main__":
    main()
