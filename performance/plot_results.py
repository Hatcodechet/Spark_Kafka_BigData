from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
RESULTS_FILE = ROOT / "performance" / "results.csv"
OUTPUT_FILE = ROOT / "performance" / "throughput_results.png"


def main() -> None:
    if not RESULTS_FILE.exists():
        raise FileNotFoundError(f"Missing results file: {RESULTS_FILE}")

    df = pd.read_csv(RESULTS_FILE)
    if df.empty:
        raise ValueError(f"No rows found in {RESULTS_FILE}")

    df["rate"] = pd.to_numeric(df["rate"], errors="coerce")
    df["duration_seconds"] = pd.to_numeric(df["duration_seconds"], errors="coerce")
    df["consumer_lag"] = pd.to_numeric(df["consumer_lag"], errors="coerce")
    df = df.dropna(subset=["rate", "duration_seconds"])
    df["expected_records"] = df["rate"] * df["duration_seconds"]

    fig, ax1 = plt.subplots(figsize=(8, 4.5))
    ax1.plot(df["rate"], df["expected_records"], marker="o", label="expected records sent")
    ax1.set_xlabel("Producer rate (records/sec)")
    ax1.set_ylabel("Expected records sent")
    ax1.grid(True, alpha=0.3)

    if df["consumer_lag"].notna().any():
        ax2 = ax1.twinx()
        ax2.plot(df["rate"], df["consumer_lag"], marker="s", color="tab:red", label="consumer lag")
        ax2.set_ylabel("Kafka consumer lag")

    fig.tight_layout()
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(OUTPUT_FILE, dpi=160)
    print(f"Saved {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
