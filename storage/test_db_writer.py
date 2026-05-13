from __future__ import annotations

import pandas as pd

from storage.db_writer import write_aggregations, write_anomalies, write_insights, write_readings


def main() -> None:
    now = pd.Timestamp.utcnow()
    meter_id = "test_meter"

    write_readings(
        pd.DataFrame(
            [
                {
                    "meter_id": meter_id,
                    "timestamp": now,
                    "kwh": 0.42,
                    "ingested_at": now,
                }
            ]
        ),
        epoch_id=0,
    )

    write_aggregations(
        pd.DataFrame(
            [
                {
                    "meter_id": meter_id,
                    "window_start": now.floor("h"),
                    "window_end": now.floor("h") + pd.Timedelta(hours=1),
                    "total_kwh": 0.42,
                    "record_count": 1,
                }
            ]
        ),
        epoch_id=0,
    )

    write_anomalies(
        pd.DataFrame(
            [
                {
                    "meter_id": meter_id,
                    "timestamp": now,
                    "kwh": 5.5,
                    "zscore": 3.8,
                    "if_score": -1,
                    "is_anomaly": True,
                }
            ]
        ),
        epoch_id=0,
    )

    write_insights(
        pd.DataFrame(
            [
                {
                    "insight_type": "peak_hour",
                    "meter_id": None,
                    "value_label": "18:00",
                    "value_numeric": 1.23,
                    "computed_at": now,
                }
            ]
        ),
        epoch_id=0,
    )

    print("Inserted test rows into all storage tables")


if __name__ == "__main__":
    main()
