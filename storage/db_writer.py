from __future__ import annotations

import os
from typing import Iterable

import pandas as pd
import psycopg2


DEFAULT_CONNSTRING = os.getenv(
    "ELECTRICITY_DB_URL",
    "postgresql://bdgrp:bdgrp2026@localhost:5432/electricity",
)


def _normalize_value(value):
    if hasattr(value, "to_pydatetime"):
        return value.to_pydatetime()
    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            return value
    return value


def _rows_from_pandas(df, columns: Iterable[str]) -> list[tuple]:
    normalized_rows: list[tuple] = []
    for row in df.loc[:, list(columns)].itertuples(index=False, name=None):
        normalized_rows.append(tuple(_normalize_value(value) for value in row))
    return normalized_rows


def _records_from_batch(batch_df) -> list[dict]:
    if hasattr(batch_df, "collect"):
        return [row.asDict(recursive=True) for row in batch_df.collect()]
    return batch_df.to_dict("records")


def _rows_from_records(records: list[dict], columns: Iterable[str]) -> list[tuple]:
    return [
        tuple(_normalize_value(record.get(column)) for column in columns)
        for record in records
    ]


def _write_rows(insert_sql: str, rows: list[tuple]) -> None:
    if not rows:
        return

    connection = psycopg2.connect(DEFAULT_CONNSTRING)
    try:
        with connection:
            with connection.cursor() as cursor:
                cursor.executemany(insert_sql, rows)
    finally:
        connection.close()


def write_readings(batch_df, epoch_id: int) -> None:
    records = _records_from_batch(batch_df)
    if not records:
        return

    for record in records:
        record.setdefault("ingested_at", None)

    rows = _rows_from_records(records, ["meter_id", "timestamp", "kwh", "ingested_at"])
    _write_rows(
        """
        INSERT INTO meter_readings (meter_id, timestamp, kwh, ingested_at)
        VALUES (%s, %s, %s, COALESCE(%s, NOW()))
        """,
        rows,
    )


def write_aggregations(batch_df, epoch_id: int) -> None:
    records = _records_from_batch(batch_df)
    if not records:
        return

    rows = _rows_from_records(
        records,
        ["meter_id", "window_start", "window_end", "total_kwh", "record_count"],
    )
    _write_rows(
        """
        INSERT INTO aggregated_hourly
            (meter_id, window_start, window_end, total_kwh, record_count)
        VALUES (%s, %s, %s, %s, %s)
        """,
        rows,
    )


def write_anomalies(batch_df, epoch_id: int) -> None:
    records = _records_from_batch(batch_df)
    if not records:
        return

    pdf = pd.DataFrame.from_records(records)
    if pdf.empty:
        return

    pdf = pdf.copy()
    pdf["kwh"] = pd.to_numeric(pdf["kwh"], errors="coerce")
    grouped = pdf.groupby("meter_id")["kwh"]
    mu = grouped.transform("mean")
    sigma = grouped.transform("std")
    computed_zscore = (pdf["kwh"] - mu) / sigma

    if "zscore" not in pdf.columns:
        pdf["zscore"] = computed_zscore
    else:
        pdf["zscore"] = pd.to_numeric(pdf["zscore"], errors="coerce").fillna(computed_zscore)

    pdf["if_score"] = pd.to_numeric(pdf["if_score"], errors="coerce").fillna(0).astype(int)
    pdf["is_anomaly"] = (pdf["zscore"].abs() > 3) | (pdf["if_score"] == -1)

    flagged = pdf[pdf["is_anomaly"] == True].copy()  # noqa: E712
    if flagged.empty:
        return

    rows = _rows_from_pandas(
        flagged,
        ["meter_id", "timestamp", "kwh", "zscore", "if_score", "is_anomaly"],
    )
    _write_rows(
        """
        INSERT INTO anomalies
            (meter_id, timestamp, kwh, zscore, if_score, is_anomaly, detected_at)
        VALUES (%s, %s, %s, %s, %s, %s, NOW())
        """,
        rows,
    )

    for row in flagged.itertuples(index=False):
        zscore = getattr(row, "zscore", None)
        zscore_str = f"{float(zscore):.2f}" if zscore is not None else "n/a"
        print(
            f"\033[91m[ALERT] Meter {row.meter_id} | {row.timestamp} | "
            f"{row.kwh:.2f} kWh | z={zscore_str}\033[0m"
        )


def write_insights(batch_df, epoch_id: int) -> None:
    records = _records_from_batch(batch_df)
    if not records:
        return

    rows = _rows_from_records(
        records,
        ["insight_type", "meter_id", "value_label", "value_numeric", "computed_at"],
    )
    _write_rows(
        """
        INSERT INTO analytics_insights
            (insight_type, meter_id, value_label, value_numeric, computed_at)
        VALUES (%s, %s, %s, %s, COALESCE(%s, NOW()))
        """,
        rows,
    )
