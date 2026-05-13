from __future__ import annotations

import argparse
from pathlib import Path

import joblib
import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    count,
    current_timestamp,
    from_json,
    lit,
    pandas_udf,
    sum,
    window,
)
from pyspark.sql.types import DoubleType, StringType, StructField, StructType, TimestampType

from storage.db_writer import (
    write_aggregations,
    write_anomalies,
    write_insights,
    write_readings,
)


DEFAULT_CHECKPOINT_DIR = Path("./checkpoints")
DEFAULT_MODEL_PATH = Path("models/isolation_forest.pkl")

message_schema = StructType(
    [
        StructField("meter_id", StringType(), nullable=False),
        StructField("timestamp", TimestampType(), nullable=False),
        StructField("kwh", DoubleType(), nullable=True),
    ]
)

jdbc_url = "jdbc:postgresql://localhost:5432/electricity"
jdbc_props = {
    "user": "bdgrp",
    "password": "bdgrp2026",
    "driver": "org.postgresql.Driver",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Electricity meter Spark streaming job")
    parser.add_argument("--bootstrap", default="localhost:9092", help="Kafka bootstrap servers")
    parser.add_argument("--topic", default="electricity_meters", help="Kafka topic name")
    parser.add_argument(
        "--group-id",
        default="spark-electricity-consumer",
        help="Kafka consumer group id",
    )
    parser.add_argument(
        "--checkpoint-dir",
        default=str(DEFAULT_CHECKPOINT_DIR),
        help="Checkpoint directory root",
    )
    parser.add_argument(
        "--model-path",
        default=str(DEFAULT_MODEL_PATH),
        help="Path to trained Isolation Forest model",
    )
    parser.add_argument("--trigger-seconds", type=int, default=30, help="Microbatch interval")
    return parser.parse_args()


def build_spark_session() -> SparkSession:
    return (
        SparkSession.builder.appName("electricity-streaming")
        .master("local[*]")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0",
        )
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def read_kafka_stream(spark: SparkSession, bootstrap: str, topic: str, group_id: str) -> DataFrame:
    df_raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap)
        .option("subscribe", topic)
        .option("group.id", group_id)
        .option("startingOffsets", "earliest")
        .load()
    )
    return df_raw


def parse_messages(df_raw: DataFrame) -> DataFrame:
    return (
        df_raw.selectExpr("CAST(value AS STRING) AS json_str")
        .select(from_json(col("json_str"), message_schema).alias("payload"))
        .select("payload.*")
    )


def clean_stream(df: DataFrame) -> DataFrame:
    return (
        df.filter(col("kwh") >= 0)
        .filter(col("kwh") < 100)
        .dropna(subset=["meter_id", "timestamp", "kwh"])
        .withColumn("ingested_at", current_timestamp())
    )


def build_hourly_aggregation(df_clean: DataFrame) -> DataFrame:
    return (
        df_clean.withWatermark("timestamp", "10 minutes")
        .groupBy(window("timestamp", "1 hour"), "meter_id")
        .agg(sum("kwh").alias("total_kwh"), count("*").alias("record_count"))
        .select(
            "meter_id",
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "total_kwh",
            "record_count",
        )
    )


def build_anomalies(df_clean: DataFrame, model_path: str) -> DataFrame:
    model = joblib.load(model_path)

    @pandas_udf("int")
    def predict_anomaly(kwh_series: pd.Series) -> pd.Series:
        clean_values = kwh_series.astype(float)
        return pd.Series(model.predict(clean_values.values.reshape(-1, 1)))

    return (
        df_clean
        .withColumn("if_score", predict_anomaly(col("kwh")))
        .withColumn("zscore", lit(None).cast("double"))
        .withColumn("is_anomaly", lit(False))
        .select(
            "meter_id",
            "timestamp",
            "kwh",
            "zscore",
            "if_score",
            "is_anomaly",
        )
    )


def _make_insight_rows(rows: list[dict]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame(
            columns=["insight_type", "meter_id", "value_label", "value_numeric", "computed_at"]
        )
    return pd.DataFrame(rows)


def _records_from_batch(batch_df) -> list[dict]:
    return [row.asDict(recursive=True) for row in batch_df.collect()]


def compute_clean_insights(batch_df, epoch_id: int) -> None:
    records = _records_from_batch(batch_df)
    if not records:
        return

    pdf = pd.DataFrame.from_records(records)
    if pdf.empty:
        return

    pdf = pdf.copy()
    pdf["timestamp"] = pd.to_datetime(pdf["timestamp"], utc=True, errors="coerce")
    pdf = pdf.dropna(subset=["timestamp", "kwh"])
    if pdf.empty:
        return

    computed_at = pd.Timestamp.utcnow()
    hour_means = pdf.groupby(pdf["timestamp"].dt.hour)["kwh"].mean().sort_values(ascending=False)
    peak_hour = int(hour_means.index[0])

    rows = [
        {
            "insight_type": "peak_hour",
            "meter_id": None,
            "value_label": f"{peak_hour:02d}:00",
            "value_numeric": float(hour_means.iloc[0]),
            "computed_at": computed_at,
        }
    ]
    write_insights(_make_insight_rows(rows), epoch_id)


def compute_agg_insights(batch_df, epoch_id: int) -> None:
    records = _records_from_batch(batch_df)
    if not records:
        return

    pdf = pd.DataFrame.from_records(records)
    if pdf.empty:
        return

    pdf = pdf.copy()
    pdf["window_start"] = pd.to_datetime(pdf["window_start"], utc=True, errors="coerce")
    pdf["total_kwh"] = pd.to_numeric(pdf["total_kwh"], errors="coerce")
    pdf = pdf.dropna(subset=["meter_id", "window_start", "total_kwh"])
    if pdf.empty:
        return

    computed_at = pd.Timestamp.utcnow()
    rows: list[dict] = []

    meter_totals = (
        pdf.groupby("meter_id", as_index=False)["total_kwh"].sum().sort_values("total_kwh", ascending=False)
    )
    for rank, row in enumerate(meter_totals.head(10).itertuples(index=False), start=1):
        rows.append(
            {
                "insight_type": "top_consumer",
                "meter_id": row.meter_id,
                "value_label": f"top_{rank}",
                "value_numeric": float(row.total_kwh),
                "computed_at": computed_at,
            }
        )

    for rank, row in enumerate(
        meter_totals.sort_values("total_kwh", ascending=True).head(10).itertuples(index=False),
        start=1,
    ):
        rows.append(
            {
                "insight_type": "low_consumer",
                "meter_id": row.meter_id,
                "value_label": f"low_{rank}",
                "value_numeric": float(row.total_kwh),
                "computed_at": computed_at,
            }
        )

    window_totals = (
        pdf.groupby("window_start", as_index=False)["total_kwh"].sum().sort_values("window_start")
    )
    if len(window_totals) >= 2:
        trend_value = float(window_totals["total_kwh"].iloc[-1] - window_totals["total_kwh"].iloc[-2])
    else:
        trend_value = 0.0

    if trend_value > 0:
        trend_label = "rising"
    elif trend_value < 0:
        trend_label = "falling"
    else:
        trend_label = "flat"

    rows.append(
        {
            "insight_type": "trend",
            "meter_id": None,
            "value_label": trend_label,
            "value_numeric": trend_value,
            "computed_at": computed_at,
        }
    )

    write_insights(_make_insight_rows(rows), epoch_id)


def start_console_probe(df: DataFrame, checkpoint_dir: Path, trigger_seconds: int):
    return (
        df.writeStream.outputMode("append")
        .format("console")
        .option("truncate", False)
        .option("checkpointLocation", str(checkpoint_dir / "console_probe"))
        .trigger(processingTime=f"{trigger_seconds} seconds")
        .start()
    )


def start_db_streams(
    df_clean: DataFrame,
    agg: DataFrame,
    anomalies: DataFrame,
    checkpoint_dir: Path,
    trigger_seconds: int,
):
    queries = []
    queries.append(
        df_clean.writeStream.outputMode("append")
        .foreachBatch(write_readings)
        .option("checkpointLocation", str(checkpoint_dir / "meter_readings"))
        .trigger(processingTime=f"{trigger_seconds} seconds")
        .start()
    )
    queries.append(
        agg.writeStream.outputMode("update")
        .foreachBatch(write_aggregations)
        .option("checkpointLocation", str(checkpoint_dir / "aggregated_hourly"))
        .trigger(processingTime=f"{trigger_seconds} seconds")
        .start()
    )
    queries.append(
        anomalies.writeStream.outputMode("append")
        .foreachBatch(write_anomalies)
        .option("checkpointLocation", str(checkpoint_dir / "anomalies"))
        .trigger(processingTime=f"{trigger_seconds} seconds")
        .start()
    )
    queries.append(
        df_clean.writeStream.outputMode("append")
        .foreachBatch(compute_clean_insights)
        .option("checkpointLocation", str(checkpoint_dir / "analytics_insights_clean"))
        .trigger(processingTime=f"{trigger_seconds} seconds")
        .start()
    )
    queries.append(
        agg.writeStream.outputMode("update")
        .foreachBatch(compute_agg_insights)
        .option("checkpointLocation", str(checkpoint_dir / "analytics_insights_agg"))
        .trigger(processingTime=f"{trigger_seconds} seconds")
        .start()
    )
    return queries


def main() -> None:
    args = parse_args()

    model_path = Path(args.model_path)
    if not model_path.exists():
        raise FileNotFoundError(
            f"Model file not found at {model_path}. Run spark_jobs/offline_training.py first."
        )

    checkpoint_dir = Path(args.checkpoint_dir)
    checkpoint_dir.mkdir(parents=True, exist_ok=True)

    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    df_raw = read_kafka_stream(
        spark=spark,
        bootstrap=args.bootstrap,
        topic=args.topic,
        group_id=args.group_id,
    )
    df = parse_messages(df_raw)
    df_clean = clean_stream(df)
    agg = build_hourly_aggregation(df_clean)
    anomalies = build_anomalies(df_clean, str(model_path))

    console_query = start_console_probe(df, checkpoint_dir, args.trigger_seconds)
    db_queries = start_db_streams(
        df_clean=df_clean,
        agg=agg,
        anomalies=anomalies,
        checkpoint_dir=checkpoint_dir,
        trigger_seconds=args.trigger_seconds,
    )

    try:
        console_query.awaitTermination()
        for query in db_queries:
            query.awaitTermination()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
