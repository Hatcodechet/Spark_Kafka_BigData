CREATE TABLE IF NOT EXISTS meter_readings (
    meter_id VARCHAR NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    kwh DOUBLE PRECISION NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS aggregated_hourly (
    meter_id VARCHAR NOT NULL,
    window_start TIMESTAMPTZ NOT NULL,
    window_end TIMESTAMPTZ NOT NULL,
    total_kwh DOUBLE PRECISION NOT NULL,
    record_count BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS anomalies (
    meter_id VARCHAR NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    kwh DOUBLE PRECISION NOT NULL,
    zscore DOUBLE PRECISION,
    if_score INTEGER NOT NULL,
    is_anomaly BOOLEAN NOT NULL,
    detected_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS analytics_insights (
    insight_type VARCHAR NOT NULL,
    meter_id VARCHAR,
    value_label VARCHAR NOT NULL,
    value_numeric DOUBLE PRECISION,
    computed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
