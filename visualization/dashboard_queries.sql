-- Real-Time Monitor
SELECT
    meter_id,
    timestamp,
    kwh
FROM meter_readings
ORDER BY timestamp DESC
LIMIT 5000;

-- Hourly Aggregation
SELECT
    window_start,
    SUM(total_kwh) AS total_kwh,
    SUM(record_count) AS record_count
FROM aggregated_hourly
GROUP BY window_start
ORDER BY window_start;

-- Meter x Hour Heat Map
SELECT
    meter_id,
    EXTRACT(HOUR FROM window_start) AS hour_of_day,
    AVG(total_kwh) AS avg_kwh
FROM aggregated_hourly
GROUP BY meter_id, EXTRACT(HOUR FROM window_start)
ORDER BY meter_id, hour_of_day;

-- Anomaly Timeline
SELECT
    meter_id,
    timestamp,
    kwh,
    zscore,
    if_score,
    is_anomaly,
    detected_at
FROM anomalies
ORDER BY detected_at DESC;

-- Top Consumers
SELECT
    meter_id,
    SUM(total_kwh) AS total_kwh
FROM aggregated_hourly
GROUP BY meter_id
ORDER BY total_kwh DESC
LIMIT 20;

-- Analytics Insights Panel
SELECT
    insight_type,
    meter_id,
    value_label,
    value_numeric,
    computed_at
FROM analytics_insights
ORDER BY computed_at DESC;
