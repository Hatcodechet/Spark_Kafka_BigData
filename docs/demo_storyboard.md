# Demo Storyboard

Target length: 90 seconds

1. Start the Kafka producer and show records entering topic `electricity_meters`.
2. Start `spark_jobs/spark_streaming.py` and show console output for parsed readings.
3. Show PostgreSQL tables receiving rows:
   - `meter_readings`
   - `aggregated_hourly`
   - `anomalies`
   - `analytics_insights`
4. Trigger or highlight an anomaly and capture the red `[ALERT]` log in the Spark console.
5. Switch to the dashboard and show live updates.

Suggested tooling:
- OBS Studio for screen capture
- QuickTime as a lightweight fallback on macOS
