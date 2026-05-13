# IoT Smart Electricity Meter Analytics

Pipeline for replaying GoiEner smart meter readings through Kafka, processing them with Spark Structured Streaming, and writing results to PostgreSQL.

## Docker Resources On Mac

Recommended Docker Desktop resources:

- Minimum: `6 CPU`, `10 GB RAM`, `30 GB disk`
- Recommended: `8 CPU`, `12-16 GB RAM`, `50 GB disk`
- If your Mac has only `16 GB` system RAM, set Docker to about `8-10 GB`
- If your Mac has `32 GB+` system RAM, set Docker to `12-16 GB`

This project does not need GPU. The model is `scikit-learn` `IsolationForest`, which runs on CPU.

## Components

- `producer/producer.py`: replays meter readings to Kafka topic `electricity_meters`
- `spark_jobs/offline_training.py`: trains `models/isolation_forest.pkl`
- `spark_jobs/spark_streaming.py`: cleans data, aggregates hourly usage, detects anomalies, computes insights
- `storage/init.sql`: PostgreSQL schema
- `storage/db_writer.py`: Spark `foreachBatch` write helpers
- `scripts/prepare_real_data.py`: prepares small real-data samples and quality summary
- `performance/run_eval.sh`: stress runner for producer rates
- `performance/plot_results.py`: performance chart generator

## 1. Python Environment

Recommended full environment with Python 3.11:

```bash
python3.11 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip setuptools wheel
pip install -r requirements.txt
```

If Python 3.11 is broken on macOS, use your existing Python 3.12 environment:

```bash
python3 -m venv .venv-train
source .venv-train/bin/activate
python -m pip install --upgrade pip setuptools wheel
pip install -r requirements-train-mac.txt
pip install pyspark==3.4.0 psycopg2-binary==2.9.7 pyarrow
```

All following Python commands assume one of these environments is active.

## 2. Start Infrastructure

If Homebrew Kafka is running on port `9092`, stop it first:

```bash
brew services stop kafka 2>/dev/null || true
```

Start Kafka, Zookeeper, and PostgreSQL:

```bash
docker compose up -d zookeeper kafka postgres
```

Check services:

```bash
docker compose ps
```

Create Kafka topic:

```bash
docker compose exec kafka kafka-topics \
  --bootstrap-server localhost:9092 \
  --create \
  --if-not-exists \
  --topic electricity_meters \
  --partitions 1 \
  --replication-factor 1
```

PostgreSQL schema is loaded automatically from `storage/init.sql`. To rerun manually:

```bash
docker compose exec -T postgres psql -U bdgrp -d electricity < storage/init.sql
```

## 3. Real Data Layout

Expected layout:

```text
data/
├── metadata.csv
└── raw_goiener_v7/
    └── <cups>.csv
```

Real raw meter files use:

```text
dt,fl,kWh
```

Mapping to Kafka:

- `meter_id`: filename stem, matching `metadata.csv.cups`
- `timestamp`: parsed from `dt`
- `kwh`: parsed from `kWh`
- `fl`: ignored

## 4. Prepare Real Data On Mac

This scans all raw files for `real_sample.csv`, but only builds a small stream sample so local testing stays fast:

```bash
python scripts/prepare_real_data.py \
  --raw-dir data/raw_goiener_v7 \
  --metadata-file data/metadata.csv \
  --sample-file data/real_sample.csv \
  --stream-file data/real_stream_sample.csv \
  --max-stream-meters 20 \
  --max-rows-per-meter 500 \
  --max-sample-files 0
```

Outputs:

- `data/real_sample.csv`: quality summary for all raw meter files
- `data/real_stream_sample.csv`: small combined stream sample for quick testing

If this scan is too slow on your Mac, use this faster command first:

```bash
python scripts/prepare_real_data.py \
  --raw-dir data/raw_goiener_v7 \
  --metadata-file data/metadata.csv \
  --sample-file data/real_sample.csv \
  --stream-file data/real_stream_sample.csv \
  --max-stream-meters 20 \
  --max-rows-per-meter 500 \
  --max-sample-files 1000
```

## 5. Train Model On Mac

Use bounded sampling across all selected meter files. This covers the full dataset without loading every row into RAM:

```bash
python spark_jobs/offline_training.py \
  --sample-file data/real_sample.csv \
  --data-dir data/raw_goiener_v7 \
  --model-path models/isolation_forest.pkl \
  --all-files \
  --max-rows-per-file 50 \
  --max-total-rows 1000000 \
  --progress-every 50
```

For a lighter Mac run:

```bash
python spark_jobs/offline_training.py \
  --sample-file data/real_sample.csv \
  --data-dir data/raw_goiener_v7 \
  --model-path models/isolation_forest.pkl \
  --max-files 200 \
  --max-rows-per-file 100 \
  --max-total-rows 200000 \
  --progress-every 20
```

Smoke test the trained model:

```bash
python -c "import joblib, numpy as np; m=joblib.load('models/isolation_forest.pkl'); print(m.predict(np.array([[0.5],[1.0],[5.0]])))"
```

Expected shape:

```text
[ 1  1 -1]
```

## 6. Run Spark Streaming

Terminal 1:

```bash
rm -rf checkpoints
PYTHONPATH=. python -m spark_jobs.spark_streaming \
  --bootstrap localhost:9092 \
  --topic electricity_meters \
  --checkpoint-dir checkpoints \
  --model-path models/isolation_forest.pkl \
  --trigger-seconds 10
```

Warnings about hostname loopback or native Hadoop on macOS are usually safe to ignore.

## 7. Replay Real Data

Terminal 2, quick test with the small prepared stream sample:

```bash
PYTHONPATH=. python -m producer.producer \
  --bootstrap localhost:9092 \
  --topic electricity_meters \
  --input-file data/real_stream_sample.csv \
  --rate 50
```

Terminal 2, replay directly from raw real files selected through metadata:

```bash
PYTHONPATH=. python -m producer.producer \
  --bootstrap localhost:9092 \
  --topic electricity_meters \
  --metadata-file data/metadata.csv \
  --data-dir data/raw_goiener_v7 \
  --meters 50 \
  --rate 100
```

For a heavier local run:

```bash
PYTHONPATH=. python -m producer.producer \
  --bootstrap localhost:9092 \
  --topic electricity_meters \
  --metadata-file data/metadata.csv \
  --data-dir data/raw_goiener_v7 \
  --meters 200 \
  --rate 200
```

## 8. Verify PostgreSQL Outputs

```bash
docker compose exec postgres psql -U bdgrp -d electricity -c "SELECT COUNT(*) FROM meter_readings;"
docker compose exec postgres psql -U bdgrp -d electricity -c "SELECT COUNT(*) FROM aggregated_hourly;"
docker compose exec postgres psql -U bdgrp -d electricity -c "SELECT COUNT(*) FROM anomalies;"
docker compose exec postgres psql -U bdgrp -d electricity -c "SELECT COUNT(*) FROM analytics_insights;"
```

Inspect rows:

```bash
docker compose exec postgres psql -U bdgrp -d electricity -c "SELECT * FROM meter_readings ORDER BY ingested_at DESC LIMIT 5;"
docker compose exec postgres psql -U bdgrp -d electricity -c "SELECT * FROM anomalies ORDER BY detected_at DESC LIMIT 5;"
docker compose exec postgres psql -U bdgrp -d electricity -c "SELECT * FROM analytics_insights ORDER BY computed_at DESC LIMIT 10;"
```

## 9. Reset Pipeline State

Use this before rerunning from scratch:

```bash
rm -rf checkpoints
docker compose exec postgres psql -U bdgrp -d electricity -c "TRUNCATE meter_readings, aggregated_hourly, anomalies, analytics_insights;"
```

Clear Kafka data too:

```bash
docker compose down -v
docker compose up -d zookeeper kafka postgres
docker compose exec kafka kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists --topic electricity_meters --partitions 1 --replication-factor 1
```

## 10. Tableau Dashboard Inputs

Connect Tableau to PostgreSQL:

- Host: `localhost`
- Port: `5432`
- Database: `electricity`
- Username: `bdgrp`
- Password: `bdgrp2026`

Dashboard SQL templates are in:

```text
visualization/dashboard_queries.sql
```

## 11. Report Metrics Commands

Use these queries after the report run finishes, or after stopping the producer
and letting Spark catch up for 30-60 seconds.

Total processed readings:

```bash
docker compose exec postgres psql -U bdgrp -d electricity -c \
  "SELECT COUNT(*) AS total_readings FROM meter_readings;"
```

Number of meters included in the processed sample:

```bash
docker compose exec postgres psql -U bdgrp -d electricity -c \
  "SELECT COUNT(DISTINCT meter_id) AS meters FROM meter_readings;"
```

Data time range:

```bash
docker compose exec postgres psql -U bdgrp -d electricity -c \
  "SELECT MIN(timestamp) AS start_time, MAX(timestamp) AS end_time FROM meter_readings;"
```

Sample cleaned readings:

```bash
docker compose exec postgres psql -U bdgrp -d electricity -c \
  "SELECT * FROM meter_readings ORDER BY ingested_at DESC LIMIT 10;"
```

Hourly aggregation count and sample rows:

```bash
docker compose exec postgres psql -U bdgrp -d electricity -c \
  "SELECT COUNT(*) AS hourly_aggregations FROM aggregated_hourly;"

docker compose exec postgres psql -U bdgrp -d electricity -c \
  "SELECT * FROM aggregated_hourly ORDER BY window_start DESC LIMIT 10;"
```

Top consuming meters:

```bash
docker compose exec postgres psql -U bdgrp -d electricity -c \
  "SELECT meter_id, SUM(kwh) AS total_kwh, COUNT(*) AS readings
   FROM meter_readings
   GROUP BY meter_id
   ORDER BY total_kwh DESC
   LIMIT 10;"
```

Average consumption by hour of day:

```bash
docker compose exec postgres psql -U bdgrp -d electricity -c \
  "SELECT EXTRACT(HOUR FROM timestamp) AS hour, AVG(kwh) AS avg_kwh
   FROM meter_readings
   GROUP BY hour
   ORDER BY hour;"
```

Anomaly count and sample rows:

```bash
docker compose exec postgres psql -U bdgrp -d electricity -c \
  "SELECT COUNT(*) AS anomalies FROM anomalies;"

docker compose exec postgres psql -U bdgrp -d electricity -c \
  "SELECT * FROM anomalies ORDER BY detected_at DESC LIMIT 10;"
```

Analytics insight rows:

```bash
docker compose exec postgres psql -U bdgrp -d electricity -c \
  "SELECT * FROM analytics_insights ORDER BY computed_at DESC LIMIT 20;"
```

Kafka messages published:

```bash
docker compose exec kafka kafka-run-class kafka.tools.GetOffsetShell \
  --bootstrap-server localhost:9092 \
  --topic electricity_meters \
  --time -1
```

Latest Spark checkpoint offset for `meter_readings`:

```bash
for file in checkpoints/meter_readings/offsets/*; do
  [ -f "$file" ] && tail -1 "$file"
done | tail -1
```

## 12. Report Run Results

Snapshot from the report run on 2026-05-13. The producer used real GoiEner
raw meter files with `--meters 50 --rate 100`; it was stopped after enough
records were collected for reporting.

### End-to-End Pipeline Counts

| Metric | Value |
|---|---:|
| Kafka messages in `electricity_meters` | 368,599 |
| Rows in `meter_readings` | 368,599 |
| Distinct processed meters | 25 |
| Rows in `aggregated_hourly` | 241,019 |
| Rows in `anomalies` | 18,461 |
| Rows in `analytics_insights` | 6,260 |
| Data start time | `2015-02-15 08:00:00+00` |
| Data end time | `2019-04-30 11:00:00+00` |

Kafka offset and PostgreSQL row count match, so Spark consumed the published
Kafka messages and persisted them to PostgreSQL for this run.

### Sample Cleaned Readings

```text
meter_id                                                           | timestamp              | kwh   | ingested_at
-------------------------------------------------------------------+------------------------+-------+----------------------------
98c4478cbedef395eede13c35ca79183a03d72582f50d079baa78fde79ccf8d7   | 2019-04-29 17:00:00+00 | 0.047 | 2026-05-13 19:35:10.016+00
a813ebcf750238731dac13e70c6b48209fb1f5ca291200b0225578384f27b262   | 2019-04-29 17:00:00+00 | 0.526 | 2026-05-13 19:35:10.016+00
94403b758e86bf4aeff39ba2e860fe8cfee08e577175484cbbc0f95558fefc0e   | 2019-04-29 17:00:00+00 | 0.667 | 2026-05-13 19:35:10.016+00
96d1b46fb4feb8871c7465ce4487237f9b0c34c8f1af9d2a0e9f0772cd623022   | 2019-04-29 17:00:00+00 | 0.062 | 2026-05-13 19:35:10.016+00
b2290ac6dd7b56a1e42a2047be0a77f1a386cd4fb08b60263ac6156f32287950   | 2019-04-29 17:00:00+00 | 0.048 | 2026-05-13 19:35:10.016+00
```

### Sample Hourly Aggregations

```text
meter_id                                                           | window_start           | window_end             | total_kwh | record_count
-------------------------------------------------------------------+------------------------+------------------------+-----------+-------------
704bddc1b60bfc6dfe9528bf19154eb0524f30e5f17545eb3ff8b96aa42c3570   | 2019-04-30 11:00:00+00 | 2019-04-30 12:00:00+00 | 0.014     | 1
300141648c8dd816d1124e66136edb43475d4536bea3fc223ad44df0a81d174d   | 2019-04-30 11:00:00+00 | 2019-04-30 12:00:00+00 | 0.007     | 1
2be0825227236334b637e7f90e9a86918095ee60dcfb45f96b91b95f49445b1c   | 2019-04-30 11:00:00+00 | 2019-04-30 12:00:00+00 | 0.038     | 1
7e5c0d33574b8c30f9952c1130bf35ab3b67bc9639589f2e31b4322ecc5a5696   | 2019-04-30 11:00:00+00 | 2019-04-30 12:00:00+00 | 0.128     | 1
96d1b46fb4feb8871c7465ce4487237f9b0c34c8f1af9d2a0e9f0772cd623022   | 2019-04-30 11:00:00+00 | 2019-04-30 12:00:00+00 | 0.068     | 1
```

### Top Consuming Meters

| Rank | Meter ID | Total kWh | Readings |
|---:|---|---:|---:|
| 1 | `81432fbd5ddc6b25726742ab0aaeeded9ca5181964e0005aa2256e18e564278a` | 129,859.000 | 9,413 |
| 2 | `c5130c4a3a3fb345914848959ee500af24ebe6672e3b824c3e7adc0fcc53cfb5` | 29,930.045 | 30,192 |
| 3 | `80516b6d6b31b537c7593e5dec43992c809c114b0c3e90a5e95f7dd62bd0b6c1` | 22,714.166 | 30,196 |
| 4 | `5381f5136a54114eb4b636f83e65602a4ee4a73ecf5650e9a80a5b77b86c1876` | 15,511.694 | 30,197 |
| 5 | `9186ebc73475b315e0ca7aa83ffbce4ae5e834256a396a8d05a6971f8e313ae9` | 10,670.988 | 13,969 |
| 6 | `4cba3f795826bdbe86397a5f6972c6c648cf096a57437e960ac2ab367411bdc5` | 9,937.493 | 10,516 |
| 7 | `1bcf064b9b16d4fb9db7500d91e572a41b683e26a8b45126b593553b7bc556a4` | 6,498.694 | 14,261 |
| 8 | `f37de2e930858efcf6c39fbcac1dd8bd8c47ee78d3353da4636695b4ea0dfd25` | 6,026.871 | 18,887 |
| 9 | `7e5c0d33574b8c30f9952c1130bf35ab3b67bc9639589f2e31b4322ecc5a5696` | 5,052.922 | 13,253 |
| 10 | `5726789c3c278be1353f4348613cafb4a2a2ebf2eef02dece1a12c5372846bd3` | 4,567.726 | 30,229 |

### Highest Average Consumption Hours

| Hour of day | Average kWh |
|---:|---:|
| 16 | 1.2285 |
| 17 | 1.1574 |
| 18 | 1.1165 |
| 19 | 1.0678 |
| 20 | 1.0143 |

### Sample Anomalies

```text
meter_id                                                           | timestamp              | kwh   | zscore             | if_score | is_anomaly
-------------------------------------------------------------------+------------------------+-------+--------------------+----------+-----------
81432fbd5ddc6b25726742ab0aaeeded9ca5181964e0005aa2256e18e564278a   | 2019-04-29 19:00:00+00 | 46.000| 1.86415111520139   | -1       | true
e17f72fc14e6778a22e0004ef2ab544b260cf324e26ae2872beb4777fa583aab   | 2019-04-29 19:00:00+00 | 1.676 | 3.938521883623028  | 1        | true
81432fbd5ddc6b25726742ab0aaeeded9ca5181964e0005aa2256e18e564278a   | 2019-04-29 18:00:00+00 | 41.000| 1.5138930768545735 | -1       | true
4cba3f795826bdbe86397a5f6972c6c648cf096a57437e960ac2ab367411bdc5   | 2019-04-29 19:00:00+00 | 3.617 | 1.8232147121271212 | -1       | true
4cba3f795826bdbe86397a5f6972c6c648cf096a57437e960ac2ab367411bdc5   | 2019-04-29 20:00:00+00 | 3.595 | 1.8050513655610252 | -1       | true
```

### Performance Benchmark Results

The producer benchmark ran each configured input rate for 120 seconds using
`data/fake_stream.csv`. Exit code `143` is expected because the benchmark script
intentionally terminates the producer after the fixed duration.

| Producer rate (records/sec) | Duration (sec) | Expected records sent | Exit code |
|---:|---:|---:|---:|
| 50 | 120 | 6,000 | 143 |
| 100 | 120 | 12,000 | 143 |
| 200 | 120 | 24,000 | 143 |
| 500 | 120 | 60,000 | 143 |

Performance chart:

```text
performance/throughput_results.png
```

## 13. Performance Evaluation

Run producer stress tests:

```bash
chmod +x performance/run_eval.sh
PYTHONPATH=. DURATION_SECONDS=120 RATES="50 100 200 500" INPUT_FILE=data/fake_stream.csv performance/run_eval.sh
```

Generate chart:

```bash
PYTHONPATH=. python performance/plot_results.py
```

Outputs:

- `performance/results.csv`
- `performance/throughput_results.png`

## Stop Services

```bash
docker compose down
```

Remove database/Kafka volumes:

```bash
docker compose down -v
```
