# IoT Smart Electricity Meter Analytics

Pipeline for replaying GoiEner smart meter readings through Kafka, processing them with Spark Structured Streaming, and writing results to PostgreSQL.

## Docker Resources On Mac

Recommended Docker Desktop resources:

- Minimum: `6 CPU`, `10 GB RAM`, `30 GB disk`
- Recommended: `8 CPU`, `12-16 GB RAM`, `50 GB disk`
- Mac with `16 GB` system RAM: give Docker about `8-10 GB`
- Mac with `32 GB+` system RAM: give Docker about `12-16 GB`

GPU is not needed. The current model is `scikit-learn` `IsolationForest`, which runs on CPU.

## Real Data Layout

Expected files:

```text
data/
├── metadata.csv
└── raw_goiener_v7/
    └── <cups>.csv
```

Raw meter CSV schema:

```text
dt,fl,kWh
```

Pipeline mapping:

- `meter_id`: filename stem, matching `metadata.csv.cups`
- `timestamp`: parsed from `dt`
- `kwh`: parsed from `kWh`
- `fl`: ignored

## Full Run For Report

Use this section for the main run with real data.

### 1. Create Python Environment

Recommended Python 3.11:

```bash
python3.11 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip setuptools wheel
pip install -r requirements.txt
```

Fallback if Python 3.11 is broken on macOS:

```bash
python3 -m venv .venv-train
source .venv-train/bin/activate
python -m pip install --upgrade pip setuptools wheel
pip install -r requirements-train-mac.txt
pip install pyspark==3.4.0 psycopg2-binary==2.9.7 pyarrow
```

### 2. Start Docker Services

Stop local Homebrew Kafka if it is using port `9092`:

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

PostgreSQL schema is loaded automatically from `storage/init.sql`. To rerun it:

```bash
docker compose exec -T postgres psql -U bdgrp -d electricity < storage/init.sql
```

### 3. Prepare Real Data Summary

This scans all raw meter files to build `data/real_sample.csv`, and creates a stream sample for debugging.

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
- `data/real_stream_sample.csv`: small combined stream sample for tests

### 4. Train Model For Report

This trains across all files in `real_sample.csv`, but samples rows per file so Mac memory stays stable.

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

Expected completion line:

```text
Saved model to models/isolation_forest.pkl
```

Smoke test:

```bash
python -c "import joblib, numpy as np; m=joblib.load('models/isolation_forest.pkl'); print(m.predict(np.array([[0.5],[1.0],[5.0]])))"
```

Expected shape:

```text
[ 1  1 -1]
```

### 5. Reset Previous Runtime State

Run before starting a fresh report run:

```bash
rm -rf checkpoints
docker compose exec postgres psql -U bdgrp -d electricity -c "TRUNCATE meter_readings, aggregated_hourly, anomalies, analytics_insights;"
```

### 6. Start Spark Streaming

Terminal 1:

```bash
PYTHONPATH=. python -m spark_jobs.spark_streaming \
  --bootstrap localhost:9092 \
  --topic electricity_meters \
  --checkpoint-dir checkpoints \
  --model-path models/isolation_forest.pkl \
  --trigger-seconds 10
```

macOS warnings about hostname loopback or native Hadoop are usually safe to ignore.

### 7. Replay Real Raw Data

Terminal 2, main report run:

```bash
PYTHONPATH=. python -m producer.producer \
  --bootstrap localhost:9092 \
  --topic electricity_meters \
  --metadata-file data/metadata.csv \
  --data-dir data/raw_goiener_v7 \
  --meters 200 \
  --rate 200
```

For a smaller but still real run:

```bash
PYTHONPATH=. python -m producer.producer \
  --bootstrap localhost:9092 \
  --topic electricity_meters \
  --metadata-file data/metadata.csv \
  --data-dir data/raw_goiener_v7 \
  --meters 50 \
  --rate 100
```

### 8. Verify PostgreSQL Outputs

```bash
docker compose exec postgres psql -U bdgrp -d electricity -c "SELECT COUNT(*) FROM meter_readings;"
docker compose exec postgres psql -U bdgrp -d electricity -c "SELECT COUNT(*) FROM aggregated_hourly;"
docker compose exec postgres psql -U bdgrp -d electricity -c "SELECT COUNT(*) FROM anomalies;"
docker compose exec postgres psql -U bdgrp -d electricity -c "SELECT COUNT(*) FROM analytics_insights;"
```

Inspect rows:

```bash
docker compose exec postgres psql -U bdgrp -d electricity -c "SELECT * FROM meter_readings ORDER BY ingested_at DESC LIMIT 5;"
docker compose exec postgres psql -U bdgrp -d electricity -c "SELECT * FROM aggregated_hourly ORDER BY window_start DESC LIMIT 5;"
docker compose exec postgres psql -U bdgrp -d electricity -c "SELECT * FROM anomalies ORDER BY detected_at DESC LIMIT 5;"
docker compose exec postgres psql -U bdgrp -d electricity -c "SELECT * FROM analytics_insights ORDER BY computed_at DESC LIMIT 10;"
```

## Quick Test Commands

Use this section only when debugging quickly.

### Prepare A Small Real Stream File

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

### Fast Model Train

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

### Replay Small Real Sample

```bash
PYTHONPATH=. python -m producer.producer \
  --bootstrap localhost:9092 \
  --topic electricity_meters \
  --input-file data/real_stream_sample.csv \
  --rate 50
```

## Monitor Running Processes

Check training progress from logs. The training script prints:

```text
Load progress: 50/1000 files | rows=5,000 | elapsed=0.3m | eta=5.7m
Fitting Isolation Forest on 200,000 rows...
Model fit finished in 1.2m
```

Check process runtime:

```bash
ps -eo pid,etime,pcpu,pmem,command | grep offline_training | grep -v grep
```

Check Docker resource usage:

```bash
docker stats
```

## Tableau Dashboard Inputs

Connect Tableau to PostgreSQL:

- Host: `localhost`
- Port: `5432`
- Database: `electricity`
- Username: `bdgrp`
- Password: `bdgrp2026`

Dashboard SQL templates:

```text
visualization/dashboard_queries.sql
```

## Performance Evaluation

Run producer stress tests:

```bash
chmod +x performance/run_eval.sh
PYTHONPATH=. DURATION_SECONDS=300 performance/run_eval.sh
```

Generate chart:

```bash
python performance/plot_results.py
```

Outputs:

- `performance/results.csv`
- `performance/throughput_results.png`

## Stop Services

Stop containers:

```bash
docker compose down
```

Remove database/Kafka volumes too:

```bash
docker compose down -v
```
