# Run Fake Pipeline

Compatibility note:
- Recommended Python version for this project: `3.11`
- Current pinned stack in `requirements.txt` is intended for `Python 3.11`
- `Python 3.12` can fail during dependency install, especially with `pandas==2.0.3`, and is also a risky match for `pyspark==3.4.0`
- If you only need to train the Isolation Forest locally on macOS, use `requirements-train-mac.txt` with your existing `Python 3.12`

## Train Only On macOS

If your immediate goal is only to generate `models/isolation_forest.pkl`, do this instead of the full stack:

```bash
python3 -m venv .venv-train
source .venv-train/bin/activate
python -m pip install --upgrade pip setuptools wheel
pip install -r requirements-train-mac.txt
python scripts/generate_fake_data.py
python spark_jobs/offline_training.py --sample-file data/sample.csv --data-dir data --model-path models/isolation_forest.pkl --max-files 3
```

This path does not need Kafka, PostgreSQL, or PySpark.

## 1. Create and activate virtual environment

```bash
python3.11 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
```

If `python3.11` is not installed yet on macOS:

```bash
brew install python@3.11
```

On a new terminal tab, reactivate it first:

```bash
source .venv/bin/activate
```

## 2. Install Python packages

```bash
pip install -r requirements.txt
```

## 3. Generate fake data

```bash
python scripts/generate_fake_data.py
```

## 4. Create PostgreSQL database and tables

```bash
createdb electricity
psql -d electricity -c "CREATE USER bdgrp WITH PASSWORD 'bdgrp2026';"
psql -d electricity -c "GRANT ALL PRIVILEGES ON DATABASE electricity TO bdgrp;"
psql -d electricity -c "GRANT ALL ON SCHEMA public TO bdgrp;"
psql -U bdgrp -d electricity -f storage/init.sql
```

If the user already exists:

```bash
psql -d electricity -c "ALTER USER bdgrp WITH PASSWORD 'bdgrp2026';"
psql -U bdgrp -d electricity -f storage/init.sql
```

## 5. Create Kafka topic

```bash
kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic electricity_meters --partitions 1 --replication-factor 1
```

## 6. Train the Isolation Forest model

```bash
python spark_jobs/offline_training.py --sample-file data/sample.csv --data-dir data --model-path models/isolation_forest.pkl --max-files 3
```

## 7. Run the Spark streaming job

Terminal 1:

```bash
source .venv/bin/activate
python spark_jobs/spark_streaming.py --bootstrap localhost:9092 --topic electricity_meters --checkpoint-dir checkpoints --model-path models/isolation_forest.pkl --trigger-seconds 10
```

## 8. Replay fake records into Kafka

Terminal 2:

```bash
source .venv/bin/activate
python producer/producer.py --bootstrap localhost:9092 --topic electricity_meters --input-file data/fake_stream.csv --rate 10
```

## 9. Inspect PostgreSQL outputs

Terminal 3:

```bash
psql -U bdgrp -d electricity -c "SELECT COUNT(*) FROM meter_readings;"
psql -U bdgrp -d electricity -c "SELECT * FROM aggregated_hourly ORDER BY window_start DESC LIMIT 10;"
psql -U bdgrp -d electricity -c "SELECT * FROM anomalies ORDER BY detected_at DESC LIMIT 10;"
psql -U bdgrp -d electricity -c "SELECT * FROM analytics_insights ORDER BY computed_at DESC LIMIT 20;"
```
