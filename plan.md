# Big Data Project Plan
## IoT-Based Smart Electricity Meter Analytics

---

## 1. Shared Interface Contract

> Agree and freeze this on Day 1 morning (30-min sync). Do not change without telling the group.

### 1.1 Kafka

| Field | Value |
|---|---|
| Bootstrap server | `localhost:9092` |
| Topic name | `electricity_meters` |
| Consumer group ID | `spark-electricity-consumer` |
| Message format | JSON |
| Partition count | `1` |

**Message schema — one record = one meter reading:**
```json
{
  "meter_id":  "bd2b4dc5f0736640d40f6a88ce0db71698177c72dbb05145d70b66d0513e26bd",
  "timestamp": "2017-06-24T00:00:00Z",
  "kwh":       0.312
}
```

| Field | Type | Rule |
|---|---|---|
| `meter_id` | string | `cups` value from `metadata.csv` (SHA-256 hashed CUPS code) |
| `timestamp` | ISO 8601 UTC string | format `YYYY-MM-DDTHH:MM:SSZ`, derived from the per-meter CSV filename/rows |
| `kwh` | float | ≥ 0; maps to `consumption_kwh` in the per-meter CSV |

> **Note:** The GoiEner dataset does **not** include an `active_power` field. The field has been removed from the schema. All downstream tables and Spark code must reflect this.

---

### 1.2 PostgreSQL

| Field | Value |
|---|---|
| Host | `localhost` |
| Port | `5432` |
| Database | `electricity` |
| Username | `bdgrp` |
| Password | `bdgrp2026` |
| JDBC URL | `jdbc:postgresql://localhost:5432/electricity` |
| Python connstring | `postgresql://bdgrp:bdgrp2026@localhost:5432/electricity` |

**Table: `meter_readings`** — raw cleaned records

| Column | Type | Notes |
|---|---|---|
| `meter_id` | `VARCHAR` | hashed CUPS code |
| `timestamp` | `TIMESTAMPTZ` | |
| `kwh` | `DOUBLE PRECISION` | |
| `ingested_at` | `TIMESTAMPTZ` | `NOW()` at write time |

**Table: `aggregated_hourly`** — one row per meter per hour

| Column | Type | Notes |
|---|---|---|
| `meter_id` | `VARCHAR` | |
| `window_start` | `TIMESTAMPTZ` | |
| `window_end` | `TIMESTAMPTZ` | |
| `total_kwh` | `DOUBLE PRECISION` | |
| `record_count` | `BIGINT` | |

**Table: `anomalies`** — flagged records

| Column | Type | Notes |
|---|---|---|
| `meter_id` | `VARCHAR` | |
| `timestamp` | `TIMESTAMPTZ` | |
| `kwh` | `DOUBLE PRECISION` | |
| `zscore` | `DOUBLE PRECISION` | null if < 2 readings in window |
| `if_score` | `INTEGER` | `-1` anomaly · `1` normal · `0` not scored |
| `is_anomaly` | `BOOLEAN` | true if zscore > 3 OR if_score = -1 |
| `detected_at` | `TIMESTAMPTZ` | `NOW()` at write time |

**Table: `analytics_insights`** — Day 3 extras

| Column | Type | Notes |
|---|---|---|
| `insight_type` | `VARCHAR` | `peak_hour` · `top_consumer` · `trend` |
| `meter_id` | `VARCHAR` | null if global insight |
| `value_label` | `VARCHAR` | human-readable label |
| `value_numeric` | `DOUBLE PRECISION` | |
| `computed_at` | `TIMESTAMPTZ` | |

---

### 1.3 Spark

| Field | Value |
|---|---|
| App name | `electricity-streaming` |
| Master | `local[*]` |
| Kafka package | `org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0` |
| Postgres JDBC jar | `postgresql-42.6.0.jar` |
| Checkpoint dir | `./checkpoints/` |
| Watermark — aggregation | `10 minutes` |
| Watermark — anomaly window | `30 minutes` |
| Batch interval | `30 seconds` |

**Spark read from Kafka (copy exactly):**
```python
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "electricity_meters") \
    .option("group.id", "spark-electricity-consumer") \
    .option("startingOffsets", "earliest") \
    .load()
```

**JSON parse schema (copy exactly):**
```python
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

message_schema = StructType([
    StructField("meter_id",  StringType(),    nullable=False),
    StructField("timestamp", TimestampType(), nullable=False),
    StructField("kwh",       DoubleType(),    nullable=True),
])
```

**JDBC write properties (copy exactly):**
```python
jdbc_url   = "jdbc:postgresql://localhost:5432/electricity"
jdbc_props = {
    "user":     "bdgrp",
    "password": "bdgrp2026",
    "driver":   "org.postgresql.Driver",
}
```

---

### 1.4 Model file

| Field | Value |
|---|---|
| Path | `models/isolation_forest.pkl` |
| Trained by | Tri in `offline_training.py` |
| Loaded by | Tri inside Spark Pandas UDF |
| Format | `joblib` |
| Input | `kwh` column, reshaped to `(-1, 1)` |
| Output | `-1` = anomaly · `1` = normal |

---

### 1.5 Producer CLI

| Flag | Default | Meaning |
|---|---|---|
| `--rate` | `100` | records per second |
| `--meters` | `50` | distinct meter IDs to simulate (sampled from `metadata.csv`) |
| `--topic` | `electricity_meters` | Kafka topic |
| `--bootstrap` | `localhost:9092` | Kafka server |

---

### 1.6 Repository structure

```
/
├── producer/
│   └── producer.py             ← Huy
├── spark_jobs/
│   ├── spark_streaming.py      ← Tri
│   └── offline_training.py     ← Tri
├── storage/
│   ├── init.sql                ← Bao
│   └── db_writer.py            ← Bao
├── models/
│   └── isolation_forest.pkl    ← Tri (generated, gitignore large files)
├── visualization/
│   └── dashboard.twbx          ← Bao
├── performance/
│   ├── run_eval.sh             ← Huy
│   ├── plot_results.py         ← Huy
│   └── results.csv             ← generated
├── data/
│   ├── metadata.csv            ← shared — meter registry (cups, tariff, postal code…)
│   ├── sample.csv              ← shared — data quality summary (fname, total_samples…)
│   └── <meter_csvs>/           ← per-meter consumption files (hashed filename = cups)
├── docs/
│   ├── report.pdf              ← Bao
│   ├── slides.pdf              ← Huy
│   └── demo.mp4                ← Tri
├── checkpoints/                ← generated, gitignore
├── docker-compose.yml          ← Huy
├── requirements.txt            ← Huy
└── README.md                   ← Huy
```

---

### 1.7 Environment

**`requirements.txt`:**
```
pyspark==3.4.0
kafka-python==2.0.2
pandas==2.0.3
scikit-learn==1.3.0
psycopg2-binary==2.9.7
joblib==1.3.2
matplotlib==3.7.2
```

---

### 1.8 Docker service names

| Service | Internal hostname | External port |
|---|---|---|
| Zookeeper | `zookeeper` | — |
| Kafka | `kafka` | `9092` |
| PostgreSQL | `postgres` | `5432` |
| Elasticsearch | `elasticsearch` | `9200` |
| Kibana | `kibana` | `5601` |

> Running Spark locally (outside Docker): use `localhost` for all hosts.  
> Running Spark inside a Docker container: use the service name, e.g. `kafka:9092`.

---

## 2. Architecture

```
GoiEner Dataset
  ├── metadata.csv          (cups → meter info: tariff, postal code, contract dates)
  ├── sample.csv            (data quality summary per meter file)
  └── <hashed_cups>.csv     (per-meter hourly consumption_kwh)
          │
  [Python Simulator]        ← HUY
  [Apache Kafka]            ← HUY
          │
  [Spark Structured Streaming]  ← TRI
      ├── Cleaning & Filtering
      ├── Window Aggregation
      ├── Anomaly Detection (Z-score + Isolation Forest)
      └── foreachBatch → db_writer.py
                   │
            [PostgreSQL]       ← BAO (schema + writes)
                   │
         [Tableau Dashboard]   ← BAO
```

---

## 3. Overview of Deliverables

| # | Deliverable | Owner |
|---|---|---|
| 1 | Data streaming simulation tool | Huy |
| 2 | Kafka collection pipeline | Huy |
| 3 | Spark analytics (clean, aggregate, anomaly) | Tri |
| 4 | Visualization dashboards | Bao |
| 5 | Performance evaluation | Huy |
| 6 | Final report | Bao |
| 7 | Presentation slides | Huy |
| 8 | Demo video | Tri |

---

## 4. Team Ownership

| Member | Task |
|---|---|
| **Tran Gia Huy** | Infrastructure (Docker, Kafka, GitHub), Kafka Producer, Performance Evaluation, Slides |
| **Pham Nguyen Viet Tri** | All Spark jobs, Isolation Forest training, Analytics insights, Demo video |
| **Nguyen Le Gia Bao** | PostgreSQL schema + write sinks, All Dashboards, Final report |

---

## 5. Dataset

**GoiEner Smart Meter Dataset** — Spanish cooperative energy dataset.

## 6. Day 1 — Build the Pipe

**Goal:** End-to-end skeleton running by end of day. Real data flowing through, even if analytics are minimal.

### Huy — Infrastructure + Producer

- [ ] Start Docker Compose (compose file in appendix)
- [ ] Create GitHub repo, push folder structure, invite teammates
- [ ] Write `producer/producer.py`:
  - Load `metadata.csv` with Pandas; sample `--meters` distinct `cups` values
  - For each selected meter, load the matching `<cups_hash>.csv` consumption file
  - Replay rows chronologically across all meters
  - Support `--rate` and `--meters` flags
  - Send JSON messages to Kafka topic `electricity_meters` using schema from §1.1
- [ ] Verify with `kafka-console-consumer` — confirm messages appear
- [ ] Write `README.md`: how to start Docker, how to run the producer

**Checkpoint:** Producer streams to Kafka. README works.

---

### Tri — Spark Skeleton + Model Training

- [ ] Write `spark_jobs/spark_streaming.py` skeleton:
  - Read from Kafka using the exact `readStream` block in §1.3
  - Parse JSON with `message_schema` from §1.3
  - Print to console sink — confirm data appears
- [ ] Explore GoiEner dataset locally with Pandas:
  - Check `sample.csv` — identify meters with low `pct` (clean data) for training
  - Check nulls, value ranges, and time span in several per-meter CSVs
  - Identify 3–5 meters with clearly unusual `consumption_kwh` readings (anomaly test cases)
  - Note the `kwh` value range — sets the `< 100` filter threshold
- [ ] Decide: Z-score only vs Z-score + Isolation Forest — commit now
- [ ] Write `spark_jobs/offline_training.py`:
  - Load several per-meter CSVs, extract `consumption_kwh` column
  - Train Isolation Forest on `kwh` values
  - Save to `models/isolation_forest.pkl` with `joblib.dump`

**Checkpoint:** Spark reads Kafka and prints. Model file exists.

---

### Bao — Storage + Schema

- [ ] Write `storage/init.sql` with all four tables from §1.2
- [ ] Run `init.sql` against Dockerized PostgreSQL, confirm all tables exist
- [ ] Write `storage/db_writer.py` with three functions:
  - `write_readings(batch_df, epoch_id)`
  - `write_aggregations(batch_df, epoch_id)`
  - `write_anomalies(batch_df, epoch_id)`
  - Each uses `psycopg2` and the connstring from §1.2
- [ ] Test each function with a small hardcoded DataFrame
- [ ] Install Tableau Public, connect to PostgreSQL, confirm it works

**Checkpoint:** All tables exist. Write functions pass tests. Tableau connects.

---

## 7. Day 2 — Build the Brain and the Face

**Critical dependency:** Bao waits for Tri's first successful DB write before building dashboards (expected: mid-morning). Bao uses the waiting time to finalize Tableau connection and plan dashboard layouts.

### Huy — Performance Instrumentation + Slides Skeleton

- [ ] Extend `producer.py`: clean support for `--rate 50`, `--rate 200`, `--rate 500`
- [ ] Write `performance/run_eval.sh`:
  - Runs producer at each rate for 5 minutes each
  - Captures Spark UI metrics: batch duration, input rows/sec
  - Captures Kafka consumer lag: `kafka-consumer-groups.sh --describe`
  - Appends rows to `performance/results.csv`
- [ ] Write `performance/plot_results.py`:
  - Reads `results.csv`
  - Plots throughput vs batch duration, saves as PNG
- [ ] Run preliminary test — confirm numbers make sense
- [ ] Draft slide deck structure (titles only, no content yet)

**Checkpoint:** Eval script runs. Preliminary results in CSV. Slide skeleton exists.

---

### Tri — Full Spark Analytics

**Step 1 — Cleaning:**
```python
df_clean = df \
    .filter(col("kwh") >= 0) \
    .filter(col("kwh") < 100) \
    .dropna(subset=["meter_id", "timestamp", "kwh"])
```

**Step 2 — Window aggregation:**
```python
agg = df_clean \
    .withWatermark("timestamp", "10 minutes") \
    .groupBy(window("timestamp", "1 hour"), "meter_id") \
    .agg(
        sum("kwh").alias("total_kwh"),
        count("*").alias("record_count")
    )
```

**Step 3 — Z-score anomaly detection:**
```python
stats = df_clean \
    .withWatermark("timestamp", "30 minutes") \
    .groupBy(window("timestamp", "24 hours", "1 hour"), "meter_id") \
    .agg(mean("kwh").alias("mu"), stddev("kwh").alias("sigma"))
# join back, compute z = (kwh - mu) / sigma, flag |z| > 3
```

**Step 4 — Isolation Forest via Pandas UDF:**
```python
import joblib
from pyspark.sql.functions import pandas_udf

model = joblib.load("models/isolation_forest.pkl")

@pandas_udf("int")
def predict_anomaly(kwh_series: pd.Series) -> pd.Series:
    return pd.Series(model.predict(kwh_series.values.reshape(-1, 1)))

df_scored = df_clean.withColumn("if_score", predict_anomaly(col("kwh")))
```

**Step 5 — Wire sinks:**
```python
from storage.db_writer import write_readings, write_aggregations, write_anomalies

df_clean.writeStream.foreachBatch(write_readings).start()
agg.writeStream.foreachBatch(write_aggregations).start()
anomalies.writeStream.foreachBatch(write_anomalies).start()
```

**Step 6 — Alert logic (inside `write_anomalies`):**
```python
for row in batch_df.filter(col("is_anomaly")).collect():
    print(f"\033[91m[ALERT] Meter {row.meter_id} | {row.timestamp} | {row.kwh:.2f} kWh | z={row.zscore:.2f}\033[0m")
```

**Checkpoint:** Full Spark job running. All three tables receiving data. Alerts printing.

---

### Bao — Build 4 Dashboards
*(start once Tri confirms first DB write)*

**Dashboard 1 — Real-Time Monitor**
- Line chart: `kwh` over time, one line per meter, filter to top 10 meters
- Auto-refresh every 60 seconds

**Dashboard 2 — Hourly Aggregation**
- Bar chart: `total_kwh` per hour across all meters
- Heat map: `meter_id` × hour of day → avg consumption

**Dashboard 3 — Anomaly Timeline**
- Scatter plot: timestamp vs `kwh`, anomalies in red, normal in grey
- Table below: `meter_id`, `timestamp`, `zscore`, sorted by severity

**Dashboard 4 — Top Consumers**
- Ranked bar chart: top 20 meters by `total_kwh`
- KPI tiles: total meters monitored · total anomalies · peak consumption hour

**Checkpoint:** All 4 dashboards built with real data.

---

## 8. Day 3 — Polish, Evaluate, Package

All three work in parallel all morning. Converge at ~3pm to hand off:
- Bao → Huy: dashboard screenshots
- Bao → Bao: screenshots into report
- Huy → Bao: perf charts + bottleneck analysis paragraph for report
- Tri → Huy: slide 6 and slide 10 content (Spark explanation + key findings)

### Huy — Stress Tests + Finalize Slides

- [ ] Run full stress test at 50 / 200 / 500 rec/sec, 5 min each, record all metrics
- [ ] Run `plot_results.py`, verify charts are clean
- [ ] Write bottleneck analysis: where does the pipeline slow down and why?
- [ ] Finalize slides:

| Slide | Content |
|---|---|
| 1 | Title + team |
| 2 | Problem & motivation |
| 3 | Architecture diagram |
| 4 | Dataset overview (GoiEner structure, metadata fields, data quality via `sample.csv`) |
| 5 | Kafka + Producer |
| 6 | Spark analytics + code snippets *(content from Tri)* |
| 7–8 | Dashboard screenshots *(from Bao)* |
| 9 | Performance results + charts |
| 10 | Key findings *(content from Tri)* |
| 11 | Conclusion + future work |

- [ ] Final GitHub push, tag `v1.0`
- [ ] Finalize `README.md`

**Checkpoint:** Stress test results done. Slides complete. Repo tagged.

---

### Tri — Analytics Insights + Demo Video

**Analytics (write to `analytics_insights` table):**
- [ ] Peak hour detection: which hour 0–23 has highest average kwh?
- [ ] Meter ranking: top 10 high consumers vs top 10 low consumers
- [ ] Trend: is aggregate consumption rising or falling across stream windows?
- [ ] Notify Bao when `analytics_insights` has data — she adds a 5th dashboard panel if time allows

**Demo video:**
- [ ] Tool: OBS Studio (free) or QuickTime
- [ ] Show in sequence: start producer → Kafka topic fills → Spark processes → dashboard updates live → anomaly alert in console
- [ ] Target: 90 seconds, no audio needed
- [ ] Save to `docs/demo.mp4`

**Checkpoint:** Demo video saved. Analytics insights in DB.

---

### Bao — Dashboard Polish + Final Report

**Dashboard polish:**
- [ ] Add proper titles, axis labels with units (kWh)
- [ ] Color legend for anomaly severity
- [ ] Tooltips showing raw values on hover
- [ ] Add 5th panel from `analytics_insights` if Tri's data is ready
- [ ] Export high-res screenshots of all dashboards
- [ ] Package `dashboard.twbx` into `/visualization/`

**Final report** (Google Docs or LaTeX → export PDF):

1. Introduction — why IoT electricity monitoring matters
2. Dataset — GoiEner structure (`metadata.csv`, `sample.csv`, per-meter CSVs), row count, time range, data quality notes (imputation rate from `pct` column)
3. System Architecture — diagram + component explanations
4. Implementation
   - 4a. Data Simulator
   - 4b. Kafka setup
   - 4c. Spark Streaming (cleaning, aggregation, Z-score, Isolation Forest — explain both)
   - 4d. Storage (schema, foreachBatch, why PostgreSQL)
   - 4e. Visualization
5. Analytics Results — anomaly examples, aggregation output, peak hour, meter ranking
6. Performance Evaluation — Huy's table + charts + bottleneck paragraph
7. Conclusion & Future Work — federated learning, edge deployment, concept drift

**Target: 15–20 pages.**

**Checkpoint:** Report PDF in `/docs/`. Screenshots committed.

---

## 9. Evening — Final Checklist

- [ ] Code pushed, `v1.0` tag, README complete — **Huy**
- [ ] Report PDF in `/docs/report.pdf` — **Bao**
- [ ] Slides PDF in `/docs/slides.pdf` — **Huy**
- [ ] Demo video in `/docs/demo.mp4` — **Tri**
- [ ] Zip: `BDGrp_2252264_2252845_2352097_final.zip`

---

## 10. Workload Summary

| Member | Day 1 | Day 2 | Day 3 |
|---|---|---|---|
| **Huy** | Docker + Kafka + producer | Perf eval script + slides skeleton | Stress tests + finalize slides |
| **Tri** | Spark skeleton + dataset explore + train model | Full Spark analytics + sinks + alerts | Analytics insights + demo video |
| **Bao** | Postgres schema + write functions + Tableau connect | 4 dashboards | Dashboard polish + full report |

---

## Appendix: Docker Compose

```yaml
version: '3.8'
services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.4.0
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181

  kafka:
    image: confluentinc/cp-kafka:7.4.0
    depends_on: [zookeeper]
    ports:
      - "9092:9092"
    environment:
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"

  postgres:
    image: postgres:15
    ports:
      - "5432:5432"
    environment:
      POSTGRES_DB: electricity
      POSTGRES_USER: bdgrp
      POSTGRES_PASSWORD: bdgrp2026

  elasticsearch:
    image: docker.elastic.co/elasticsearch/elasticsearch:8.10.0
    environment:
      - discovery.type=single-node
      - xpack.security.enabled=false
    ports:
      - "9200:9200"

  kibana:
    image: docker.elastic.co/kibana/kibana:8.10.0
    depends_on: [elasticsearch]
    ports:
      - "5601:5601"
```

---

## Appendix: Tools & Versions

| Tool | Version | Notes |
|---|---|---|
| Python | `3.10` | use `venv` |
| Apache Kafka | `3.x` via Docker | `confluentinc/cp-kafka:7.4.0` |
| Apache Spark | `3.4.x` | PySpark |
| PostgreSQL | `15` | via Docker |
| Tableau | Public (free) | connects to PostgreSQL |
| Pandas | `2.0.3` | |
| scikit-learn | `1.3.0` | Isolation Forest |
| Docker Compose | latest | |