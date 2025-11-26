# FunnelPulse · Real-time E-commerce Funnel Analytics

FunnelPulse is an end-to-end big data pipeline for monitoring an e-commerce funnel in (near) real time, using the _eCommerce Events History in Cosmetics Shop_ dataset.

The current implementation runs on the course JupyterHub Spark cluster and uses:

- Spark batch processing
- Spark Structured Streaming (file-based source)
- A simple lakehouse layout (bronze → silver → gold)
- A statistical anomaly detection layer
- Basic visualizations in notebooks

The **next developer** is expected to:

- Move this to a cloud environment (GCP, or similar)
- Swap the file-based stream for a Kafka-based stream
- Add richer visualizations and possibly a service layer

This document explains the current structure so you can pick up exactly where the work stopped.

---

## 1. Project Structure

All project files live under the user home:

```text
~/funnelpulse/
  notebooks/
    01_batch_bronze_silver_gold.ipynb
    02_additional_gold_tables.ipynb
    03_build_stream_input_from_bronze.ipynb
    04_streaming_funnel_hourly_brand.ipynb
    05_anomaly_detection_hourly_brand.ipynb
    06_visualizations.ipynb
  data_raw/
    (temporary CSV uploads, e.g. 2019-Oct.csv, 2019-Nov.csv)
  tables/
    bronze_events/                         (Parquet)
    silver_events/                         (Parquet)
    gold_funnel_hourly_brand/              (Parquet)
    gold_funnel_daily_brand/               (Parquet)
    gold_funnel_daily_category/            (Parquet)
    gold_funnel_hourly_price_band/         (Parquet)
    gold_stream_funnel_hourly_brand/       (Parquet, streaming output)
    gold_anomalies_hourly_brand/           (Parquet)
  stream_input/
    (many small Parquet files that simulate a streaming source)
  checkpoints/
    stream_hourly_brand/                   (Spark streaming checkpoint)
```

All notebooks assume a root path:

```
home = os.path.expanduser("~")
project_root = os.path.join(home, "funnelpulse")
tables_dir = os.path.join(project_root, "tables")
```

## 2. High Level Architecture

### Dataset

- Kaggle: **eCommerce Events History in Cosmetics Shop**
- Event-level logs for a medium sized cosmetics shop
- Key columns: `event_time`, `event_type`, `user_id`, `user_session`, `product_id`, `category_code`, `brand`, `price`

### Logical Pipeline

1. **Bronze**  
   Raw events, lightly normalized, partitioned by `event_date`.

2. **Silver**  
   Cleaned, deduplicated, normalized events, still at event granularity.

3. **Gold (batch)**  
   Aggregated funnel metrics:

   - Hourly by brand
   - Daily by brand
   - Daily by category root
   - Hourly by price band

4. **Streaming gold**  
   Spark Structured Streaming pipeline computing the same hourly funnel metrics by brand, from a simulated event stream.

5. **Anomaly detection**  
   Statistical anomaly scoring over hourly brand funnel metrics, stored as a dedicated anomalies table.

6. **Visualizations**  
   Notebooks that read the gold and anomaly tables and produce charts for brands, categories, price bands, and anomalies.

---

## 3. Notebooks Overview

### 3.1 `01_batch_bronze_silver_gold.ipynb`

**Goal**  
Build the core batch lakehouse pipeline:

- Bronze: ingest raw monthly CSV files
- Silver: clean and normalize events
- First gold table: hourly funnel metrics by brand

**Main steps**

1. **Bronze creation**

   - Read raw monthly CSV from `data_raw/` (e.g. `2019-Oct.csv`, `2019-Nov.csv`)
   - Convert `event_time` to `timestamp`
   - Derive `event_date` from `event_time`
   - Write to `tables/bronze_events` as Parquet, partitioned by `event_date`, in append mode

2. **Silver creation**

   - Read all `bronze_events` for the chosen months
   - Filter:
     - Remove events with null or nonpositive `price`
     - Remove events with null `event_time` or `event_type`
   - Normalize:
     - `brand_norm` = lowercased `brand`
     - `category_code_norm` = sanitized, lowercased `category_code`
   - Deduplicate with a composite key:
     - `event_time`, `user_id`, `user_session`, `product_id`, `event_type`
   - Derive basic data quality flags
   - Write to `tables/silver_events`, partitioned by `event_date`

3. **Gold (hourly brand)**
   - Read `silver_events`
   - Group by 1-hour window on `event_time` and `brand_norm`
   - Aggregate:
     - `views`, `carts`, `purchases`, `revenue`
   - Derive:
     - `view_to_cart_rate`, `cart_to_purchase_rate`, `conversion_rate`
   - Add `window_date` for partitioning
   - Write to `tables/gold_funnel_hourly_brand`, partitioned by `window_date`

> This notebook must be run before any of the others.

---

### 3.2 `02_additional_gold_tables.ipynb`

**Goal**  
Extend the batch gold layer with more business-friendly views:

- Daily funnels by brand
- Daily funnels by top level category
- Hourly funnels by price band

**Main steps**

1. Read `silver_events`.

2. Derive dimensions:

   - `event_date`
   - `category_root` from the first segment of `category_code_norm`
   - `price_band` buckets, e.g. `<10`, `10–30`, `30–60`, `60+`

3. Build three gold tables:

   - `gold_funnel_daily_brand`
     - Grain: `date`, `brand`
   - `gold_funnel_daily_category`
     - Grain: `date`, `category_root`
   - `gold_funnel_hourly_price_band`
     - Grain: 1-hour window on `event_time`, `price_band`

4. Each table stores:
   - `views`, `carts`, `purchases`, `revenue`
   - Derived funnel rates
   - Time partitions (`date` or `window_date`)

> These tables are used later for visualizations and additional analysis.

---

### 3.3 `03_build_stream_input_from_bronze.ipynb`

**Goal**  
Prepare a **file-based streaming input** for Spark Structured Streaming using historical bronze data.

This simulates a real-time Kafka or log stream in an environment where Kafka is not available.

**Main steps**

1. Read `bronze_events`.

2. Filter to a chosen date range to simulate “live” traffic, for example:

   - `2019-10-15` to `2019-10-31`

3. Repartition into many small partitions (e.g. 50) and write them as Parquet files into:

   - `~/funnelpulse/stream_input/`

4. Spark Structured Streaming in the next notebook uses this directory as its source, treating each file like a new batch of incoming events.

> In production, this step will be replaced by an actual Kafka producer, but the semantics remain the same.

---

### 3.4 `04_streaming_funnel_hourly_brand.ipynb`

**Goal**  
Implement a Spark Structured Streaming pipeline that computes hourly funnel metrics by brand in near real time.

**Main steps**

1. **Streaming source (file-based)**

   - Define a streaming DataFrame using:
     - `.format("parquet")`
     - `.schema(bronze_schema)` derived from `bronze_events`
     - `.option("maxFilesPerTrigger", 1)` so each microbatch processes one file from `stream_input/`

2. **Streaming “silver”**

   - Apply the same cleaning logic as batch silver:
     - Filter bad rows
     - Normalize `brand_norm` and `category_code_norm`
     - Derive `event_date` and optional data quality flags

3. **Streaming “gold”**

   - Use a 1-hour time window on `event_time`, grouped by `brand_norm`
   - Add a watermark on `event_time`, for example `2 hours`
   - Aggregate:
     - `views`, `carts`, `purchases`, `revenue`
   - Derive funnel rates and `window_date`

4. **Streaming sink**
   - Write to `tables/gold_stream_funnel_hourly_brand` as Parquet
   - Use a dedicated checkpoint directory `checkpoints/stream_hourly_brand`
   - Use `outputMode("append")` and time-based triggers

> This notebook mirrors the batch hourly brand gold, but runs in a continuous streaming fashion. On the course cluster it is fed from `stream_input/`; in production it will read from Kafka.

---

### 3.5 `05_anomaly_detection_hourly_brand.ipynb`

**Goal**  
Build a statistical anomaly detection layer on top of the batch hourly brand funnel table.

**Main steps**

1. Read `gold_funnel_hourly_brand`.

2. Filter to windows with sufficient traffic (for example `views >= N`).

3. Derive `hour_of_day` from `window_start`.

4. Compute baselines:

   - Per brand across all hours:
     - `conv_mean_brand`
     - `conv_std_brand`
   - Per brand × `hour_of_day`:
     - `conv_mean_brand_hour`
     - `conv_std_brand_hour`
   - All via window functions.

5. Compute z-scores:

   - `z_brand` = (current conversion − brand mean) / brand std
   - `z_brand_hour` = (current conversion − brand-hour mean) / brand-hour std
   - Handle zero or null stddev by assigning a z-score of 0.

6. Flag anomalies:

   - `is_drop_anomaly` for large negative deviations (e.g. `z <= −2.0`)
   - `is_spike_anomaly` for large positive deviations (e.g. `z >= +2.0`)
   - `anomaly_type ∈ { "drop", "spike", null }`

7. Write anomalies to `tables/gold_anomalies_hourly_brand`, partitioned by `window_date`.

> This table is the starting point for incident lists and dashboards.

---

### 3.6 `06_visualizations.ipynb`

**Goal**  
Produce visualizations and exploratory analytics on top of the gold and anomaly tables.

**Main plots**

- **Brand performance**

  - Top brands by total revenue
  - Daily revenue trends for top brands
  - Daily conversion rate trends for top brands

- **Category performance**

  - Total revenue by `category_root` (with `NULL` treated as `"unknown"`)
  - Overall conversion by `category_root`

- **Price bands**

  - Total revenue by price band
  - Overall conversion by price band
  - Optional: daily average conversion over time by price band

- **Anomalies**

  - Daily counts of anomalies by `anomaly_type`

- **Brand deep dive**
  - Hourly conversion time series for a focus brand
  - Vertical markers where anomalies are flagged

> These charts feed directly into the Results and Discussion section of the report and give concrete business narratives about funnel health.

---

## 4. How to Re-run the Pipeline (Current Environment)

On the course JupyterHub system, the rough order is:

1. **Run batch pipeline**

   - `01_batch_bronze_silver_gold.ipynb`
   - `02_additional_gold_tables.ipynb`

2. **Prepare streaming input**

   - `03_build_stream_input_from_bronze.ipynb`

3. **Run streaming job**

   - `04_streaming_funnel_hourly_brand.ipynb`
   - Start the query, allow some files to process, optionally stop it.

4. **Run anomaly detection**

   - `05_anomaly_detection_hourly_brand.ipynb`

5. **Generate visualizations**
   - `06_visualizations.ipynb`

Each notebook has a markdown header that describes its role. All paths are relative to `~/funnelpulse`.

---

## 5. Future Work for GCP + Kafka

The next developer is expected to adapt this system to a more production-like cloud setup. At a high level:

### 5.1 Storage and Compute

- Move Parquet tables from `~/funnelpulse/tables` to cloud storage, for example:
  - `gs://<bucket>/funnelpulse/bronze_events`
  - `gs://<bucket>/funnelpulse/silver_events`
  - `gs://<bucket>/funnelpulse/gold_*`
- Run Spark on:
  - Dataproc
  - Dataproc Serverless
  - Or Spark on Kubernetes

The batch notebooks (bronze → silver → gold → anomalies) can be ported almost unchanged, with only the paths and Spark configs updated.

### 5.2 Kafka Producer (Log Replay)

Replace `03_build_stream_input_from_bronze.ipynb` with a **Kafka replay producer** that reads bronze events and publishes them to a Kafka topic, in event-time order:

- Input: `bronze_events` on GCS
- Output: Kafka topic, for example `funnelpulse_events`
- Message format: JSON containing:
  - `event_time`, `event_type`, `user_id`, `user_session`, `product_id`, `category_code`, `brand`, `price`
- Use `user_session` or `user_id` as Kafka key to keep session events together in partitions.
- Throttle replay rate if needed for backfill vs live simulation.

This can be a standalone Python script using `kafka-python` and a Spark job to read bronze partitions.

### 5.3 Streaming Job from Kafka

Replace the file-based streaming source in `04_streaming_funnel_hourly_brand.ipynb` with a Kafka source:

- **Source:**
  - `.format("kafka")`
  - `.option("kafka.bootstrap.servers", "...")`
  - `.option("subscribe", "funnelpulse_events")`
- **Parse:**
  - `CAST(value AS STRING)` and `from_json` with the same schema as bronze.
- Apply the same streaming cleaning (streaming silver) and windowed aggregation logic to compute hourly funnel metrics by brand.
- **Sink:**
  - Write to `gs://.../gold_stream_funnel_hourly_brand` with checkpoint on GCS.

The aggregation, watermarks, and metrics stay the same; only the source and sink locations change.

### 5.4 Serving and Dashboards

Possible next steps:

- Connect a BI tool (Superset, Looker, Data Studio, etc.) to the gold and anomaly tables for:

  - Executive dashboards
  - Incident drill-down

- Optional: build a small FastAPI service that:
  - Lists recent anomalies
  - Returns funnel metrics for selected brands and time ranges
  - Sits in front of the gold/anomaly tables

### 5.5 Additional Analytics

If time allows, future work could include:

- Funnel analysis by device, region, or marketing channel (if present in the raw data)
- Session-based funnel definitions instead of raw event counts
- More advanced anomaly models:
  - Seasonality-aware baselines
  - Multivariate models that consider volume and revenue together
- Automatic root cause hints, for example:
  - Within an anomaly window, show top contributing categories or price bands

---

## 6. Summary

The current state of FunnelPulse provides:

- A fully functional batch lakehouse with bronze, silver, and multiple gold tables.
- A streaming pipeline that mirrors hourly funnel metrics by brand using Spark Structured Streaming.
- A statistical anomaly detection layer over hourly brand funnels.
- A set of visualizations that highlight revenue concentration, conversion behavior, price band dynamics, and anomaly patterns.

The next developer should treat this as a working reference implementation and focus on:

1. Migrating storage and compute to GCP or similar.
2. Replacing the file-based streaming input with a Kafka topic.
3. Enhancing visualizations and adding serving interfaces as needed.

Everything else is already in place and ready to be lifted into a more production-oriented environment.

---

## 7. Kafka Integration Helpers

This repo already includes two Python scripts to help the next developer move FunnelPulse from the current file-based streaming simulation to a **Kafka-based** setup:

- `kafka_replay_producer.py`
- `funnel_funnel_hourly_brand_from_kafka.py` (Kafka-based streaming job)

These are reference implementations meant to be adapted to the actual GCP/Kafka environment (cluster URL, topic names, storage paths, etc.).

### 7.1 `kafka_replay_producer.py`

**Role**

This script replaces the current “file-based stream input” built in `03_build_stream_input_from_bronze.ipynb`.

Instead of writing many small Parquet files into `stream_input/`, it:

- Reads historical **bronze events** from storage (e.g. GCS).
- Sorts them by `event_time`.
- Publishes each event as a JSON message to a Kafka topic, in order.
- Uses `user_session` (or `user_id`) as the Kafka key so that events for a session tend to land on the same partition.
- Throttles sends to approximate a desired event rate.

**Where it fits**

- Input: `bronze_events` table (moved to GCS in a real deployment), for example:
  - `gs://<bucket>/funnelpulse/tables/bronze_events`
- Output: Kafka topic, for example:
  - `funnelpulse_events`

**Key configuration points inside the script**

Look for these constants at the top of `kafka_replay_producer.py`:

```python
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"       # change to your Kafka cluster
KAFKA_TOPIC            = "funnelpulse_events"
BRONZE_PATH            = "gs://your-bucket/funnelpulse/tables/bronze_events"

TARGET_EVENTS_PER_SEC  = 1000
SLEEP_EVERY_N          = 5000
SLEEP_SECONDS          = 0.5
```

- Update them for your environment:
  - KAFKA_BOOTSTRAP_SERVERS → Confluent / self-hosted / GKE Kafka bootstrap.
  - KAFKA_TOPIC → whatever topic name you choose (keep in sync with the consumer job).
  - BRONZE_PATH → GCS (or S3) location of the bronze table.

How to run (conceptually)

- From a VM, Dataproc node, or container that has:

  - Network access to Kafka
  - Access to the storage location for bronze_events
  - Python with kafka-python and PySpark installed

- Run:

```python
python kafka_replay_producer.py
```

This will read bronze data and replay it into Kafka as a continuous stream of events. The replay logic (event_time ordering, simple throttling) is already handled inside the script.

### 7.2 `funnel_funnel_hourly_brand_from_kafka.py`

**Role**

This script is the Kafka-based version of the streaming notebook `04_streaming_funnel_hourly_brand.ipynb`.

Instead of:

- `readStream` from `stream_input/` (Parquet files),

it does:

- `readStream` from a Kafka topic (`funnelpulse_events`),
- parses each message as JSON into the same event schema as bronze,
- applies the same cleaning (streaming “silver”),
- aggregates hourly funnel metrics by brand with watermarking (streaming “gold”),
- writes the results to a cloud storage sink (e.g. GCS) with a checkpoint.

**Where it fits**

- **Input**: Kafka topic
  - `funnelpulse_events` (or whatever you configured)
- **Output**: streaming gold table on cloud storage, for example:
  - `gs://<bucket>/funnelpulse/tables/gold_stream_funnel_hourly_brand`
- **Checkpoint**: a dedicated directory, for example:
  - `gs://<bucket>/funnelpulse/checkpoints/stream_hourly_brand`

**Key configuration points inside the script**

At the top of `funnel_funnel_hourly_brand_from_kafka.py`:

```python
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"        # change in prod
KAFKA_TOPIC            = "funnelpulse_events"

GOLD_STREAM_PATH      = "gs://your-bucket/funnelpulse/tables/gold_stream_funnel_hourly_brand"
CHECKPOINT_LOCATION   = "gs://your-bucket/funnelpulse/checkpoints/stream_hourly_brand"
```

Update them to:

- Match your Kafka cluster and topic.
- Point `GOLD_STREAM_PATH` and `CHECKPOINT_LOCATION` to your cloud storage locations.

The schema inside the script (`BRONZE_SCHEMA`) matches the bronze event schema and is used by `from_json` to parse Kafka messages.

**How to run (conceptually)**

This should be submitted as a long-running Spark Structured Streaming job (Dataproc, Spark on K8s, etc.). For example on Dataproc:

```text
gcloud dataproc jobs submit pyspark \
  --cluster=<your-cluster> \
  --region=<region> \
  funnel_funnel_hourly_brand_from_kafka.py
```

Once running, it will:

- Consume from `funnelpulse_events`
- Clean and normalize events
- Compute hourly funnel metrics by brand with watermarks
- Append results to `gold_stream_funnel_hourly_brand` on GCS

The existing batch anomaly detection notebook (`05_anomaly_detection_hourly_brand.ipynb`) can then be pointed at this streaming gold table instead of the batch `gold_funnel_hourly_brand` if you want to run anomaly detection on the live stream.

⸻

### 7.3 How these scripts replace the current file-based simulation

**Today (class environment):**

- `03_build_stream_input_from_bronze.ipynb` creates a file-based streaming input.
- `04_streaming_funnel_hourly_brand.ipynb` reads from `stream_input/` and writes streaming gold.

**In a Kafka + GCP environment:**

- Use `kafka_replay_producer.py` instead of `03_build_stream_input_from_bronze.ipynb`.
  - Bronze → Kafka topic.
- Use `funnel_funnel_hourly_brand_from_kafka.py` instead of `04_streaming_funnel_hourly_brand.ipynb`.
  - Kafka topic → streaming hourly brand gold table on GCS.

The higher-level logic (cleaning, aggregations, anomaly detection, visualizations) stays the same. Only the transport layer (file-based vs Kafka) and storage paths (local vs GCS) change.
