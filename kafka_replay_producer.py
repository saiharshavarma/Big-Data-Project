"""
1. When you’re on GCP:
	•	Put bronze on GCS (e.g., gs://bucket/funnelpulse/tables/bronze_events).
	•	Point BRONZE_PATH to that GCS path.
	•	Point KAFKA_BOOTSTRAP_SERVERS to your actual Kafka cluster (Confluent, self-hosted, etc.).
	•	Run this script on a VM / Dataproc node / container.

That gives you a “replay producer” very close to what your proposal describes.


2. Spark Structured Streaming from Kafka instead of files

Right now your streaming notebook does:

                    stream_raw = (
                    spark.readStream
                        .schema(bronze_schema)
                        .option("maxFilesPerTrigger", 1)
                        .parquet(stream_input_path)
                    )
In Kafka-land, that becomes a Kafka source with format("kafka"), and then you parse the JSON value column into your schema.
"""

#!/usr/bin/env python3
import json
import time
from datetime import datetime
from typing import Optional

from kafka import KafkaProducer  # pip install kafka-python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col


# ----------------------------
# CONFIG: adjust for your env
# ----------------------------

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"            # or your Confluent/GCP endpoint
KAFKA_TOPIC            = "funnelpulse_events"
BRONZE_PATH            = "gs://your-bucket/funnelpulse/tables/bronze_events"  # or s3://, etc.

# replay speed: events per second globally
TARGET_EVENTS_PER_SEC   = 1000
# optional: sleep every N records to roughly control rate
SLEEP_EVERY_N           = 5000
SLEEP_SECONDS           = 0.5


def create_spark() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("FunnelPulse Kafka Replay Producer")
        .getOrCreate()
    )
    return spark


def create_producer() -> KafkaProducer:
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda v: v.encode("utf-8") if v is not None else None,
        linger_ms=5,
        batch_size=32_768,
    )
    return producer


def row_to_event(row) -> dict:
    """Convert a Spark Row to a JSON-serializable dict."""
    return {
        "event_time": row.event_time.isoformat() if row.event_time else None,
        "event_type": row.event_type,
        "user_id": row.user_id,
        "user_session": row.user_session,
        "product_id": row.product_id,
        "category_id": row.category_id,
        "category_code": row.category_code,
        "brand": row.brand,
        "price": float(row.price) if row.price is not None else None,
    }


def main():
    spark = create_spark()
    producer = create_producer()

    bronze = spark.read.parquet(BRONZE_PATH)

    # Optional: restrict to some dates if you don't want entire history
    # bronze = bronze.filter((col("event_date") >= "2019-10-01") & (col("event_date") <= "2019-11-30"))

    # Sort by event_time to preserve time ordering
    bronze_sorted = bronze.orderBy("event_time")

    # Collect in batches to avoid pulling everything into memory at once
    # In a real backfill, you might iterate by date partitions instead.
    count = 0
    start_wall = time.time()

    for row in bronze_sorted.toLocalIterator():
        event = row_to_event(row)

        # Use session or user as key so Kafka partitions consistently
        key = event["user_session"] or event["user_id"] or "unknown"

        producer.send(
            KAFKA_TOPIC,
            key=key,
            value=event,
        )

        count += 1

        if count % SLEEP_EVERY_N == 0:
            elapsed = time.time() - start_wall
            print(f"Sent {count} messages in {elapsed:.1f}s")
            time.sleep(SLEEP_SECONDS)

    producer.flush()
    print(f"Finished sending {count} events.")


if __name__ == "__main__":
    main()