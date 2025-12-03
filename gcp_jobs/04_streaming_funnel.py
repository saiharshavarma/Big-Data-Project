"""
FunnelPulse GCP Job 04: Streaming Hourly Funnel by Brand
=========================================================
Spark Structured Streaming job that consumes events from stream_input
and computes hourly funnel metrics by brand in real-time.

Usage:
    gcloud dataproc jobs submit pyspark gs://funnelpulse-data-479512/jobs/04_streaming_funnel.py \
        --cluster=funnelpulse-cluster --region=us-central1

Note: This is a streaming job. It will run until stopped or until
      all files in stream_input have been processed.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lower, regexp_replace, when, to_date, window,
    sum as _sum
)
import time

# GCS Configuration
GCS_BUCKET = "gs://big-data-project-480103-funnelpulse-data"
TABLES_DIR = f"{GCS_BUCKET}/tables"
STREAM_INPUT_DIR = f"{GCS_BUCKET}/stream_input"
CHECKPOINTS_DIR = f"{GCS_BUCKET}/checkpoints"

BRONZE_PATH = f"{TABLES_DIR}/bronze_events"
STREAM_GOLD_PATH = f"{TABLES_DIR}/gold_stream_funnel_hourly_brand"
STREAM_CHECKPOINT = f"{CHECKPOINTS_DIR}/stream_hourly_brand"

# Streaming parameters
MAX_FILES_PER_TRIGGER = 2  # Process 2 files per micro-batch
PROCESSING_TIME = "30 seconds"  # Trigger interval
MAX_BATCHES = 3  # Stop after this many batches (for demo purposes)


def create_spark_session():
    return SparkSession.builder \
        .appName("FunnelPulse 04: Streaming Hourly Brand") \
        .getOrCreate()


def get_bronze_schema(spark):
    """Get schema from bronze table for streaming source."""
    bronze_sample = spark.read.parquet(BRONZE_PATH)
    return bronze_sample.schema


def create_streaming_source(spark, schema):
    """Create file-based streaming source from stream_input."""
    print("\n" + "=" * 60)
    print("Creating Streaming Source")
    print("=" * 60)

    stream_raw = spark.readStream \
        .schema(schema) \
        .option("maxFilesPerTrigger", MAX_FILES_PER_TRIGGER) \
        .parquet(STREAM_INPUT_DIR)

    print(f"Is streaming: {stream_raw.isStreaming}")
    print("Schema:")
    stream_raw.printSchema()

    return stream_raw


def apply_streaming_silver(stream_raw):
    """Apply silver-like cleaning to streaming data."""
    print("\n" + "=" * 60)
    print("Applying Streaming Silver Transformations")
    print("=" * 60)

    stream_clean = stream_raw

    # Filter invalid rows
    stream_clean = stream_clean.filter(
        (col("price").isNotNull()) &
        (col("price") > 0) &
        (col("event_time").isNotNull()) &
        (col("event_type").isNotNull())
    )

    # Normalize text fields
    stream_clean = stream_clean \
        .withColumn("brand_norm", lower(col("brand"))) \
        .withColumn("category_code_norm", lower(col("category_code"))) \
        .withColumn(
            "category_code_norm",
            regexp_replace(col("category_code_norm"), "[^a-z0-9\\.]", "_")
        ) \
        .withColumn("event_date", to_date(col("event_time"))) \
        .withColumn("dq_missing_session", col("user_session").isNull()) \
        .withColumn("dq_missing_brand", col("brand").isNull()) \
        .withColumn("dq_missing_category", col("category_code").isNull())

    return stream_clean


def create_streaming_gold(stream_clean):
    """Create streaming aggregation for hourly brand funnel."""
    print("\n" + "=" * 60)
    print("Creating Streaming Gold Aggregation")
    print("=" * 60)

    # Windowed aggregation with watermark
    stream_windowed = stream_clean \
        .withWatermark("event_time", "2 hours") \
        .groupBy(
            window(col("event_time"), "1 hour").alias("w"),
            col("brand_norm").alias("brand")
        ) \
        .agg(
            _sum((col("event_type") == "view").cast("int")).alias("views"),
            _sum((col("event_type") == "cart").cast("int")).alias("carts"),
            _sum((col("event_type") == "purchase").cast("int")).alias("purchases"),
            _sum(
                when(col("event_type") == "purchase", col("price")).otherwise(0.0)
            ).alias("revenue")
        )

    # Extract window columns and add funnel rates
    stream_gold = stream_windowed \
        .withColumn("window_start", col("w.start")) \
        .withColumn("window_end", col("w.end")) \
        .drop("w")

    stream_gold = stream_gold \
        .withColumn(
            "view_to_cart_rate",
            when(col("views") > 0, col("carts") / col("views"))
        ) \
        .withColumn(
            "cart_to_purchase_rate",
            when(col("carts") > 0, col("purchases") / col("carts"))
        ) \
        .withColumn(
            "conversion_rate",
            when(col("views") > 0, col("purchases") / col("views"))
        ) \
        .withColumn("window_date", to_date(col("window_start")))

    return stream_gold


def start_streaming_query(stream_gold):
    """Start the streaming query to write results."""
    print("\n" + "=" * 60)
    print("Starting Streaming Query")
    print("=" * 60)

    query = stream_gold \
        .writeStream \
        .outputMode("append") \
        .format("parquet") \
        .option("checkpointLocation", STREAM_CHECKPOINT) \
        .option("path", STREAM_GOLD_PATH) \
        .trigger(processingTime=PROCESSING_TIME) \
        .start()

    print(f"Query ID: {query.id}")
    print(f"Output path: {STREAM_GOLD_PATH}")
    print(f"Checkpoint: {STREAM_CHECKPOINT}")

    return query


def monitor_query(query, max_batches):
    """Monitor the streaming query and stop after max_batches."""
    print("\n" + "=" * 60)
    print(f"Monitoring Query (will stop after {max_batches} batches)")
    print("=" * 60)

    batch_count = 0
    last_batch_id = -1

    while query.isActive and batch_count < max_batches:
        status = query.status
        progress = query.lastProgress

        if progress and progress.get("batchId", -1) > last_batch_id:
            last_batch_id = progress["batchId"]
            batch_count += 1
            print(f"\nBatch {batch_count}/{max_batches}:")
            print(f"  Batch ID: {last_batch_id}")
            print(f"  Input rows: {progress.get('numInputRows', 0)}")
            print(f"  Status: {status.get('message', 'N/A')}")

        time.sleep(10)

    return batch_count


def main():
    print("=" * 60)
    print("FunnelPulse: Streaming Hourly Brand Funnel Pipeline")
    print("=" * 60)

    spark = create_spark_session()

    # Get schema from bronze
    bronze_schema = get_bronze_schema(spark)

    # Create streaming pipeline
    stream_raw = create_streaming_source(spark, bronze_schema)
    stream_clean = apply_streaming_silver(stream_raw)
    stream_gold = create_streaming_gold(stream_clean)

    # Start query
    query = start_streaming_query(stream_gold)

    # Monitor and wait
    batches_processed = monitor_query(query, MAX_BATCHES)

    # Stop query
    print("\n" + "=" * 60)
    print("Stopping Streaming Query")
    print("=" * 60)
    query.stop()

    # Verify output
    print("\nVerifying streaming output...")
    try:
        stream_result = spark.read.parquet(STREAM_GOLD_PATH)
        print(f"Streaming gold rows: {stream_result.count():,}")
        print("\nSample output:")
        stream_result.orderBy("window_start", "brand").show(20, truncate=False)
    except Exception as e:
        print(f"Could not read output (may be empty): {e}")

    print("\n" + "=" * 60)
    print(f"Streaming Pipeline Complete! Processed {batches_processed} batches.")
    print("=" * 60)

    spark.stop()


if __name__ == "__main__":
    main()
