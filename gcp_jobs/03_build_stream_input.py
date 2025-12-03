"""
FunnelPulse GCP Job 03: Build Stream Input from Bronze
=======================================================
Prepares streaming input by extracting a date range from Bronze
and writing as many small Parquet files for streaming simulation.

Usage:
    gcloud dataproc jobs submit pyspark gs://funnelpulse-data-479512/jobs/03_build_stream_input.py \
        --cluster=funnelpulse-cluster --region=us-central1
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# GCS Configuration
GCS_BUCKET = "gs://big-data-project-480103-funnelpulse-data"
TABLES_DIR = f"{GCS_BUCKET}/tables"
STREAM_INPUT_DIR = f"{GCS_BUCKET}/stream_input"

BRONZE_PATH = f"{TABLES_DIR}/bronze_events"

# Streaming simulation parameters
STREAM_START_DATE = "2019-10-15"
STREAM_END_DATE = "2019-10-31"
NUM_PARTITIONS = 50  # Number of files to create (more = more micro-batches)


def create_spark_session():
    return SparkSession.builder \
        .appName("FunnelPulse 03: Build Stream Input") \
        .getOrCreate()


def build_stream_input(spark):
    """Extract a subset of Bronze data for streaming simulation."""
    print("\n" + "=" * 60)
    print("Building Stream Input from Bronze Events")
    print("=" * 60)

    # Load bronze
    bronze = spark.read.parquet(BRONZE_PATH)
    total_rows = bronze.count()
    print(f"Total Bronze rows: {total_rows:,}")

    # Filter to streaming date range
    print(f"\nFiltering to date range: {STREAM_START_DATE} to {STREAM_END_DATE}")
    bronze_subset = bronze.filter(
        (col("event_date") >= STREAM_START_DATE) &
        (col("event_date") <= STREAM_END_DATE)
    )

    subset_rows = bronze_subset.count()
    print(f"Rows in streaming subset: {subset_rows:,}")

    # Show sample
    print("\nSample events:")
    bronze_subset.orderBy("event_time").show(10, truncate=False)

    # Repartition to create many small files
    print(f"\nRepartitioning into {NUM_PARTITIONS} files...")
    bronze_subset \
        .repartition(NUM_PARTITIONS) \
        .write \
        .mode("overwrite") \
        .parquet(STREAM_INPUT_DIR)

    print(f"Stream input written to: {STREAM_INPUT_DIR}")

    # Verify
    verify_df = spark.read.parquet(STREAM_INPUT_DIR)
    print(f"Verified rows in stream_input: {verify_df.count():,}")

    return bronze_subset


def main():
    print("=" * 60)
    print("FunnelPulse: Build Stream Input Pipeline")
    print("=" * 60)

    spark = create_spark_session()

    build_stream_input(spark)

    print("\n" + "=" * 60)
    print("Stream Input Created Successfully!")
    print("=" * 60)
    print(f"\nThe stream_input directory now contains {NUM_PARTITIONS} Parquet files")
    print("that can be used as a file-based streaming source.")

    spark.stop()


if __name__ == "__main__":
    main()
