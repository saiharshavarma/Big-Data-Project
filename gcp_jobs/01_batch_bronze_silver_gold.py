"""
FunnelPulse GCP Job 01: Batch Bronze → Silver → Gold Pipeline
==============================================================
Processes raw CSV data through the lakehouse layers.

Usage:
    gcloud dataproc jobs submit pyspark gs://funnelpulse-data-479512/jobs/01_batch_bronze_silver_gold.py \
        --cluster=funnelpulse-cluster --region=us-central1
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_date, lower, regexp_replace, when, window,
    sum as _sum
)

# GCS Configuration
GCS_BUCKET = "gs://big-data-project-480103-funnelpulse-data"
RAW_DIR = f"{GCS_BUCKET}/data_raw"
TABLES_DIR = f"{GCS_BUCKET}/tables"

BRONZE_PATH = f"{TABLES_DIR}/bronze_events"
SILVER_PATH = f"{TABLES_DIR}/silver_events"
GOLD_PATH = f"{TABLES_DIR}/gold_funnel_hourly_brand"


def create_spark_session():
    return SparkSession.builder \
        .appName("FunnelPulse 01: Bronze-Silver-Gold") \
        .getOrCreate()


def build_bronze(spark):
    """Read raw CSVs and create Bronze layer."""
    print("\n" + "=" * 60)
    print("BRONZE LAYER: Loading raw CSV files")
    print("=" * 60)

    # Read all CSV files from raw directory
    raw_df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(f"{RAW_DIR}/*.csv")

    print(f"Raw rows loaded: {raw_df.count():,}")
    raw_df.printSchema()

    # Add event_date partition column
    bronze = raw_df.withColumn("event_date", to_date(col("event_time")))

    # Write Bronze layer
    bronze.write \
        .mode("overwrite") \
        .partitionBy("event_date") \
        .parquet(BRONZE_PATH)

    print(f"Bronze layer written to: {BRONZE_PATH}")
    return bronze


def build_silver(spark, bronze):
    """Clean and normalize Bronze to create Silver layer."""
    print("\n" + "=" * 60)
    print("SILVER LAYER: Cleaning and normalizing")
    print("=" * 60)

    # If bronze not passed, read from storage
    if bronze is None:
        bronze = spark.read.parquet(BRONZE_PATH)

    print(f"Bronze rows: {bronze.count():,}")

    # Filter invalid rows
    silver = bronze.filter(
        (col("price").isNotNull()) &
        (col("price") > 0) &
        (col("event_time").isNotNull()) &
        (col("event_type").isNotNull())
    )

    # Normalize text fields
    silver = silver \
        .withColumn("brand_norm", lower(col("brand"))) \
        .withColumn("category_code_norm", lower(col("category_code"))) \
        .withColumn(
            "category_code_norm",
            regexp_replace(col("category_code_norm"), "[^a-z0-9\\.]", "_")
        )

    # Add data quality flags
    silver = silver \
        .withColumn("dq_missing_session", col("user_session").isNull()) \
        .withColumn("dq_missing_brand", col("brand").isNull()) \
        .withColumn("dq_missing_category", col("category_code").isNull())

    # Ensure event_date exists
    if "event_date" not in silver.columns:
        silver = silver.withColumn("event_date", to_date(col("event_time")))

    # Deduplicate
    silver = silver.dropDuplicates([
        "event_time", "user_id", "user_session", "product_id", "event_type"
    ])

    print(f"Silver rows after cleaning: {silver.count():,}")

    # Write Silver layer
    silver.write \
        .mode("overwrite") \
        .partitionBy("event_date") \
        .parquet(SILVER_PATH)

    print(f"Silver layer written to: {SILVER_PATH}")
    return silver


def build_gold_hourly_brand(spark, silver):
    """Aggregate Silver to create Gold hourly brand funnel metrics."""
    print("\n" + "=" * 60)
    print("GOLD LAYER: Hourly funnel metrics by brand")
    print("=" * 60)

    # If silver not passed, read from storage
    if silver is None:
        silver = spark.read.parquet(SILVER_PATH)

    # Aggregate by 1-hour window and brand
    gold = silver.groupBy(
        window(col("event_time"), "1 hour").alias("w"),
        col("brand_norm").alias("brand")
    ).agg(
        _sum((col("event_type") == "view").cast("int")).alias("views"),
        _sum((col("event_type") == "cart").cast("int")).alias("carts"),
        _sum((col("event_type") == "purchase").cast("int")).alias("purchases"),
        _sum(
            when(col("event_type") == "purchase", col("price")).otherwise(0.0)
        ).alias("revenue")
    )

    # Extract window columns
    gold = gold \
        .withColumn("window_start", col("w.start")) \
        .withColumn("window_end", col("w.end")) \
        .drop("w")

    # Calculate funnel rates
    gold = gold \
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

    print(f"Gold hourly brand rows: {gold.count():,}")

    # Write Gold layer
    gold.write \
        .mode("overwrite") \
        .partitionBy("window_date") \
        .parquet(GOLD_PATH)

    print(f"Gold layer written to: {GOLD_PATH}")

    # Show sample
    print("\nSample gold data:")
    gold.orderBy("window_start", "brand").show(10, truncate=False)

    return gold


def main():
    print("=" * 60)
    print("FunnelPulse Batch Pipeline: Bronze → Silver → Gold")
    print("=" * 60)

    spark = create_spark_session()

    # Check if raw data exists, if not use existing bronze
    try:
        raw_check = spark.read.csv(f"{RAW_DIR}/*.csv")
        has_raw = True
    except:
        has_raw = False
        print("No raw CSV files found, using existing Bronze layer")

    if has_raw:
        bronze = build_bronze(spark)
        silver = build_silver(spark, bronze)
    else:
        bronze = spark.read.parquet(BRONZE_PATH)
        silver = build_silver(spark, bronze)

    gold = build_gold_hourly_brand(spark, silver)

    print("\n" + "=" * 60)
    print("Pipeline Complete!")
    print("=" * 60)

    spark.stop()


if __name__ == "__main__":
    main()
