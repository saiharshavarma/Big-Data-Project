"""
FunnelPulse GCP Job 02: Additional Gold Tables
===============================================
Creates additional gold-level aggregate tables from Silver:
- gold_funnel_daily_brand
- gold_funnel_daily_category
- gold_funnel_hourly_price_band

Usage:
    gcloud dataproc jobs submit pyspark gs://funnelpulse-data-479512/jobs/02_additional_gold_tables.py \
        --cluster=funnelpulse-cluster --region=us-central1
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_date, split, when, window,
    sum as _sum
)

# GCS Configuration
GCS_BUCKET = "gs://funnelpulse-ss18851-data"
TABLES_DIR = f"{GCS_BUCKET}/tables"

SILVER_PATH = f"{TABLES_DIR}/silver_events"
GOLD_DAILY_BRAND_PATH = f"{TABLES_DIR}/gold_funnel_daily_brand"
GOLD_DAILY_CATEGORY_PATH = f"{TABLES_DIR}/gold_funnel_daily_category"
GOLD_HOURLY_PRICE_PATH = f"{TABLES_DIR}/gold_funnel_hourly_price_band"


def create_spark_session():
    return SparkSession.builder \
        .appName("FunnelPulse 02: Additional Gold Tables") \
        .getOrCreate()


def load_and_enrich_silver(spark):
    """Load silver and add derived dimensions."""
    print("\n" + "=" * 60)
    print("Loading Silver Events and Adding Dimensions")
    print("=" * 60)

    silver = spark.read.parquet(SILVER_PATH)
    print(f"Silver rows: {silver.count():,}")

    # Ensure event_date exists
    if "event_date" not in silver.columns:
        silver = silver.withColumn("event_date", to_date(col("event_time")))

    # Derive category_root from category_code_norm
    if "category_code_norm" in silver.columns:
        silver = silver.withColumn(
            "category_root",
            split(col("category_code_norm"), "\\.").getItem(0)
        )
    else:
        silver = silver.withColumn("category_root", col("category_code"))

    # Derive price_band
    silver = silver.withColumn(
        "price_band",
        when(col("price") < 10, "<10")
        .when((col("price") >= 10) & (col("price") < 30), "10-30")
        .when((col("price") >= 30) & (col("price") < 60), "30-60")
        .otherwise("60_plus")
    )

    return silver


def build_gold_daily_brand(silver):
    """Create daily funnel metrics by brand."""
    print("\n" + "=" * 60)
    print("GOLD TABLE: Daily Funnel by Brand")
    print("=" * 60)

    daily_brand = silver.groupBy(
        col("event_date").alias("date"),
        col("brand_norm").alias("brand")
    ).agg(
        _sum((col("event_type") == "view").cast("int")).alias("views"),
        _sum((col("event_type") == "cart").cast("int")).alias("carts"),
        _sum((col("event_type") == "purchase").cast("int")).alias("purchases"),
        _sum(
            when(col("event_type") == "purchase", col("price")).otherwise(0.0)
        ).alias("revenue")
    )

    # Funnel rates
    daily_brand = daily_brand \
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
        )

    print(f"Daily brand rows: {daily_brand.count():,}")

    daily_brand.write \
        .mode("overwrite") \
        .partitionBy("date") \
        .parquet(GOLD_DAILY_BRAND_PATH)

    print(f"Written to: {GOLD_DAILY_BRAND_PATH}")
    return daily_brand


def build_gold_daily_category(silver):
    """Create daily funnel metrics by category."""
    print("\n" + "=" * 60)
    print("GOLD TABLE: Daily Funnel by Category")
    print("=" * 60)

    daily_category = silver.groupBy(
        col("event_date").alias("date"),
        col("category_root").alias("category_root")
    ).agg(
        _sum((col("event_type") == "view").cast("int")).alias("views"),
        _sum((col("event_type") == "cart").cast("int")).alias("carts"),
        _sum((col("event_type") == "purchase").cast("int")).alias("purchases"),
        _sum(
            when(col("event_type") == "purchase", col("price")).otherwise(0.0)
        ).alias("revenue")
    )

    daily_category = daily_category \
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
        )

    print(f"Daily category rows: {daily_category.count():,}")

    daily_category.write \
        .mode("overwrite") \
        .partitionBy("date") \
        .parquet(GOLD_DAILY_CATEGORY_PATH)

    print(f"Written to: {GOLD_DAILY_CATEGORY_PATH}")
    return daily_category


def build_gold_hourly_price_band(silver):
    """Create hourly funnel metrics by price band."""
    print("\n" + "=" * 60)
    print("GOLD TABLE: Hourly Funnel by Price Band")
    print("=" * 60)

    hourly_price = silver.groupBy(
        window(col("event_time"), "1 hour").alias("w"),
        col("price_band").alias("price_band")
    ).agg(
        _sum((col("event_type") == "view").cast("int")).alias("views"),
        _sum((col("event_type") == "cart").cast("int")).alias("carts"),
        _sum((col("event_type") == "purchase").cast("int")).alias("purchases"),
        _sum(
            when(col("event_type") == "purchase", col("price")).otherwise(0.0)
        ).alias("revenue")
    )

    hourly_price = hourly_price \
        .withColumn("window_start", col("w.start")) \
        .withColumn("window_end", col("w.end")) \
        .drop("w")

    hourly_price = hourly_price \
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

    print(f"Hourly price band rows: {hourly_price.count():,}")

    hourly_price.write \
        .mode("overwrite") \
        .partitionBy("window_date") \
        .parquet(GOLD_HOURLY_PRICE_PATH)

    print(f"Written to: {GOLD_HOURLY_PRICE_PATH}")
    return hourly_price


def main():
    print("=" * 60)
    print("FunnelPulse: Additional Gold Tables Pipeline")
    print("=" * 60)

    spark = create_spark_session()

    # Load and enrich silver
    silver = load_and_enrich_silver(spark)

    # Build all three gold tables
    build_gold_daily_brand(silver)
    build_gold_daily_category(silver)
    build_gold_hourly_price_band(silver)

    print("\n" + "=" * 60)
    print("All Additional Gold Tables Created Successfully!")
    print("=" * 60)

    spark.stop()


if __name__ == "__main__":
    main()
