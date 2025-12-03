"""
FunnelPulse GCP Job 06: Summary Report
=======================================
Generates a summary report of all gold tables and anomalies.
(Note: Visualizations are best done in notebooks or locally)

Usage:
    gcloud dataproc jobs submit pyspark gs://funnelpulse-data-479512/jobs/06_summary_report.py \
        --cluster=funnelpulse-cluster --region=us-central1
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum as _sum, count, avg, min as _min, max as _max, desc
)

# GCS Configuration
GCS_BUCKET = "gs://big-data-project-480103-funnelpulse-data"
TABLES_DIR = f"{GCS_BUCKET}/tables"

# All table paths
BRONZE_PATH = f"{TABLES_DIR}/bronze_events"
SILVER_PATH = f"{TABLES_DIR}/silver_events"
GOLD_HOURLY_BRAND_PATH = f"{TABLES_DIR}/gold_funnel_hourly_brand"
GOLD_DAILY_BRAND_PATH = f"{TABLES_DIR}/gold_funnel_daily_brand"
GOLD_DAILY_CATEGORY_PATH = f"{TABLES_DIR}/gold_funnel_daily_category"
GOLD_HOURLY_PRICE_PATH = f"{TABLES_DIR}/gold_funnel_hourly_price_band"
ANOMALY_PATH = f"{TABLES_DIR}/gold_anomalies_hourly_brand"
STREAM_GOLD_PATH = f"{TABLES_DIR}/gold_stream_funnel_hourly_brand"


def create_spark_session():
    return SparkSession.builder \
        .appName("FunnelPulse 06: Summary Report") \
        .getOrCreate()


def print_section(title):
    print("\n" + "=" * 70)
    print(title)
    print("=" * 70)


def safe_read_parquet(spark, path, name):
    """Safely read a parquet table, returning None if it doesn't exist."""
    try:
        df = spark.read.parquet(path)
        return df
    except Exception as e:
        print(f"  Warning: Could not read {name}: {e}")
        return None


def report_table_stats(spark):
    """Report row counts and basic stats for all tables."""
    print_section("TABLE STATISTICS")

    tables = [
        ("Bronze Events", BRONZE_PATH),
        ("Silver Events", SILVER_PATH),
        ("Gold Hourly Brand", GOLD_HOURLY_BRAND_PATH),
        ("Gold Daily Brand", GOLD_DAILY_BRAND_PATH),
        ("Gold Daily Category", GOLD_DAILY_CATEGORY_PATH),
        ("Gold Hourly Price Band", GOLD_HOURLY_PRICE_PATH),
        ("Anomalies Hourly Brand", ANOMALY_PATH),
        ("Streaming Gold Hourly Brand", STREAM_GOLD_PATH),
    ]

    for name, path in tables:
        df = safe_read_parquet(spark, path, name)
        if df is not None:
            row_count = df.count()
            col_count = len(df.columns)
            print(f"\n  {name}:")
            print(f"    Path: {path}")
            print(f"    Rows: {row_count:,}")
            print(f"    Columns: {col_count}")


def report_top_brands(spark):
    """Report top brands by revenue."""
    print_section("TOP 10 BRANDS BY REVENUE")

    df = safe_read_parquet(spark, GOLD_DAILY_BRAND_PATH, "Gold Daily Brand")
    if df is None:
        return

    # Filter out NULL brands
    df_nonnull = df.filter(col("brand").isNotNull())

    top_brands = df_nonnull.groupBy("brand").agg(
        _sum("revenue").alias("total_revenue"),
        _sum("views").alias("total_views"),
        _sum("purchases").alias("total_purchases")
    ).orderBy(desc("total_revenue")).limit(10)

    print("\n")
    top_brands.show(truncate=False)


def report_category_performance(spark):
    """Report performance by category."""
    print_section("CATEGORY PERFORMANCE")

    df = safe_read_parquet(spark, GOLD_DAILY_CATEGORY_PATH, "Gold Daily Category")
    if df is None:
        return

    cat_perf = df.groupBy("category_root").agg(
        _sum("revenue").alias("total_revenue"),
        _sum("views").alias("total_views"),
        _sum("purchases").alias("total_purchases")
    ).orderBy(desc("total_revenue"))

    print("\n")
    cat_perf.show(truncate=False)


def report_price_band_analysis(spark):
    """Report conversion by price band."""
    print_section("PRICE BAND ANALYSIS")

    df = safe_read_parquet(spark, GOLD_HOURLY_PRICE_PATH, "Gold Hourly Price")
    if df is None:
        return

    price_perf = df.groupBy("price_band").agg(
        _sum("views").alias("total_views"),
        _sum("purchases").alias("total_purchases"),
        _sum("revenue").alias("total_revenue")
    ).orderBy("price_band")

    # Show with computed conversion rate
    price_perf.withColumn(
        "conversion_rate",
        col("total_purchases") / col("total_views")
    ).show(truncate=False)


def report_anomaly_summary(spark):
    """Report anomaly statistics."""
    print_section("ANOMALY SUMMARY")

    df = safe_read_parquet(spark, ANOMALY_PATH, "Anomalies")
    if df is None:
        print("  No anomalies table found.")
        return

    total_anomalies = df.count()
    print(f"\n  Total anomalies: {total_anomalies:,}")

    # By type
    by_type = df.groupBy("anomaly_type").agg(
        count("*").alias("count")
    )
    print("\n  Anomalies by type:")
    by_type.show()

    # Top brands with anomalies
    print("\n  Top 10 brands with most anomalies:")
    df.groupBy("brand").agg(
        count("*").alias("anomaly_count")
    ).orderBy(desc("anomaly_count")).limit(10).show(truncate=False)

    # Most severe drops
    print("\n  Top 10 most severe drops (by z-score):")
    df.filter(col("anomaly_type") == "drop") \
        .orderBy("z_brand") \
        .select("window_start", "brand", "views", "conversion_rate", "z_brand") \
        .limit(10).show(truncate=False)


def report_date_range(spark):
    """Report the date range of the data."""
    print_section("DATA DATE RANGE")

    df = safe_read_parquet(spark, GOLD_HOURLY_BRAND_PATH, "Gold Hourly Brand")
    if df is None:
        return

    date_range = df.agg(
        _min("window_date").alias("min_date"),
        _max("window_date").alias("max_date"),
        count("*").alias("total_windows")
    ).collect()[0]

    print(f"\n  Start date: {date_range['min_date']}")
    print(f"  End date:   {date_range['max_date']}")
    print(f"  Total hourly windows: {date_range['total_windows']:,}")


def report_overall_funnel(spark):
    """Report overall funnel metrics."""
    print_section("OVERALL FUNNEL METRICS")

    df = safe_read_parquet(spark, SILVER_PATH, "Silver Events")
    if df is None:
        return

    # Count events by type
    event_counts = df.groupBy("event_type").agg(
        count("*").alias("count")
    ).orderBy(desc("count"))

    print("\n  Events by type:")
    event_counts.show()

    # Overall funnel
    from pyspark.sql.functions import lit

    funnel = df.agg(
        _sum((col("event_type") == "view").cast("int")).alias("total_views"),
        _sum((col("event_type") == "cart").cast("int")).alias("total_carts"),
        _sum((col("event_type") == "purchase").cast("int")).alias("total_purchases")
    ).collect()[0]

    views = funnel["total_views"]
    carts = funnel["total_carts"]
    purchases = funnel["total_purchases"]

    print(f"\n  Overall Funnel:")
    print(f"    Views:     {views:,}")
    print(f"    Carts:     {carts:,}")
    print(f"    Purchases: {purchases:,}")
    print(f"\n    View → Cart rate:     {carts/views*100:.2f}%")
    print(f"    Cart → Purchase rate: {purchases/carts*100:.2f}%")
    print(f"    Overall conversion:   {purchases/views*100:.2f}%")


def main():
    print("=" * 70)
    print("           FUNNELPULSE SUMMARY REPORT")
    print("=" * 70)

    spark = create_spark_session()

    # Generate all reports
    report_table_stats(spark)
    report_date_range(spark)
    report_overall_funnel(spark)
    report_top_brands(spark)
    report_category_performance(spark)
    report_price_band_analysis(spark)
    report_anomaly_summary(spark)

    print("\n" + "=" * 70)
    print("           REPORT COMPLETE")
    print("=" * 70)
    print("\nNote: For visualizations, use Jupyter notebooks locally or on")
    print("the Dataproc cluster's JupyterHub interface.")

    spark.stop()


if __name__ == "__main__":
    main()
