"""
FunnelPulse GCP Job 05: Anomaly Detection on Hourly Brand Funnels
==================================================================
Detects anomalies in hourly brand conversion rates using z-score
based statistical analysis.

Usage:
    gcloud dataproc jobs submit pyspark gs://funnelpulse-data-479512/jobs/05_anomaly_detection.py \
        --cluster=funnelpulse-cluster --region=us-central1
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, hour, when, to_date, avg, stddev_samp
)
from pyspark.sql.window import Window

# GCS Configuration
GCS_BUCKET = "gs://funnelpulse-ss18851-data"
TABLES_DIR = f"{GCS_BUCKET}/tables"

GOLD_HOURLY_BRAND_PATH = f"{TABLES_DIR}/gold_funnel_hourly_brand"
ANOMALY_PATH = f"{TABLES_DIR}/gold_anomalies_hourly_brand"

# Anomaly detection parameters
MIN_VIEWS_BASELINE = 20  # Minimum views for baseline calculation
MIN_VIEWS_ANOMALY = 50   # Minimum views to flag as anomaly
Z_THRESHOLD = 2.0        # Z-score threshold for anomaly detection


def create_spark_session():
    return SparkSession.builder \
        .appName("FunnelPulse 05: Anomaly Detection") \
        .getOrCreate()


def load_and_filter_gold(spark):
    """Load hourly brand funnel and filter for reliable data."""
    print("\n" + "=" * 60)
    print("Loading Hourly Brand Funnel Data")
    print("=" * 60)

    gold = spark.read.parquet(GOLD_HOURLY_BRAND_PATH)
    total_rows = gold.count()
    print(f"Total hourly brand rows: {total_rows:,}")

    # Filter for baseline calculation
    gold_filtered = gold.filter(
        (col("views") >= MIN_VIEWS_BASELINE) &
        (col("brand").isNotNull())
    )

    filtered_rows = gold_filtered.count()
    print(f"Rows after baseline filter (views >= {MIN_VIEWS_BASELINE}): {filtered_rows:,}")

    # Add hour of day for hourly baselines
    gold_feat = gold_filtered.withColumn(
        "hour_of_day",
        hour(col("window_start"))
    )

    return gold_feat


def compute_baselines(gold_feat):
    """Compute per-brand and per-brand-hour baselines."""
    print("\n" + "=" * 60)
    print("Computing Baseline Statistics")
    print("=" * 60)

    # Windows for aggregation
    w_brand = Window.partitionBy("brand")
    w_brand_hour = Window.partitionBy("brand", "hour_of_day")

    # Compute baselines
    gold_with_baselines = gold_feat \
        .withColumn("conv_mean_brand", avg("conversion_rate").over(w_brand)) \
        .withColumn("conv_std_brand", stddev_samp("conversion_rate").over(w_brand)) \
        .withColumn("conv_mean_brand_hour", avg("conversion_rate").over(w_brand_hour)) \
        .withColumn("conv_std_brand_hour", stddev_samp("conversion_rate").over(w_brand_hour))

    print("Baseline statistics computed.")
    print("\nSample with baselines:")
    gold_with_baselines.select(
        "window_start", "brand", "views", "conversion_rate",
        "hour_of_day", "conv_mean_brand", "conv_std_brand"
    ).orderBy("window_start", "brand").show(10, truncate=False)

    return gold_with_baselines


def compute_anomaly_scores(df):
    """Compute z-scores and flag anomalies."""
    print("\n" + "=" * 60)
    print("Computing Anomaly Scores")
    print("=" * 60)

    # Compute z-scores (handle null/zero std)
    df = df.withColumn(
        "z_brand",
        when(
            (col("conv_std_brand").isNull()) | (col("conv_std_brand") == 0),
            0.0
        ).otherwise(
            (col("conversion_rate") - col("conv_mean_brand")) / col("conv_std_brand")
        )
    )

    df = df.withColumn(
        "z_brand_hour",
        when(
            (col("conv_std_brand_hour").isNull()) | (col("conv_std_brand_hour") == 0),
            0.0
        ).otherwise(
            (col("conversion_rate") - col("conv_mean_brand_hour")) / col("conv_std_brand_hour")
        )
    )

    # Flag anomalies
    df = df.withColumn(
        "is_drop_anomaly",
        when(
            (col("views") >= MIN_VIEWS_ANOMALY) &
            ((col("z_brand") <= -Z_THRESHOLD) | (col("z_brand_hour") <= -Z_THRESHOLD)),
            True
        ).otherwise(False)
    )

    df = df.withColumn(
        "is_spike_anomaly",
        when(
            (col("views") >= MIN_VIEWS_ANOMALY) &
            ((col("z_brand") >= Z_THRESHOLD) | (col("z_brand_hour") >= Z_THRESHOLD)),
            True
        ).otherwise(False)
    )

    df = df.withColumn(
        "anomaly_type",
        when(col("is_drop_anomaly"), "drop")
        .when(col("is_spike_anomaly"), "spike")
        .otherwise(None)
    )

    return df


def write_anomalies(df):
    """Filter and write anomalies to output table."""
    print("\n" + "=" * 60)
    print("Writing Anomalies Table")
    print("=" * 60)

    # Filter to anomalies only
    anomalies = df.filter(col("anomaly_type").isNotNull())
    anomaly_count = anomalies.count()
    print(f"Total anomalies detected: {anomaly_count:,}")

    # Breakdown by type
    drop_count = anomalies.filter(col("anomaly_type") == "drop").count()
    spike_count = anomalies.filter(col("anomaly_type") == "spike").count()
    print(f"  - Drops: {drop_count:,}")
    print(f"  - Spikes: {spike_count:,}")

    # Add window_date for partitioning
    anomalies_out = anomalies.withColumn(
        "window_date",
        to_date(col("window_start"))
    )

    # Write
    anomalies_out.write \
        .mode("overwrite") \
        .partitionBy("window_date") \
        .parquet(ANOMALY_PATH)

    print(f"\nAnomalies written to: {ANOMALY_PATH}")

    # Show sample
    print("\nTop drops by z-score:")
    anomalies.filter(col("anomaly_type") == "drop") \
        .orderBy("z_brand") \
        .select(
            "window_start", "brand", "views", "purchases",
            "conversion_rate", "conv_mean_brand", "z_brand"
        ).show(10, truncate=False)

    print("\nTop spikes by z-score:")
    anomalies.filter(col("anomaly_type") == "spike") \
        .orderBy(col("z_brand").desc()) \
        .select(
            "window_start", "brand", "views", "purchases",
            "conversion_rate", "conv_mean_brand", "z_brand"
        ).show(10, truncate=False)

    return anomalies


def main():
    print("=" * 60)
    print("FunnelPulse: Anomaly Detection Pipeline")
    print("=" * 60)
    print(f"\nParameters:")
    print(f"  Min views for baseline: {MIN_VIEWS_BASELINE}")
    print(f"  Min views for anomaly:  {MIN_VIEWS_ANOMALY}")
    print(f"  Z-score threshold:      {Z_THRESHOLD}")

    spark = create_spark_session()

    # Load and prepare data
    gold_feat = load_and_filter_gold(spark)

    # Compute baselines
    gold_with_baselines = compute_baselines(gold_feat)

    # Compute anomaly scores
    gold_scored = compute_anomaly_scores(gold_with_baselines)

    # Write anomalies
    anomalies = write_anomalies(gold_scored)

    print("\n" + "=" * 60)
    print("Anomaly Detection Complete!")
    print("=" * 60)

    spark.stop()


if __name__ == "__main__":
    main()
