"""
FunnelPulse GCP Job 06: Advanced Anomaly Detection
===================================================
Enhanced anomaly detection using multiple advanced models:
- LSTM Autoencoder for temporal patterns
- Prophet for seasonality
- Isolation Forest for multivariate outliers
- Changepoint detection for structural breaks
- Ensemble framework for combining predictions

Usage:
    gcloud dataproc jobs submit pyspark gs://funnelpulse-data-479512/jobs/06_advanced_anomaly_detection.py \
        --cluster=funnelpulse-cluster --region=us-central1 \
        --py-files=gs://funnelpulse-data-479512/jobs/advanced_anomaly_detection.zip
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date
import sys
import os

# Add advanced_anomaly_detection to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'advanced_anomaly_detection'))

# GCS Configuration
GCS_BUCKET = "gs://funnelpulse-ss18851-data"
TABLES_DIR = f"{GCS_BUCKET}/tables"

GOLD_HOURLY_BRAND_PATH = f"{TABLES_DIR}/gold_funnel_hourly_brand"
ADVANCED_ANOMALY_PATH = f"{TABLES_DIR}/gold_anomalies_advanced"
LEGACY_ANOMALY_PATH = f"{TABLES_DIR}/gold_anomalies_hourly_brand"

# Configuration
MIN_VIEWS_BASELINE = 20
MIN_VIEWS_ANOMALY = 50
TRAIN_ON_FULL_DATA = True  # Whether to train on all data or use split


def create_spark_session():
    """Create Spark session with appropriate configuration."""
    return SparkSession.builder \
        .appName("FunnelPulse 06: Advanced Anomaly Detection") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .getOrCreate()


def load_gold_data(spark):
    """Load hourly brand funnel data."""
    print("\n" + "=" * 60)
    print("Loading Hourly Brand Funnel Data")
    print("=" * 60)
    
    gold = spark.read.parquet(GOLD_HOURLY_BRAND_PATH)
    total_rows = gold.count()
    print(f"Total hourly brand rows: {total_rows:,}")
    
    # Filter for reliable data
    gold_filtered = gold.filter(
        (col("views") >= MIN_VIEWS_BASELINE) &
        (col("brand").isNotNull())
    )
    
    filtered_rows = gold_filtered.count()
    print(f"Rows after filter (views >= {MIN_VIEWS_BASELINE}): {filtered_rows:,}")
    
    return gold_filtered


def run_advanced_detection_spark_native(gold_filtered, spark):
    """
    Run anomaly detection using Spark-native operations.
    This is a simplified version that doesn't require external libraries.
    """
    from pyspark.sql.functions import avg, stddev, when, abs as spark_abs
    from pyspark.sql.window import Window
    
    print("\n" + "=" * 60)
    print("Running Spark-Native Anomaly Detection")
    print("=" * 60)
    print("Note: Using simplified Z-score method (external ML libraries not available)")
    
    # Add time features
    from pyspark.sql.functions import hour, dayofweek
    gold_feat = gold_filtered.withColumn("hour_of_day", hour(col("window_start")))
    gold_feat = gold_feat.withColumn("day_of_week", dayofweek(col("window_start")))
    
    # Compute baselines
    w_brand = Window.partitionBy("brand")
    gold_feat = gold_feat \
        .withColumn("conv_mean_brand", avg("conversion_rate").over(w_brand)) \
        .withColumn("conv_std_brand", stddev("conversion_rate").over(w_brand))
    
    # Compute Z-score
    gold_feat = gold_feat.withColumn(
        "z_score",
        when(
            (col("conv_std_brand").isNotNull()) & (col("conv_std_brand") > 0),
            (col("conversion_rate") - col("conv_mean_brand")) / col("conv_std_brand")
        ).otherwise(0.0)
    )
    
    # Enhanced anomaly score (normalized)
    gold_feat = gold_feat.withColumn(
        "ensemble_anomaly_score",
        when(spark_abs(col("z_score")) > 3, 1.0)
        .when(spark_abs(col("z_score")) > 2.5, 0.8)
        .when(spark_abs(col("z_score")) > 2.0, 0.6)
        .when(spark_abs(col("z_score")) > 1.5, 0.4)
        .otherwise(0.0)
    )
    
    # Anomaly type
    gold_feat = gold_feat.withColumn(
        "anomaly_type",
        when(col("z_score") <= -2.0, "drop")
        .when(col("z_score") >= 2.0, "spike")
        .otherwise("none")
    )
    
    # Severity
    gold_feat = gold_feat.withColumn(
        "anomaly_severity",
        when(spark_abs(col("z_score")) >= 3.0, "critical")
        .when(spark_abs(col("z_score")) >= 2.5, "high")
        .when(spark_abs(col("z_score")) >= 2.0, "medium")
        .when(spark_abs(col("z_score")) >= 1.5, "low")
        .otherwise("normal")
    )
    
    # Is anomaly flag
    gold_feat = gold_feat.withColumn(
        "is_anomaly",
        (col("views") >= MIN_VIEWS_ANOMALY) & (spark_abs(col("z_score")) >= 2.0)
    )
    
    # Confidence (based on number of views)
    gold_feat = gold_feat.withColumn(
        "anomaly_confidence",
        when(col("views") >= 200, 0.9)
        .when(col("views") >= 100, 0.7)
        .when(col("views") >= MIN_VIEWS_ANOMALY, 0.5)
        .otherwise(0.3)
    )
    
    # Expected value
    gold_feat = gold_feat.withColumn("expected_value", col("conv_mean_brand"))
    
    # Deviation percentage
    gold_feat = gold_feat.withColumn(
        "deviation_percentage",
        when(col("expected_value") > 0,
            ((col("conversion_rate") - col("expected_value")) / col("expected_value")) * 100
        ).otherwise(0.0)
    )
    
    # Impact metrics (simplified)
    gold_feat = gold_feat.withColumn(
        "estimated_conversion_loss",
        (col("expected_value") - col("conversion_rate")) * col("views")
    )
    
    gold_feat = gold_feat.withColumn(
        "estimated_revenue_impact",
        col("estimated_conversion_loss") * 50.0  # Assume $50 AOV
    )
    
    return gold_feat


def run_advanced_detection_with_ml(gold_filtered, spark):
    """
    Run full advanced anomaly detection with ML models.
    Requires external libraries to be installed.
    """
    print("\n" + "=" * 60)
    print("Running Advanced ML-Based Anomaly Detection")
    print("=" * 60)
    
    try:
        # Import pipeline
        from advanced_anomaly_pipeline import create_pipeline
        
        # Create pipeline
        pipeline = create_pipeline(spark)
        
        # Run detection (without training on first run to save time)
        # In production, you'd train periodically
        df_features = pipeline.prepare_features(gold_filtered)
        
        # Train models
        pipeline.train_models(df_features, train_split=0.8)
        
        # Detect anomalies
        df_anomalies = pipeline.detect_anomalies(df_features)
        
        # Apply ensemble
        df_ensemble = pipeline.apply_ensemble(df_anomalies)
        
        # Convert back to Spark
        result_df = spark.createDataFrame(df_ensemble)
        
        return result_df
        
    except ImportError as e:
        print(f"\nWarning: Could not import ML libraries: {e}")
        print("Falling back to Spark-native detection")
        return None


def write_anomalies(df, output_path):
    """Write anomalies to output table."""
    print("\n" + "=" * 60)
    print("Writing Anomalies Table")
    print("=" * 60)
    
    # Filter to anomalies only
    anomalies = df.filter(col("is_anomaly"))
    anomaly_count = anomalies.count()
    print(f"Total anomalies detected: {anomaly_count:,}")
    
    if anomaly_count > 0:
        # Breakdown by severity
        print("\nBy severity:")
        anomalies.groupBy("anomaly_severity").count().orderBy("count", ascending=False).show()
        
        # Breakdown by type
        print("\nBy type:")
        anomalies.groupBy("anomaly_type").count().orderBy("count", ascending=False).show()
    
    # Add window_date for partitioning
    anomalies_out = anomalies.withColumn("window_date", to_date(col("window_start")))
    
    # Write
    anomalies_out.write \
        .mode("overwrite") \
        .partitionBy("window_date") \
        .parquet(output_path)
    
    print(f"\nAnomalies written to: {output_path}")
    
    # Show sample
    if anomaly_count > 0:
        print("\nTop critical anomalies:")
        anomalies.filter(col("anomaly_severity") == "critical") \
            .orderBy(col("ensemble_anomaly_score").desc()) \
            .select(
                "window_start", "brand", "views", "purchases",
                "conversion_rate", "expected_value", "deviation_percentage",
                "anomaly_type", "anomaly_severity"
            ).show(10, truncate=False)
    
    return anomalies


def main():
    print("=" * 60)
    print("FunnelPulse: Advanced Anomaly Detection Pipeline")
    print("=" * 60)
    print(f"\nParameters:")
    print(f"  Min views for baseline: {MIN_VIEWS_BASELINE}")
    print(f"  Min views for anomaly:  {MIN_VIEWS_ANOMALY}")
    
    spark = create_spark_session()
    
    # Load data
    gold_filtered = load_gold_data(spark)
    
    # Try advanced detection with ML
    result_df = run_advanced_detection_with_ml(gold_filtered, spark)
    
    # Fall back to Spark-native if ML fails
    if result_df is None:
        result_df = run_advanced_detection_spark_native(gold_filtered, spark)
    
    # Write results
    anomalies = write_anomalies(result_df, ADVANCED_ANOMALY_PATH)
    
    # Also update legacy path for backward compatibility
    print("\nUpdating legacy anomaly table for backward compatibility...")
    legacy_cols = [
        "window_start", "window_date", "brand", "views", "purchases",
        "conversion_rate", "is_anomaly", "anomaly_type"
    ]
    available_cols = [col for col in legacy_cols if col in result_df.columns]
    
    legacy_anomalies = result_df.filter(col("is_anomaly")).select(available_cols)
    legacy_anomalies.write \
        .mode("overwrite") \
        .partitionBy("window_date") \
        .parquet(LEGACY_ANOMALY_PATH)
    
    print("\n" + "=" * 60)
    print("Advanced Anomaly Detection Complete!")
    print("=" * 60)
    
    spark.stop()


if __name__ == "__main__":
    main()
