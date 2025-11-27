"""
FunnelPulse GCP Test Job
========================
Tests reading data from GCS on Dataproc cluster.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import count, sum as _sum

# GCS paths
GCS_BUCKET = "gs://funnelpulse-data-479512"
TABLES_DIR = f"{GCS_BUCKET}/tables"

def main():
    # Create Spark session (Dataproc handles the configuration)
    spark = SparkSession.builder \
        .appName("FunnelPulse GCP Test") \
        .getOrCreate()

    print("=" * 60)
    print("FunnelPulse GCP Pipeline Test")
    print("=" * 60)

    # Test 1: Read Bronze layer
    print("\n[1] Reading Bronze Events...")
    bronze_path = f"{TABLES_DIR}/bronze_events"
    bronze = spark.read.parquet(bronze_path)
    bronze_count = bronze.count()
    print(f"    Bronze rows: {bronze_count:,}")

    # Test 2: Read Silver layer
    print("\n[2] Reading Silver Events...")
    silver_path = f"{TABLES_DIR}/silver_events"
    silver = spark.read.parquet(silver_path)
    silver_count = silver.count()
    print(f"    Silver rows: {silver_count:,}")

    # Test 3: Read Gold Hourly Brand
    print("\n[3] Reading Gold Funnel Hourly Brand...")
    gold_path = f"{TABLES_DIR}/gold_funnel_hourly_brand"
    gold = spark.read.parquet(gold_path)
    gold_count = gold.count()
    print(f"    Gold hourly brand rows: {gold_count:,}")

    # Test 4: Read Anomalies
    print("\n[4] Reading Anomalies...")
    anomaly_path = f"{TABLES_DIR}/gold_anomalies_hourly_brand"
    anomalies = spark.read.parquet(anomaly_path)
    anomaly_count = anomalies.count()
    print(f"    Anomaly rows: {anomaly_count:,}")

    # Test 5: Sample aggregation query
    print("\n[5] Running sample aggregation...")
    top_brands = gold.groupBy("brand") \
        .agg(_sum("revenue").alias("total_revenue")) \
        .orderBy("total_revenue", ascending=False) \
        .limit(10)

    print("    Top 10 brands by revenue:")
    top_brands.show(truncate=False)

    print("\n" + "=" * 60)
    print("GCP Pipeline Test PASSED!")
    print("=" * 60)

    spark.stop()

if __name__ == "__main__":
    main()
