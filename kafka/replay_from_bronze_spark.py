# kafka/replay_from_bronze_spark.py

import os
from pyspark.sql import SparkSession, functions as F

# Import shared config (paths + Kafka topic)
from config import BRONZE_PATH, KAFKA_CONFIG

def main():
    # Decide which bootstrap servers to use
    kafka_bootstrap = os.environ.get(
        "KAFKA_BOOTSTRAP_SERVERS",
        KAFKA_CONFIG.get("bootstrap_servers", "localhost:9092"),
    )
    kafka_topic = os.environ.get(
        "KAFKA_TOPIC_EVENTS",
        KAFKA_CONFIG.get("topic", "funnelpulse_events"),
    )

    print("=== Replay bronze events to Kafka ===")
    print("BRONZE_PATH          :", BRONZE_PATH)
    print("KAFKA_BOOTSTRAP_SERVERS:", kafka_bootstrap)
    print("KAFKA_TOPIC_EVENTS     :", kafka_topic)
    print("=====================================")

    spark = (
        SparkSession.builder
        .appName("ReplayBronzeToKafka")
        .getOrCreate()
    )

    # 1. Read bronze events from GCS
    bronze_df = spark.read.parquet(str(BRONZE_PATH))

    # Optional: filter to a smaller date range so we don't blast Kafka
    # Adjust these dates to whatever you want to replay.
    # This assumes bronze has an `event_date` column (as in your pipeline).
    bronze_subset = (
        bronze_df
        .filter(
            (F.col("event_date") >= F.lit("2019-10-15")) &
            (F.col("event_date") <= F.lit("2019-10-16"))
        )
        .filter(F.col("event_time").isNotNull())
        .filter(F.col("event_type").isin("view", "cart", "purchase"))
    )

    count_subset = bronze_subset.count()
    print(f"Bronze subset rows to send: {count_subset}")

    if count_subset == 0:
        print("No rows found in the selected date range; exiting.")
        spark.stop()
        return

    # 2. Build Kafka key/value columns
    # Use user_session as key (keeps session events together in a partition)
    # Value is JSON with relevant fields.
    events_for_kafka = bronze_subset.select(
        F.col("user_session").cast("string").alias("key"),
        F.to_json(
            F.struct(
                "event_time",
                "event_type",
                "user_id",
                "user_session",
                "product_id",
                "category_id",
                "category_code",
                "brand",
                "price",
            )
        ).alias("value"),
    )

    # 3. Write the DataFrame to Kafka in batch
    (
        events_for_kafka
        .write
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap)
        .option("topic", kafka_topic)
        .save()
    )

    print("Finished writing bronze events to Kafka.")
    spark.stop()


if __name__ == "__main__":
    main()