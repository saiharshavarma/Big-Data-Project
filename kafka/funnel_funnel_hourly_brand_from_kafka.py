#!/usr/bin/env python

"""
funnel_funnel_hourly_brand_from_kafka.py

Spark Structured Streaming job:
  Kafka topic (bronze-like JSON events)
    -> streaming "silver"
    -> hourly streaming "gold" funnel by brand
"""

from pyspark.sql import SparkSession, functions as F, types as T

from config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC_EVENTS,
    GOLD_STREAM_FUNNEL_HOURLY_BRAND_KAFKA,
    CHECKPOINT_STREAM_HOURLY_BRAND_KAFKA,
)


def create_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("FunnelPulseStreamingFromKafka")
        .getOrCreate()
    )


def get_bronze_schema() -> T.StructType:
    """
    Schema for the JSON messages coming from Kafka.
    Must match what kafka_replay_producer.py sends.
    """
    return T.StructType([
        T.StructField("event_time", T.StringType(), True),
        T.StructField("event_type", T.StringType(), True),
        T.StructField("user_id", T.LongType(), True),
        T.StructField("user_session", T.StringType(), True),
        T.StructField("product_id", T.LongType(), True),
        T.StructField("category_id", T.LongType(), True),
        T.StructField("category_code", T.StringType(), True),
        T.StructField("brand", T.StringType(), True),
        T.StructField("price", T.DoubleType(), True),
    ])


def build_streaming_silver(df_kafka, schema):
    """
    From Kafka raw messages -> cleaned, normalized streaming "silver".
    """
    # Parse Kafka value as JSON into columns
    json_df = (
        df_kafka
        .selectExpr("CAST(value AS STRING) AS json_str")
        .select(F.from_json("json_str", schema).alias("data"))
        .select("data.*")
    )

    # Convert event_time string to timestamp, derive event_date
    df = json_df.withColumn("event_time", F.to_timestamp("event_time"))

    df = df.withColumn("event_date", F.to_date("event_time"))

    # Basic filters (same as batch/streaming silver)
    df = df.filter(F.col("event_time").isNotNull())
    df = df.filter(F.col("event_type").isNotNull())
    df = df.filter((F.col("price").isNotNull()) & (F.col("price") > 0))

    # Normalize brand and category_code
    df = df.withColumn("brand_norm", F.lower(F.col("brand")))
    df = df.withColumn(
        "category_code_norm",
        F.lower(F.regexp_replace(F.col("category_code"), r"[^a-zA-Z0-9\._]", "_")),
    )

    return df


def build_streaming_gold(df_silver):
    """
    From streaming silver -> hourly funnel metrics by brand with watermark.
    """
    # 1-hour window by event_time, group by brand
    windowed = (
        df_silver
        .withWatermark("event_time", "2 hours")
        .groupBy(
            F.window("event_time", "1 hour").alias("w"),
            F.col("brand_norm").alias("brand"),
        )
        .agg(
            F.sum(F.when(F.col("event_type") == "view", 1).otherwise(0)).alias("views"),
            F.sum(F.when(F.col("event_type") == "cart", 1).otherwise(0)).alias("carts"),
            F.sum(
                F.when(F.col("event_type") == "purchase", 1).otherwise(0)
            ).alias("purchases"),
            F.sum(
                F.when(F.col("event_type") == "purchase", F.col("price")).otherwise(0.0)
            ).alias("revenue"),
        )
    )

    # Derived metrics and window boundaries
    result = (
        windowed
        .withColumn("window_start", F.col("w.start"))
        .withColumn("window_end", F.col("w.end"))
        .drop("w")
        .withColumn(
            "view_to_cart_rate",
            F.when(F.col("views") > 0, F.col("carts") / F.col("views")).otherwise(None),
        )
        .withColumn(
            "cart_to_purchase_rate",
            F.when(F.col("carts") > 0, F.col("purchases") / F.col("carts")).otherwise(None),
        )
        .withColumn(
            "conversion_rate",
            F.when(F.col("views") > 0, F.col("purchases") / F.col("views")).otherwise(None),
        )
        .withColumn("window_date", F.to_date("window_start"))
    )

    return result


def main():
    print("=== FunnelPulse Streaming from Kafka ===")
    print(f"KAFKA_BOOTSTRAP_SERVERS: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"KAFKA_TOPIC_EVENTS     : {KAFKA_TOPIC_EVENTS}")
    print(f"Output GOLD_STREAM_PATH: {GOLD_STREAM_FUNNEL_HOURLY_BRAND_KAFKA}")
    print(f"Checkpoint Location    : {CHECKPOINT_STREAM_HOURLY_BRAND_KAFKA}")
    print("========================================")

    spark = create_spark()
    schema = get_bronze_schema()

    # Kafka source
    df_kafka = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC_EVENTS)
        .option("startingOffsets", "earliest")  # for replay; "latest" for live
        .load()
    )

    # Streaming silver
    df_silver = build_streaming_silver(df_kafka, schema)

    # Streaming gold
    df_gold = build_streaming_gold(df_silver)

    # Sink to Parquet on GCS (or local), append mode
    query = (
        df_gold
        .writeStream
        .outputMode("append")
        .format("parquet")
        .option("checkpointLocation", str(CHECKPOINT_STREAM_HOURLY_BRAND_KAFKA))
        .option("path", str(GOLD_STREAM_FUNNEL_HOURLY_BRAND_KAFKA))
        .trigger(processingTime="30 seconds")
        .start()
    )

    print("Streaming query started. Awaiting termination...")
    query.awaitTermination()


if __name__ == "__main__":
    main()