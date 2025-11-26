"""
Key differences vs your current file-based streaming notebook:
	•	Source is .format("kafka") instead of .parquet(stream_input_path).
	•	We parse the Kafka value column as JSON into the same event schema.
	•	Everything downstream (cleaning, aggregations, metrics, sink) is conceptually the same as you already implemented.

So when you “shift to GCP”:
	•	GCS replaces your ~/funnelpulse/tables/ paths.
	•	Kafka source replaces your file source.
	•	The rest of the streaming code is almost identical to what you’ve already debugged.
"""

# Kafka-based streaming job: funnel_funnel_hourly_brand_from_kafka.py-ish

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, window, sum as _sum, when, to_date
)
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType, DoubleType
)

# ----------------------------
# CONFIG
# ----------------------------

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"        # change in prod
KAFKA_TOPIC            = "funnelpulse_events"

BRONZE_SCHEMA = StructType([
    StructField("event_time",    TimestampType(), True),
    StructField("event_type",    StringType(),    True),
    StructField("user_id",       StringType(),    True),
    StructField("user_session",  StringType(),    True),
    StructField("product_id",    StringType(),    True),
    StructField("category_id",   StringType(),    True),
    StructField("category_code", StringType(),    True),
    StructField("brand",         StringType(),    True),
    StructField("price",         DoubleType(),    True),
])

GOLD_STREAM_PATH      = "gs://your-bucket/funnelpulse/tables/gold_stream_funnel_hourly_brand"
CHECKPOINT_LOCATION   = "gs://your-bucket/funnelpulse/checkpoints/stream_hourly_brand"


spark = (
    SparkSession.builder
    .appName("FunnelPulse Streaming from Kafka")
    .getOrCreate()
)

# ----------------------------
# 1. Kafka source
# ----------------------------

kafka_raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "latest")   # or "earliest" for full backfill
    .load()
)

# kafka_raw has: key (binary), value (binary), topic, partition, offset, timestamp, etc.

# Parse JSON payload in 'value' into structured columns
events = (
    kafka_raw
    .selectExpr("CAST(value AS STRING) as json_str")
    .select(from_json(col("json_str"), BRONZE_SCHEMA).alias("e"))
    .select("e.*")
)

# ----------------------------
# 2. Streaming "silver" cleaning (same logic as in your file-based stream)
# ----------------------------

clean = (
    events
    .filter(col("event_time").isNotNull())
    .filter(col("event_type").isNotNull())
    .filter(col("price").isNotNull() & (col("price") > 0.0))
    .withColumn("brand_norm", col("brand"))
    .withColumn("category_code_norm", col("category_code"))
    .withColumn("event_date", to_date(col("event_time")))
    # you can re-add DQ flags if you like
)

# ----------------------------
# 3. Streaming "gold": hourly funnel by brand with watermark
# ----------------------------

from pyspark.sql.functions import hour

stream_windowed = (
    clean
    .withWatermark("event_time", "2 hours")
    .groupBy(
        window(col("event_time"), "1 hour").alias("w"),
        col("brand_norm").alias("brand")
    )
    .agg(
        _sum((col("event_type") == "view").cast("int")).alias("views"),
        _sum((col("event_type") == "cart").cast("int")).alias("carts"),
        _sum((col("event_type") == "purchase").cast("int")).alias("purchases"),
        _sum(
            when(col("event_type") == "purchase", col("price")).otherwise(0.0)
        ).alias("revenue"),
    )
)

stream_gold = (
    stream_windowed
    .withColumn("window_start", col("w.start"))
    .withColumn("window_end", col("w.end"))
    .drop("w")
    .withColumn(
        "view_to_cart_rate",
        when(col("views") > 0, col("carts") / col("views"))
    )
    .withColumn(
        "cart_to_purchase_rate",
        when(col("carts") > 0, col("purchases") / col("carts"))
    )
    .withColumn(
        "conversion_rate",
        when(col("views") > 0, col("purchases") / col("views"))
    )
    .withColumn("window_date", to_date(col("window_start")))
)

# ----------------------------
# 4. Sink to GCS (or any object store) with checkpoint
# ----------------------------

query = (
    stream_gold
    .writeStream
    .outputMode("append")
    .format("parquet")
    .option("checkpointLocation", CHECKPOINT_LOCATION)
    .option("path", GOLD_STREAM_PATH)
    .trigger(processingTime="30 seconds")
    .start()
)

print("Streaming query started. Waiting for termination...")
query.awaitTermination()