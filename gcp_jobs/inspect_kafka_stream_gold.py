from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from config import GOLD_STREAM_FUNNEL_HOURLY_BRAND_KAFKA

spark = SparkSession.builder.appName("InspectKafkaStreamGold").getOrCreate()

print("Reading streaming gold from:", GOLD_STREAM_FUNNEL_HOURLY_BRAND_KAFKA)

df = spark.read.parquet(str(GOLD_STREAM_FUNNEL_HOURLY_BRAND_KAFKA))

print("Total rows:", df.count())
print("Schema:")
df.printSchema()

print("Sample rows (ordered by window_start, brand):")
df.orderBy(col("window_start"), col("brand")).show(20, truncate=False)

print("testbrand rows:")
df.filter(col("brand") == "testbrand").orderBy("window_start").show(20, truncate=False)