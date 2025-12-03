"""
Real-time Streaming Anomaly Detection
======================================
Spark Streaming integration for real-time anomaly detection
with online model updates and Kafka support.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, from_json, to_json, struct, window, current_timestamp
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, TimestampType
import pandas as pd
from typing import Optional

import sys
sys.path.append('..')
from config.anomaly_config import STREAMING_CONFIG


class RealtimeAnomalyDetector:
    """
    Real-time anomaly detection using Spark Structured Streaming.
    Supports Kafka integration and sliding window analysis.
    """
    
    def __init__(self, spark: SparkSession, config=None):
        """
        Initialize real-time detector.
        
        Args:
            spark: SparkSession instance
            config: Streaming configuration. Defaults to STREAMING_CONFIG.
        """
        self.spark = spark
        self.config = config or STREAMING_CONFIG
        self.models = None  # Will store trained models
    
    def _get_event_schema(self) -> StructType:
        """Define schema for streaming events."""
        return StructType([
            StructField("event_time", TimestampType(), True),
            StructField("event_type", StringType(), True),
            StructField("brand", StringType(), True),
            StructField("user_session", StringType(), True),
            StructField("price", DoubleType(), True),
        ])
    
    def read_from_kafka(self, topic: Optional[str] = None, 
                       bootstrap_servers: Optional[str] = None) -> DataFrame:
        """
        Read streaming data from Kafka.
        
        Args:
            topic: Kafka topic name
            bootstrap_servers: Kafka bootstrap servers
            
        Returns:
            Streaming DataFrame
        """
        topic = topic or self.config.get("kafka_topic_input", "funnelpulse_events")
        bootstrap_servers = bootstrap_servers or self.config.get("kafka_bootstrap_servers", "localhost:9092")
        
        print(f"Reading from Kafka: {bootstrap_servers}, topic: {topic}")
        
        # Read from Kafka
        df = self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", bootstrap_servers) \
            .option("subscribe", topic) \
            .option("startingOffsets", "latest") \
            .load()
        
        # Parse JSON values
        schema = self._get_event_schema()
        df = df.select(
            from_json(col("value").cast("string"), schema).alias("data")
        ).select("data.*")
        
        return df
    
    def read_from_file_stream(self, input_path: str) -> DataFrame:
        """
        Read streaming data from file system (for testing).
        
        Args:
            input_path: Path to streaming input files
            
        Returns:
            Streaming DataFrame
        """
        print(f"Reading from file stream: {input_path}")
        
        schema = self._get_event_schema()
        
        df = self.spark \
            .readStream \
            .schema(schema) \
            .parquet(input_path)
        
        return df
    
    def apply_windowing(self, df: DataFrame) -> DataFrame:
        """
        Apply sliding window aggregations.
        
        Args:
            df: Input streaming DataFrame
            
        Returns:
            Windowed aggregated DataFrame
        """
        window_size = self.config.get("sliding_window_size", "1 hour")
        window_slide = self.config.get("sliding_window_slide", "5 minutes")
        watermark = self.config.get("watermark_delay", "10 minutes")
        
        # Add watermark for late data handling
        df = df.withWatermark("event_time", watermark)
        
        # Aggregate by window and brand
        windowed_df = df \
            .groupBy(
                window(col("event_time"), window_size, window_slide).alias("window"),
                col("brand")
            ) \
            .agg(
                {"*": "count", "price": "sum"}
            ) \
            .withColumnRenamed("count(1)", "views") \
            .withColumnRenamed("sum(price)", "revenue") \
            .select(
                col("window.start").alias("window_start"),
                col("window.end").alias("window_end"),
                col("brand"),
                col("views"),
                col("revenue")
            )
        
        return windowed_df
    
    def detect_anomalies_batch(self, batch_df: DataFrame, batch_id: int):
        """
        Process a micro-batch for anomaly detection.
        This function is called by foreachBatch.
        
        Args:
            batch_df: Micro-batch DataFrame
            batch_id: Batch identifier
        """
        if batch_df.isEmpty():
            return
        
        print(f"\nProcessing batch {batch_id} with {batch_df.count()} records")
        
        # Convert to Pandas for model inference
        pdf = batch_df.toPandas()
        
        # Simple anomaly detection (Z-score based)
        # In production, this would use the trained ensemble models
        brand_groups = pdf.groupby('brand')
        
        anomalies = []
        for brand, group in brand_groups:
            if len(group) < 2:
                continue
            
            mean_views = group['views'].mean()
            std_views = group['views'].std()
            
            if std_views > 0:
                group['z_score'] = (group['views'] - mean_views) / std_views
                group['is_anomaly'] = abs(group['z_score']) > 2.0
                
                brand_anomalies = group[group['is_anomaly']]
                if len(brand_anomalies) > 0:
                    anomalies.append(brand_anomalies)
        
        if anomalies:
            anomalies_df = pd.concat(anomalies, ignore_index=True)
            print(f"Detected {len(anomalies_df)} anomalies in batch {batch_id}")
            
            # Convert back to Spark DataFrame
            spark_anomalies = self.spark.createDataFrame(anomalies_df)
            
            # Write to output (append mode)
            output_path = self.config.get("checkpoint_location", "").replace("checkpoints", "output")
            if output_path:
                spark_anomalies.write \
                    .mode("append") \
                    .parquet(f"{output_path}/anomalies_realtime")
    
    def write_to_kafka(self, df: DataFrame, topic: Optional[str] = None,
                      bootstrap_servers: Optional[str] = None):
        """
        Write anomalies to Kafka topic.
        
        Args:
            df: DataFrame with anomalies
            topic: Kafka topic name
            bootstrap_servers: Kafka bootstrap servers
            
        Returns:
            Streaming query
        """
        topic = topic or self.config.get("kafka_topic_output", "funnelpulse_anomalies")
        bootstrap_servers = bootstrap_servers or self.config.get("kafka_bootstrap_servers", "localhost:9092")
        
        print(f"Writing to Kafka: {bootstrap_servers}, topic: {topic}")
        
        # Convert DataFrame to JSON
        json_df = df.select(
            to_json(struct("*")).alias("value")
        )
        
        # Write to Kafka
        query = json_df \
            .writeStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", bootstrap_servers) \
            .option("topic", topic) \
            .option("checkpointLocation", f"{self.config['checkpoint_location']}/kafka_output") \
            .start()
        
        return query
    
    def write_to_console(self, df: DataFrame, truncate: bool = False):
        """
        Write streaming output to console (for debugging).
        
        Args:
            df: Streaming DataFrame
            truncate: Whether to truncate output
            
        Returns:
            Streaming query
        """
        query = df \
            .writeStream \
            .outputMode("append") \
            .format("console") \
            .option("truncate", truncate) \
            .start()
        
        return query
    
    def write_to_parquet(self, df: DataFrame, output_path: str):
        """
        Write streaming output to Parquet files.
        
        Args:
            df: Streaming DataFrame
            output_path: Output path
            
        Returns:
            Streaming query
        """
        checkpoint_location = self.config.get("checkpoint_location", "/tmp/checkpoints")
        
        query = df \
            .writeStream \
            .outputMode("append") \
            .format("parquet") \
            .option("path", output_path) \
            .option("checkpointLocation", f"{checkpoint_location}/parquet_output") \
            .trigger(processingTime=self.config.get("trigger_interval", "5 minutes")) \
            .start()
        
        return query
    
    def start_streaming_pipeline(self, source_type: str = "kafka",
                                 source_path: Optional[str] = None,
                                 output_type: str = "parquet",
                                 output_path: Optional[str] = None):
        """
        Start the complete streaming anomaly detection pipeline.
        
        Args:
            source_type: "kafka" or "file"
            source_path: Path for file source
            output_type: "kafka", "parquet", or "console"
            output_path: Output path
            
        Returns:
            Streaming query
        """
        print("=" * 60)
        print("Starting Real-time Anomaly Detection Pipeline")
        print("=" * 60)
        
        # Read from source
        if source_type == "kafka":
            df = self.read_from_kafka()
        else:
            if not source_path:
                raise ValueError("source_path required for file source")
            df = self.read_from_file_stream(source_path)
        
        # Apply windowing
        windowed_df = self.apply_windowing(df)
        
        # Write output
        if output_type == "kafka":
            query = self.write_to_kafka(windowed_df)
        elif output_type == "parquet":
            if not output_path:
                output_path = f"{self.config.get('checkpoint_location', '')}/output"
            query = self.write_to_parquet(windowed_df, output_path)
        else:  # console
            query = self.write_to_console(windowed_df)
        
        print(f"\nStreaming query started. Query ID: {query.id}")
        print(f"Status: {query.status}")
        
        return query
    
    def stop_all_streams(self):
        """Stop all active streaming queries."""
        for query in self.spark.streams.active:
            print(f"Stopping query: {query.id}")
            query.stop()
        
        print("All streaming queries stopped.")


def create_realtime_detector(spark: SparkSession, config=None) -> RealtimeAnomalyDetector:
    """
    Factory function to create real-time detector.
    
    Args:
        spark: SparkSession instance
        config: Streaming configuration
        
    Returns:
        RealtimeAnomalyDetector instance
    """
    return RealtimeAnomalyDetector(spark, config=config)
