#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
import json
import logging
from datetime import datetime

# Add findspark to locate PySpark installation
import findspark
findspark.init()

from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window, count, explode, split
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, FloatType, MapType, ArrayType

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger('twitter-spark-streaming')

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
KAFKA_TOPIC = 'twitter-geo-data'

# Schema for the Twitter data with geolocation
tweet_schema = StructType([
    StructField("id", StringType(), True),
    StructField("text", StringType(), True),
    StructField("user", StructType([
        StructField("screen_name", StringType(), True),
        StructField("followers_count", StringType(), True)
    ]), True),
    StructField("created_at", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("geo", StructType([
        StructField("type", StringType(), True),
        StructField("coordinates", ArrayType(FloatType()), True)
    ]), True)
])

class TwitterSparkStreaming:
    """
    A Spark Streaming application that processes Twitter data from Kafka,
    performs analysis, and outputs results.
    """
    
    def __init__(self, bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS, topic=KAFKA_TOPIC):
        """Initialize the Spark Streaming application."""
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.spark = None
        self.init_spark()
        
    def init_spark(self):
        """Initialize Spark session."""
        try:
            self.spark = SparkSession.builder \
                .appName("TwitterGeoDataProcessing") \
                .config("spark.sql.shuffle.partitions", "2") \
                .config("spark.streaming.stopGracefullyOnShutdown", "true") \
                .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
                .getOrCreate()
            
            # Set log level
            self.spark.sparkContext.setLogLevel("WARN")
            
            logger.info("Spark session initialized")
        except Exception as e:
            logger.error(f"Failed to initialize Spark: {str(e)}")
            raise
    
    def read_from_kafka(self):
        """Read streaming data from Kafka."""
        try:
            # Read from Kafka
            df = self.spark \
                .readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", self.bootstrap_servers) \
                .option("subscribe", self.topic) \
                .option("startingOffsets", "latest") \
                .load()
            
            # Parse the value column from Kafka
            parsed_df = df.selectExpr("CAST(value AS STRING) as json_data")
            
            # Parse JSON data with the defined schema
            tweets_df = parsed_df.select(
                from_json(col("json_data"), tweet_schema).alias("tweet")
            ).select("tweet.*")
            
            return tweets_df
        except Exception as e:
            logger.error(f"Error reading from Kafka: {str(e)}")
            raise
    
    def process_tweets(self, tweets_df):
        """Process tweets with various transformations and analyses."""
        try:
            # 1. Extract hashtags from tweet text
            hashtags_df = tweets_df.select(
                col("id"),
                col("text"),
                col("timestamp"),
                col("geo.coordinates").getItem(0).alias("latitude"),
                col("geo.coordinates").getItem(1).alias("longitude"),
                explode(split(col("text"), " ")).alias("word")
            ).filter(col("word").startswith("#"))
            
            # 2. Count hashtags in sliding windows
            hashtag_counts = hashtags_df \
                .withWatermark("timestamp", "1 minute") \
                .groupBy(
                    window(col("timestamp"), "5 minutes", "1 minute"),
                    col("word")
                ) \
                .count() \
                .orderBy(col("count").desc())
            
            # 3. Geo-clustering of tweets
            geo_clusters = tweets_df \
                .withWatermark("timestamp", "1 minute") \
                .groupBy(
                    window(col("timestamp"), "5 minutes", "1 minute"),
                    col("geo.coordinates").getItem(0).cast("int").alias("lat_group"),
                    col("geo.coordinates").getItem(1).cast("int").alias("lon_group")
                ) \
                .count() \
                .orderBy(col("count").desc())
            
            return {
                "hashtag_counts": hashtag_counts,
                "geo_clusters": geo_clusters
            }
        except Exception as e:
            logger.error(f"Error processing tweets: {str(e)}")
            raise
    
    def start_streaming(self):
        """Start the streaming process and output results."""
        try:
            # Read from Kafka
            tweets_df = self.read_from_kafka()
            
            # Process tweets
            processed_data = self.process_tweets(tweets_df)
            
            # Output hashtag counts to console
            hashtag_query = processed_data["hashtag_counts"] \
                .writeStream \
                .outputMode("complete") \
                .format("console") \
                .option("truncate", "false") \
                .start()
            
            # Output geo clusters to console
            geo_query = processed_data["geo_clusters"] \
                .writeStream \
                .outputMode("complete") \
                .format("console") \
                .option("truncate", "false") \
                .start()
            
            # Wait for termination
            logger.info("Streaming started. Waiting for termination...")
            spark_queries = [hashtag_query, geo_query]
            for query in spark_queries:
                query.awaitTermination()
                
        except Exception as e:
            logger.error(f"Error in streaming: {str(e)}")
            raise
        finally:
            if self.spark:
                self.spark.stop()
                logger.info("Spark session stopped")

def main():
    """Main function to start the Spark Streaming application."""
    streaming_app = TwitterSparkStreaming()
    streaming_app.start_streaming()

if __name__ == "__main__":
    main()
