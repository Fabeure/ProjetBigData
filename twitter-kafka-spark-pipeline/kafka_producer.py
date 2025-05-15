#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import KafkaError
import logging
import sys
import os

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger('twitter-kafka-producer')

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
KAFKA_TOPIC = 'twitter-geo-data'

class TwitterKafkaProducer:
    """
    A Kafka producer that processes Twitter data from now-playing-streamer
    and adds geolocation information before sending to Kafka.
    """
    
    def __init__(self, bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS, topic=KAFKA_TOPIC):
        """Initialize the Kafka producer with the given settings."""
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.producer = None
        self.connect()
        
    def connect(self):
        """Connect to Kafka broker."""
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                acks='all',
                retries=3,
                linger_ms=5
            )
            logger.info(f"Connected to Kafka at {self.bootstrap_servers}")
        except Exception as e:
            logger.error(f"Failed to connect to Kafka: {str(e)}")
            raise
    
    def add_geo_data(self, tweet_data):
        """
        Add geolocation data to the tweet.
        For demonstration, we're generating random coordinates.
        In a real application, you would extract this from the Twitter API.
        """
        # Generate random coordinates (latitude, longitude)
        # Latitude range: -90 to 90
        # Longitude range: -180 to 180
        lat = random.uniform(-90, 90)
        lon = random.uniform(-180, 180)
        
        # Add geolocation to tweet data
        tweet_data['geo'] = {
            'type': 'Point',
            'coordinates': [lat, lon]
        }
        
        # Add a timestamp if not present
        if 'timestamp' not in tweet_data:
            tweet_data['timestamp'] = datetime.now().isoformat()
            
        return tweet_data
    
    def send_to_kafka(self, data):
        """Send the data to Kafka topic."""
        if not self.producer:
            logger.error("Kafka producer not connected")
            return False
            
        try:
            # Add geolocation data
            enriched_data = self.add_geo_data(data)
            
            # Send to Kafka
            future = self.producer.send(self.topic, value=enriched_data)
            
            # Block until the message is sent (or timeout)
            record_metadata = future.get(timeout=10)
            
            logger.info(f"Message sent to {record_metadata.topic} partition {record_metadata.partition} offset {record_metadata.offset}")
            return True
        except KafkaError as e:
            logger.error(f"Failed to send message to Kafka: {str(e)}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error: {str(e)}")
            return False
    
    def close(self):
        """Close the Kafka producer."""
        if self.producer:
            self.producer.flush()
            self.producer.close()
            logger.info("Kafka producer closed")

def main():
    """Main function to test the Kafka producer."""
    producer = TwitterKafkaProducer()
    
    # Generate some test data
    for i in range(10):
        # Simulate tweet data
        tweet_data = {
            'id': f"tweet_{i}",
            'text': f"This is a test tweet {i} #bigdata #streaming",
            'user': {
                'screen_name': f"user_{i}",
                'followers_count': random.randint(1, 10000)
            },
            'created_at': datetime.now().isoformat()
        }
        
        # Send to Kafka
        producer.send_to_kafka(tweet_data)
        
        # Wait a bit
        time.sleep(1)
    
    # Close the producer
    producer.close()

if __name__ == "__main__":
    main()
