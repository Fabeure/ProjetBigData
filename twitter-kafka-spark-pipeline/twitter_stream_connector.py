#!/usr/bin/env python
# -*- coding: utf-8 -*-

import asyncio
import json
import logging
import sys
import websockets
from datetime import datetime
from kafka_producer import TwitterKafkaProducer

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger('twitter-stream-connector')

# WebSocket configuration
WEBSOCKET_URL = "ws://localhost:8765"

class TwitterStreamConnector:
    """
    Connects to the now-playing-streamer WebSocket and forwards
    the data to Kafka after enriching it with Twitter-like metadata.
    """
    
    def __init__(self, websocket_url=WEBSOCKET_URL):
        """Initialize the connector with WebSocket URL."""
        self.websocket_url = websocket_url
        self.kafka_producer = TwitterKafkaProducer()
        logger.info(f"Initialized connector to WebSocket at {websocket_url}")
    
    def enrich_data(self, data):
        """
        Enrich the now-playing-streamer data with Twitter-like metadata.
        This transforms the Spotify data into a format resembling Twitter data.
        """
        try:
            # Create a Twitter-like structure from the now-playing data
            tweet_data = {
                'id': f"tweet_{datetime.now().timestamp()}",
                'text': f"Now playing: {data.get('track_name', 'Unknown')} by {data.get('artist_name', 'Unknown')} #NowPlaying #Music",
                'user': {
                    'screen_name': f"music_lover_{data.get('user_id', '0000')}",
                    'followers_count': data.get('popularity', '0')
                },
                'created_at': data.get('timestamp', datetime.now().isoformat()),
                'song_data': {
                    'track': data.get('track_name', 'Unknown'),
                    'artist': data.get('artist_name', 'Unknown'),
                    'album': data.get('album_name', 'Unknown'),
                    'popularity': data.get('popularity', '0')
                }
            }
            return tweet_data
        except Exception as e:
            logger.error(f"Error enriching data: {str(e)}")
            return data  # Return original data if enrichment fails
    
    async def connect_and_process(self):
        """Connect to WebSocket and process incoming data."""
        try:
            async with websockets.connect(self.websocket_url) as websocket:
                logger.info(f"Connected to WebSocket at {self.websocket_url}")
                
                while True:
                    # Receive data from WebSocket
                    data_str = await websocket.recv()
                    
                    try:
                        # Parse JSON data
                        data = json.loads(data_str)
                        
                        # Enrich data with Twitter-like metadata
                        enriched_data = self.enrich_data(data)
                        
                        # Send to Kafka
                        self.kafka_producer.send_to_kafka(enriched_data)
                        
                    except json.JSONDecodeError:
                        logger.error(f"Failed to parse JSON: {data_str}")
                    except Exception as e:
                        logger.error(f"Error processing message: {str(e)}")
                        
        except websockets.exceptions.ConnectionClosed:
            logger.warning("WebSocket connection closed")
        except Exception as e:
            logger.error(f"Error in WebSocket connection: {str(e)}")
        finally:
            # Close Kafka producer
            self.kafka_producer.close()
    
    def start(self):
        """Start the connector."""
        try:
            # Run the async event loop
            asyncio.run(self.connect_and_process())
        except KeyboardInterrupt:
            logger.info("Connector stopped by user")
        except Exception as e:
            logger.error(f"Unexpected error: {str(e)}")

def main():
    """Main function to start the connector."""
    connector = TwitterStreamConnector()
    connector.start()

if __name__ == "__main__":
    main()
