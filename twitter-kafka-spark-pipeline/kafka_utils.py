#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import sys
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger('kafka-utils')

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
KAFKA_TOPIC = 'twitter-geo-data'

def create_topic(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS, topic_name=KAFKA_TOPIC, 
                 num_partitions=1, replication_factor=1):
    """
    Create a Kafka topic if it doesn't exist.
    
    Args:
        bootstrap_servers (str): Kafka bootstrap servers
        topic_name (str): Name of the topic to create
        num_partitions (int): Number of partitions for the topic
        replication_factor (int): Replication factor for the topic
        
    Returns:
        bool: True if topic was created or already exists, False otherwise
    """
    try:
        # Create admin client
        admin_client = KafkaAdminClient(
            bootstrap_servers=bootstrap_servers,
            client_id='kafka-admin'
        )
        
        # Create topic
        topic_list = [
            NewTopic(
                name=topic_name,
                num_partitions=num_partitions,
                replication_factor=replication_factor
            )
        ]
        
        admin_client.create_topics(new_topics=topic_list, validate_only=False)
        logger.info(f"Topic '{topic_name}' created successfully")
        admin_client.close()
        return True
        
    except TopicAlreadyExistsError:
        logger.info(f"Topic '{topic_name}' already exists")
        return True
    except Exception as e:
        logger.error(f"Failed to create topic '{topic_name}': {str(e)}")
        return False

def delete_topic(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS, topic_name=KAFKA_TOPIC):
    """
    Delete a Kafka topic.
    
    Args:
        bootstrap_servers (str): Kafka bootstrap servers
        topic_name (str): Name of the topic to delete
        
    Returns:
        bool: True if topic was deleted, False otherwise
    """
    try:
        # Create admin client
        admin_client = KafkaAdminClient(
            bootstrap_servers=bootstrap_servers,
            client_id='kafka-admin'
        )
        
        # Delete topic
        admin_client.delete_topics([topic_name])
        logger.info(f"Topic '{topic_name}' deleted successfully")
        admin_client.close()
        return True
        
    except Exception as e:
        logger.error(f"Failed to delete topic '{topic_name}': {str(e)}")
        return False

def main():
    """Main function to create the default Kafka topic."""
    create_topic()

if __name__ == "__main__":
    main()
