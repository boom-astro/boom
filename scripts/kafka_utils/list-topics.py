#!/usr/bin/env python3
# /// script
# dependencies = [
#   "confluent-kafka",
# ]
# ///
"""
Script to list all topics available on a Kafka server.
Run with: uv run list_kafka_topics.py <bootstrap_servers>
"""

import sys
from confluent_kafka import Consumer, KafkaException


def list_kafka_topics(bootstrap_servers):
    """
    Connect to Kafka and list all available topics.
    
    Args:
        bootstrap_servers: Comma-separated list of Kafka bootstrap servers
                          (e.g., 'localhost:9092' or 'server1:9092,server2:9092')
    """
    try:
        # Create consumer configuration
        conf = {
            'bootstrap.servers': bootstrap_servers,
            'group.id': 'topic-lister',
            'auto.offset.reset': 'earliest'
        }
        
        # Create consumer
        consumer = Consumer(conf)
        
        # Get cluster metadata
        metadata = consumer.list_topics(timeout=10)
        
        topics = metadata.topics
        
        print(f"Connected to Kafka at: {bootstrap_servers}")
        print(f"\nFound {len(topics)} topics:\n")
        
        for topic in sorted(topics.keys()):
            print(f"  - {topic}")
        
        consumer.close()
        return 0
        
    except KafkaException as e:
        print(f"Error connecting to Kafka: {e}", file=sys.stderr)
        return 1
    except Exception as e:
        print(f"Unexpected error: {e}", file=sys.stderr)
        return 1


def main():
    if len(sys.argv) != 2:
        print("Usage: python list_kafka_topics.py <bootstrap_servers>")
        print("Example: python list_kafka_topics.py localhost:9092")
        print("Example: python list_kafka_topics.py server1:9092,server2:9092")
        sys.exit(1)
    
    bootstrap_servers = sys.argv[1]
    sys.exit(list_kafka_topics(bootstrap_servers))


if __name__ == "__main__":
    main()