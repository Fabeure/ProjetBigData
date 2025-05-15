# Twitter Data Processing Pipeline with Kafka and Spark Streaming

This project implements a real-time processing pipeline for Twitter data with geolocation information using Apache Kafka and Apache Spark Streaming. The pipeline connects to the now-playing-streamer, enriches the data with Twitter-like metadata and geolocation information, and processes it using Spark Streaming.

## Architecture

The pipeline consists of the following components:

1. **Twitter Stream Connector**: Connects to the now-playing-streamer WebSocket and transforms the data into a Twitter-like format.
2. **Kafka Producer**: Adds geolocation information to the Twitter data and sends it to a Kafka topic.
3. **Spark Streaming Application**: Processes the Twitter data from Kafka, performs analysis, and outputs results.
4. **Kafka Utilities**: Helper functions for managing Kafka topics.

## Prerequisites

- Python 3.8+
- Apache Kafka 2.8+
- Apache Spark 3.5.0
- Java 8+

## Installation

1. Install the required Python packages:

```bash
pip install -r requirements.txt
```

2. Start Kafka:

```bash
# Start Zookeeper
bin/zookeeper-server-start.sh config/zookeeper.properties

# Start Kafka
bin/kafka-server-start.sh config/server.properties
```

3. Create the Kafka topic:

```bash
python kafka_utils.py
```

## Usage

1. Start the now-playing-streamer:

```bash
cd ../now-playing-streamer/main
python main.py
```

2. Start the Twitter Stream Connector:

```bash
python twitter_stream_connector.py
```

3. Start the Spark Streaming application:

```bash
python spark_streaming.py
```

## Components

### Twitter Stream Connector (`twitter_stream_connector.py`)

Connects to the now-playing-streamer WebSocket, enriches the data with Twitter-like metadata, and forwards it to the Kafka producer.

### Kafka Producer (`kafka_producer.py`)

Adds geolocation information to the Twitter data and sends it to a Kafka topic.

### Spark Streaming Application (`spark_streaming.py`)

Processes the Twitter data from Kafka, performs analysis such as hashtag counting and geo-clustering, and outputs results.

### Kafka Utilities (`kafka_utils.py`)

Helper functions for managing Kafka topics.

## Data Flow

1. The now-playing-streamer generates streaming data via WebSocket.
2. The Twitter Stream Connector receives the data and transforms it into a Twitter-like format.
3. The Kafka Producer adds geolocation information and sends the data to a Kafka topic.
4. The Spark Streaming application processes the data from Kafka and performs analysis.

## Analysis

The Spark Streaming application performs the following analyses:

1. **Hashtag Counting**: Extracts hashtags from tweets and counts them in sliding windows.
2. **Geo-Clustering**: Groups tweets by geographic location to identify hotspots.

## Output

The Spark Streaming application outputs the results to the console in real-time.

## License

This project is licensed under the MIT License - see the LICENSE file for details.
