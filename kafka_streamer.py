# kafka_streamer.py
import json
import os
from sys import api_version

import pandas as pd
from confluent_kafka import Producer
from logger import log

from config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    #bootstrap_servers=['localhost:9092'],
    api_version=(0, 10),
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

producer1 = Producer(
        {'bootstrap.servers': 'localhost:9092',
         'compression.type': 'lz4'
         }
)



def stream_delta(file_path):
    """
    Read a CSV file using Pandas and stream its content (as a list of records) to Kafka.
    """
    try:
        df = pd.read_csv(file_path)
        records = df.to_dict(orient='records')
        message = {
            "filename": os.path.basename(file_path).replace(".csv", ""),
            "records": records
        }
        producer1.produce(KAFKA_TOPIC, key=message["filename"], value=json.dumps(message["records"]))
        log.debug(f"Produced message to Kafka topic '{KAFKA_TOPIC}': {message['filename']}, {records}")
        producer1.flush()
        log.info(f"Streamed data from {file_path} to Kafka topic '{KAFKA_TOPIC}' with {len(records)} records.")
    except Exception as e:
        log.error(f"Error streaming data from {file_path}: {e}")


if __name__ == "__main__":
    stream_delta("sample.csv")
