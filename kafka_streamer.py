# kafka_streamer.py
import json
import os

import pandas as pd
from logger import log

from config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
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
        producer.send(KAFKA_TOPIC, value=message)
        producer.flush()
        log.info(f"Streamed data from {file_path} to Kafka topic '{KAFKA_TOPIC}' with {len(records)} records.")
    except Exception as e:
        log.error(f"Error streaming data from {file_path}: {e}")


if __name__ == "__main__":
    stream_delta("sample.csv")
