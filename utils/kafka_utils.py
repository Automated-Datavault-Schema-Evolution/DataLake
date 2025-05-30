import json

from kafka import KafkaConsumer

from config import KAFKA_TOPIC, KAFKA_BOOTSTRAP_SERVERS


def get_kafka_consumer(group_id="delta-bulk"):
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        group_id=group_id,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset='earliest',
        enable_auto_commit=False,
        value_deserializer=lambda value: json.loads(value.decode('utf-8'))
    )
    return consumer
