"""Kafka helper facade (thin wrapper over utils.kafka_utils)."""

from utils.kafka_utils import get_kafka_consumer, sanity_check_kafka, get_topic_backlog


__all__ = [
    "get_kafka_consumer",
    "sanity_check_kafka",
    "get_topic_backlog",
]
