import json

from kafka import KafkaConsumer, errors as kafka_errors
from logger import log

from config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC


def get_kafka_consumer(group_id="delta-bulk"):
    log.info(f"[Kafka] Connecting to bootstrap servers: {KAFKA_BOOTSTRAP_SERVERS} with group_id='{group_id}'")
    try:
        consumer = KafkaConsumer(
            group_id=group_id,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            auto_offset_reset='earliest',
            enable_auto_commit=False,
            value_deserializer=lambda value: json.loads(value.decode('utf-8'))
        )
        log.info("[Kafka] KafkaConsumer created successfully.")

        # Sanity check: available topics
        topics = consumer.topics()
        log.info(f"[Kafka] Available topics: {topics}")
        if KAFKA_TOPIC not in topics:
            log.warning(f"[Kafka] Kafka topic '{KAFKA_TOPIC}' does not exist! Check if the topic is created.")
            create_topic_if_not_exists(topic=KAFKA_TOPIC)

        # Sanity check: partitions for topic
        partitions = consumer.partitions_for_topic(KAFKA_TOPIC)
        if partitions:
            log.info(f"[Kafka] Partitions for topic '{KAFKA_TOPIC}': {partitions}")
        else:
            log.warning(f"[Kafka] No partitions found for topic '{KAFKA_TOPIC}'.")

        consumer.subscribe([KAFKA_TOPIC])
        return consumer
    except kafka_errors.NoBrokersAvailable:
        log.critical(f"[Kafka] Could not connect to Kafka at {KAFKA_BOOTSTRAP_SERVERS}. No brokers available!")
        raise
    except Exception as e:
        log.critical(f"[Kafka] Exception creating KafkaConsumer: {e}", exc_info=True)
        raise


def sanity_check_kafka():
    try:
        consumer = get_kafka_consumer()
        log.info("[Kafka] Sanity check passed: connected and got consumer.")
        consumer.close()
    except Exception as e:
        log.critical(f"[Kafka] Sanity check failed: {e}", exc_info=True)


def create_topic_if_not_exists(topic, num_partitions=3, replication_factor=1):
    from kafka.admin import KafkaAdminClient, NewTopic

    admin = KafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
    existing_topics = admin.list_topics()
    if topic not in existing_topics:
        new = NewTopic(name=topic, num_partitions=num_partitions, replication_factor=replication_factor)
        admin.create_topics([new])
        log.info(f"Topic {topic} created successfully.")
    admin.close()
