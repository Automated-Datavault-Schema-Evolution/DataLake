import json
from pathlib import Path

from kafka import KafkaConsumer, errors as kafka_errors
from kafka.admin import KafkaAdminClient
from kafka.errors import TopicAlreadyExistsError
from kafka.structs import TopicPartition
from logger import log

from config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC, KAFKA_GROUP_ID, STREAMING_CHECKPOINT_PATH


def get_kafka_consumer(group_id=None):
    if group_id is None:
        group_id = KAFKA_GROUP_ID
    log.info(
        f"[Kafka] Connecting to bootstrap servers: {KAFKA_BOOTSTRAP_SERVERS} with group_id='{group_id}'")
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
        log.debug(f"[Kafka] Subscribed to topic '{KAFKA_TOPIC}'")
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
        log.debug("[Kafka] Consumer closed after sanity check")
    except Exception as e:
        log.critical(f"[Kafka] Sanity check failed: {e}", exc_info=True)


def create_topic_if_not_exists(topic, num_partitions=3, replication_factor=1):
    from kafka.admin import KafkaAdminClient, NewTopic

    admin = KafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
    log.debug(
        f"[Kafka] Checking for topic '{topic}' (partitions={num_partitions}, replication={replication_factor})"
    )
    try:
        existing_topics = admin.list_topics()
        if topic not in existing_topics:
            log.info(f"[Kafka] Topic '{topic}' does not exist. Creating …")
            new = NewTopic(name=topic, num_partitions=num_partitions, replication_factor=replication_factor)
            try:
                admin.create_topics([new])
            except TopicAlreadyExistsError:
                log.debug(f"[Kafka] Topic '{topic}' was created concurrently.")
            else:
                log.info(f"[Kafka] Topic '{topic}' created successfully.")
    finally:
        admin.close()
        log.debug("[Kafka] Admin client closed")


def _extract_partition_offsets(obj):
    """Recursively search ``obj`` for partition offset mappings."""
    if isinstance(obj, dict):
        if all(isinstance(k, str) and k.isdigit() for k in obj.keys()):
            try:
                return {int(k): int(obj[k]) for k in obj}
            except Exception:
                return {}
        if KAFKA_TOPIC in obj:
            res = _extract_partition_offsets(obj[KAFKA_TOPIC])
            if res:
                return res
        for v in obj.values():
            res = _extract_partition_offsets(v)
            if res:
                return res
    elif isinstance(obj, str):
        try:
            data = json.loads(obj)
            return _extract_partition_offsets(data)
        except Exception:
            return {}
    return {}


def _read_checkpoint_offsets():
    """Load latest processed offsets from the streaming checkpoint."""
    offsets_dir = Path(STREAMING_CHECKPOINT_PATH) / "commits"
    if not offsets_dir.exists():
        return {}
    files = sorted(offsets_dir.glob("*.json"))
    if not files:
        return {}
    latest = files[-1]
    try:
        with open(latest) as fh:
            data = json.load(fh)
        sources = data.get("sources") or []
        for src in sources:
            if "Kafka" in src.get("description", ""):
                end_offset = src.get("endOffset") or src.get("endOffsets")
                if end_offset is not None:
                    return _extract_partition_offsets(end_offset)
    except Exception as exc:  # pragma: no cover - runtime safety
        log.error(f"[Kafka] Failed to read checkpoint offsets: {exc}")
    return {}


def get_topic_backlog(group_id=None):
    """Return the number of messages not yet consumed for the given group.

    This uses the Kafka Admin API to read committed offsets without joining the
    consumer group, avoiding rebalances of the streaming consumer."""
    admin = None
    consumer = None
    try:
        if group_id is None:
            group_id = KAFKA_GROUP_ID

        admin = KafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
        consumer = KafkaConsumer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)

        partitions = consumer.partitions_for_topic(KAFKA_TOPIC)
        if not partitions:
            log.warning("[Kafka] No partitions found for backlog check")
            return 0
        tps = [TopicPartition(KAFKA_TOPIC, p) for p in partitions]
        end_offsets = consumer.end_offsets(tps)

        cp_offsets = _read_checkpoint_offsets()

        backlog = 0
        for tp in tps:
            committed = cp_offsets.get(tp.partition, 0)
            backlog += end_offsets.get(tp, 0) - committed

        log.info(f"[Kafka] Calculated backlog: {backlog} messages")
        return backlog
    except Exception as e:
        log.error(f"[Kafka] Failed to compute backlog: {e}", exc_info=True)
        return 0
    finally:
        if consumer:
            consumer.close()
        if admin:
            admin.close()
