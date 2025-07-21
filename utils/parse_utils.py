from logger import log


def _log_message_overview(message):
    """Log basic info about an incoming Kafka message."""
    try:
        offset = getattr(message, "offset", "n/a")
        partition = getattr(message, "partition", "n/a")
        log.debug(f"Parsing message at offset {offset} from partition {partition}")
        log.debug(f"Message keys: {list(message.value.keys())}")
    except Exception:
        # Don't let logging errors interrupt processing
        log.debug("Failed to log message overview", exc_info=True)


def parse_message_to_row(message):
    """Convert a Kafka message to a list of enriched row dictionaries."""
    _log_message_overview(message)
    msg = message.value
    if 'data' not in msg or not isinstance(msg['data'], list):
        log.warning('Message does not contain data')
        return None

    records = msg['data']
    log.debug(f"Message contains {len(records)} records")
    for row in records:
        row['source_filename'] = msg.get('filename')
        row['source_data_format'] = msg.get('data_format')
        row['source_ingestion_timestamp'] = msg.get('ingestion_timestamp')
    log.debug(f"Returning {len(records)} enriched records")
    return records
