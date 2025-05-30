from logger import log


def parse_message_to_row(message):
    msg = message.value
    if 'data' not in msg or not isinstance(msg['data'], list):
        log.warning('Message does not contain data')
        return None

    records = msg['data']
    for row in records:
        row['source_filename'] = msg.get('filename')
        row['source_data_format'] = msg.get('data_format')
        row['source_ingestion_timestamp'] = msg.get('ingestion_timestamp')

    return records
