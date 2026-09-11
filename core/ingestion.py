from __future__ import annotations
from logger import log, log_step
from pyspark.sql.functions import col, explode_outer, from_json
from pyspark.sql.types import ArrayType, MapType, StringType, StructField, StructType
from config import BACKLOG_BATCH_SIZE, CHECKPOINT_LOCATION, DELTA_PATH, LAKE_TYPE, KAFKA_BOOTSTRAP_SERVERS, KAFKA_GROUP_ID, KAFKA_STARTING_OFFSETS, KAFKA_TOPIC
from core.batch_processing import process_batch
from domain.row_batches import materialized_columns, normalize_rows
from domain.table_naming import delta_target_path, logical_table_name_from_filename
from helper.delta_helper import write_to_delta
from helper.kafka_helper import get_kafka_consumer, get_topic_backlog
from helper.spark_helper import build_typed_df_from_rows, normalize_date_columns_dynamic, normalize_ingestion_timestamp
from utils.parse_utils import parse_message_to_row

def bulk_ingest(spark, max_messages=None):
    with log_step('Bulk fallback: draining any buffered records from Kafka'):
        try:
            consumer = get_kafka_consumer()
            log.info(f'Kafka consumer created with servers: {KAFKA_BOOTSTRAP_SERVERS}')
        except Exception as exc:
            log.error(f'Failed to create Kafka consumer: {exc}')
            return
        total_rows_written = 0
        empty_polls = 0
        try:
            while empty_polls < 3 and (max_messages is None or total_rows_written < max_messages):
                max_per_poll = BACKLOG_BATCH_SIZE if max_messages is None else min(BACKLOG_BATCH_SIZE, max_messages - total_rows_written)
                log.debug(f'Polling Kafka (max_records={max_per_poll})...')
                try:
                    batch = consumer.poll(timeout_ms=1000, max_records=max_per_poll)
                except Exception as exc:
                    log.error(f'Error polling Kafka: {exc}')
                    break
                if not batch:
                    empty_polls += 1
                    log.debug(f'No messages polled. Empty polls so far: {empty_polls}')
                    continue
                empty_polls = 0
                rows_by_table: dict[str, list[dict]] = {}
                for topic_partition, messages in batch.items():
                    log.debug(f'Polled {len(messages)} messages from partition {topic_partition.partition}')
                    for message in messages:
                        parsed_rows = parse_message_to_row(message) or []
                        payload = message.value or {}
                        filename = payload.get('filename')
                        table_name = logical_table_name_from_filename(filename)
                        enriched_rows = [{**row, 'filename': filename, 'data_format': payload.get('data_format')} for row in parsed_rows]
                        rows_by_table.setdefault(table_name, []).extend(enriched_rows)
                if not rows_by_table:
                    continue
                poll_rows_written = 0
                for table_name, table_rows in rows_by_table.items():
                    columns = materialized_columns(table_rows)
                    if not columns:
                        log.info(f'[DLH][{table_name}] No materialized columns in this poll; skipping.')
                        continue
                    df_table = build_typed_df_from_rows(spark, normalize_rows(table_rows, columns))
                    df_table = normalize_ingestion_timestamp(df_table)
                    df_table = normalize_date_columns_dynamic(df_table)
                    from pyspark.sql import functions as F
                    projections = [F.col(column_name) if dtype in ('date', 'timestamp') or column_name == 'ingestion_timestamp' else F.col(column_name).cast('string').alias(column_name) for column_name, dtype in df_table.dtypes]
                    df_table = df_table.select(*projections)
                    df_table = df_table.coalesce(max(1, min(8, df_table.rdd.getNumPartitions())))
                    target_path = delta_target_path(DELTA_PATH, table_name, LAKE_TYPE) if DELTA_PATH else table_name
                    row_count = df_table.count()
                    log.info(f'[DLH][{table_name}] Writing {row_count} rows to target {target_path}')
                    write_to_delta(df_table, DELTA_PATH.rstrip('/'), table_name=table_name)
                    poll_rows_written += row_count
                    total_rows_written += row_count
                log.info(f'Bulk fallback wrote {poll_rows_written} rows across {len(rows_by_table)} table(s) in this poll')
        finally:
            try:
                consumer.commit()
                log.debug('Bulk fallback committed offsets')
            except Exception as exc:
                log.error(f'Failed to commit offsets after bulk ingestion: {exc}')
            try:
                backlog = get_topic_backlog()
                log.info(f'Backlog after bulk ingestion: {backlog} messages')
            except Exception as exc:
                log.error(f'Failed to get topic backlog: {exc}')
            try:
                consumer.close()
                log.debug('Bulk fallback Kafka consumer closed')
            except Exception:
                pass
        log.info(f'Bulk fallback wrote {total_rows_written} rows to delta-lake')

def streaming_ingest(spark):
    with log_step('Start streaming ingestion'):
        log.debug(f'Kafka topic={KAFKA_TOPIC}, bootstrap={KAFKA_BOOTSTRAP_SERVERS}, group-id={KAFKA_GROUP_ID}')
        try:
            schema = StructType([StructField('filename', StringType()), StructField('data_format', StringType()), StructField('ingestion_timestamp', StringType()), StructField('data', ArrayType(MapType(StringType(), StringType())))])
            log.debug(f'Streaming schema: {schema.simpleString()}')
            df = spark.readStream.format('kafka').option('kafka.bootstrap.servers', KAFKA_BOOTSTRAP_SERVERS).option('subscribe', KAFKA_TOPIC).option('startingOffsets', KAFKA_STARTING_OFFSETS).option('kafka.group.id', KAFKA_GROUP_ID).option('maxOffsetsPerTrigger', '5000').option('failOnDataLoss', 'false').load()
            parsed = df.selectExpr('partition', 'offset', 'CAST(value AS STRING) AS json_value').select('partition', 'offset', from_json(col('json_value'), schema).alias('msg')).select(col('partition'), col('offset'), col('msg.filename').alias('filename'), col('msg.data_format').alias('data_format'), col('msg.ingestion_timestamp').alias('ingestion_timestamp'), col('msg.data').alias('rows'))
            flattened = parsed.select('partition', 'offset', 'filename', 'data_format', 'ingestion_timestamp', explode_outer(col('rows')).alias('row'))
            return flattened.writeStream.trigger(processingTime='1 second').foreachBatch(process_batch).outputMode('append').option('checkpointLocation', f'{CHECKPOINT_LOCATION}/by_table').start()
        except Exception as exc:
            log.critical(f'STREAMING FAILURE: {exc} — switching to bulk ingestion', exc_info=True)
            bulk_ingest(spark)
            raise
__all__ = ['bulk_ingest', 'streaming_ingest']
