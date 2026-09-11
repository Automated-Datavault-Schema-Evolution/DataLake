import time
from logger import log, log_step
from config import PROCESSING_MODE, LAKE_TYPE, BACKLOG_BATCH_SIZE
from core.ingestion import bulk_ingest, streaming_ingest
from helper.kafka_helper import sanity_check_kafka, get_topic_backlog
from helper.postgres_helper import ensure_postgres_ready
from helper.scheduler_helper import schedule_bulk
from helper.spark_helper import get_spark_session
from utils.spark_work_autoscaler import check_and_scale_workers

def run():
    with log_step('Starting Delta Lake Handler'):
        if LAKE_TYPE == 'rdbms':
            ensure_postgres_ready()
        spark = get_spark_session()
        sanity_check_kafka()
        if PROCESSING_MODE == 'streaming':
            while True:
                backlog = get_topic_backlog()
                check_and_scale_workers()
                if backlog > 0:
                    log.info(f'Auto consuming backlog of {backlog} messages')
                    while backlog > 0:
                        to_drain = min(backlog, BACKLOG_BATCH_SIZE)
                        log.info(f'Draining {to_drain} messages from backlog (auto mode)')
                        bulk_ingest(spark, max_messages=to_drain)
                        backlog = get_topic_backlog()
                        if backlog > 0:
                            log.info(f'{backlog} messages remain in backlog')
                    log.info('Re-checking backlog in 5s…')
                    time.sleep(5)
                    continue
                else:
                    break
            while True:
                try:
                    query = streaming_ingest(spark)
                    log.info('Streaming ingestion started. Awaiting termination...')
                    query.awaitTermination()
                except Exception:
                    log.error('STREAMING FAILED – falling back to bulk drain', exc_info=True)
                    bulk_ingest(spark)
                    log.info('Re-starting streaming ingestion in 30s…')
                    time.sleep(30)
        elif PROCESSING_MODE == 'bulk':
            schedule_bulk(lambda: bulk_ingest(spark))
            while True:
                time.sleep(60)
        else:
            log.critical(f"Invalid MODE '{PROCESSING_MODE}' in .env. Use 'streaming' or 'bulk'.")
__all__ = ['run']
