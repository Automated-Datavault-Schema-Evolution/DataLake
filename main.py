import time

import pandas as pd
from apscheduler.schedulers.background import BackgroundScheduler
from logger import log
from pyspark.sql.types import StructType, StructField, StringType

from config import (
    KAFKA_TOPIC,
    DELTA_PATH,
    KAFKA_BOOTSTRAP_SERVERS,
    SCHEDULE_TYPE,
    SCHEDULE_CRON,
    SCHEDULE_INTERVAL_HOURS,
    PROCESSING_MODE,
    KAFKA_STARTING_OFFSETS,
    KAFKA_GROUP_ID,
    KAFKA_BACKLOG_THRESHOLD,
    BACKLOG_BATCH_SIZE,
)
from utils.kafka_utils import get_kafka_consumer, sanity_check_kafka, get_topic_backlog, commit_consumer_offsets
from utils.lake_utils import write_to_delta
from utils.parse_utils import parse_message_to_row
from utils.spark_utiils import get_spark_session
from utils.spark_work_autoscaler import check_and_scale_workers


def process_batch(batch_df, epoch_id):
    """Write each micro-batch of the streaming query to Delta Lake."""
    from pyspark.sql.functions import col

    sample_row = batch_df.select("row").head()
    log.debug(sample_row)
    columns = list(sample_row["row"].keys()) if sample_row else []
    select_cols = [
                      col("filename"),
                      col("data_format"),
                      col("ingestion_timestamp"),
                  ] + [col("row")[k].alias(k) for k in columns]

    exploded_flat = batch_df.select(*select_cols)
    if exploded_flat.head(1):
        write_to_delta(exploded_flat, DELTA_PATH)
        log.info(f"Streaming batch written, epoch {epoch_id}")


def bulk_ingest(spark, max_messages=None):
    log.info("Bulk fallback: draining any buffered records from Kafka…")
    # Use unified consumer group and let Kafka track offsets
    consumer = get_kafka_consumer()

    rows = []
    count = 0
    empty_polls = 0

    while empty_polls < 3 and (max_messages is None or count < max_messages):
        max_records = 1000
        if max_messages is not None:
            remaining = max_messages - count
            max_records = min(max_records, remaining)
        batch = consumer.poll(timeout_ms=1000, max_records=max_records)
        if not batch:
            empty_polls += 1
            continue
        empty_polls = 0
        for tp, messages in batch.items():
            log.debug(f"Polled {len(messages)} messages from partition {tp.partition}")
            for msg in messages:
                parsed_rows = parse_message_to_row(msg) or []
                for row in parsed_rows:
                    # keep filename and format only for routing, not for storage
                    row['filename'] = msg.value.get('filename')
                    row['data_format'] = msg.value.get('data_format')
                rows.extend(parsed_rows)
                count += len(parsed_rows)

    if rows:
        pdf = pd.DataFrame(rows)
        pdf = pdf.where(pd.notnull(pdf), None)
        pdf = pdf.astype(str)
        schema = StructType([StructField(col, StringType(), True) for col in pdf.columns])
        df = spark.createDataFrame(pdf, schema=schema)
        write_to_delta(df, DELTA_PATH)
        log.info(f"Bulk fallback wrote {count} rows to delta-lake")
    else:
        log.info("Bulk fallback: no new records to drain.")

    # commit offsets so backlog calculation reflects drained records
    try:
        consumer.commit()
        log.debug("Bulk fallback committed offsets")
    except Exception as e:
        log.error(f"Failed to commit offsets after bulk ingestion: {e}")
    # Log updated backlog after draining
    backlog = get_topic_backlog()
    log.info(f"Backlog after bulk ingestion: {backlog} messages")

    consumer.close()
    log.debug("Bulk fallback Kafka consumer closed")


def streaming_ingest(spark):
    log.info("Start streaming ingestion")
    log.debug(f"Kafka topic={KAFKA_TOPIC}, bootstrap={KAFKA_BOOTSTRAP_SERVERS}, group-id={KAFKA_GROUP_ID}")
    try:
        from pyspark.sql.functions import from_json, col, udf, explode, expr
        from pyspark.sql.types import ArrayType, MapType

        schema = StructType([
            StructField("filename", StringType()),
            StructField("data_format", StringType()),
            StructField("ingestion_timestamp", StringType()),
            StructField("data", ArrayType(MapType(StringType(), StringType()))),
        ])
        log.debug(f"Streaming schema: {schema.simpleString()}")

        df = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
            .option("subscribe", KAFKA_TOPIC)
            .option("startingOffsets", KAFKA_STARTING_OFFSETS)
            # .option("kafka.group.id", KAFKA_GROUP_ID)
            # .option("kafka.commit.groupOffsets", "true")
            .load()
        )
        log.debug(f"Connected to kafka server {KAFKA_BOOTSTRAP_SERVERS} and topic {KAFKA_TOPIC}")

        parsed = (
            df.selectExpr("CAST(value AS STRING) as json_value")
            .select(from_json(col("json_value"), schema).alias("data"))
            .select("data.*")
        )

        parsed_with_ts = parsed.select(
            "filename",
            "data_format",
            "ingestion_timestamp",
            expr(
                "transform(data, x -> map_concat(x, map('source_ingestion_timestamp', ingestion_timestamp)))"
            ).alias("rows"),
        )

        flattened = parsed_with_ts.select(
            "filename",
            "data_format",
            "ingestion_timestamp",
            explode(col("rows")).alias("row"),
        )

        (flattened.writeStream.trigger(processingTime="1 second")
         .foreachBatch(process_batch) \
         .outputMode("append") \
         .option("checkpointLocation", "/tmp/delta/checkpoints/streaming/") \
         .start() \
         .awaitTermination())

    except Exception as e:
        log.critical(f"STREAMING FAILURE: {e} — switching to bulk ingestion", exc_info=True)
        bulk_ingest(spark)


def schedule_bulk(spark):
    scheduler = BackgroundScheduler()
    if SCHEDULE_TYPE == "cron":
        from apscheduler.triggers.cron import CronTrigger

        scheduler.add_job(
            lambda: bulk_ingest(spark), CronTrigger.from_crontab(SCHEDULE_CRON)
        )
        log.info(f"Scheduled bulk ingestion with CRON: {SCHEDULE_CRON}")
    else:
        scheduler.add_job(
            lambda: bulk_ingest(spark), 'interval', hours=SCHEDULE_INTERVAL_HOURS
        )
        log.info(f"Scheduled bulk ingestion every {SCHEDULE_INTERVAL_HOURS} hours")
    scheduler.start()


def main():
    log.info("Starting Delta Lake Handler.")

    sanity_check_kafka()
    spark = get_spark_session()

    if PROCESSING_MODE == 'streaming':
        while True:
            try:
                backlog = get_topic_backlog()
                check_and_scale_workers()
                # if backlog > 0 and CONSUME_FULL_BACKLOG: # always consume the backlog
                if backlog > 0:  #
                    log.info(
                        # f"Auto consuming backlog of {backlog} messages as CONSUME_FULL_BACKLOG is enabled"
                        f"Auto consuming backlog of {backlog} messages"
                    )
                    while backlog > 0:
                        to_drain = min(backlog, BACKLOG_BATCH_SIZE)
                        log.info(f"Draining {to_drain} messages from backlog (auto mode)")
                        bulk_ingest(spark, max_messages=to_drain)
                        backlog = get_topic_backlog()
                        if backlog > 0:
                            log.info(f"{backlog} messages remain in backlog")
                    log.info("Re-checking backlog in 5s…")
                    time.sleep(5)
                    continue
                if backlog > KAFKA_BACKLOG_THRESHOLD:
                    log.warning(
                        f"Kafka backlog {backlog} exceeds threshold {KAFKA_BACKLOG_THRESHOLD}. Using bulk ingestion"
                    )
                    while backlog > 0:
                        to_drain = min(backlog, BACKLOG_BATCH_SIZE)
                        log.info(f"Draining {to_drain} messages from backlog")
                        bulk_ingest(spark, max_messages=to_drain)
                        backlog = get_topic_backlog()
                        if backlog > 0:
                            log.info(f"{backlog} messages remain in backlog")
                    log.info("Re-checking backlog in 5s…")
                    time.sleep(5)
                    continue
                streaming_ingest(spark)
            except Exception:
                log.error(
                    "STREAMING FAILED – falling back to bulk drain", exc_info=True
                )
                bulk_ingest(spark)
                log.info("Re-starting streaming ingestion in 30s…")
                time.sleep(30)
    elif PROCESSING_MODE == 'bulk':
        schedule_bulk(spark)
        # keep process alive for scheduled jobs
        while True:
            time.sleep(60)
    else:
        log.critical(
            f"Invalid MODE '{PROCESSING_MODE}' in .env. Use 'streaming' or 'bulk'."
        )


if __name__ == "__main__":
    main()
