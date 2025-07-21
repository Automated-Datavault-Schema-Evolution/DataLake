import time

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
    PROCESSING_MODE, KAFKA_STARTING_OFFSETS, KAFKA_GROUP_ID,
)
from utils.kafka_utils import get_kafka_consumer, sanity_check_kafka
from utils.lake_utils import write_to_delta
from utils.parse_utils import parse_message_to_row
from utils.spark_utiils import get_spark_session


def bulk_ingest(spark):
    log.info("Bulk fallback: draining any buffered records from Kafka…")
    # Use unified consumer group and let Kafka track offsets
    consumer = get_kafka_consumer()
    # consumer.subscribe([KAFKA_TOPIC])

    rows = []
    count = 0

    while True:
        batch = consumer.poll(timeout_ms=1000, max_records=1000)
        if not batch:
            break
        for tp, messages in batch.items():
            for msg in messages:
                rows.extend(parse_message_to_row(msg))
                count += 1

    if rows:
        df = spark.createDataFrame(rows)
        write_to_delta(df, DELTA_PATH, partition_by="source_filename")
        log.info(f"Bulk fallback wrote {count} rows to Δ-lake")
    else:
        log.info("Bulk fallback: no new records to drain.")

    consumer.close()


def streaming_ingest(spark):
    log.info("Start streaming ingestion")
    try:
        from pyspark.sql.functions import from_json, col, udf, explode
        from pyspark.sql.types import ArrayType, MapType

        schema = StructType([
            StructField("filename", StringType()),
            StructField("data_format", StringType()),
            StructField("ingestion_timestamp", StringType()),
            StructField("data", ArrayType(MapType(StringType(), StringType()))),
        ])

        df = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
            .option("subscribe", KAFKA_TOPIC)
            .option("startingOffsets", KAFKA_STARTING_OFFSETS)
            .option("kafka.group.id", KAFKA_GROUP_ID)
            .option("kafka.commit.groupOffsets", "true")
            .load()
        )
        log.debug(f"Connected to kafka server {KAFKA_BOOTSTRAP_SERVERS} and topic {KAFKA_TOPIC}")

        # .option("startingOffsets", "earliest") \
        # .option("kafka.group.id", "delta-streaming") \

        def enrich_rows(data, filename, data_format, ingestion_timestamp):
            if not isinstance(data, list):
                return []

            for row in data:
                row['source_filename'] = filename
                row['source_data_format'] = data_format
                row['source_ingestion_timestamp'] = ingestion_timestamp
            return data

        enrich_udf = udf(enrich_rows, ArrayType(MapType(StringType(), StringType())))

        parsed = df.selectExpr("CAST(value AS STRING) as json_value") \
            .select(from_json(col("json_value"), schema).alias("data")) \
            .select("data.*") \
            .withColumn(
            "rows",
            enrich_udf(col("data"), col("filename"), col("data_format"), col("ingestion_timestamp")),
        )

        def process_batch(batch_df, epoch_id):
            exploded = batch_df.select(
                col("filename"),
                col("data_format"),
                col("ingestion_timestamp"),
                explode(col("rows")).alias("row"),
            )
            sample_row = exploded.select("row").head()
            log.debug(sample_row)
            columns = list(sample_row["row"].keys()) if sample_row else []
            select_cols = [
                              col("filename"),
                              col("data_format"),
                              col("ingestion_timestamp"),
                          ] + [col("row")[k].alias(k) for k in columns]

            exploded_flat = exploded.select(*select_cols)
            if exploded_flat.count() > 0:
                write_to_delta(exploded_flat, DELTA_PATH, partition_by="source_filename")
                log.info(f"Streaming batch written, epoch {epoch_id}")

        (parsed.writeStream.trigger(processingTime="1 second")
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
