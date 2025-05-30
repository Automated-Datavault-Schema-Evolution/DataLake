from apscheduler.schedulers.background import BackgroundScheduler
from kafka import TopicPartition
from logger import log
from pyspark.sql.types import StructType, StructField, StringType

from config import BULK_OFFSET_FILE, KAFKA_TOPIC, DELTA_PATH, KAFKA_BOOTSTRAP_SERVERS, SCHEDULE_TYPE, SCHEDULE_CRON, \
    SCHEDULE_INTERVAL_HOURS, PROCESSING_MODE
from utils.kafka_utils import get_kafka_consumer
from utils.lake_utils import write_to_delta
from utils.offset_utils import read_last_offset, write_last_offset
from utils.parse_utils import parse_message_to_row
from utils.spark_utiils import get_spark_session


def bulk_ingest(spark):
    log.info("Start bulk ingestion")
    offset_file = BULK_OFFSET_FILE
    last_offsets = read_last_offset(offset_file)

    consumer = get_kafka_consumer()
    topic = KAFKA_TOPIC
    partition_ids = consumer.partitions_for_topic(topic)
    if not partition_ids:
        log.warning(f"No partitions found for topic '{topic}'")
        consumer.close()
        return

    partitions = [TopicPartition(topic, p) for p in partition_ids]
    consumer.assign(partitions)

    for tp in partitions:
        last_offset = last_offsets.get(tp.partition, None)
        if last_offset is not None:
            consumer.seek(tp, last_offset + 1)
        else:
            consumer.seek_to_beginning(tp)

    rows = []
    count = 0
    new_offsets = last_offsets.copy()

    while True:
        batch = consumer.poll(timeout_ms=1000, max_records=1000)
        if not batch:
            break
        for tp, messages in batch.items():
            for msg in messages:
                rows.extend(parse_message_to_row(msg))
                count += 1
                new_offsets[tp.partition] = msg.offset

    if rows:
        spark_df = spark.createDataFrame(rows)
        write_to_delta(spark_df, DELTA_PATH, partition_by="source_filename")
        log.info(f"Bulk ingest done. Rows: {count}")
        write_last_offset(offset_file, new_offsets)
    else:
        log.info("Bulk ingest: No new data to ingest.")

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
            StructField("data", ArrayType(MapType(StringType(), StringType())))
        ])

        df = spark.readStream.format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
            .option("subscribe", KAFKA_TOPIC) \
            .option("startingOffsets", "latest") \
            .load()

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
            .withColumn("rows",
                        enrich_udf(col("data"), col("filename"), col("data_format"), col("ingestion_timestamp")))

        def process_batch(batch_df, epoch_id):
            exploded = batch_df.select(
                col("filename"), col("data_format"), col("ingestion_timestamp"), explode(col("rows")).alias("row")
            )
            for c in exploded.select("row.*").columns:
                exploded = exploded.withColumn(c, col("row")[c])
            exploded = exploded.drop("row")
            if exploded.count() > 0:
                write_to_delta(exploded, DELTA_PATH, partition_by="source_filename")
                log.info(f"Streaming batch written, epoch {epoch_id}")

        parsed.writeStream.foreachBatch(process_batch) \
            .outputMode("update") \
            .option("checkpointLocation", "/tmp/delta/checkpoints/streaming/") \
            .start().awaitTermination()
    except Exception as e:
        log.critical(f"STREAMING FAILURE: {e} — switching to bulk ingestion", exc_info=True)
        bulk_ingest(spark)


def schedule_bulk(spark):
    scheduler = BackgroundScheduler()
    if SCHEDULE_TYPE == "cron":
        cron_str = SCHEDULE_CRON
        from apscheduler.triggers.cron import CronTrigger
        scheduler.add_job(lambda: bulk_ingest(spark), CronTrigger.from_crontab(cron_str))
        log.info(f"Scheduled bulk ingestion with CRON: {cron_str}")
    else:
        hours = SCHEDULE_INTERVAL_HOURS
        scheduler.add_job(lambda: bulk_ingest(spark), 'interval', hours=hours)
        log.info(f"Scheduled bulk ingestion every {hours} hours")
    scheduler.start()


def main():
    log.info("Starting Delta Lake Handler.")

    spark = get_spark_session()

    mode = PROCESSING_MODE
    if mode == 'streaming':
        schedule_bulk(spark)  # always schedule fallback bulk
        streaming_ingest(spark)
    elif mode == 'bulk':
        schedule_bulk(spark)
        while True:
            pass  # APScheduler will keep the app alive and running bulk jobs
    else:
        log.critical(f"Invalid MODE '{mode}' in .env. Use 'streaming' or 'bulk'.")


if __name__ == "__main__":
    main()
