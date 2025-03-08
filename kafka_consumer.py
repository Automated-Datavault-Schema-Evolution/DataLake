import glob
import signal
import sys
import threading

from logger import log
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, explode, col
from pyspark.sql.types import StructType, StructField, StringType, ArrayType

from config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC, LAKE_TYPE, DATA_DIRECTORY
from data_lake import bulk_insert_dataframe


def write_to_postgres(batch_df, batch_id):
    """
    Process a batch from the Kafka stream by converting to Pandas and bulk inserting into PostgreSQL.
    Expects that the first record in the batch contains a 'filename' field to derive the table name.
    """
    try:
        pdf = batch_df.toPandas()
        log.debug(f"Batch {batch_id}: Converted Spark DataFrame to Pandas with shape {pdf.shape}")
        if pdf.empty:
            log.info(f"Batch {batch_id} is empty. Skipping insertion.")
            return

        # Derive table name from the first record and drop the 'filename' column for insertion
        table_name = pdf.iloc[0]['filename']
        log.debug(f"Batch {batch_id}: Target table derived as '{table_name}'.")
        bulk_insert_dataframe(pdf, table_name, drop_columns=['filename'], context=f"Batch {batch_id}: ")
    except Exception as e:
        log.error(f"Error writing batch {batch_id} to PostgreSQL: {e}")


def create_topic_if_not_exists():
    """
    Create the Kafka topic if it does not already exist.
    """
    from confluent_kafka.admin import AdminClient, NewTopic

    admin_client = AdminClient({'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS})
    # admin_client = AdminClient({'bootstrap.servers': '0.0.0.0:9092' })
    metadata = admin_client.list_topics(timeout=10)
    if KAFKA_TOPIC in metadata.topics:
        log.debug(f"Topic '{KAFKA_TOPIC}' already exists.")
        return True
    else:
        new_topic = NewTopic(KAFKA_TOPIC, num_partitions=3, replication_factor=1)
        fs = admin_client.create_topics([new_topic])
        for topic_name, future in fs.items():
            try:
                future.result(timeout=10)
                log.debug(f"Topic '{topic_name}' created successfully.")
            except Exception as e:
                log.error(f"Failed to create topic '{topic_name}': {e}")
                return False
        return True


def process_kafka_stream():
    """
    Initialize a Spark session to read streaming data from Kafka,
    parse the JSON messages, and write the resulting DataFrame either to PostgreSQL or to Parquet.
    """
    spark = SparkSession.builder \
        .appName("KafkaConsumerToDataLake") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.4") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    spark.conf.set("spark.sql.debug.maxToStringFields", 1000)
    log.debug("Created new Spark session with Kafka support.")

    # create_topic_if_not_exists()

    log.debug(f"Reading from Kafka topic '{KAFKA_TOPIC}' with bootstrap servers {KAFKA_BOOTSTRAP_SERVERS}")
    kafka_df = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", ",".join(KAFKA_BOOTSTRAP_SERVERS)) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "latest") \
        .load()
    json_df = kafka_df.selectExpr("CAST(value AS STRING) as json_str")
    log.debug("Converted Kafka binary values to strings.")

    # Infer schema from a sample CSV file
    sample_files = glob.glob(DATA_DIRECTORY + "/*.csv")
    if sample_files:
        sample_file = sample_files[0]
        sample_df = spark.read.option("header", "true").option("inferSchema", "true").csv(sample_file)
        record_schema = sample_df.schema
        log.info(f"Inferred record schema from sample file '{sample_file}': {record_schema}")
    else:
        log.error("No sample CSV file found in DATA_DIRECTORY for schema inference.")
        return

    message_schema = StructType([
        StructField("filename", StringType(), True),
        StructField("records", ArrayType(record_schema), True)
    ])
    parsed_df = json_df.select(from_json(col("json_str"), message_schema).alias("data"))
    log.debug("Parsed Kafka messages using inferred schema.")
    exploded_df = parsed_df.select(
        col("data.filename").alias("filename"),
        explode(col("data.records")).alias("record")
    )
    final_df = exploded_df.select("filename", "record.*")
    log.debug("Exploded records; final DataFrame schema:")
    final_df.printSchema()

    if LAKE_TYPE == "rdbms":
        log.info("Writing streaming data to PostgreSQL using psycopg2.")
        query = final_df.writeStream \
            .foreachBatch(write_to_postgres) \
            .outputMode("append") \
            .option("checkpointLocation", "/tmp/checkpoints_psycopg2") \
            .start()
    else:
        log.info("Writing streaming data to Parquet files.")
        query = final_df.writeStream \
            .format("parquet") \
            .option("checkpointLocation", "/tmp/checkpoints_parquet") \
            .option("path", "./datalake/parquet/kafka_output") \
            .outputMode("append") \
            .start()

    def shutdown_handler(signum, frame):
        log.info("Shutdown signal received. Stopping streaming query and Spark session...")
        global query, spark
        if query is not None:
            query.stop()
            log.debug("Streaming query stopped.")
        if spark is not None:
            spark.stop()
            log.debug("Spark session stopped.")
        sys.exit(0)

    if threading.current_thread() == threading.main_thread():
        signal.signal(signal.SIGTERM, shutdown_handler)
        signal.signal(signal.SIGINT, shutdown_handler)
        log.info("Kafka consumer is now awaiting termination.")
        query.awaitTermination()


if __name__ == "__main__":
    process_kafka_stream()
