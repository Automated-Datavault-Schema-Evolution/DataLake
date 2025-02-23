import glob
import logging
import signal
import sys

import psycopg2
import psycopg2.extras
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, explode, col
from pyspark.sql.types import StructType, StructField, StringType, ArrayType

from config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC, LAKE_TYPE, DATA_DIRECTORY
from data_lake import table_exists, create_table_if_not_exists, get_postgres_connection

logger = logging.getLogger(__name__)

spark = None
query = None


def write_to_postgres(batch_df, batch_id):
    pdf = batch_df.toPandas()
    logger.debug(f"Batch {batch_id}: Converted Spark DataFrame to Pandas with shape {pdf.shape}")
    if pdf.empty:
        logger.info(f"Batch {batch_id} is empty. Skipping.")
        return
    table_name = pdf.iloc[0]['filename']
    logger.debug(f"Batch {batch_id}: Using target table '{table_name}'.")
    pdf = pdf.drop(columns=['filename'])
    logger.debug(f"Batch {batch_id}: DataFrame shape after dropping 'filename': {pdf.shape}")
    if "insertion_timestamp" not in pdf.columns:
        from datetime import datetime
        pdf["insertion_timestamp"] = datetime.now()
        logger.debug(f"Batch {batch_id}: Added insertion_timestamp column.")
    if not table_exists(table_name):
        logger.info(f"Table '{table_name}' does not exist. Creating table.")
        create_table_if_not_exists(table_name, pdf)
    else:
        logger.info(f"Table '{table_name}' exists. Appending data.")
    columns = list(pdf.columns)
    col_names = ", ".join([f'"{col}"' for col in columns])
    insert_sql = f"INSERT INTO {table_name} ({col_names}) VALUES %s"
    logger.debug(f"Batch {batch_id}: INSERT SQL: {insert_sql}")
    data = [tuple(row) for row in pdf.values]
    logger.debug(f"Batch {batch_id}: Prepared {len(data)} rows for insertion.")
    try:
        conn = get_postgres_connection()
        cur = conn.cursor()
        psycopg2.extras.execute_values(cur, insert_sql, data, page_size=1000)
        conn.commit()
        cur.close()
        conn.close()
        logger.info(f"Batch {batch_id} inserted into table '{table_name}'")
    except Exception as e:
        logger.error(f"Error writing batch {batch_id} to PostgreSQL: {e}")


def process_kafka_stream():
    global spark, query
    existing = SparkSession.getActiveSession()
    if existing is not None:
        logger.debug("Stopping existing Spark session.")
        existing.stop()
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("KafkaConsumerToDataLake") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.4") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark.conf.set("spark.sql.debug.maxToStringFields", 1000)
    logger.debug("Created new Spark session with Kafka support.")

    logger.debug(f"Reading from Kafka topic '{KAFKA_TOPIC}' with bootstrap servers {KAFKA_BOOTSTRAP_SERVERS}")
    kafka_df = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", ",".join(KAFKA_BOOTSTRAP_SERVERS)) \
        .option("subscribe", KAFKA_TOPIC) \
        .load()
    json_df = kafka_df.selectExpr("CAST(value AS STRING) as json_str")
    logger.debug("Converted Kafka binary values to strings.")

    sample_files = glob.glob(DATA_DIRECTORY + "/*.csv")
    if sample_files:
        sample_file = sample_files[0]
        sample_df = spark.read.option("header", "true").option("inferSchema", "true").csv(sample_file)
        record_schema = sample_df.schema
        logger.info(f"Inferred record schema from sample file '{sample_file}': {record_schema}")
    else:
        logger.error("No sample CSV file found in DATA_DIRECTORY for schema inference.")
        return

    message_schema = StructType([
        StructField("filename", StringType(), True),
        StructField("records", ArrayType(record_schema), True)
    ])
    parsed_df = json_df.select(from_json(col("json_str"), message_schema).alias("data"))
    logger.debug("Parsed Kafka messages using inferred schema.")
    exploded_df = parsed_df.select(
        col("data.filename").alias("filename"),
        explode(col("data.records")).alias("record")
    )
    final_df = exploded_df.select("filename", "record.*")
    logger.debug("Exploded records; final DataFrame schema:")
    final_df.printSchema()

    if LAKE_TYPE == "rdbms":
        logger.info("Writing streaming data to PostgreSQL using psycopg2.")
        query = final_df.writeStream \
            .foreachBatch(write_to_postgres) \
            .outputMode("append") \
            .option("checkpointLocation", "/tmp/checkpoints_psycopg2") \
            .start()
    else:
        logger.info("Writing streaming data to Parquet files.")
        query = final_df.writeStream \
            .format("parquet") \
            .option("checkpointLocation", "/tmp/checkpoints_parquet") \
            .option("path", "./datalake/parquet/kafka_output") \
            .outputMode("append") \
            .start()

    def shutdown_handler(signum, frame):
        logger.info("Shutdown signal received. Stopping streaming query and Spark session...")
        global query, spark
        if query is not None:
            query.stop()
            logger.debug("Streaming query stopped.")
        if spark is not None:
            spark.stop()
            logger.debug("Spark session stopped.")
        sys.exit(0)

    signal.signal(signal.SIGTERM, shutdown_handler)
    signal.signal(signal.SIGINT, shutdown_handler)
    logger.info("Kafka consumer is now awaiting termination.")
    query.awaitTermination()


if __name__ == "__main__":
    process_kafka_stream()
