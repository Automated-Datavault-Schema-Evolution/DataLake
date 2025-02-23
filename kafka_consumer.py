import glob
import signal
import sys

import psycopg2
from logger import log
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, explode, col
from pyspark.sql.types import StructType, StructField, StringType, ArrayType

from config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC, LAKE_TYPE, DATA_DIRECTORY
from data_lake import table_exists, create_table_if_not_exists, get_postgres_connection

# Global variables to hold Spark session and streaming query
spark = None
query = None


def write_to_postgres(batch_df, batch_id):
    # Convert Spark DataFrame to Pandas DataFrame.
    pdf = batch_df.toPandas()
    if pdf.empty:
        log.info(f"Batch {batch_id} is empty. Skipping.")
        return
    # Assume that each batch has a "filename" column; use the first row's value as the target table name.
    table_name = pdf.iloc[0]['filename']
    # Drop the 'filename' column (we don't want to store it).
    pdf = pdf.drop(columns=['filename'])
    # Ensure insertion_timestamp exists.
    if "insertion_timestamp" not in pdf.columns:
        from datetime import datetime
        pdf["insertion_timestamp"] = datetime.now()
    # Create the table if it does not exist.
    if not table_exists(table_name):
        log.info(f"Table '{table_name}' does not exist. Creating table.")
        create_table_if_not_exists(table_name, pdf)
    else:
        log.info(f"Table '{table_name}' exists. Appending data.")

    # Dynamically build the INSERT statement.
    columns = list(pdf.columns)
    col_names = ", ".join([f'"{col}"' for col in columns])
    insert_sql = f"INSERT INTO {table_name} ({col_names}) VALUES %s"

    # Convert rows to tuples.
    data = [tuple(row) for row in pdf.values]
    try:
        conn = get_postgres_connection()
        cur = conn.cursor()
        psycopg2.extras.execute_values(cur, insert_sql, data, page_size=1000)
        conn.commit()
        cur.close()
        conn.close()
        log.info(f"Batch {batch_id} inserted into table '{table_name}'")
    except Exception as e:
        log.error(f"Error writing batch {batch_id} to PostgreSQL: {e}")


def process_kafka_stream():
    global spark, query
    # Stop any active Spark session.
    existing = SparkSession.getActiveSession()
    if existing is not None:
        existing.stop()
    # Create a new Spark session with Kafka package.
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("KafkaConsumerToDataLake") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.4") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark.conf.set("spark.sql.debug.maxToStringFields", 1000)

    # Read from Kafka as a streaming DataFrame.
    kafka_df = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", ",".join(KAFKA_BOOTSTRAP_SERVERS)) \
        .option("subscribe", KAFKA_TOPIC) \
        .load()
    # Convert binary value to string.
    json_df = kafka_df.selectExpr("CAST(value AS STRING) as json_str")

    # Infer record schema from a sample CSV in DATA_DIRECTORY.
    sample_files = glob.glob(DATA_DIRECTORY + "/*.csv")
    if sample_files:
        sample_df = spark.read.option("header", "true").option("inferSchema", "true").csv(sample_files[0])
        record_schema = sample_df.schema
        log.info(f"Inferred record schema from {sample_files[0]}: {record_schema}")
    else:
        log.error("No sample CSV file found in DATA_DIRECTORY for schema inference.")
        return

    # Define Kafka message schema: 'filename' and 'records' (array of records).
    message_schema = StructType([
        StructField("filename", StringType(), True),
        StructField("records", ArrayType(record_schema), True)
    ])
    parsed_df = json_df.select(from_json(col("json_str"), message_schema).alias("data"))
    exploded_df = parsed_df.select(col("data.filename").alias("filename"),
                                   explode(col("data.records")).alias("record"))
    final_df = exploded_df.select("filename", "record.*")

    if LAKE_TYPE == "rdbms":
        query = final_df.writeStream \
            .foreachBatch(write_to_postgres) \
            .outputMode("append") \
            .option("checkpointLocation", "/tmp/checkpoints_psycopg2") \
            .start()
    else:
        query = final_df.writeStream \
            .format("parquet") \
            .option("checkpointLocation", "/tmp/checkpoints_parquet") \
            .option("path", "./datalake/parquet/kafka_output") \
            .outputMode("append") \
            .start()

    # Define a shutdown handler to gracefully stop the query and Spark session.
    def shutdown_handler(signum, frame):
        log.info("Shutdown signal received. Stopping streaming query and Spark session...")
        global query, spark
        if query is not None:
            query.stop()
            log.info("Streaming query stopped.")
        if spark is not None:
            spark.stop()
            log.info("Spark session stopped.")
        sys.exit(0)

    # Register the shutdown handler for SIGTERM and SIGINT.
    signal.signal(signal.SIGTERM, shutdown_handler)
    signal.signal(signal.SIGINT, shutdown_handler)

    query.awaitTermination()


if __name__ == "__main__":
    process_kafka_stream()
