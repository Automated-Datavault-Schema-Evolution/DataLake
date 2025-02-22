import glob
import signal
import sys

from logger import log
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, explode, col
from pyspark.sql.types import StructType, StructField, StringType, ArrayType

from config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC, LAKE_TYPE, SQLALCHEMY_DATABASE_URI, DATA_DIRECTORY

# Global variables to hold Spark session and streaming query
spark = None
query = None


def process_kafka_stream():
    global spark, query

    # Ensure no active session exists.
    existing = SparkSession.getActiveSession()
    if existing is not None:
        existing.stop()
    # Create a new Spark session with the required packages.
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("KafkaConsumerToDataLake") \
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2,org.postgresql:postgresql:42.5.0") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # Read from Kafka as a streaming DataFrame.
    kafka_df = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", ",".join(KAFKA_BOOTSTRAP_SERVERS)) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "latest") \
        .load()

    # Convert binary Kafka 'value' column to string.
    json_df = kafka_df.selectExpr("CAST(value AS STRING) as json_str")

    # Automatically infer record schema from a sample CSV file in DATA_DIRECTORY.
    sample_files = glob.glob(DATA_DIRECTORY + "/*.csv")
    if sample_files:
        sample_df = spark.read.option("header", "true").option("inferSchema", "true").csv(sample_files[0])
        record_schema = sample_df.schema
        log.info(f"Inferred record schema from {sample_files[0]}: {record_schema}")
    else:
        log.error("No sample CSV file found in DATA_DIRECTORY for schema inference.")
        return

    # Define the schema for the Kafka message:
    #   - 'filename': string (table name, derived from the CSV filename)
    #   - 'records': an array of records following the inferred schema.
    message_schema = StructType([
        StructField("filename", StringType(), True),
        StructField("records", ArrayType(record_schema), True)
    ])

    # Parse the JSON string using the defined schema.
    parsed_df = json_df.select(from_json(col("json_str"), message_schema).alias("data"))
    # Flatten: extract the filename and explode the records array.
    exploded_df = parsed_df.select(col("data.filename").alias("filename"),
                                   explode(col("data.records")).alias("record"))
    final_df = exploded_df.select("filename", "record.*")

    if LAKE_TYPE == "rdbms":
        # Write each microbatch to PostgreSQL via JDBC.
        def write_to_jdbc(batch_df, batch_id):
            if batch_df.count() > 0:
                # Get table name from the 'filename' field (assuming all rows have the same filename).
                first_row = batch_df.select("filename").first()
                table_name = first_row["filename"]
                # Drop the filename column before writing.
                data_df = batch_df.drop("filename")
                data_df.write \
                    .format("jdbc") \
                    .option("url", SQLALCHEMY_DATABASE_URI) \
                    .option("dbtable", table_name) \
                    .option("driver", "org.postgresql.Driver") \
                    .mode("append") \
                    .save()
                log.info(f"Batch {batch_id} written to PostgreSQL table '{table_name}'.")

        query = final_df.writeStream \
            .foreachBatch(write_to_jdbc) \
            .outputMode("append") \
            .option("checkpointLocation", "/tmp/checkpoints_jdbc") \
            .start()
    else:
        # Write the stream as Parquet files.
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
