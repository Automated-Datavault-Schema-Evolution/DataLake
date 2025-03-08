import glob

from kafka import KafkaConsumer
import json
from logger import log
from pyarrow import StructType
from pyspark.sql.functions import from_json, col, explode
from pyspark.sql.types import StructType, StructField, StringType, ArrayType


from config import KAFKA_TOPIC, KAFKA_BOOTSTRAP_SERVERS, DATA_DIRECTORY

from pyspark.sql import SparkSession

TOPIC_NAME = "csv_deltas"
BOOTSTRAP_SERVERS = ["localhost:9092"]


if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("KafkaConsumerToDataLake") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    spark.conf.set("spark.sql.debug.maxToStringFields", 1000)
    log.debug("Created new Spark session with Kafka support.")

    # create_topic_if_not_exists()

    log.debug(f"Reading from Kafka topic '{KAFKA_TOPIC}' with bootstrap servers {KAFKA_BOOTSTRAP_SERVERS}")
    kafka_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("includeHeaders", "true") \
        .load()
    #  .option("kafka.bootstrap.servers", ",".join(KAFKA_BOOTSTRAP_SERVERS)) \
    json_df = kafka_df.selectExpr("CAST(value AS STRING) as json_str")
    log.debug("Converted Kafka binary values to strings.")

    sample_files = ("../data/*.csv")
    if sample_files:
        sample_file = sample_files[0]
        sample_df = spark.read.option("header", "true").option("inferSchema", "true").csv(sample_file)
        record_schema = sample_df.schema
        log.info(f"Inferred record schema from sample file '{sample_file}': {record_schema}")
    else:
        log.error("No sample CSV file found in DATA_DIRECTORY for schema inference.")


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