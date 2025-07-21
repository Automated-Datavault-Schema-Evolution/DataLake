import os
import sys

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

from config import SPARK_MASTER


def get_spark_session(app_name="Kafka_Consumer_Lake_Handler"):
    spark_master = SPARK_MASTER
    builder = (
        SparkSession.builder
        .appName(app_name)
        .master(spark_master)
    )

    # Ensure Spark uses the same Python interpreter for driver and executors
    python_exec = sys.executable
    os.environ.setdefault("PYSPARK_PYTHON", python_exec)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", python_exec)
    builder = builder.config("spark.pyspark.python", python_exec) \
        .config("spark.pyspark.driver.python", python_exec) \
        .config("spark.sql.streaming.kafka.useUninterruptibleThread", "true") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    my_packages = [
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6",
        "org.apache.kafka:kafka-clients:3.5.1",
        "org.apache.spark:spark-token-provider-kafka-0-10_2.12:3.5.6"
    ]
    spark = configure_spark_with_delta_pip(builder, extra_packages=my_packages).getOrCreate()

    return spark
