from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

from config import SPARK_MASTER


def get_spark_session(app_name="Kafka_Consumer_Lake_Handler"):
    spark_master = SPARK_MASTER
    builder = (
        SparkSession.builder
        .appName(app_name)
        .master(spark_master)
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2,org.apache.kafka:kafka-clients:3.3.2")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()
