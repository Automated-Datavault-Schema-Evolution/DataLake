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
    my_packages = [
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6",
        "org.apache.kafka:kafka-clients:3.5.1",
        "org.apache.spark:spark-token-provider-kafka-0-10_2.12:3.5.6"
    ]
    spark = configure_spark_with_delta_pip(builder, extra_packages=my_packages).getOrCreate()
    print(spark.sparkContext.getConf().getAll())

    return spark
