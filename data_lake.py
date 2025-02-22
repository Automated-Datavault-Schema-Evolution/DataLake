# data_lake.py
import os
from logger import log
from pyspark.sql import SparkSession
from config import LAKE_TYPE, SQLALCHEMY_DATABASE_URI

# Initialize Spark session
spark = SparkSession.builder.appName("DataLakeIngestion").getOrCreate()

def store_to_parquet(dest_path, df):
    try:
        df.write.mode("overwrite").parquet(dest_path)
        log.info(f"Saved data to Parquet: {dest_path}")
    except Exception as e:
        log.error(f"Error saving Parquet file {dest_path}: {e}")

def store_to_rdbms(table_name, df):
    try:
        # For PostgreSQL, set connection properties
        connection_properties = {
            "driver": "org.postgresql.Driver"
        }
        # Write to PostgreSQL using Spark JDBC
        df.write.jdbc(url=SQLALCHEMY_DATABASE_URI, table=table_name, mode="overwrite", properties=connection_properties)
        log.info(f"Saved data to table '{table_name}' in PostgreSQL")
    except Exception as e:
        log.error(f"Error saving to PostgreSQL: {e}")

def create_data_lake_entry(file_name):
    """
    Read the CSV file using Spark and save its contents into the data lake.
    """
    try:
        df = spark.read.option("header", "true").option("inferSchema", "true").csv(file_name)
        log.info(f"Read CSV file: {file_name}")
    except Exception as e:
        log.error(f"Error reading CSV file {file_name}: {e}")
        return

    if LAKE_TYPE == "parquet":
        dest_dir = "./datalake/parquet"
        os.makedirs(dest_dir, exist_ok=True)
        log.debug(f"Created directory {dest_dir}.")
        dest_path = os.path.join(dest_dir, os.path.basename(file_name).replace(".csv", ".parquet"))
        store_to_parquet(dest_path, df)
    elif LAKE_TYPE == "rdbms":
        table_name = os.path.basename(file_name).replace(".csv", "")

        store_to_rdbms(table_name, df)
    else:
        log.error("Invalid LAKE_TYPE specified in config.")
