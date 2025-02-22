# config.py
import os
from dotenv import load_dotenv
from logger import log

# Load environment variables from .env file
load_dotenv()

# Configuration variables
LAKE_TYPE = os.getenv("LAKE_TYPE", "parquet")
# Note: for Spark JDBC, we use a JDBC connection string:
SQLALCHEMY_DATABASE_URI = os.getenv("SQLALCHEMY_DATABASE_URI", "jdbc:postgresql://postgres:5432/datalake?user=postgres&password=postgres")
DATA_DIRECTORY = os.getenv("DATA_DIRECTORY", "./data")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092").split(',')
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "csv_deltas")
PROCESSING_MODE = os.getenv("PROCESSING_MODE", "stream")
