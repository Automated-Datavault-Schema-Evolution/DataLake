# config.py
import os

from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Configuration variables
LAKE_TYPE = os.getenv("LAKE_TYPE", "parquet")
DATA_DIRECTORY = os.getenv("DATA_DIRECTORY", "./data")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092").split(',')
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "csv_deltas")
PROCESSING_MODE = os.getenv("PROCESSING_MODE", "stream")

# PostgreSQL connection details (without JDBC)
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_DB = os.getenv("POSTGRES_DB", "datalake")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
