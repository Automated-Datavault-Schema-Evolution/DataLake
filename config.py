import os

from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv(dotenv_path=".env")
load_dotenv(dotenv_path="postgres/db.env")

# Configuration variables
LAKE_TYPE = os.getenv("LAKE_TYPE", "parquet")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "csv_deltas")
KAFKA_STARTING_OFFSETS = os.getenv("KAFKA_STARTING_OFFSETS", "earliest")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "datalake-stream")
PROCESSING_MODE = os.getenv("PROCESSING_MODE", "streaming")
KAFKA_BACKLOG_THRESHOLD = int(os.getenv("KAFKA_BACKLOG_THRESHOLD", "1000"))

SCHEDULE_TYPE = os.getenv("SCHEDULE_TYPE", "interval")
SCHEDULE_CRON = os.getenv("SCHEDULE_CRON", "0 3 * * *")
SCHEDULE_INTERVAL_HOURS = int(os.getenv("SCHEDULE_INTERVAL_HOURS", 1))

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "host.docker.internal")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")

SPARK_MASTER = os.getenv("SPARK_MASTER", "spark://spark-master:7077")
# Spark resource tuning parameters
SPARK_DRIVER_MEMORY = os.getenv("SPARK_DRIVER_MEMORY", "2g")
SPARK_EXECUTOR_MEMORY = os.getenv("SPARK_EXECUTOR_MEMORY", "2g")
SPARK_DRIVER_CORES = os.getenv("SPARK_DRIVER_CORES", "1")
SPARK_EXECUTOR_CORES = os.getenv("SPARK_EXECUTOR_CORES", "1")
SPARK_SQL_SHUFFLE_PARTITIONS = int(os.getenv("SPARK_SQL_SHUFFLE_PARTITIONS", "200"))
SPARK_DYNAMIC_ALLOCATION = os.getenv("SPARK_DYNAMIC_ALLOCATION", "false").lower() == "true"

DELTA_PATH = os.getenv("DELTA_PATH", "delta_files")
