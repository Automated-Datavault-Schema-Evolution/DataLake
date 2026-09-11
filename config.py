import os
from dotenv import load_dotenv
env_type = os.getenv('ENV_TYPE', 'local')
load_dotenv(f'.env.{env_type}')
load_dotenv(dotenv_path='postgres/db.env')
LAKE_TYPE = os.getenv('LAKE_TYPE', 'parquet')
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'csv_deltas')
KAFKA_STARTING_OFFSETS = os.getenv('KAFKA_STARTING_OFFSETS', 'earliest')
KAFKA_GROUP_ID = os.getenv('KAFKA_GROUP_ID', 'datalake-stream')
PROCESSING_MODE = os.getenv('PROCESSING_MODE', 'streaming')
BACKLOG_BATCH_SIZE = int(os.getenv('BACKLOG_BATCH_SIZE', '500'))
SCHEDULE_TYPE = os.getenv('SCHEDULE_TYPE', 'interval')
SCHEDULE_CRON = os.getenv('SCHEDULE_CRON', '0 3 * * *')
SCHEDULE_INTERVAL_HOURS = int(os.getenv('SCHEDULE_INTERVAL_HOURS', '1'))
POSTGRES_HOST = os.getenv('POSTGRES_HOST', 'host.docker.internal')
POSTGRES_PORT = int(os.getenv('POSTGRES_PORT', '5432'))
POSTGRES_DB = os.getenv('POSTGRES_DB')
POSTGRES_USER = os.getenv('POSTGRES_USER')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD')
POSTGRES_POOL_MIN = int(os.getenv('POSTGRES_POOL_MIN', '1'))
POSTGRES_POOL_MAX = int(os.getenv('POSTGRES_POOL_MAX', '5'))
POSTGRES_MAINTENANCE_DB = os.getenv('POSTGRES_MAINTENANCE_DB', 'postgres')
POSTGRES_DB_OWNER = os.getenv('POSTGRES_DB_OWNER') or POSTGRES_USER
SPARK_MASTER = os.getenv('SPARK_MASTER', 'spark://localhost:7077')
SPARK_DRIVER_MEMORY = os.getenv('SPARK_DRIVER_MEMORY', '4g')
SPARK_EXECUTOR_MEMORY = os.getenv('SPARK_EXECUTOR_MEMORY', '4g')
SPARK_DRIVER_CORES = os.getenv('SPARK_DRIVER_CORES', '2')
SPARK_EXECUTOR_CORES = os.getenv('SPARK_EXECUTOR_CORES', '2')
SPARK_SQL_SHUFFLE_PARTITIONS = int(os.getenv('SPARK_SQL_SHUFFLE_PARTITIONS', '200'))
SPARK_SERIALIZER = os.getenv('SPARK_SERIALIZER', 'org.apache.spark.serializer.KryoSerializer')
SPARK_KRYO_BUFFER_MAX = os.getenv('SPARK_KRYO_BUFFER_MAX', '256m')
SPARK_ADAPTIVE_EXECUTION = os.getenv('SPARK_ADAPTIVE_EXECUTION', 'true').lower() == 'true'
SPARK_AUTOSCALE = os.getenv('SPARK_AUTOSCALE', 'false').lower() == 'true'
SPARK_WORKER_MAX = int(os.getenv('SPARK_WORKER_MAX', '5'))
SPARK_WORKER_MIN = int(os.getenv('SPARK_WORKER_MIN', '1'))
SPARK_WORKER_IMAGE = os.getenv('SPARK_WORKER_IMAGE', 'bitnami/spark:latest')
SPARK_WORKER_CONTAINER_PREFIX = os.getenv('SPARK_WORKER_CONTAINER_PREFIX', 'spark-worker-')
SPARK_WORKER_CPU_THRESHOLD = float(os.getenv('SPARK_WORKER_CPU_THRESHOLD', '80'))
DOCKER_NETWORK = os.getenv('DOCKER_NETWORK', 'data_automation-net')
SPARK_SQL_ADAPTIVE_COALESCE_PARTITIONS = os.getenv('SPARK_SQL_ADAPTIVE_COALESCE_PARTITIONS', 'true').lower() == 'true'
SPARK_SQL_ADAPTIVE_ADVISORY_PARTITION_SIZE = os.getenv('SPARK_SQL_ADAPTIVE_ADVISORY_PARTITION_SIZE', '64m')
ALLOW_MAVEN = os.getenv('ALLOW_MAVEN', '0') == '1'
DELTA_PATH = os.getenv('DELTA_PATH', 'delta_files')
CHECKPOINT_PATH = os.getenv('CHECKPOINT_PATH', './tmp/delta/checkpoints')
CHECKPOINT_LOCATION = os.getenv('CHECKPOINT_LOCATION', '/tmp/delta/checkpoints')
DRIVER_PY = os.getenv('DRIVER_PY', '/usr/local/bin/python')
EXEC_PY = os.getenv('EXEC_PY', '/opt/bitnami/python/bin/python')
SPARK_IVY_PATH = os.getenv('SPARK_IVY_PATH', '/tmp/.ivy2')
LAKE_HANDLER_GRPC_PORT = int(os.getenv('LAKE_HANDLER_GRPC_PORT', '50051'))
LAKE_HANDLER_GRPC_MAX_WORKERS = int(os.getenv('LAKE_HANDLER_GRPC_MAX_WORKERS', '10'))


def create_docker_client():
    import docker
    return docker.from_env()
