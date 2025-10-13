# Data Lake Ingestion Service

This repository provides a Python based ingestion service that consumes delta from a Kafka topic created by the
[Filewatcher Service](https://github.com/Automated-Datavault-Schema-Evolution/FileWatcher) and persists the data into a
Delta Lake or relational database. The service can operate in **streaming** or **bulk** mode and is packaged to run
locally or via Docker Compose.

## Quick Start

1. Copy `.env.docker` from the example in this README and adjust any paths or credentials.
2. Launch the service with `docker compose up --build`.
3. Messages arriving at the Kafka topic will be persisted to Delta Lake or PostgreSQL depending on `LAKE_TYPE`.
4. For local testing without Docker compose you can run `pip install -r requirements.txt` followed by `python main.py`.

## Architecture Overview

1. **Message Source** – Kafka topic containing JSON messages. Each message represents an incremental CSV update and
   includes the filename, file format, ingestion timestamp and an array of record dictionaries.
2. **Spark Structured Streaming** – `main.py` establishes a Spark session and listens to the Kafka topic. Records are
   parsed and enriched before being written to Delta files. If `LAKE_TYPE` is `rdbms`, each batch is also written to
   PostgreSQL.
3. **Bulk Fallback** – If streaming fails or the Kafka backlog exceeds `KAFKA_BACKLOG_THRESHOLD`,
   the application switches to a plain `KafkaConsumer` to drain buffered messages and
   write them in one batch to the data lake.
4. **Delta Storage / Postgres** – Data is stored in Delta format under `DELTA_PATH` and optionally mirrored to a
   PostgreSQL database. Initial bulk ingestion from existing CSV files is handled by `data_lake.py`.

## Data Flow

```
Kafka Topic -> Spark Streaming -> Parse & Enrich -> Delta Lake (+ optional Postgres)
```

* Messages on `KAFKA_TOPIC` are JSON structures with a `data` array. `utils.parse_utils.parse_message_to_row` converts
  each message to row dictionaries and adds metadata such as the source filename.
* `main.streaming_ingest()` reads the stream and writes batches to Delta Lake through `utils.lake_utils.write_to_delta`.
* In bulk mode or as a fallback, `main.bulk_ingest()` consumes records with a traditional Kafka consumer and writes them
  in one batch.

## Setup

### Requirements

- Python 3.10
- Java 11 (required by Spark)
- Apache Spark with Delta Lake (installed automatically in the Docker image)
- Access to a Kafka broker and optionally a PostgreSQL database

### Installation

The easiest way to run the ingestion service is via Docker Compose:

```bash
docker compose up --build
```

A `.env.docker` file must be present with the environment variables described below.
For local development you can create an `.env.local` file instead. Database credentials
may also be supplied in `postgres/db.env`, which is loaded automatically if present.

### Environment Variables

````yaml
HOST_DATA_ROOT=D:/automated_datavault_schema_evolution__stack/
HOST_KAFKA_DATA=${HOST_DATA_ROOT}/kafka/data
HOST_KAFKA_LOGS=${HOST_DATA_ROOT}/kafka/logs
HOST_ZK_DATA=${HOST_DATA_ROOT}/zookeeper/data
HOST_ZK_LOG=${HOST_DATA_ROOT}/zookeeper/log
HOST_PG_DATA=${HOST_DATA_ROOT}/postgres/data
HOST_HIVE_WAREHOUSE=${HOST_DATA_ROOT}/hive/warehouse
HOST_SPARK_WAREHOUSE=${HOST_DATA_ROOT}/spark/warehouse
HOST_SPARK_CHECKPOINTS=${HOST_DATA_ROOT}/spark/checkpoints
HOST_SPARK_SCRATCH=${HOST_DATA_ROOT}/spark/scratch

  # Python executor paths
DRIVER_PY=/usr/local/bin/python
EXEC_PY=/opt/bitnami/python/bin/python
HOME=/tmp

  # Maven Download of JARS or via docker packages
ALLOW_MAVEN=0

  # container paths
CONTAINER_SPARK_WAREHOUSE_DIR=/data/spark/warehouse
CONTAINER_SPARK_CHECKPOINTS_DIR=/data/spark/checkpoints
CONTAINER_SCRATCH_DIR=/data/spark/scratch

  # Data lake type: "parquet" to store as Parquet files, or "rdbms" to write to a relational database
LAKE_TYPE=rdbms

  # Kafka configuration (these point to the externally managed Kafka)
KAFKA_BOOTSTRAP_SERVERS=kafka:9092
KAFKA_TOPIC=csv_deltas
KAFKA_GROUP_ID=delta-streaming
KAFKA_STARTING_OFFSETS=earliest     # 'earliest' to read all messages
BACKLOG_BATCH_SIZE=500              # messages drained per backlog batch

PROCESSING_MODE=streaming           # options: streaming or bulk

SCHEDULE_TYPE=interval      # options: "cron" or "interval"
SCHEDULE_CRON=0 3 * * *     # Used if SCHEDULE_TYPE=cron (at 03:00 daily, cron syntax)
SCHEDULE_INTERVAL_HOURS=4   # Used if SCHEDULE_TYPE=interval

SPARK_MASTER=spark://datalake-ingestion-spark-master:7077
SPARK_DRIVER_MEMORY=16g      # Spark driver JVM memory
SPARK_EXECUTOR_MEMORY=16g    # Executor JVM memory per worker
SPARK_DRIVER_CORES=8        # Cores for the driver
SPARK_EXECUTOR_CORES=8      # Cores per executor
SPARK_SQL_SHUFFLE_PARTITIONS=48
SPARK_DYNAMIC_ALLOCATION=true
SPARK_DYNAMIC_ALLOCATION_MIN_EXECUTORS=1
SPARK_DYNAMIC_ALLOCATION_MAX_EXECUTORS=10
SPARK_DYNAMIC_ALLOCATION_INITIAL_EXECUTORS=1
SPARK_SERIALIZER=org.apache.spark.serializer.KryoSerializer
SPARK_KRYO_BUFFER_MAX=256m
SPARK_ADAPTIVE_EXECUTION=true
SPARK_DYNAMIC_SHUFFLE_TRACKING=true
SPARK_AUTOSCALE=false           # enable Docker based scaling of workers
SPARK_WORKER_MAX=5              # upper limit for auto-scaled workers
SPARK_WORKER_MIN=1              # minimum number of workers to maintain
SPARK_WORKER_IMAGE=bitnami/spark:3.5.6
SPARK_WORKER_CONTAINER_PREFIX=spark-worker-
SPARK_WORKER_CPU_THRESHOLD=60   # CPU percent usage triggering scale up
DOCKER_NETWORK=data-automation-net
SPARK_SQL_ADAPTIVE_COALESCE_PARTITIONS=true
SPARK_SQL_ADAPTIVE_ADVISORY_PARTITION_SIZE=64m


HOST_DATA_DIRECTORY=${HOST_DATA_ROOT}/data_lake
CHECKPOINT_PATH=${HOST_DATA_DIRECTORY}/checkpoints
DELTA_PATH=${HOST_DATA_DIRECTORY}/data

  ## Postgres
POSTGRES_HOST=host.docker.internal
POSTGRES_PORT=5432
POSTGRES_DB=datalake
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres
POSTGRES_POOL_MIN=1
POSTGRES_POOL_MAX=5

````

Enabling `SPARK_DYNAMIC_ALLOCATION` allows the Spark cluster to grow or shrink
between `SPARK_DYNAMIC_ALLOCATION_MIN_EXECUTORS` and
`SPARK_DYNAMIC_ALLOCATION_MAX_EXECUTORS` based on load.
`SPARK_SERIALIZER` and `SPARK_KRYO_BUFFER_MAX` enable the faster Kryo serializer
with an increased buffer to avoid large task warnings. `SPARK_ADAPTIVE_EXECUTION`
and `SPARK_DYNAMIC_SHUFFLE_TRACKING` allow Spark to optimize shuffle partitions
and scale executors dynamically without restarting the application.

When `SPARK_AUTOSCALE` is enabled the application will use the Docker API to
start additional Spark worker containers when existing workers exceed
`SPARK_WORKER_CPU_THRESHOLD` percent CPU usage. Workers are named using
`SPARK_WORKER_CONTAINER_PREFIX` and will not exceed `SPARK_WORKER_MAX`.

When the Kafka backlog exceeds `KAFKA_BACKLOG_THRESHOLD`, messages are drained
in batches of size `BACKLOG_BATCH_SIZE` until the backlog is cleared.
If the application recognizes a backlog, then the whole backlog will be drained.

Database settings if using the RDBMS mode
Below is a brief description of the most important variables:

- `LAKE_TYPE` defines whether data is only kept in Delta files (`parquet`) or also mirrored to Postgres (`rdbms`).
- `PROCESSING_MODE` chooses between continuous streaming from Kafka or scheduled bulk imports.
- `KAFKA_BACKLOG_THRESHOLD` controls when the service falls back to batch mode if the topic accumulates too many
  messages.
- `BACKLOG_BATCH_SIZE` limits how many messages are drained in one run during backlog processing.
- The `SPARK_*` parameters tune Spark resources and enable automatic scaling when dynamic allocation is turned on.

````yaml
POSTGRES_HOST=host.docker.internal
POSTGRES_PORT=5432
POSTGRES_DB=datalake
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres
POSTGRES_POOL_MIN=1
POSTGRES_POOL_MAX=5
````

Use these parameters to point the service to your PostgreSQL database. Leave them blank if you do not need RDBMS
support.

## Components

| File                  | Description                                                                                                                         |
|-----------------------|-------------------------------------------------------------------------------------------------------------------------------------|
| `main.py`             | Entry point for the ingestion service. Handles streaming ingestion from Kafka with a fallback to bulk mode and optional scheduling. |
| `utils/`              | Helper modules for Kafka connectivity, Spark session creation, parsing messages, offset handling and writing to Delta/Postgres.     |
| `docker-compose.yaml` | Defines the containerized setup for running the service together with its dependencies.                                             |
| `Dockerfile`          | Builds the Python image with Java, Spark and all required libraries.                                                                |

## Running the Service

1. Prepare your environment variables in `.env` or `.env.docker`.
   If PostgreSQL is used, place the credentials in `postgres/db.env`.
2. Ensure Kafka and (optionally) PostgreSQL are accessible.
3. Start the application using Docker Compose or run `python main.py` locally.

## Data Persistence

- **Delta Lake** – Parquet-Files are stored under the directory pointed to by `DELTA_PATH`. If files are not existed,
  the application creates them automatically.
- **PostgreSQL** – When `LAKE_TYPE=rdbms`, `utils.lake_utils.store_to_rdbms` writes each batch to a table named after
  the source filename. Tables are created automatically if they do not exist.

## Scheduling

Bulk mode can be scheduled via APScheduler using either cron expressions or fixed hourly intervals, defined by
`SCHEDULE_TYPE`, `SCHEDULE_CRON` and `SCHEDULE_INTERVAL_HOURS`.

## License