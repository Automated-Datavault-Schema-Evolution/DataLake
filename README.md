# Data Lake Ingestion Service

This repository provides a Python based ingestion service that consumes CSV deltas from a Kafka topic and persists the
data into a Delta Lake or relational database. The service can operate in **streaming** or **bulk** mode and is packaged
to run locally or via Docker Compose.

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
For local development you can create an `.env` file instead. Database credentials
may also be supplied in `postgres/db.env`, which is loaded automatically if present.

### Environment Variables

````yaml
# Data lake type: "parquet" to store as Parquet files, or "rdbms" to write to a relational database
LAKE_TYPE=rdbms

  # Kafka configuration
KAFKA_BOOTSTRAP_SERVERS=kafka:9092
KAFKA_TOPIC=csv_deltas
KAFKA_STARTING_OFFSETS=earliest   # 'earliest' to read all messages
KAFKA_GROUP_ID=datalake-stream
KAFKA_BACKLOG_THRESHOLD=1000     # switch to bulk mode if backlog exceeds this
PROCESSING_MODE=streaming   # options: streaming or bulk

SCHEDULE_TYPE=interval      # options: "cron" or "interval"
SCHEDULE_CRON=0 3 * * *     # Used if SCHEDULE_TYPE=cron
SCHEDULE_INTERVAL_HOURS=4   # Used if SCHEDULE_TYPE=interval

DELTA_PATH=delta_files         # path where Delta tables are stored


SPARK_MASTER=spark://spark-master:7077
SPARK_DRIVER_MEMORY=2g      # Spark driver JVM memory
SPARK_EXECUTOR_MEMORY=2g    # Executor JVM memory per worker
SPARK_DRIVER_CORES=1        # Cores for the driver
SPARK_EXECUTOR_CORES=1      # Cores per executor
SPARK_SQL_SHUFFLE_PARTITIONS=200
SPARK_DYNAMIC_ALLOCATION=false
CHECKPOINT_PATH=/tmp/delta/checkpoints
````

Database settings if using the RDBMS mode:

````yaml
POSTGRES_HOST=host.docker.internal
POSTGRES_PORT=5432
POSTGRES_DB=datalake
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres
````

## Components

| File                  | Description                                                                                                                         |
|-----------------------|-------------------------------------------------------------------------------------------------------------------------------------|
| `main.py`             | Entry point for the ingestion service. Handles streaming ingestion from Kafka with a fallback to bulk mode and optional scheduling. |
| `data_lake.py`        | One-time importer that loads CSV files from `DATA_DIRECTORY` into the data lake and database.                                       |
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