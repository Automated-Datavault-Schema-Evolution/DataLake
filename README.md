# ENV-Files

## Global

````yaml
# Data lake type: "parquet" to store as Parquet files, or "rdbms" to write to a relational database
LAKE_TYPE=rdbms

  # Kafka configuration (these point to the externally managed Kafka)
KAFKA_BOOTSTRAP_SERVERS=kafka:9092
KAFKA_TOPIC=csv_deltas
PROCESSING_MODE=stream

SCHEDULE_TYPE=interval      # options: "cron" or "interval"
SCHEDULE_CRON=0 3 * * *     # Used if SCHEDULE_TYPE=cron (at 03:00 daily, cron syntax)
SCHEDULE_INTERVAL_HOURS=4   # Used if SCHEDULE_TYPE=interval
BULK_OFFSET_FILE=last_ingest_offset.pkl

SPARK_MASTER=spark://spark-master:7077
````

## Database

````yaml
# PostgreSQL connection details (pointing to the external Postgres container)
POSTGRES_HOST=host.docker.internal
POSTGRES_PORT=5432
POSTGRES_DB=datalake
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres

````