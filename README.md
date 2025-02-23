# ENV-Files

## Global

````yaml
# Data lake type: "parquet" to store as Parquet files, or "rdbms" to write to a relational database
LAKE_TYPE=rdbms

  # Directory where CSV files are located (inside the application container)
DATA_DIRECTORY=./data
HOST_DATA_DIRECTORY=C:\Users\alexm\Desktop\repos\automated_datavault_schema_evolution\data

  # Kafka configuration (these point to the externally managed Kafka)
KAFKA_BOOTSTRAP_SERVERS=host.docker.internal:9092
KAFKA_TOPIC=csv_deltas
PROCESSING_MODE=stream
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