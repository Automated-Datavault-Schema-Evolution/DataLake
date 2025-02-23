# data_lake.py
import os

import psycopg2
from logger import log
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp

from config import LAKE_TYPE, DATA_DIRECTORY, POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB, POSTGRES_USER, \
    POSTGRES_PASSWORD

# Initialize Spark session
spark = SparkSession.builder.appName("DataLakeIngestion").getOrCreate()


def get_postgres_connection():
    """
    Create and return a psycopg2 connection using the PostgreSQL parameters.
    """
    try:
        conn = psycopg2.connect(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            dbname=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD
        )
        return conn
    except Exception as e:
        log.error(f"Error connecting to PostgreSQL: {e}")
        raise


def table_exists(table_name):
    """
    Check whether a table exists in PostgreSQL using the to_regclass() function.
    """
    try:
        conn = get_postgres_connection()
        cur = conn.cursor()
        cur.execute("SELECT to_regclass(%s)", (f"public.{table_name}",))
        result = cur.fetchone()
        cur.close()
        conn.close()
        return result[0] is not None
    except Exception as e:
        log.error(f"Error checking existence of table '{table_name}': {e}")
        return False


def create_table_if_not_exists(table_name, pdf):
    """
    Generate and execute a CREATE TABLE statement based on the Pandas DataFrame schema.
    An extra column 'insertion_timestamp' is added.
    """
    col_defs = []
    for col, dtype in pdf.dtypes.items():
        # Map common pandas types to PostgreSQL data types.
        dtype_str = str(dtype)
        if "int" in dtype_str:
            pg_type = "BIGINT"
        elif "float" in dtype_str:
            pg_type = "DOUBLE PRECISION"
        elif "datetime" in dtype_str:
            pg_type = "TIMESTAMP"
        else:
            pg_type = "TEXT"
        col_defs.append(f'"{col}" {pg_type}')
    # Add the insertion_timestamp column
    col_defs.append('"insertion_timestamp" TIMESTAMP')
    col_defs_str = ", ".join(col_defs)
    create_sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({col_defs_str});"
    try:
        conn = get_postgres_connection()
        cur = conn.cursor()
        cur.execute(create_sql)
        conn.commit()
        cur.close()
        conn.close()
        log.info(f"Created table '{table_name}' with schema: {col_defs_str}")
    except Exception as e:
        log.error(f"Error creating table '{table_name}': {e}")
        raise


def store_to_parquet(dest_path, df):
    try:
        df.write.mode("overwrite").parquet(dest_path)
        log.info(f"Saved data to Parquet: {dest_path}")
    except Exception as e:
        log.error(f"Error saving Parquet file {dest_path}: {e}")


def store_to_rdbms(table_name, df):
    try:
        # Convert Spark DataFrame to Pandas DataFrame.
        pdf = df.toPandas()
        if pdf.empty:
            log.info(f"No data to insert for table {table_name}")
            return

        # Check if table exists; if not, create it.
        if not table_exists(table_name):
            log.info(f"Table '{table_name}' does not exist. Creating table.")
            create_table_if_not_exists(table_name, pdf)
        else:
            log.info(f"Table '{table_name}' exists. Appending data.")

        # Add the insertion timestamp column to the DataFrame.
        if 'insertion_timestamp' not in pdf.columns:
            from datetime import datetime
            pdf["insertion_timestamp"] = datetime.now()

        # Build an INSERT statement dynamically based on DataFrame columns.
        columns = list(pdf.columns)
        col_names = ", ".join([f'"{col}"' for col in columns])
        placeholders = ", ".join(["%s"] * len(columns))
        insert_sql = f"INSERT INTO {table_name} ({col_names}) VALUES ({placeholders})"

        # Convert DataFrame rows to a list of tuples.
        data = [tuple(row) for row in pdf.values]

        conn = get_postgres_connection()
        cur = conn.cursor()
        cur.executemany(insert_sql, data)
        conn.commit()
        cur.close()
        conn.close()
        log.info(f"Saved data to table '{table_name}' in PostgreSQL")
    except Exception as e:
        log.error(f"Error saving to PostgreSQL: {e}")


def create_data_lake_entry(file_name):
    """
    Read the CSV file using Spark, add an insertion timestamp to each record,
    and save its contents into the data lake.
    """
    try:
        df = spark.read.option("header", "true").option("inferSchema", "true").csv(file_name)
        log.info(f"Read CSV file: {file_name}")
    except Exception as e:
        log.error(f"Error reading CSV file {file_name}: {e}")
        return

    # Add an insertion timestamp to each record (using Spark's current_timestamp).
    df = df.withColumn("insertion_timestamp", current_timestamp())

    if LAKE_TYPE == "parquet":
        dest_dir = "./datalake/parquet"
        os.makedirs(dest_dir, exist_ok=True)
        log.debug(f"Ensured directory exists: {dest_dir}")
        dest_path = os.path.join(dest_dir, os.path.basename(file_name).replace(".csv", ".parquet"))
        store_to_parquet(dest_path, df)
    elif LAKE_TYPE == "rdbms":
        table_name = os.path.basename(file_name).replace(".csv", "")
        store_to_rdbms(table_name, df)
    else:
        log.error("Invalid LAKE_TYPE specified in config.")


def initial_setup_data_lake():
    """
    Perform an initial bulk ingestion of the entire DATA_DIRECTORY into the data lake.
    For RDBMS:
      - For each CSV file in the directory, if the corresponding table does not exist, ingest it.
    For Parquet:
      - If the target Parquet directory is empty or missing, ingest all CSV files.
    """
    import glob

    csv_files = glob.glob(os.path.join(DATA_DIRECTORY, "*.csv"))
    if not csv_files:
        log.info("No CSV files found in the data directory.")
        return

    if LAKE_TYPE == "rdbms":
        for file in csv_files:
            table_name = os.path.basename(file).replace(".csv", "")
            if table_exists(table_name):
                log.info(f"Table '{table_name}' already exists. Skipping bulk insert for this file.")
            else:
                log.info(f"Table '{table_name}' does not exist. Bulk ingesting file '{file}'.")
                create_data_lake_entry(file)
    elif LAKE_TYPE == "parquet":
        dest_dir = "./datalake/parquet"
        if not os.path.exists(dest_dir) or not os.listdir(dest_dir):
            os.makedirs(dest_dir, exist_ok=True)
            log.info(f"Parquet directory '{dest_dir}' is empty or missing. Bulk ingesting CSV files.")
            for file in csv_files:
                create_data_lake_entry(file)
        else:
            log.info(f"Parquet directory '{dest_dir}' is not empty. Skipping bulk ingestion.")
    else:
        log.error("Invalid LAKE_TYPE specified in config.")


if __name__ == "__main__":
    initial_setup_data_lake()
