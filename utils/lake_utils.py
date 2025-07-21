import os
from datetime import datetime

from logger import log
from psycopg2.extras import execute_values
from psycopg2.pool import SimpleConnectionPool
from pyspark.errors import AnalysisException

from config import (
    POSTGRES_HOST,
    POSTGRES_PORT,
    POSTGRES_DB,
    POSTGRES_USER,
    POSTGRES_PASSWORD,
    LAKE_TYPE,
)

# Global connection pool for PostgreSQL
PG_POOL = None


def write_to_delta(df, delta_path, partition_by=None):
    """
    Always writes to Delta Lake at the given path, optionally partitioned by column.
    If LAKE_TYPE is 'rdbms', also writes to Postgres with table name based on 'source_filename'.
    """
    # Get the filename/table from the data if present
    if 'filename' in df.columns:
        # Single filename for this batch
        filename = df.select('filename').head()['filename']
        table_name = os.path.splitext(os.path.basename(filename))[0].replace('.', '_').replace('-', '_')
        target_path = os.path.join(delta_path, table_name)
    else:
        # Fallback if not present
        table_name = None
        target_path = delta_path

    try:
        if partition_by and partition_by in df.columns:
            df.write.format("delta").mode("append").partitionBy(partition_by).save(target_path)
        else:
            df.write.format("delta").mode("append").save(target_path)
        log.info(f"Written batch to Delta Lake at {target_path}")
    except AnalysisException as e:
        log.error(f"Delta Lake AnalysisException: {e}")
    except Exception as e:
        log.error(f"Error saving Delta file {target_path}: {e}")

    # If configured, also store in RDBMS with table name from filename
    if LAKE_TYPE == "rdbms" and table_name is not None:
        store_to_rdbms(table_name, df)


def write_to_parquet(spark_df, output_path, mode='append', partition_by=None):
    try:
        writer = spark_df.write.format("delta").mode(mode)
        if partition_by:
            writer = writer.partitionBy(partition_by)

        writer.parquet(output_path)
        log.info(f"Data written to Parquet file {output_path} with mode={mode}")
    except Exception as e:
        log.error(f"Failed to write to Parquet file: {e}", exc_info=True)


def init_postgres_pool(minconn=1, maxconn=5):
    """
        Initialize and return a global psycopg2 connection pool.
        This pool will be used by all threads.
        """
    global PG_POOL
    if PG_POOL is None:
        try:
            PG_POOL = SimpleConnectionPool(
                minconn,
                maxconn,
                host=POSTGRES_HOST,
                port=POSTGRES_PORT,
                dbname=POSTGRES_DB,
                user=POSTGRES_USER,
                password=POSTGRES_PASSWORD
            )
            log.info(f"PostgreSQL connection pool created (min={minconn}, max={maxconn}).")
        except Exception as e:
            log.error(f"Error establishing PostgreSQL connection pool: {e}")
            raise
    else:
        log.debug("Reusing existing PostgreSQL connection pool.")
    return PG_POOL


def get_postgres_connection():
    """
    Get a connection from the pool.
    """
    pool = init_postgres_pool()
    try:
        conn = pool.getconn()
        log.debug("Acquired connection from pool.")
        return conn
    except Exception as e:
        log.error(f"Error getting connection from pool: {e}")
        raise


def release_postgres_connection(conn):
    """
    Return the connection back to the pool.
    """
    pool = init_postgres_pool()
    pool.putconn(conn)
    log.debug("Released connection back to pool.")


def table_exists(table_name):
    """
    Check whether a table exists in PostgreSQL
    """
    try:
        conn = get_postgres_connection()
        cur = conn.cursor()
        cur.execute("SELECT to_regclass(%s)", (f"public.{table_name}",))
        result = cur.fetchone()
        cur.close()
        release_postgres_connection(conn)
        log.debug(f"Checked existence for table '{table_name}': {result[0]}")
        return result[0] is not None
    except Exception as e:
        log.error(f"Error checking existence of table '{table_name}': {e}")
        return False


def create_table_if_not_exists(table_name, pdf):
    """
    Generate and execute a CREATE TABLE statement based on the Pandas DataFrame schema.
    Adds an extra column 'insertion_timestamp' if not already present.
    """
    col_defs = []
    add_insertion = "insertion_timestamp" not in pdf.columns
    log.debug(f"Creating table '{table_name}', insertion_timestamp added: {add_insertion}")
    for col, dtype in pdf.dtypes.items():
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
    if add_insertion:
        col_defs.append('"insertion_timestamp" TIMESTAMP')
    col_defs_str = ", ".join(col_defs)
    create_sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({col_defs_str});"
    log.debug(f"CREATE TABLE SQL for '{table_name}': {create_sql}")
    try:
        conn = get_postgres_connection()
        cur = conn.cursor()
        cur.execute(create_sql)
        conn.commit()
        cur.close()
        release_postgres_connection(conn)
        log.info(f"Created table '{table_name}' with schema: {col_defs_str}")
    except Exception as e:
        log.error(f"Error creating table '{table_name}': {e}")
        raise


def bulk_insert_dataframe(pdf, table_name, drop_columns=None, context=""):
    """
    Helper function to bulk insert a Pandas DataFrame into a PostgreSQL table.

    Parameters:
      pdf         : Pandas DataFrame to insert.
      table_name  : Target table name.
      drop_columns: Optional list of columns to drop before insertion.
      context     : String prefix for log messages (e.g., batch id or function name).
    """
    if drop_columns:
        pdf = pdf.drop(columns=drop_columns)
        log.debug(f"{context}Dropped columns {drop_columns}. New shape: {pdf.shape}")

    if pdf.empty:
        log.info(f"{context}DataFrame is empty; nothing to insert into '{table_name}'.")
        return

    if "insertion_timestamp" not in pdf.columns:
        pdf["insertion_timestamp"] = datetime.now()
        log.debug(f"{context}Added 'insertion_timestamp' column to DataFrame for table '{table_name}'.")

    if not table_exists(table_name):
        log.info(f"{context}Table '{table_name}' does not exist. Creating table.")
        create_table_if_not_exists(table_name, pdf)
    else:
        log.info(f"{context}Table '{table_name}' exists. Appending data.")

    columns = list(pdf.columns)
    col_names = ", ".join([f'"{col}"' for col in columns])
    insert_sql = f"INSERT INTO {table_name} ({col_names}) VALUES %s"
    log.debug(f"{context}INSERT SQL for '{table_name}': {insert_sql}")

    data = [tuple(row) for row in pdf.values]
    log.debug(f"{context}Prepared {len(data)} rows for insertion into '{table_name}'.")

    try:
        conn = get_postgres_connection()
        cur = conn.cursor()

        execute_values(cur, insert_sql, data, page_size=1000)
        conn.commit()
        cur.close()
        release_postgres_connection(conn)
        log.info(f"{context}Bulk insert successful: Data saved to table '{table_name}'.")
    except Exception as e:
        log.error(f"{context}Error during bulk insert to table '{table_name}': {e}")


def store_to_rdbms(table_name, df):
    """
    Convert a Spark DataFrame to Pandas and insert into the RDBMS.
    """
    try:
        pdf = df.toPandas()
        log.debug(f"Converted Spark DataFrame to Pandas for table '{table_name}', shape: {pdf.shape}")
        bulk_insert_dataframe(pdf, table_name, context=f"store_to_rdbms for '{table_name}': ")
    except Exception as e:
        log.error(f"Error in store_to_rdbms for table '{table_name}': {e}")


def store_to_parquet(dest_path, df):
    """
    Save a Spark DataFrame to a Parquet file.
    """
    try:
        df.write.mode("overwrite").parquet(dest_path)
        log.info(f"Saved data to Parquet: {dest_path}")
    except Exception as e:
        log.error(f"Error saving Parquet file {dest_path}: {e}")
