import os
from datetime import datetime

from logger import log, log_step
from psycopg2.extras import execute_values
from psycopg2.pool import SimpleConnectionPool
from pyspark.errors import AnalysisException
from pyspark.sql import functions as F

from config import (
    POSTGRES_HOST,
    POSTGRES_PORT,
    POSTGRES_DB,
    POSTGRES_USER,
    POSTGRES_PASSWORD,
    LAKE_TYPE, POSTGRES_POOL_MAX, POSTGRES_POOL_MIN,
)

# Global connection pool for PostgreSQL
PG_POOL = None


def write_to_delta(df, delta_path, partition_by=None):
    """Write rows from a Spark DataFrame to Delta files and optionally Postgres.

    When Spark batches combine several Kafka messages the resulting DataFrame may
    contain rows for multiple source files. To avoid mixing their schemas, the
    data is grouped by ``filename`` and each group is written separately.  If the
    DataFrame only contains one filename the grouping step is skipped for
    efficiency.
    """

    with log_step(f"Write batch to Delta path {delta_path}"):
        spark = df.sparkSession

        def _write_single(sub_df, filename):
            table_name = None
            target_path = delta_path
            if filename is not None:
                table_name = os.path.splitext(os.path.basename(filename))[0].replace(".", "_").replace("-", "_")
                target_path = os.path.join(delta_path, table_name)

            if "data_format" in sub_df.columns:
                sub_df = sub_df.drop("data_format")
            drop_cols = [c for c in ["source_filename", "source_data_format"] if c in sub_df.columns]
            if drop_cols:
                sub_df = sub_df.drop(*drop_cols)

            if LAKE_TYPE != "rdbms":
                try:
                    if partition_by and partition_by in sub_df.columns:
                        sub_df.write.format("delta").mode("append").partitionBy(partition_by).save(target_path)
                    else:
                        sub_df.write.format("delta").mode("append").save(target_path)
                    log.info(f"Written batch to Delta Lake at {target_path}")
                except AnalysisException as e:
                    log.error(f"Delta Lake AnalysisException: {e}")
                except Exception as e:
                    log.error(f"Error saving Delta file {target_path}: {e}")

            if LAKE_TYPE == "rdbms" and table_name is not None:
                store_to_rdbms(table_name, sub_df)

        if "filename" in df.columns:
            filenames = [r["filename"] for r in df.select("filename").distinct().collect()]
            if len(filenames) == 1:
                _write_single(df.drop("filename"), filenames[0])
            else:

                for fname in filenames:
                    subset = df.where(F.col("filename") == fname).drop("filename")
                    _write_single(subset, fname)
        else:
            _write_single(df, None)


def write_to_parquet(spark_df, output_path, mode='append', partition_by=None):
    with log_step(f"Write DataFrame to Parquet {output_path}"):
        try:
            writer = spark_df.write.format("delta").mode(mode)
            if partition_by:
                writer = writer.partitionBy(partition_by)

            writer.parquet(output_path)
            log.info(f"Data written to Parquet file {output_path} with mode={mode}")
        except Exception as e:
            log.error(f"Failed to write to Parquet file: {e}", exc_info=True)


def init_postgres_pool(minconn=None, maxconn=None):
    """
        Initialize and return a global psycopg2 connection pool.
        This pool will be used by all threads.
        """
    with log_step("Initialize PostgreSQL connection pool"):
        global PG_POOL
        if minconn is None:
            minconn = POSTGRES_POOL_MIN
        if maxconn is None:
            maxconn = POSTGRES_POOL_MAX
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
        if conn.closed:
            log.warning("Received closed connection from pool; replacing it.")
            pool.putconn(conn, close=True)
            conn = pool.getconn()
        log.debug("Acquired connection from pool.")
        return conn
    except Exception as e:
        msg = str(e)
        if "connection pool exhausted" in msg.lower():
            new_max = pool.maxconn + 5
            log.warning(
                f"Connection pool exhausted. Expanding pool to {new_max} connections."
            )
            # close existing pool and recreate with larger size
            try:
                pool.closeall()
            except Exception:
                pass
            # reinitialize pool with larger max
            init_postgres_pool(pool.minconn, new_max)
            pool = PG_POOL
            conn = pool.getconn()
            return conn
        log.error(f"Error getting connection from pool: {e}")
        raise


def release_postgres_connection(conn):
    """
    Return the connection back to the pool.
    """
    pool = init_postgres_pool()
    try:
        if conn.closed:
            pool.putconn(conn, close=True)
            log.debug("Closed dead connection from pool.")
        else:
            pool.putconn(conn)
            log.debug("Released connection back to pool.")
    except Exception as e:
        log.error(f"Error releasing connection: {e}")


def table_exists(table_name):
    """
    Check whether a table exists in PostgreSQL
    """
    conn = None
    try:
        conn = get_postgres_connection()
        cur = conn.cursor()
        cur.execute("SELECT to_regclass(%s)", (f"public.{table_name}",))
        result = cur.fetchone()
        cur.close()

        log.debug(f"Checked existence for table '{table_name}': {result[0]}")
        return result[0] is not None
    except Exception as e:
        log.error(f"Error checking existence of table '{table_name}': {e}")
        return False
    finally:
        if conn:
            release_postgres_connection(conn)


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

    conn = None
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
    finally:
        if conn:
            release_postgres_connection(conn)


def bulk_insert_dataframe(pdf, table_name, drop_columns=None, context=""):
    """
    Helper function to bulk insert a Pandas DataFrame into a PostgreSQL table.

    Parameters:
      pdf         : Pandas DataFrame to insert.
      table_name  : Target table name.
      drop_columns: Optional list of columns to drop before insertion.
      context     : String prefix for log messages (e.g., batch id or function name).
    """
    with log_step(f"{context}Bulk insert into '{table_name}'"):
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

        conn = None
        try:
            conn = get_postgres_connection()
            cur = conn.cursor()

            execute_values(cur, insert_sql, data, page_size=1000)
            conn.commit()
            cur.close()

            log.info(f"{context}Bulk insert successful: Data saved to table '{table_name}'.")
        except Exception as e:
            log.error(f"{context}Error during bulk insert to table '{table_name}': {e}")
        finally:
            if conn:
                release_postgres_connection(conn)


def store_to_rdbms(table_name, df, batch_size: int = 5000):
    """
    Write a Spark DataFrame to PostgreSQL via psycopg2 using a connection pool.
    - No pandas
    - Auto-creates schema/table from the *actual* df schema it receives
    - Streams rows in batches with execute_values
    - Logs the exact DDL used
    """
    with log_step(f"store_to_rdbms for '{table_name}'"):
        # Parse schema.table; default to public
        if "." in table_name:
            schema_name, tbl_name = table_name.split(".", 1)
        else:
            schema_name, tbl_name = "public", table_name

        def _pg_identifier(name: str) -> str:
            """Quote an identifier for PostgreSQL."""
            return '"' + name.replace('"', '""') + '"'

        def _spark_to_pg_type(dt) -> str:
            from pyspark.sql.types import (
                IntegerType, LongType, ShortType, ByteType,
                DoubleType, FloatType, DecimalType, BooleanType,
                DateType, TimestampType
            )
            # Map Spark SQL data types to PostgreSQL column types.
            if isinstance(dt, (ByteType, ShortType, IntegerType)): return "INTEGER"
            if isinstance(dt, LongType):                            return "BIGINT"
            if isinstance(dt, (FloatType, DoubleType)):             return "DOUBLE PRECISION"
            if isinstance(dt, DecimalType):                         return f"NUMERIC({dt.precision},{dt.scale})"
            if isinstance(dt, BooleanType):                         return "BOOLEAN"
            if isinstance(dt, DateType):                            return "DATE"
            if isinstance(dt, TimestampType):                       return "TIMESTAMP"
            # fallback for arrays/maps/structs or unknowns
            return "TEXT"

        cols = df.columns
        # Build column DDL from Spark schema
        field_types = {f.name: f.dataType for f in df.schema.fields}
        # Helpful schema log (ordered) so you can diff quickly
        pretty_types = ", ".join(f"{c}:{type(field_types[c]).__name__}" for c in cols)
        log.info(f"[{schema_name}.{tbl_name}] Batch schema ({len(cols)} cols): {pretty_types}")

        col_defs = ", ".join(f"{_pg_identifier(c)} {_spark_to_pg_type(field_types[c])}" for c in cols)

        create_schema_sql = f'CREATE SCHEMA IF NOT EXISTS {_pg_identifier(schema_name)};'
        create_table_sql = (
            f'CREATE TABLE IF NOT EXISTS {_pg_identifier(schema_name)}.{_pg_identifier(tbl_name)} ('
            f'{col_defs}'
            f');'
        )
        insert_cols_sql = ", ".join(_pg_identifier(c) for c in cols)
        insert_sql = (
            f'INSERT INTO {_pg_identifier(schema_name)}.{_pg_identifier(tbl_name)} '
            f'({insert_cols_sql}) VALUES %s'
        )
        log.info("[DDL] Ensure schema SQL:\n" + create_schema_sql + " ")
        log.info(f"[DDL] Ensure table SQL for {schema_name}.{tbl_name}:\n" + create_table_sql + " ")

        conn = None
        try:
            conn = get_postgres_connection()
            cur = conn.cursor()

            # Ensure schema/table
            cur.execute(create_schema_sql)
            cur.execute(create_table_sql)
            conn.commit()

            # Discover existing columns (for extra safety / visibility)
            cur.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = %s
                  AND table_name = %s
                ORDER BY ordinal_position
                """,
                (schema_name, tbl_name)
            )
            existing_cols = [r[0] for r in cur.fetchall()]
            log.info(f"[{schema_name}.{tbl_name}] Existing columns in DB ({len(existing_cols)}): {existing_cols}")
            log.info(f"[{schema_name}.{tbl_name}] Insert columns ({len(cols)}): {cols}")
            log.debug(f'[INSERT SQL] {insert_sql}')

            from psycopg2.extras import execute_values

            # Stream rows from Spark without collecting everything at once
            batch = []
            written = 0
            for r in df.select(*cols).toLocalIterator():
                batch.append(tuple(r[c] for c in cols))
                if len(batch) >= batch_size:
                    execute_values(cur, insert_sql, batch, page_size=batch_size)
                    conn.commit()
                    written += len(batch)
                    batch.clear()

            if batch:
                execute_values(cur, insert_sql, batch, page_size=batch_size)
                conn.commit()
                written += len(batch)

            cur.close()
            log.info(f"Inserted {written} rows into {schema_name}.{tbl_name}")
        except Exception as e:
            if conn:
                try:
                    conn.rollback()
                except Exception:
                    pass
            log.error(f"Error in store_to_rdbms for table '{table_name}': {e}")
        finally:
            if conn:
                release_postgres_connection(conn)


def store_to_parquet(dest_path, df, mode: str = "overwrite", partition_by=None):
    """
    Save a Spark DataFrame to a Parquet file using Spark's writer.
    """
    with log_step(f"Save DataFrame to Parquet {dest_path}"):
        try:
            writer = df.write.mode(mode)
            if partition_by:
                writer = writer.partitionBy(partition_by)
            writer.parquet(dest_path)
            log.info(f"Saved data to Parquet: {dest_path} (mode={mode})")
        except Exception as e:
            log.error(f"Error saving Parquet file {dest_path}: {e}", exc_info=True)


def ensure_postgres_database():
    """
    Ensure the target Postgres database exists.
    - Connects to a maintenance DB (default 'postgres') WITHOUT using the pool.
    - Creates the target DB if missing.
    - Only after that, verifies connectivity via the pool helpers.
    """
    import os
    from contextlib import closing
    import psycopg2
    from psycopg2 import errorcodes
    from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

    host = os.getenv("POSTGRES_HOST", "localhost")
    port = int(os.getenv("POSTGRES_PORT", "5432"))
    db = os.getenv("POSTGRES_DB", "postgres")
    user = os.getenv("POSTGRES_USER", "postgres")
    pwd = os.getenv("POSTGRES_PASSWORD", "")
    maint_db = os.getenv("POSTGRES_MAINTENANCE_DB", "postgres")
    owner = os.getenv("POSTGRES_DB_OWNER", user)

    def _pg_ident(name: str) -> str:
        return '"' + name.replace('"', '""') + '"'

    with log_step("Ensure PostgreSQL database exists"):
        # 1) Create if missing using a direct admin connection (no pool yet!)
        try:
            with closing(
                    psycopg2.connect(host=host, port=port, dbname=maint_db, user=user, password=pwd)) as admin_conn:
                admin_conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
                with admin_conn.cursor() as cur:
                    cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (db,))
                    exists = cur.fetchone() is not None
                    if not exists:
                        create_sql = f"CREATE DATABASE {_pg_ident(db)} ENCODING 'UTF8' TEMPLATE template0"
                        log.info(f"[DDL] {create_sql};")
                        cur.execute(create_sql)
                        if owner:
                            alter_sql = f"ALTER DATABASE {_pg_ident(db)} OWNER TO {_pg_ident(owner)}"
                            log.info(f"[DDL] {alter_sql};")
                            cur.execute(alter_sql)
                    else:
                        log.info(f"Database '{db}' already exists.")
        except psycopg2.Error as e:
            # If it’s a race and someone else created it, continue; otherwise fail loudly.
            if getattr(e, "pgcode", None) == errorcodes.DUPLICATE_DATABASE:
                log.warning(f"Database '{db}' was created concurrently by another process.")
            else:
                log.critical(
                    f"Unable to verify/create database '{db}'. "
                    f"Host={host}:{port} maint_db='{maint_db}' user='{user}'. Error: {e}",
                    exc_info=True
                )
                return False

        # 2) Now that DB should exist, verify via your pool helpers.
        try:
            conn = get_postgres_connection()
            release_postgres_connection(conn)
            log.info(f"Database '{db}' is ready (verified via pool).")
            return True
        except Exception as e:
            log.critical(f"Database '{db}' creation succeeded but pool verification failed: {e}", exc_info=True)
            return False


def ensure_postgres_ready():
    """
    Call once at startup, BEFORE any code tries to initialize or use the pool.
    """
    ok = ensure_postgres_database()
    if not ok:
        raise RuntimeError("PostgreSQL database is not available and could not be created.")

    # Optional: warm the pool (exercise get/release once more).
    try:
        conn = get_postgres_connection()
        release_postgres_connection(conn)
        log.debug("PostgreSQL pool warmed successfully.")
    except Exception as e:
        log.critical(f"Failed to warm PostgreSQL pool: {e}", exc_info=True)
        raise
