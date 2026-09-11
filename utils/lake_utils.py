import os
from contextlib import closing
import psycopg2
from psycopg2 import errorcodes
from psycopg2.extras import execute_values
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from psycopg2.pool import SimpleConnectionPool
from pyspark.sql import functions as F
from config import DELTA_PATH, LAKE_TYPE, POSTGRES_DB, POSTGRES_DB_OWNER, POSTGRES_HOST, POSTGRES_MAINTENANCE_DB, POSTGRES_PASSWORD, POSTGRES_POOL_MAX, POSTGRES_POOL_MIN, POSTGRES_PORT, POSTGRES_USER
from domain.table_naming import physical_table_name
from logger import log, log_step
PG_POOL = None

def _physical_rdbms_table_name(name: str) -> str:
    return physical_table_name(name, 'rdbms')

def write_to_delta(df, delta_path, partition_by=None, table_name=None):
    if not isinstance(delta_path, (str, os.PathLike)) or not str(delta_path):
        raise ValueError(f'write_to_delta: invalid delta_path={delta_path!r}')
    lake_root_norm = os.path.normpath(str(DELTA_PATH))

    def _sanitize_name(name: str) -> str:
        return physical_table_name(name, LAKE_TYPE)

    def _resolve_target_path(base_path: str, effective_table: str | None) -> tuple[str, str | None]:
        base_path = str(base_path)
        base_norm = os.path.normpath(base_path)
        if effective_table:
            sanitized_name = _sanitize_name(effective_table)
            base_name = os.path.basename(base_norm)
            if LAKE_TYPE != 'rdbms':
                if base_name.lower() == sanitized_name.lower():
                    return (base_path, sanitized_name)
            elif base_name == sanitized_name:
                return (base_path, sanitized_name)
            return (os.path.join(base_path, sanitized_name), sanitized_name)
        if base_norm == lake_root_norm:
            raise ValueError('write_to_delta: delta_path points to DELTA_PATH root but no table_name/filename provided; refusing to write into lake root.')
        if LAKE_TYPE != 'rdbms':
            parent = os.path.dirname(base_norm)
            leaf = os.path.basename(base_norm)
            leaf_lower = leaf.lower()
            if leaf != leaf_lower:
                base_path = os.path.join(parent, leaf_lower)
        return (base_path, None)
    with log_step(f'Write batch to Delta path {delta_path}'):

        def _write_single(sub_df, effective_table: str | None):
            target_path, sanitized_table = _resolve_target_path(delta_path, effective_table)
            if 'data_format' in sub_df.columns:
                sub_df = sub_df.drop('data_format')
            drop_cols = [column for column in ('source_filename', 'source_data_format') if column in sub_df.columns]
            if drop_cols:
                sub_df = sub_df.drop(*drop_cols)
            if LAKE_TYPE != 'rdbms':
                writer = sub_df.write.format('delta').mode('append')
                if partition_by and partition_by in sub_df.columns:
                    writer = writer.partitionBy(partition_by)
                writer.save(target_path)
                log.info(f'Written batch to Delta Lake at {target_path}')
            if LAKE_TYPE == 'rdbms':
                if sanitized_table is None:
                    raise ValueError('write_to_delta: rdbms mode requires table_name or filename to infer target table.')
                store_to_rdbms(sanitized_table, sub_df)
        if 'filename' in df.columns:
            filenames = [row['filename'] for row in df.select('filename').distinct().collect()]
            if len(filenames) == 1:
                filename = filenames[0]
                table = os.path.splitext(os.path.basename(filename))[0]
                _write_single(df.drop('filename'), table)
            else:
                for filename in filenames:
                    subset = df.where(F.col('filename') == filename).drop('filename')
                    table = os.path.splitext(os.path.basename(filename))[0]
                    _write_single(subset, table)
            return
        if table_name:
            _write_single(df, table_name)
            return
        if LAKE_TYPE == 'rdbms':
            raise ValueError('write_to_delta: rdbms mode requires table_name or filename to infer target table.')
        _write_single(df, None)

def init_postgres_pool(minconn=None, maxconn=None):
    with log_step('Initialize PostgreSQL connection pool'):
        global PG_POOL
        if minconn is None:
            minconn = POSTGRES_POOL_MIN
        if maxconn is None:
            maxconn = POSTGRES_POOL_MAX
        if PG_POOL is None:
            try:
                PG_POOL = SimpleConnectionPool(minconn, maxconn, host=POSTGRES_HOST, port=POSTGRES_PORT, dbname=POSTGRES_DB, user=POSTGRES_USER, password=POSTGRES_PASSWORD)
                log.info(f'PostgreSQL connection pool created (min={minconn}, max={maxconn}).')
            except Exception as exc:
                log.error(f'Error establishing PostgreSQL connection pool: {exc}')
                raise
        else:
            log.debug('Reusing existing PostgreSQL connection pool.')
        return PG_POOL

def get_postgres_connection():
    pool = init_postgres_pool()
    try:
        conn = pool.getconn()
        if conn.closed:
            log.warning('Received closed connection from pool; replacing it.')
            pool.putconn(conn, close=True)
            conn = pool.getconn()
        log.debug('Acquired connection from pool.')
        return conn
    except Exception as exc:
        message = str(exc)
        if 'connection pool exhausted' in message.lower():
            new_max = pool.maxconn + 5
            log.warning(f'Connection pool exhausted. Expanding pool to {new_max} connections.')
            try:
                pool.closeall()
            except Exception:
                pass
            init_postgres_pool(pool.minconn, new_max)
            pool = PG_POOL
            return pool.getconn()
        log.error(f'Error getting connection from pool: {exc}')
        raise

def release_postgres_connection(conn):
    pool = init_postgres_pool()
    try:
        if conn.closed:
            pool.putconn(conn, close=True)
            log.debug('Closed dead connection from pool.')
        else:
            pool.putconn(conn)
            log.debug('Released connection back to pool.')
    except Exception as exc:
        log.error(f'Error releasing connection: {exc}')

def store_to_rdbms(table_name, df, batch_size: int=5000):
    with log_step(f"store_to_rdbms for '{table_name}'"):
        if '.' in table_name:
            schema_name, tbl_name = table_name.split('.', 1)
        else:
            schema_name, tbl_name = ('public', table_name)
        tbl_name = _physical_rdbms_table_name(tbl_name)

        def _pg_identifier(name: str) -> str:
            return '"' + name.replace('"', '""') + '"'

        def _spark_to_pg_type(dt) -> str:
            from pyspark.sql.types import BooleanType, ByteType, DateType, DecimalType, DoubleType, FloatType, IntegerType, LongType, ShortType, TimestampType
            if isinstance(dt, (ByteType, ShortType, IntegerType)):
                return 'INTEGER'
            if isinstance(dt, LongType):
                return 'BIGINT'
            if isinstance(dt, (FloatType, DoubleType)):
                return 'DOUBLE PRECISION'
            if isinstance(dt, DecimalType):
                return f'NUMERIC({dt.precision},{dt.scale})'
            if isinstance(dt, BooleanType):
                return 'BOOLEAN'
            if isinstance(dt, DateType):
                return 'DATE'
            if isinstance(dt, TimestampType):
                return 'TIMESTAMP'
            return 'TEXT'
        cols = df.columns
        field_types = {field.name: field.dataType for field in df.schema.fields}
        pretty_types = ', '.join((f'{column}:{type(field_types[column]).__name__}' for column in cols))
        log.info(f'[DLH][{schema_name}.{tbl_name}] Batch schema ({len(cols)} cols): {pretty_types}')
        col_defs = ', '.join((f'{_pg_identifier(column)} {_spark_to_pg_type(field_types[column])}' for column in cols))
        create_schema_sql = f'CREATE SCHEMA IF NOT EXISTS {_pg_identifier(schema_name)};'
        create_table_sql = f'CREATE TABLE IF NOT EXISTS {_pg_identifier(schema_name)}.{_pg_identifier(tbl_name)} ({col_defs});'
        insert_cols_sql = ', '.join((_pg_identifier(column) for column in cols))
        insert_sql = f'INSERT INTO {_pg_identifier(schema_name)}.{_pg_identifier(tbl_name)} ({insert_cols_sql}) VALUES %s'
        log.info('[DLH][DDL] Ensure schema SQL:\n' + create_schema_sql + ' ')
        log.info(f'[DLH][DDL] Ensure table SQL for {schema_name}.{tbl_name}:\n' + create_table_sql + ' ')
        conn = None
        try:
            conn = get_postgres_connection()
            cur = conn.cursor()
            cur.execute(create_schema_sql)
            cur.execute(create_table_sql)
            conn.commit()
            cur.execute('\n                SELECT column_name\n                FROM information_schema.columns\n                WHERE table_schema = %s\n                  AND table_name = %s\n                ORDER BY ordinal_position\n                ', (schema_name, tbl_name))
            existing_cols = [row[0] for row in cur.fetchall()]
            log.info(f'[DLH][{schema_name}.{tbl_name}] Existing columns in DB ({len(existing_cols)}): {existing_cols}')
            log.info(f'[DLH][{schema_name}.{tbl_name}] Insert columns ({len(cols)}): {cols}')
            log.debug(f'[DLH][INSERT SQL] {insert_sql}')
            batch = []
            written = 0
            for row in df.select(*cols).toLocalIterator():
                batch.append(tuple((row[column] for column in cols)))
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
            log.info(f'Inserted {written} rows into {schema_name}.{tbl_name}')
        except Exception as exc:
            if conn:
                try:
                    conn.rollback()
                except Exception:
                    pass
            log.error(f"Error in store_to_rdbms for table '{table_name}': {exc}")
        finally:
            if conn:
                release_postgres_connection(conn)

def ensure_postgres_database():

    def _pg_ident(name: str) -> str:
        return '"' + name.replace('"', '""') + '"'
    with log_step('Ensure PostgreSQL database exists'):
        try:
            with closing(psycopg2.connect(host=POSTGRES_HOST, port=POSTGRES_PORT, dbname=POSTGRES_MAINTENANCE_DB, user=POSTGRES_USER, password=POSTGRES_PASSWORD)) as admin_conn:
                admin_conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
                with admin_conn.cursor() as cur:
                    cur.execute('SELECT 1 FROM pg_database WHERE datname = %s', (POSTGRES_DB,))
                    exists = cur.fetchone() is not None
                    if not exists:
                        create_sql = f"CREATE DATABASE {_pg_ident(POSTGRES_DB)} ENCODING 'UTF8' TEMPLATE template0"
                        log.info(f'[DLH][DDL] {create_sql};')
                        cur.execute(create_sql)
                        if POSTGRES_DB_OWNER:
                            alter_sql = f'ALTER DATABASE {_pg_ident(POSTGRES_DB)} OWNER TO {_pg_ident(POSTGRES_DB_OWNER)}'
                            log.info(f'[DLH][DDL] {alter_sql};')
                            cur.execute(alter_sql)
                    else:
                        log.info(f"Database '{POSTGRES_DB}' already exists.")
        except psycopg2.Error as exc:
            if getattr(exc, 'pgcode', None) == errorcodes.DUPLICATE_DATABASE:
                log.warning(f"Database '{POSTGRES_DB}' was created concurrently by another process.")
            else:
                log.critical(f"Unable to verify/create database '{POSTGRES_DB}'. Host={POSTGRES_HOST}:{POSTGRES_PORT} maint_db='{POSTGRES_MAINTENANCE_DB}' user='{POSTGRES_USER}'. Error: {exc}", exc_info=True)
                return False
        try:
            conn = get_postgres_connection()
            release_postgres_connection(conn)
            log.info(f"Database '{POSTGRES_DB}' is ready (verified via pool).")
            return True
        except Exception as exc:
            log.critical(f"Database '{POSTGRES_DB}' creation succeeded but pool verification failed: {exc}", exc_info=True)
            return False

def ensure_postgres_ready():
    if not ensure_postgres_database():
        raise RuntimeError('PostgreSQL database is not available and could not be created.')
    try:
        conn = get_postgres_connection()
        release_postgres_connection(conn)
        log.debug('PostgreSQL pool warmed successfully.')
    except Exception as exc:
        log.critical(f'Failed to warm PostgreSQL pool: {exc}', exc_info=True)
        raise
