from __future__ import annotations
import json
import threading
import time
from concurrent import futures
from typing import List, Dict, Any, Tuple
import grpc
from delta.tables import DeltaTable
from logger import log, log_step
from config import DELTA_PATH, LAKE_HANDLER_GRPC_MAX_WORKERS, LAKE_HANDLER_GRPC_PORT, LAKE_TYPE
from proto import sef_handlers_pb2 as pb
from proto import sef_handlers_pb2_grpc as pb_grpc
from utils.lake_utils import get_postgres_connection, release_postgres_connection
from utils.spark_utiils import get_spark_session
_SPARK = None

def _normalize_table_name(name: str) -> str:
    safe = _sanitize_table_name(name)
    return safe if LAKE_TYPE == 'rdbms' else safe.lower()

def _rdbms_table_exists(cur, schema_name: str, table_name: str) -> bool:
    cur.execute('\n        SELECT 1\n        FROM information_schema.tables\n        WHERE table_schema = %s\n          AND table_name = %s LIMIT 1\n        ', (schema_name, table_name))
    return cur.fetchone() is not None

def _is_missing_relation_error(exc: Exception) -> bool:
    pgcode = getattr(exc, 'pgcode', None)
    if not pgcode and hasattr(exc, 'orig'):
        pgcode = getattr(exc.orig, 'pgcode', None)
    if pgcode == '42P01':
        return True
    msg = str(exc).lower()
    if 'does not exist' in msg and ('relation' in msg or 'table' in msg):
        return True
    return False

def _is_transient_db_error(exc: Exception) -> bool:
    msg = str(exc).lower()
    transient_markers = ['could not connect', 'connection refused', 'connection reset', 'terminating connection', 'the database system is starting up', 'timeout', 'timed out', 'temporary failure', 'no route to host', 'server closed the connection']
    return any((m in msg for m in transient_markers))

def _handle_drop_column_rdbms(operation: pb.Operation) -> pb.OperationResult:
    params = dict(operation.params)
    table_name = operation.target or params.get('table_name')
    column_name = params.get('column_name') or params.get('attribute')
    if not table_name:
        return pb.OperationResult(correlation_id=operation.correlation_id, plan_id=operation.plan_id, idempotency_key=operation.idempotency_key, status=pb.OPERATION_STATUS_PERMANENT_ERROR, error_code='MISSING_TARGET', error_message='target (table_name) is required for OPERATION_DROP_COLUMN')
    if not column_name:
        return pb.OperationResult(correlation_id=operation.correlation_id, plan_id=operation.plan_id, idempotency_key=operation.idempotency_key, status=pb.OPERATION_STATUS_PERMANENT_ERROR, error_code='MISSING_PARAM', error_message='column_name parameter is required for OPERATION_DROP_COLUMN')
    evidence_id = _make_evidence_id(operation)
    conn = None
    try:
        conn = get_postgres_connection()
        with conn.cursor() as cur:
            cur.execute("\n                SELECT 1\n                FROM information_schema.columns\n                WHERE table_schema = 'public'\n                  AND table_name = %s\n                  AND column_name = %s LIMIT 1\n                ", (table_name, column_name))
            exists = cur.fetchone() is not None
            if not exists:
                return pb.OperationResult(correlation_id=operation.correlation_id, plan_id=operation.plan_id, idempotency_key=operation.idempotency_key, status=pb.OPERATION_STATUS_ALREADY_APPLIED, error_code='', error_message='', evidence_snapshot_id=evidence_id)
            cur.execute(f'ALTER TABLE "public"."{table_name}" DROP COLUMN "{column_name}"')
        conn.commit()
        return pb.OperationResult(correlation_id=operation.correlation_id, plan_id=operation.plan_id, idempotency_key=operation.idempotency_key, status=pb.OPERATION_STATUS_OK, error_code='', error_message='', evidence_snapshot_id=evidence_id)
    except Exception as exc:
        if conn:
            try:
                conn.rollback()
            except Exception:
                pass
        return pb.OperationResult(correlation_id=operation.correlation_id, plan_id=operation.plan_id, idempotency_key=operation.idempotency_key, status=pb.OPERATION_STATUS_PERMANENT_ERROR, error_code='LAKE_DROP_COLUMN_FAILED', error_message=str(exc), evidence_snapshot_id=evidence_id)
    finally:
        if conn:
            release_postgres_connection(conn)

def _handle_drop_column_delta(operation: pb.Operation) -> pb.OperationResult:
    params = dict(operation.params)
    table_name = operation.target or params.get('table_name')
    column_name = params.get('column_name') or params.get('attribute')
    if not table_name:
        return pb.OperationResult(correlation_id=operation.correlation_id, plan_id=operation.plan_id, idempotency_key=operation.idempotency_key, status=pb.OPERATION_STATUS_PERMANENT_ERROR, error_code='MISSING_TARGET', error_message='target (table_name) is required for OPERATION_DROP_COLUMN')
    if not column_name:
        return pb.OperationResult(correlation_id=operation.correlation_id, plan_id=operation.plan_id, idempotency_key=operation.idempotency_key, status=pb.OPERATION_STATUS_PERMANENT_ERROR, error_code='MISSING_PARAM', error_message='column_name parameter is required for OPERATION_DROP_COLUMN')
    evidence_id = _make_evidence_id(operation)
    spark = _get_spark()
    path = _delta_table_path(table_name)
    if not _delta_exists(spark, path):
        return pb.OperationResult(correlation_id=operation.correlation_id, plan_id=operation.plan_id, idempotency_key=operation.idempotency_key, status=pb.OPERATION_STATUS_TRANSIENT_ERROR, error_code='DELTA_NOT_FOUND', error_message=f'Delta table not found at {path}', evidence_snapshot_id=evidence_id)
    try:
        df = spark.read.format('delta').load(path)
        cols = df.columns
        if column_name not in cols:
            return pb.OperationResult(correlation_id=operation.correlation_id, plan_id=operation.plan_id, idempotency_key=operation.idempotency_key, status=pb.OPERATION_STATUS_ALREADY_APPLIED, error_code='', error_message='', evidence_snapshot_id=evidence_id)
        kept = [c for c in cols if c != column_name]
        out = df.select(*kept)
        out.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').save(path)
        return pb.OperationResult(correlation_id=operation.correlation_id, plan_id=operation.plan_id, idempotency_key=operation.idempotency_key, status=pb.OPERATION_STATUS_OK, error_code='', error_message='', evidence_snapshot_id=evidence_id)
    except Exception as exc:
        return pb.OperationResult(correlation_id=operation.correlation_id, plan_id=operation.plan_id, idempotency_key=operation.idempotency_key, status=pb.OPERATION_STATUS_PERMANENT_ERROR, error_code='DELTA_DROP_COLUMN_FAILED', error_message=str(exc), evidence_snapshot_id=evidence_id)

def _handle_drop_column(operation: pb.Operation) -> pb.OperationResult:
    if LAKE_TYPE == 'rdbms':
        return _handle_drop_column_rdbms(operation)
    return _handle_drop_column_delta(operation)

def _handle_change_type_delta(operation: pb.Operation) -> pb.OperationResult:
    params = dict(operation.params)
    table_name = operation.target or params.get('table_name')
    column_name = params.get('column_name')
    to_logical_type = params.get('to_logical_type') or params.get('logical_type')
    evidence_id = _make_evidence_id(operation)
    if not table_name:
        return pb.OperationResult(correlation_id=operation.correlation_id, plan_id=operation.plan_id, idempotency_key=operation.idempotency_key, status=pb.OPERATION_STATUS_PERMANENT_ERROR, error_code='MISSING_TARGET', error_message='target (table_name) is required for OPERATION_CHANGE_TYPE', evidence_snapshot_id=evidence_id)
    if not column_name or not to_logical_type:
        return pb.OperationResult(correlation_id=operation.correlation_id, plan_id=operation.plan_id, idempotency_key=operation.idempotency_key, status=pb.OPERATION_STATUS_PERMANENT_ERROR, error_code='MISSING_PARAM', error_message='column_name and to_logical_type are required for OPERATION_CHANGE_TYPE', evidence_snapshot_id=evidence_id)
    spark = _get_spark()
    table_name_norm = _normalize_table_name(table_name)
    path = f"{DELTA_PATH.rstrip('/')}/{table_name_norm}"

    def _norm(t: str) -> str:
        t = (t or '').strip().lower()
        if t in ('int', 'integer'):
            return 'integer'
        if t == 'long':
            return 'bigint'
        if t.startswith('decimal'):
            return 'decimal'
        return t
    target_type = _norm(_map_logical_to_spark_type(to_logical_type))
    with log_step(f"LakeHandler CHANGE_TYPE (Delta) for table '{table_name}' at path '{path}'"):
        if not _delta_exists(spark, path):
            msg = f'Delta table not ready at path: {path}'
            log.warning(msg)
            return pb.OperationResult(correlation_id=operation.correlation_id, plan_id=operation.plan_id, idempotency_key=operation.idempotency_key, status=pb.OPERATION_STATUS_TRANSIENT_ERROR, error_code='DELTA_TABLE_NOT_READY', error_message=msg, evidence_snapshot_id=evidence_id)
        try:
            df = spark.read.format('delta').load(path)
        except Exception as exc:
            msg = f'Failed to read Delta table at {path}: {exc}'
            log.warning(msg)
            return pb.OperationResult(correlation_id=operation.correlation_id, plan_id=operation.plan_id, idempotency_key=operation.idempotency_key, status=pb.OPERATION_STATUS_TRANSIENT_ERROR, error_code='DELTA_READ_TRANSIENT', error_message=msg, evidence_snapshot_id=evidence_id)
        field = next((f for f in df.schema.fields if f.name.lower() == str(column_name).strip().lower()), None)
        if field is None:
            msg = f'Column {column_name} not ready on Delta table {table_name}'
            log.warning(msg)
            return pb.OperationResult(correlation_id=operation.correlation_id, plan_id=operation.plan_id, idempotency_key=operation.idempotency_key, status=pb.OPERATION_STATUS_TRANSIENT_ERROR, error_code='DELTA_COLUMN_NOT_READY', error_message=msg, evidence_snapshot_id=evidence_id)
        current_type = _norm(field.dataType.simpleString())
        if current_type == target_type or (current_type.startswith('decimal') and target_type == 'decimal'):
            return pb.OperationResult(correlation_id=operation.correlation_id, plan_id=operation.plan_id, idempotency_key=operation.idempotency_key, status=pb.OPERATION_STATUS_ALREADY_APPLIED, error_code='', error_message='', evidence_snapshot_id=evidence_id)
        col_ident = '`' + str(column_name).replace('`', '``') + '`'
        ddl_errors = []
        ddl_1 = f'ALTER TABLE delta.`{path}` ALTER COLUMN {col_ident} TYPE {target_type}'
        ddl_2 = f'ALTER TABLE delta.`{path}` CHANGE COLUMN {col_ident} {col_ident} {target_type}'
        for ddl in (ddl_1, ddl_2):
            try:
                log.info(f'Executing Delta ALTER TABLE: {ddl}')
                spark.sql(ddl)
                return pb.OperationResult(correlation_id=operation.correlation_id, plan_id=operation.plan_id, idempotency_key=operation.idempotency_key, status=pb.OPERATION_STATUS_OK, error_code='', error_message='', evidence_snapshot_id=evidence_id)
            except Exception as exc:
                ddl_errors.append(f'{ddl} -> {exc}')
        try:
            from pyspark.sql import functions as F
            log.warning(f'Delta ALTER COLUMN failed; attempting rewrite-cast fallback for {table_name_norm}.{column_name}: {current_type} -> {target_type}')
            casted = df.withColumn(str(column_name), F.col(str(column_name)).cast(target_type))
            casted.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').save(path)
            df_check = spark.read.format('delta').load(path)
            new_field = next((f for f in df_check.schema.fields if f.name.lower() == str(column_name).strip().lower()), None)
            if new_field is None:
                raise RuntimeError(f'Post-rewrite schema missing column {column_name}')
            new_type = _norm(new_field.dataType.simpleString())
            if new_type != target_type and (not (new_type.startswith('decimal') and target_type == 'decimal')):
                raise RuntimeError(f'Post-rewrite type mismatch: got={new_type} expected={target_type}')
            return pb.OperationResult(correlation_id=operation.correlation_id, plan_id=operation.plan_id, idempotency_key=operation.idempotency_key, status=pb.OPERATION_STATUS_OK, error_code='', error_message='', evidence_snapshot_id=evidence_id)
        except Exception as exc:
            ddl_errors.append(f'REWRITE_CAST -> {exc}')
            return pb.OperationResult(correlation_id=operation.correlation_id, plan_id=operation.plan_id, idempotency_key=operation.idempotency_key, status=pb.OPERATION_STATUS_PERMANENT_ERROR, error_code='DELTA_CHANGE_TYPE_FAILED', error_message='; '.join(ddl_errors), evidence_snapshot_id=evidence_id)

def _get_spark():
    global _SPARK
    if _SPARK is None:
        _SPARK = get_spark_session('LakeHandler_GRPC')
    return _SPARK

def _make_evidence_id(operation: pb.Operation) -> str:
    table = operation.target or 'unknown'
    plan_id = operation.plan_id or 'no_plan'
    idem = operation.idempotency_key or 'no_idem'
    return f'lake:{table}:{plan_id}:{idem}'

def _sanitize_table_name(name: str) -> str:
    safe = (name or '').replace('.', '_').replace('-', '_')
    if LAKE_TYPE != 'rdbms':
        safe = safe.lower()
    return safe

def _delta_table_path(table_name: str) -> str:
    safe = _sanitize_table_name(table_name)
    return os.path.join(DELTA_PATH, safe)

def _map_logical_to_pg_type(logical_type: str | None) -> str:
    if not logical_type:
        return 'TEXT'
    t = logical_type.lower()
    if 'bool' in t:
        return 'BOOLEAN'
    if 'bigint' in t or 'long' in t:
        return 'BIGINT'
    if 'int' in t:
        return 'INTEGER'
    if 'double' in t or 'float' in t or 'real' in t:
        return 'DOUBLE PRECISION'
    if 'decimal' in t or 'numeric' in t:
        return 'NUMERIC'
    if 'date' in t and 'time' not in t:
        return 'DATE'
    if 'timestamp' in t or 'datetime' in t or 'time' in t:
        return 'TIMESTAMP'
    return 'TEXT'

def _map_logical_to_spark_type(logical_type: str | None) -> str:
    if not logical_type:
        return 'string'
    t = logical_type.lower()
    if 'bool' in t:
        return 'boolean'
    if 'bigint' in t or 'long' in t:
        return 'bigint'
    if 'int' in t:
        return 'int'
    if 'double' in t or 'float' in t or 'real' in t:
        return 'double'
    if 'decimal' in t or 'numeric' in t:
        return 'decimal(38,10)'
    if 'date' in t and 'time' not in t:
        return 'date'
    if 'timestamp' in t or 'datetime' in t or 'time' in t:
        return 'timestamp'
    return 'string'

def _delta_exists(spark, path: str) -> bool:
    try:
        return DeltaTable.isDeltaTable(spark, path)
    except Exception:
        return False

def _handle_add_column_delta(operation: pb.Operation) -> pb.OperationResult:
    params = dict(operation.params)
    column_name = params.get('column_name')
    logical_type = params.get('logical_type')
    if not column_name:
        return pb.OperationResult(correlation_id=operation.correlation_id, plan_id=operation.plan_id, idempotency_key=operation.idempotency_key, status=pb.OPERATION_STATUS_PERMANENT_ERROR, error_code='MISSING_PARAM', error_message='column_name parameter is required for OPERATION_ADD_COLUMN')
    table_name = operation.target or params.get('table_name')
    if not table_name:
        return pb.OperationResult(correlation_id=operation.correlation_id, plan_id=operation.plan_id, idempotency_key=operation.idempotency_key, status=pb.OPERATION_STATUS_PERMANENT_ERROR, error_code='MISSING_TARGET', error_message='target (table_name) is required for OPERATION_ADD_COLUMN')
    spark = _get_spark()
    path = _delta_table_path(table_name)
    with log_step(f"LakeHandler ADD_COLUMN (Delta) for table '{table_name}' at path '{path}'"):
        if not _delta_exists(spark, path):
            msg = f'Delta table not ready at path: {path}'
            log.warning(msg)
            return pb.OperationResult(correlation_id=operation.correlation_id, plan_id=operation.plan_id, idempotency_key=operation.idempotency_key, status=pb.OPERATION_STATUS_TRANSIENT_ERROR, error_code='DELTA_TABLE_NOT_READY', error_message=msg, evidence_snapshot_id=_make_evidence_id(operation))
        df = spark.read.format('delta').load(path)
        existing_cols = [f.name for f in df.schema.fields]
        if column_name in existing_cols:
            log.info(f'Column {column_name} already exists in Delta table {table_name}; treating as ALREADY_APPLIED')
            return pb.OperationResult(correlation_id=operation.correlation_id, plan_id=operation.plan_id, idempotency_key=operation.idempotency_key, status=pb.OPERATION_STATUS_ALREADY_APPLIED, error_code='', error_message='', evidence_snapshot_id=_make_evidence_id(operation))
        spark_type = _map_logical_to_spark_type(logical_type)
        alter_sql = f'ALTER TABLE delta.`{path}` ADD COLUMNS ({column_name} {spark_type})'
        log.info(f'Executing Delta ALTER TABLE: {alter_sql}')
        try:
            spark.sql(alter_sql)
            status = pb.OPERATION_STATUS_OK
            error_code = ''
            error_message = ''
        except Exception as exc:
            log.exception(f'Delta ALTER TABLE failed for {table_name}: {exc}')
            status = pb.OPERATION_STATUS_PERMANENT_ERROR
            error_code = 'DELTA_ALTER_FAILED'
            error_message = str(exc)
    return pb.OperationResult(correlation_id=operation.correlation_id, plan_id=operation.plan_id, idempotency_key=operation.idempotency_key, status=status, error_code=error_code, error_message=error_message, evidence_snapshot_id=_make_evidence_id(operation))

def _handle_add_column_rdbms(operation: pb.Operation) -> pb.OperationResult:
    params = dict(operation.params)
    table_name = operation.target or params.get('table_name')
    column_name = params.get('column_name') or params.get('attribute')
    logical_type = params.get('to_logical_type') or params.get('logical_type')
    schema_name = params.get('schema_name') or 'public'
    if not table_name:
        return pb.OperationResult(correlation_id=operation.correlation_id, plan_id=operation.plan_id, idempotency_key=operation.idempotency_key, status=pb.OPERATION_STATUS_PERMANENT_ERROR, error_code='MISSING_TARGET', error_message='target (table_name) is required for OPERATION_ADD_COLUMN')
    if not column_name:
        return pb.OperationResult(correlation_id=operation.correlation_id, plan_id=operation.plan_id, idempotency_key=operation.idempotency_key, status=pb.OPERATION_STATUS_PERMANENT_ERROR, error_code='MISSING_PARAM', error_message='column_name parameter is required for OPERATION_ADD_COLUMN')
    evidence_id = _make_evidence_id(operation)
    type_map = {'string': 'TEXT', 'text': 'TEXT', 'integer': 'INTEGER', 'int': 'INTEGER', 'bigint': 'BIGINT', 'float': 'DOUBLE PRECISION', 'double': 'DOUBLE PRECISION', 'boolean': 'BOOLEAN', 'bool': 'BOOLEAN', 'timestamp': 'TIMESTAMP', 'timestamptz': 'TIMESTAMPTZ', 'date': 'DATE'}
    sql_type = type_map.get(str(logical_type).strip().lower(), 'TEXT') if logical_type else 'TEXT'
    conn = None
    try:
        conn = get_postgres_connection()
        with conn.cursor() as cur:
            if not _rdbms_table_exists(cur, schema_name, table_name):
                return pb.OperationResult(correlation_id=operation.correlation_id, plan_id=operation.plan_id, idempotency_key=operation.idempotency_key, status=pb.OPERATION_STATUS_TRANSIENT_ERROR, error_code='RDBMS_TABLE_NOT_READY', error_message=f'Table {schema_name}.{table_name} not found yet', evidence_snapshot_id=evidence_id)
            cur.execute('\n                SELECT 1\n                FROM information_schema.columns\n                WHERE table_schema = %s\n                  AND table_name = %s\n                  AND column_name = %s LIMIT 1\n                ', (schema_name, table_name, column_name))
            if cur.fetchone() is not None:
                return pb.OperationResult(correlation_id=operation.correlation_id, plan_id=operation.plan_id, idempotency_key=operation.idempotency_key, status=pb.OPERATION_STATUS_ALREADY_APPLIED, error_code='', error_message='', evidence_snapshot_id=evidence_id)
            cur.execute(f'ALTER TABLE "{schema_name}"."{table_name}" ADD COLUMN "{column_name}" {sql_type}')
        conn.commit()
        return pb.OperationResult(correlation_id=operation.correlation_id, plan_id=operation.plan_id, idempotency_key=operation.idempotency_key, status=pb.OPERATION_STATUS_OK, error_code='', error_message='', evidence_snapshot_id=evidence_id)
    except Exception as exc:
        if conn:
            try:
                conn.rollback()
            except Exception:
                pass
        if _is_missing_relation_error(exc) or _is_transient_db_error(exc):
            status = pb.OPERATION_STATUS_TRANSIENT_ERROR
            error_code = 'RDBMS_ADD_COLUMN_TRANSIENT'
        else:
            status = pb.OPERATION_STATUS_PERMANENT_ERROR
            error_code = 'RDBMS_ADD_COLUMN_FAILED'
        return pb.OperationResult(correlation_id=operation.correlation_id, plan_id=operation.plan_id, idempotency_key=operation.idempotency_key, status=status, error_code=error_code, error_message=str(exc), evidence_snapshot_id=evidence_id)
    finally:
        if conn:
            release_postgres_connection(conn)

def _handle_add_column(operation: pb.Operation) -> pb.OperationResult:
    if LAKE_TYPE == 'rdbms':
        return _handle_add_column_rdbms(operation)
    else:
        return _handle_add_column_delta(operation)

def _handle_change_type_rdbms(operation: pb.Operation) -> pb.OperationResult:
    params = dict(operation.params)
    table_name = operation.target or params.get('table_name')
    column_name = params.get('column_name') or params.get('attribute')
    to_logical_type = params.get('to_logical_type') or params.get('logical_type')
    schema_name = params.get('schema_name') or 'public'
    if not table_name:
        return pb.OperationResult(correlation_id=operation.correlation_id, plan_id=operation.plan_id, idempotency_key=operation.idempotency_key, status=pb.OPERATION_STATUS_PERMANENT_ERROR, error_code='MISSING_TARGET', error_message='target (table_name) is required for OPERATION_CHANGE_TYPE')
    if not column_name or not to_logical_type:
        return pb.OperationResult(correlation_id=operation.correlation_id, plan_id=operation.plan_id, idempotency_key=operation.idempotency_key, status=pb.OPERATION_STATUS_PERMANENT_ERROR, error_code='MISSING_PARAM', error_message='column_name and to_logical_type are required for OPERATION_CHANGE_TYPE')
    evidence_id = _make_evidence_id(operation)
    type_map = {'string': 'TEXT', 'text': 'TEXT', 'integer': 'INTEGER', 'int': 'INTEGER', 'bigint': 'BIGINT', 'float': 'DOUBLE PRECISION', 'double': 'DOUBLE PRECISION', 'boolean': 'BOOLEAN', 'bool': 'BOOLEAN', 'timestamp': 'TIMESTAMP', 'timestamptz': 'TIMESTAMPTZ', 'date': 'DATE'}
    sql_type = type_map.get(str(to_logical_type).strip().lower(), 'TEXT')
    conn = None
    try:
        conn = get_postgres_connection()
        with conn.cursor() as cur:
            if not _rdbms_table_exists(cur, schema_name, table_name):
                return pb.OperationResult(correlation_id=operation.correlation_id, plan_id=operation.plan_id, idempotency_key=operation.idempotency_key, status=pb.OPERATION_STATUS_TRANSIENT_ERROR, error_code='RDBMS_TABLE_NOT_READY', error_message=f'Table {schema_name}.{table_name} not found yet', evidence_snapshot_id=evidence_id)
            cur.execute('\n                SELECT 1\n                FROM information_schema.columns\n                WHERE table_schema = %s\n                  AND table_name = %s\n                  AND column_name = %s LIMIT 1\n                ', (schema_name, table_name, column_name))
            if cur.fetchone() is None:
                return pb.OperationResult(correlation_id=operation.correlation_id, plan_id=operation.plan_id, idempotency_key=operation.idempotency_key, status=pb.OPERATION_STATUS_TRANSIENT_ERROR, error_code='RDBMS_COLUMN_NOT_READY', error_message=f'Column {column_name} not found yet on {schema_name}.{table_name}', evidence_snapshot_id=evidence_id)
            ident_schema = '"' + schema_name.replace('"', '""') + '"'
            ident_table = '"' + table_name.replace('"', '""') + '"'
            ident_col = '"' + column_name.replace('"', '""') + '"'
            target_type = str(sql_type).upper()
            numeric_types = {'INTEGER', 'BIGINT', 'DOUBLE PRECISION', 'NUMERIC', 'REAL', 'DOUBLE'}
            if target_type in numeric_types:
                using_expr = f"NULLIF({ident_col}, '')::{sql_type}"
            else:
                using_expr = f'{ident_col}::{sql_type}'
            cur.execute(f'ALTER TABLE {ident_schema}.{ident_table} ALTER COLUMN {ident_col} TYPE {sql_type} USING {using_expr}')
        conn.commit()
        return pb.OperationResult(correlation_id=operation.correlation_id, plan_id=operation.plan_id, idempotency_key=operation.idempotency_key, status=pb.OPERATION_STATUS_OK, error_code='', error_message='', evidence_snapshot_id=evidence_id)
    except Exception as exc:
        if conn:
            try:
                conn.rollback()
            except Exception:
                pass
        if _is_missing_relation_error(exc) or _is_transient_db_error(exc):
            status = pb.OPERATION_STATUS_TRANSIENT_ERROR
            error_code = 'RDBMS_CHANGE_TYPE_TRANSIENT'
        else:
            status = pb.OPERATION_STATUS_PERMANENT_ERROR
            error_code = 'RDBMS_CHANGE_TYPE_FAILED'
        return pb.OperationResult(correlation_id=operation.correlation_id, plan_id=operation.plan_id, idempotency_key=operation.idempotency_key, status=status, error_code=error_code, error_message=str(exc), evidence_snapshot_id=evidence_id)
    finally:
        if conn:
            release_postgres_connection(conn)

def _handle_change_type(operation: pb.Operation) -> pb.OperationResult:
    if LAKE_TYPE == 'rdbms':
        return _handle_change_type_rdbms(operation)
    return _handle_change_type_delta(operation)

def _introspect_delta_table(table_name: str) -> Tuple[List[pb.TableDescriptor], Dict[str, Any]]:
    spark = _get_spark()
    path = _delta_table_path(table_name)
    if not _delta_exists(spark, path):
        log.warning(f'IntrospectEvidence: Delta table not found at {path}')
        return ([], {'backend': 'delta', 'table_name': table_name, 'exists': False, 'path': path})
    df = spark.read.format('delta').load(path)
    fields = df.schema.fields
    attrs = [pb.AttributeDescriptor(name=f.name, logical_type=f.dataType.simpleString(), physical_type=f.dataType.simpleString(), nullable=f.nullable) for f in fields]
    td = pb.TableDescriptor(name=table_name, attributes=attrs)
    raw = {'backend': 'delta', 'table_name': table_name, 'path': path, 'columns': [f.name for f in fields], 'schema': [f.simpleString() for f in fields]}
    return ([td], raw)

def _introspect_rdbms_table(table_name: str) -> Tuple[List[pb.TableDescriptor], Dict[str, Any]]:
    if '.' in table_name:
        schema_name, tbl_name = table_name.split('.', 1)
    else:
        schema_name, tbl_name = ('public', table_name)
    conn = None
    rows = []
    try:
        conn = get_postgres_connection()
        cur = conn.cursor()
        cur.execute('\n            SELECT column_name, data_type, is_nullable\n            FROM information_schema.columns\n            WHERE table_schema = %s\n              AND table_name = %s\n            ORDER BY ordinal_position\n            ', (schema_name, tbl_name))
        rows = cur.fetchall()
        cur.close()
    except Exception as exc:
        log.exception(f'IntrospectEvidence: failed to read information_schema for {schema_name}.{tbl_name}: {exc}')
    finally:
        if conn:
            release_postgres_connection(conn)
    if not rows:
        raw = {'backend': 'rdbms', 'schema': schema_name, 'table_name': tbl_name, 'exists': False}
        return ([], raw)
    attrs = [pb.AttributeDescriptor(name=name, logical_type=db_type, physical_type=db_type, nullable=nullable == 'YES') for name, db_type, nullable in rows]
    td = pb.TableDescriptor(name=f'{schema_name}.{tbl_name}', attributes=attrs)
    raw = {'backend': 'rdbms', 'schema': schema_name, 'table_name': tbl_name, 'exists': True, 'columns': [r[0] for r in rows]}
    return ([td], raw)

class LakeHandlerService(pb_grpc.LakeHandlerServicer):

    def ApplyOperations(self, request: pb.OperationBatch, context: grpc.ServicerContext) -> pb.OperationBatchResult:
        results: List[pb.OperationResult] = []
        for op in request.operations:
            kind_name = pb.OperationKind.Name(op.kind)
            layer_name = pb.Layer.Name(op.layer)
            log.info(f'LakeHandler.ApplyOperations: plan_id={op.plan_id} correlation_id={op.correlation_id} layer={layer_name} target={op.target} kind={kind_name} params={dict(op.params)}')
            if op.layer != pb.LAYER_LAKE:
                result = pb.OperationResult(correlation_id=op.correlation_id, plan_id=op.plan_id, idempotency_key=op.idempotency_key, status=pb.OPERATION_STATUS_ALREADY_APPLIED, error_code='WRONG_LAYER', error_message=f'Operation layer {layer_name} not handled by LakeHandler')
            elif op.kind == pb.OPERATION_ADD_COLUMN:
                result = _handle_add_column(op)
            elif op.kind == pb.OPERATION_CHANGE_TYPE:
                result = _handle_change_type(op)
            elif op.kind == pb.OPERATION_DROP_COLUMN:
                result = _handle_drop_column(op)
            else:
                msg = f'Operation kind {kind_name} not supported by LakeHandler'
                log.error(msg)
                result = pb.OperationResult(correlation_id=op.correlation_id, plan_id=op.plan_id, idempotency_key=op.idempotency_key, status=pb.OPERATION_STATUS_PERMANENT_ERROR, error_code='UNSUPPORTED_OPERATION', error_message=msg)
            results.append(result)
        return pb.OperationBatchResult(results=results)

    def IntrospectEvidence(self, request: pb.EvidenceRequest, context: grpc.ServicerContext) -> pb.EvidenceResponse:
        table_name = _normalize_table_name(request.dataset_id)
        log.info(f'LakeHandler.IntrospectEvidence: plan_id={request.plan_id} correlation_id={request.correlation_id} dataset_id={table_name}')
        if LAKE_TYPE == 'rdbms':
            tables, raw = _introspect_rdbms_table(table_name)
        else:
            tables, raw = _introspect_delta_table(table_name)
        return pb.EvidenceResponse(correlation_id=request.correlation_id, plan_id=request.plan_id, tables=tables, vault_structures=[], raw_evidence_json=json.dumps(raw))

def serve(stop_event: 'threading.Event | None'=None) -> None:
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=LAKE_HANDLER_GRPC_MAX_WORKERS))
    pb_grpc.add_LakeHandlerServicer_to_server(LakeHandlerService(), server)
    listen_addr = f'[::]:{LAKE_HANDLER_GRPC_PORT}'
    server.add_insecure_port(listen_addr)
    log.info(f'Starting Lake gRPC handler on {listen_addr}')
    server.start()
    log.info('Lake gRPC handler started; waiting for requests from SEF core.')
    if stop_event is None:
        server.wait_for_termination()
    else:
        try:
            while not stop_event.is_set():
                time.sleep(0.5)
        finally:
            log.info('Lake gRPC stop_event set, stopping server...')
            server.stop(grace=5)
            log.info('Lake gRPC server stopped.')
if __name__ == '__main__':
    serve()
