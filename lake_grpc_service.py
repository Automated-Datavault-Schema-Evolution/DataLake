from __future__ import annotations

import json
import os
import threading
import time
from concurrent import futures
from typing import List, Dict, Any, Tuple

import grpc
from delta.tables import DeltaTable
from logger import log, log_step

from config import LAKE_TYPE, DELTA_PATH
from proto import sef_handlers_pb2 as pb
from proto import sef_handlers_pb2_grpc as pb_grpc
from utils.lake_utils import get_postgres_connection, release_postgres_connection
from utils.spark_utiils import get_spark_session

_SPARK = None


def _get_spark():
    """
    Lazily create and cache a Spark session for the gRPC server.
    """
    global _SPARK
    if _SPARK is None:
        _SPARK = get_spark_session("LakeHandler_GRPC")
    return _SPARK


def _make_evidence_id(operation: pb.Operation) -> str:
    """
    Build an evidence id for an operation.
    """
    table = operation.target or "unknown"
    plan_id = operation.plan_id or "no_plan"
    idem = operation.idempotency_key or "no_idem"
    return f"lake:{table}:{plan_id}:{idem}"


def _sanitize_table_name(name: str) -> str:
    """
    Mirror the sanitisation used in write_to_delta: replace dots/dashes with underscores.
    """
    return name.replace(".", "_").replace("-", "_")


def _delta_table_path(table_name: str) -> str:
    """
    Compute the Delta path for a logical table name.
    """
    safe = _sanitize_table_name(table_name)
    return os.path.join(DELTA_PATH, safe)


def _map_logical_to_pg_type(logical_type: str | None) -> str:
    """
    Best-effort mapping from logical type label to a PostgreSQL column type.
    """
    if not logical_type:
        return "TEXT"
    t = logical_type.lower()
    if "bool" in t:
        return "BOOLEAN"
    if "bigint" in t or "long" in t:
        return "BIGINT"
    if "int" in t:
        return "INTEGER"
    if "double" in t or "float" in t or "real" in t:
        return "DOUBLE PRECISION"
    if "decimal" in t or "numeric" in t:
        return "NUMERIC"
    if "date" in t and "time" not in t:
        return "DATE"
    if "timestamp" in t or "datetime" in t or "time" in t:
        return "TIMESTAMP"
    return "TEXT"


def _map_logical_to_spark_type(logical_type: str | None) -> str:
    """
    Best-effort mapping from logical type label to a Spark SQL type string.
    """
    if not logical_type:
        return "string"
    t = logical_type.lower()
    if "bool" in t:
        return "boolean"
    if "bigint" in t or "long" in t:
        return "bigint"
    if "int" in t:
        return "int"
    if "double" in t or "float" in t or "real" in t:
        return "double"
    if "decimal" in t or "numeric" in t:
        return "decimal(38,10)"
    if "date" in t and "time" not in t:
        return "date"
    if "timestamp" in t or "datetime" in t or "time" in t:
        return "timestamp"
    return "string"


def _delta_exists(spark, path: str) -> bool:
    """
    Check whether a Delta table exists at the given path.
    """
    try:
        return DeltaTable.isDeltaTable(spark, path)
    except Exception:
        return False


# ---------------------------------------------------------------------------
# Operation handlers
# ---------------------------------------------------------------------------


def _handle_add_column_delta(operation: pb.Operation) -> pb.OperationResult:
    """
    Apply ADD_COLUMN to a Delta Lake-backed table.
    """
    params = dict(operation.params)
    column_name = params.get("column_name")
    logical_type = params.get("logical_type")

    if not column_name:
        return pb.OperationResult(
            correlation_id=operation.correlation_id,
            plan_id=operation.plan_id,
            idempotency_key=operation.idempotency_key,
            status=pb.OPERATION_STATUS_PERMANENT_ERROR,
            error_code="MISSING_PARAM",
            error_message="column_name parameter is required for OPERATION_ADD_COLUMN",
        )

    table_name = operation.target or params.get("table_name")
    if not table_name:
        return pb.OperationResult(
            correlation_id=operation.correlation_id,
            plan_id=operation.plan_id,
            idempotency_key=operation.idempotency_key,
            status=pb.OPERATION_STATUS_PERMANENT_ERROR,
            error_code="MISSING_TARGET",
            error_message="target (table_name) is required for OPERATION_ADD_COLUMN",
        )

    spark = _get_spark()
    path = _delta_table_path(table_name)

    with log_step(f"LakeHandler ADD_COLUMN (Delta) for table '{table_name}' at path '{path}'"):
        if not _delta_exists(spark, path):
            msg = f"Delta table path does not exist: {path}"
            log.error(msg)
            return pb.OperationResult(
                correlation_id=operation.correlation_id,
                plan_id=operation.plan_id,
                idempotency_key=operation.idempotency_key,
                status=pb.OPERATION_STATUS_PERMANENT_ERROR,
                error_code="LAKE_TABLE_NOT_FOUND",
                error_message=msg,
            )

        # Inspect existing schema
        df = spark.read.format("delta").load(path)
        existing_cols = [f.name for f in df.schema.fields]

        if column_name in existing_cols:
            log.info(
                "Column %s already exists in Delta table %s; treating as ALREADY_APPLIED",
                column_name,
                table_name,
            )
            return pb.OperationResult(
                correlation_id=operation.correlation_id,
                plan_id=operation.plan_id,
                idempotency_key=operation.idempotency_key,
                status=pb.OPERATION_STATUS_ALREADY_APPLIED,
                error_code="",
                error_message="",
                evidence_snapshot_id=_make_evidence_id(operation),
            )

        spark_type = _map_logical_to_spark_type(logical_type)
        alter_sql = f"ALTER TABLE delta.`{path}` ADD COLUMNS ({column_name} {spark_type})"
        log.info("Executing Delta ALTER TABLE: %s", alter_sql)

        try:
            spark.sql(alter_sql)
            status = pb.OPERATION_STATUS_OK
            error_code = ""
            error_message = ""
        except Exception as exc:
            log.exception("Delta ALTER TABLE failed for %s: %s", table_name, exc)
            status = pb.OPERATION_STATUS_PERMANENT_ERROR
            error_code = "DELTA_ALTER_FAILED"
            error_message = str(exc)

    return pb.OperationResult(
        correlation_id=operation.correlation_id,
        plan_id=operation.plan_id,
        idempotency_key=operation.idempotency_key,
        status=status,
        error_code=error_code,
        error_message=error_message,
        evidence_snapshot_id=_make_evidence_id(operation),
    )


def _handle_add_column_rdbms(operation: pb.Operation) -> pb.OperationResult:
    """
    Apply ADD_COLUMN to a PostgreSQL-backed lake table.
    """
    params = dict(operation.params)
    column_name = params.get("column_name")
    logical_type = params.get("logical_type")

    if not column_name:
        return pb.OperationResult(
            correlation_id=operation.correlation_id,
            plan_id=operation.plan_id,
            idempotency_key=operation.idempotency_key,
            status=pb.OPERATION_STATUS_PERMANENT_ERROR,
            error_code="MISSING_PARAM",
            error_message="column_name parameter is required for OPERATION_ADD_COLUMN",
        )

    table_name = operation.target or params.get("table_name")
    if not table_name:
        return pb.OperationResult(
            correlation_id=operation.correlation_id,
            plan_id=operation.plan_id,
            idempotency_key=operation.idempotency_key,
            status=pb.OPERATION_STATUS_PERMANENT_ERROR,
            error_code="MISSING_TARGET",
            error_message="target (table_name) is required for OPERATION_ADD_COLUMN",
        )

    # Parse optional schema in table_name
    if "." in table_name:
        schema_name, tbl_name = table_name.split(".", 1)
    else:
        schema_name, tbl_name = "public", table_name

    pg_type = _map_logical_to_pg_type(logical_type)
    evidence_id = _make_evidence_id(operation)

    with log_step(f"LakeHandler ADD_COLUMN (RDBMS) for {schema_name}.{tbl_name}"):
        conn = None
        try:
            conn = get_postgres_connection()
            cur = conn.cursor()

            # Check if column already exists
            cur.execute(
                """
                SELECT 1
                FROM information_schema.columns
                WHERE table_schema = %s
                  AND table_name = %s
                  AND column_name = %s
                """,
                (schema_name, tbl_name, column_name),
            )
            if cur.fetchone():
                log.info(
                    "Column %s already exists in %s.%s; treating as ALREADY_APPLIED",
                    column_name,
                    schema_name,
                    tbl_name,
                )
                cur.close()
                conn.commit()
                return pb.OperationResult(
                    correlation_id=operation.correlation_id,
                    plan_id=operation.plan_id,
                    idempotency_key=operation.idempotency_key,
                    status=pb.OPERATION_STATUS_ALREADY_APPLIED,
                    error_code="",
                    error_message="",
                    evidence_snapshot_id=evidence_id,
                )

            # Build and execute ALTER TABLE
            ident_schema = '"' + schema_name.replace('"', '""') + '"'
            ident_table = '"' + tbl_name.replace('"', '""') + '"'
            ident_col = '"' + column_name.replace('"', '""') + '"'
            alter_sql = f"ALTER TABLE {ident_schema}.{ident_table} ADD COLUMN {ident_col} {pg_type}"

            log.info("Executing PostgreSQL ALTER TABLE: %s", alter_sql)
            cur.execute(alter_sql)
            cur.close()
            conn.commit()

            status = pb.OPERATION_STATUS_OK
            error_code = ""
            error_message = ""

        except Exception as exc:
            log.exception("PostgreSQL ALTER TABLE failed for %s.%s: %s", schema_name, tbl_name, exc)
            if conn:
                try:
                    conn.rollback()
                except Exception:
                    pass
            status = pb.OPERATION_STATUS_PERMANENT_ERROR
            error_code = "RDBMS_ALTER_FAILED"
            error_message = str(exc)
        finally:
            if conn:
                release_postgres_connection(conn)

    return pb.OperationResult(
        correlation_id=operation.correlation_id,
        plan_id=operation.plan_id,
        idempotency_key=operation.idempotency_key,
        status=status,
        error_code=error_code,
        error_message=error_message,
        evidence_snapshot_id=evidence_id,
    )


def _handle_add_column(operation: pb.Operation) -> pb.OperationResult:
    """
    Dispatch ADD_COLUMN to the correct backend.
    """
    if LAKE_TYPE == "rdbms":
        return _handle_add_column_rdbms(operation)
    else:
        return _handle_add_column_delta(operation)


def _handle_change_type_rdbms(operation: pb.Operation) -> pb.OperationResult:
    params = dict(operation.params)
    column_name = params.get("column_name")
    logical_type = params.get("to_logical_type") or params.get("logical_type")

    if not column_name:
        return pb.OperationResult(
            correlation_id=operation.correlation_id,
            plan_id=operation.plan_id,
            idempotency_key=operation.idempotency_key,
            status=pb.OPERATION_STATUS_PERMANENT_ERROR,
            error_code="MISSING_PARAM",
            error_message="column_name parameter is required for OPERATION_CHANGE_TYPE",
        )

    table_name = operation.target or params.get("table_name")
    if not table_name:
        return pb.OperationResult(
            correlation_id=operation.correlation_id,
            plan_id=operation.plan_id,
            idempotency_key=operation.idempotency_key,
            status=pb.OPERATION_STATUS_PERMANENT_ERROR,
            error_code="MISSING_TARGET",
            error_message="target (table_name) is required for OPERATION_CHANGE_TYPE",
        )

    if "." in table_name:
        schema_name, tbl_name = table_name.split(".", 1)
    else:
        schema_name, tbl_name = "public", table_name

    pg_type = _map_logical_to_pg_type(logical_type)
    evidence_id = _make_evidence_id(operation)

    with log_step(f"LakeHandler CHANGE_TYPE (RDBMS) for {schema_name}.{tbl_name}.{column_name}"):
        conn = None
        try:
            conn = get_postgres_connection()
            cur = conn.cursor()

            cur.execute(
                """
                SELECT udt_name
                FROM information_schema.columns
                WHERE table_schema = %s
                  AND table_name = %s
                  AND column_name = %s
                """,
                (schema_name, tbl_name, column_name),
            )
            row = cur.fetchone()
            if not row:
                cur.close()
                conn.commit()
                return pb.OperationResult(
                    correlation_id=operation.correlation_id,
                    plan_id=operation.plan_id,
                    idempotency_key=operation.idempotency_key,
                    status=pb.OPERATION_STATUS_PERMANENT_ERROR,
                    error_code="COLUMN_NOT_FOUND",
                    error_message=f"Column {column_name} not found in {schema_name}.{tbl_name}",
                    evidence_snapshot_id=evidence_id,
                )

            current_udt = (row[0] or "").lower()
            comparable = current_udt
            if comparable in {"varchar", "bpchar"}:
                comparable = "text"
            if comparable == "int4":
                comparable = "integer"
            if comparable == "int8":
                comparable = "bigint"
            if comparable == "float8":
                comparable = "double precision"

            if comparable == pg_type.lower():
                cur.close()
                conn.commit()
                return pb.OperationResult(
                    correlation_id=operation.correlation_id,
                    plan_id=operation.plan_id,
                    idempotency_key=operation.idempotency_key,
                    status=pb.OPERATION_STATUS_ALREADY_APPLIED,
                    error_code="",
                    error_message="",
                    evidence_snapshot_id=evidence_id,
                )

            ident_schema = '"' + schema_name.replace('"', '""') + '"'
            ident_table = '"' + tbl_name.replace('"', '""') + '"'
            ident_col = '"' + column_name.replace('"', '""') + '"'

            alter_sql = (
                f"ALTER TABLE {ident_schema}.{ident_table} "
                f"ALTER COLUMN {ident_col} TYPE {pg_type} "
                f"USING {ident_col}::{pg_type}"
            )
            cur.execute(alter_sql)
            cur.close()
            conn.commit()

            status = pb.OPERATION_STATUS_OK
            error_code = ""
            error_message = ""

        except Exception as exc:
            if conn:
                try:
                    conn.rollback()
                except Exception:
                    pass
            status = pb.OPERATION_STATUS_PERMANENT_ERROR
            error_code = "RDBMS_ALTER_FAILED"
            error_message = str(exc)
        finally:
            if conn:
                release_postgres_connection(conn)

    return pb.OperationResult(
        correlation_id=operation.correlation_id,
        plan_id=operation.plan_id,
        idempotency_key=operation.idempotency_key,
        status=status,
        error_code=error_code,
        error_message=error_message,
        evidence_snapshot_id=evidence_id,
    )


def _handle_change_type_delta(operation: pb.Operation) -> pb.OperationResult:
    kind_name = pb.OperationKind.Name(operation.kind)
    return pb.OperationResult(
        correlation_id=operation.correlation_id,
        plan_id=operation.plan_id,
        idempotency_key=operation.idempotency_key,
        status=pb.OPERATION_STATUS_PERMANENT_ERROR,
        error_code="UNSUPPORTED_BACKEND",
        error_message=f"{kind_name} not implemented for Delta backend",
        evidence_snapshot_id=_make_evidence_id(operation),
    )


def _handle_change_type(operation: pb.Operation) -> pb.OperationResult:
    if LAKE_TYPE == "rdbms":
        return _handle_change_type_rdbms(operation)
    else:
        return _handle_change_type_delta(operation)


# ---------------------------------------------------------------------------
# Evidence introspection
# ---------------------------------------------------------------------------


def _introspect_delta_table(table_name: str) -> Tuple[List[pb.TableDescriptor], Dict[str, Any]]:
    """
    Build TableDescriptor information for a Delta-backed table.
    """
    spark = _get_spark()
    path = _delta_table_path(table_name)

    if not _delta_exists(spark, path):
        log.warning("IntrospectEvidence: Delta table not found at %s", path)
        return [], {"backend": "delta", "table_name": table_name, "exists": False, "path": path}

    df = spark.read.format("delta").load(path)
    fields = df.schema.fields

    attrs = [
        pb.AttributeDescriptor(
            name=f.name,
            logical_type=f.dataType.simpleString(),
            physical_type=f.dataType.simpleString(),
            nullable=f.nullable,
        )
        for f in fields
    ]

    td = pb.TableDescriptor(
        name=table_name,
        attributes=attrs,
    )

    raw = {
        "backend": "delta",
        "table_name": table_name,
        "path": path,
        "columns": [f.name for f in fields],
        "schema": [f.simpleString() for f in fields],
    }
    return [td], raw


def _introspect_rdbms_table(table_name: str) -> Tuple[List[pb.TableDescriptor], Dict[str, Any]]:
    """
    Build TableDescriptor information for a PostgreSQL-backed table.
    """
    if "." in table_name:
        schema_name, tbl_name = table_name.split(".", 1)
    else:
        schema_name, tbl_name = "public", table_name

    conn = None
    rows = []
    try:
        conn = get_postgres_connection()
        cur = conn.cursor()
        cur.execute(
            """
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_schema = %s
              AND table_name = %s
            ORDER BY ordinal_position
            """,
            (schema_name, tbl_name),
        )
        rows = cur.fetchall()
        cur.close()
    except Exception as exc:
        log.exception(
            "IntrospectEvidence: failed to read information_schema for %s.%s: %s",
            schema_name,
            tbl_name,
            exc,
        )
    finally:
        if conn:
            release_postgres_connection(conn)

    if not rows:
        raw = {
            "backend": "rdbms",
            "schema": schema_name,
            "table_name": tbl_name,
            "exists": False,
        }
        return [], raw

    attrs = [
        pb.AttributeDescriptor(
            name=name,
            logical_type=db_type,
            physical_type=db_type,
            nullable=(nullable == "YES"),
        )
        for (name, db_type, nullable) in rows
    ]

    td = pb.TableDescriptor(
        name=f"{schema_name}.{tbl_name}",
        attributes=attrs,
    )

    raw = {
        "backend": "rdbms",
        "schema": schema_name,
        "table_name": tbl_name,
        "exists": True,
        "columns": [r[0] for r in rows],
    }
    return [td], raw


# ---------------------------------------------------------------------------
# gRPC service implementation
# ---------------------------------------------------------------------------


class LakeHandlerService(pb_grpc.LakeHandlerServicer):
    """
    gRPC implementation for the Lake handler.

    Supports:
      - OPERATION_ADD_COLUMN for both Delta and RDBMS lake backends.
    """

    def ApplyOperations(
            self,
            request: pb.OperationBatch,
            context: grpc.ServicerContext,
    ) -> pb.OperationBatchResult:
        results: List[pb.OperationResult] = []

        for op in request.operations:
            kind_name = pb.OperationKind.Name(op.kind)
            layer_name = pb.Layer.Name(op.layer)

            log.info(
                "LakeHandler.ApplyOperations: plan_id=%s correlation_id=%s layer=%s target=%s kind=%s params=%s",
                op.plan_id,
                op.correlation_id,
                layer_name,
                op.target,
                kind_name,
                dict(op.params),
            )

            if op.layer != pb.LAYER_LAKE:
                result = pb.OperationResult(
                    correlation_id=op.correlation_id,
                    plan_id=op.plan_id,
                    idempotency_key=op.idempotency_key,
                    status=pb.OPERATION_STATUS_ALREADY_APPLIED,
                    error_code="WRONG_LAYER",
                    error_message=f"Operation layer {layer_name} not handled by LakeHandler",
                )
            elif op.kind == pb.OPERATION_ADD_COLUMN:
                result = _handle_add_column(op)
            elif op.kind == pb.OPERATION_CHANGE_TYPE:
                result = _handle_change_type(op)
            else:
                msg = f"Operation kind {kind_name} not supported by LakeHandler"
                log.error(msg)
                result = pb.OperationResult(
                    correlation_id=op.correlation_id,
                    plan_id=op.plan_id,
                    idempotency_key=op.idempotency_key,
                    status=pb.OPERATION_STATUS_PERMANENT_ERROR,
                    error_code="UNSUPPORTED_OPERATION",
                    error_message=msg,
                )

            results.append(result)

        return pb.OperationBatchResult(results=results)

    def IntrospectEvidence(
            self,
            request: pb.EvidenceRequest,
            context: grpc.ServicerContext,
    ) -> pb.EvidenceResponse:
        table_name = request.dataset_id

        log.info(
            "LakeHandler.IntrospectEvidence: plan_id=%s correlation_id=%s dataset_id=%s",
            request.plan_id,
            request.correlation_id,
            table_name,
        )

        if LAKE_TYPE == "rdbms":
            tables, raw = _introspect_rdbms_table(table_name)
        else:
            tables, raw = _introspect_delta_table(table_name)

        return pb.EvidenceResponse(
            correlation_id=request.correlation_id,
            plan_id=request.plan_id,
            tables=tables,
            vault_structures=[],  # lake handler does not report vault structures
            raw_evidence_json=json.dumps(raw),
        )


def serve(stop_event: "threading.Event | None" = None) -> None:
    """
    Start the Lake gRPC server.

    If stop_event is None, this will block with server.wait_for_termination()
    and can be used as a standalone entrypoint.

    If stop_event is provided, this function will return when the event is set,
    stopping the server gracefully. This is suitable for running in a
    background thread from main.py.
    """
    port = int(os.getenv("LAKE_HANDLER_GRPC_PORT", "50051"))
    max_workers = int(os.getenv("LAKE_HANDLER_GRPC_MAX_WORKERS", "10"))

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=max_workers))
    pb_grpc.add_LakeHandlerServicer_to_server(LakeHandlerService(), server)

    listen_addr = f"[::]:{port}"
    server.add_insecure_port(listen_addr)
    log.info("Starting Lake gRPC handler on %s", listen_addr)

    server.start()
    log.info("Lake gRPC handler started; waiting for requests from SEF core.")

    if stop_event is None:
        # Standalone mode
        server.wait_for_termination()
    else:
        # Cooperative shutdown mode
        try:
            while not stop_event.is_set():
                time.sleep(0.5)
        finally:
            log.info("Lake gRPC stop_event set, stopping server...")
            server.stop(grace=5)
            log.info("Lake gRPC server stopped.")


if __name__ == "__main__":
    serve()
