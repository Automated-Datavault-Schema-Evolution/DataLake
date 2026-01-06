import atexit
import signal
import sys
import time

from apscheduler.schedulers.background import BackgroundScheduler
from delta.tables import DeltaTable
from logger import log, log_step
from pyspark.sql import functions as F
from pyspark.sql.functions import from_json, col, explode_outer
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, MapType

from config import (
    KAFKA_TOPIC,
    DELTA_PATH,
    KAFKA_BOOTSTRAP_SERVERS,
    SCHEDULE_TYPE,
    SCHEDULE_CRON,
    SCHEDULE_INTERVAL_HOURS,
    PROCESSING_MODE,
    KAFKA_STARTING_OFFSETS,
    KAFKA_GROUP_ID,
    BACKLOG_BATCH_SIZE, CHECKPOINT_LOCATION, LAKE_TYPE, )
from utils.kafka_utils import get_kafka_consumer, sanity_check_kafka, get_topic_backlog
from utils.lake_utils import write_to_delta, store_to_rdbms, ensure_postgres_ready
from utils.parse_utils import parse_message_to_row
from utils.spark_utiils import get_spark_session, normalize_ingestion_timestamp, normalize_date_columns_dynamic, \
    build_typed_df_from_rows
from utils.spark_work_autoscaler import check_and_scale_workers, cleanup_workers


def process_batch_old(batch_df, epoch_id):
    """Write each micro-batch of the streaming query to Delta Lake."""
    from pyspark.sql.functions import col

    sample_row = batch_df.select("row").head()

    columns = list(sample_row["row"].keys()) if sample_row else []
    select_cols = [
                      col("filename"),
                      col("data_format"),
                      col("ingestion_timestamp"),
                  ] + [col("row")[k].alias(k) for k in columns]

    exploded_flat = batch_df.select(*select_cols)
    # Ensure proper TIMESTAMP (trimmed to seconds) before writing
    exploded_flat = normalize_ingestion_timestamp(exploded_flat)
    exploded_flat = normalize_date_columns_dynamic(exploded_flat)
    if exploded_flat.head(1):
        write_to_delta(exploded_flat, DELTA_PATH)
        log.info(f"Streaming batch written, epoch {epoch_id}")


def _delta_exists(spark, path: str) -> bool:
    try:
        return DeltaTable.isDeltaTable(spark, path)
    except Exception:
        return False


def _table_name_from_filename_col(col):
    # Extract base filename without extension from a path-like string
    # Works with "depots.csv", "s3://bucket/depots.csv", "dir\\depots.csv", etc.
    return F.regexp_extract(col, r'([^/\\]+?)(?:\.[^.]+)?$', 1)


def process_batch(batch_df, batch_id):
    """
    Build per-table micro-batch DataFrames strictly from the keys present
    in that table's records. We *hard scope* the final projection to only
    those keys (plus ingestion_timestamp) so no helper can introduce
    “global” columns by accident.
    """
    from pyspark.sql import functions as F
    from pyspark.sql import Row

    spark = batch_df.sparkSession

    log.debug(f"process_batch: batch_id={batch_id}, incoming columns={batch_df.columns}")

    # 1) Ensure we have one-record-per-row map as column "row"
    if "row" in batch_df.columns and "data" not in batch_df.columns:
        df = batch_df  # already flattened upstream
    elif "data" in batch_df.columns and "row" not in batch_df.columns:
        df = batch_df.withColumn("row", F.explode_outer(F.col("data"))).drop("data")
    else:
        raise ValueError(
            f"process_batch expected either 'row' (map) or 'data' (array<map>). "
            f"Got columns: {batch_df.columns}"
        )

    # 2) Derive table name from filename (basename without extension)
    df = df.withColumn(
        "table_name",
        F.regexp_extract(F.col("filename"), r"([^/\\]+?)(?:\.[^.]+)?$", 1)
    ).select(
        "partition", "offset", "filename", "data_format", "ingestion_timestamp",
        "table_name", "row"
    )

    # 3) Collect minimal payload to driver (table_name + map payload)
    rows = (df.select("table_name", F.col("row"))
            .rdd.map(lambda r: (r["table_name"], r["row"]))
            .collect())

    if not rows:
        log.info("process_batch: no rows in this micro-batch.")
        return

    from collections import defaultdict
    rows_by_table = defaultdict(list)
    for tbl, m in rows:
        payload = dict(m) if m is not None else {}
        rows_by_table[tbl].append(payload)

    # 4) Write each table independently (infer columns strictly per table)
    total_written = 0
    for table_name, tbl_rows in rows_by_table.items():
        # infer columns for THIS table only
        colset = [c for c in sorted({k for r in tbl_rows for k in (r.keys() if r else [])})]
        if not colset:
            log.info(f"process_batch: table '{table_name}' has no materialized keys in this batch.")
            continue

        # normalize rows to same key set
        normalized = [{c: r.get(c) for c in colset} for r in tbl_rows]

        # Build a DF from normalized rows
        spark_rows = [Row(**{c: (None if v == "" else v) for c, v in rec.items()})
                      for rec in normalized]
        local_df = spark.createDataFrame(spark_rows)

        # --- inline, safe date casting (NO new columns are created) ---
        # Only cast columns that actually exist and look like dates by name.
        dateish = [c for c in colset if c.lower().endswith("date") or c.lower() in {"dob", "dateofbirth"}]
        for c in dateish:
            # try cast strings like 'YYYY-MM-DD' to date
            local_df = local_df.withColumn(
                c,
                F.when(F.col(c).cast("string").rlike(r"^\d{4}-\d{2}-\d{2}$"), F.to_date(F.col(c)))
                .otherwise(F.col(c))
            )

        # Add ingestion_timestamp (computed now) and keep only intended columns
        local_df = local_df.withColumn("ingestion_timestamp", F.current_timestamp())

        # Cast non-date/timestamp to string for consistency, but *only* inside the final projection
        final_cols = colset + ["ingestion_timestamp"]
        select_exprs = []
        dtypes = dict(local_df.dtypes)
        for c in final_cols:
            t = dtypes.get(c)
            if c == "ingestion_timestamp" or t in ("date", "timestamp"):
                select_exprs.append(F.col(c))
            else:
                select_exprs.append(F.col(c).cast("string").alias(c))
        local_df = local_df.select(*select_exprs)

        # Log the per-table, per-batch column list we will actually write
        log.info(f"[process_batch] '{table_name}' final projected columns ({len(final_cols)}): {final_cols}")

        # Delta write (append)
        delta_path = f"{DELTA_PATH.rstrip('/')}/{table_name}"
        n = local_df.count()  # single action for logging
        log.info(f"Writing {n} rows to Delta table '{table_name}' at {delta_path}")
        write_to_delta(local_df, delta_path)

        # Fan-out to RDBMS ONLY when configured
        if LAKE_TYPE == "rdbms":
            try:
                store_to_rdbms(table_name, local_df)
            except Exception as e:
                log.error(f"store_to_rdbms failed for '{table_name}': {e}")

        total_written += n

    log.info(
        f"process_batch: batch_id={batch_id} wrote {total_written} total rows across {len(rows_by_table)} table(s).")


def bulk_ingest(spark, max_messages=None):
    """
    Drain buffered Kafka records in small per-poll micro-batches and write immediately,
    but keep schemas *per table* (derived from filename) to avoid a global merged schema.
    """
    import os
    from collections import defaultdict
    from pyspark.sql import functions as F

    def _table_from_filename(fname: str) -> str:
        if not fname:
            return "unknown"
        # strip dirs + extension -> 'loans' from '/path/loans.csv'
        base = os.path.basename(str(fname))
        return os.path.splitext(base)[0] or base

    with log_step("Bulk fallback: draining any buffered records from Kafka"):
        try:
            consumer = get_kafka_consumer()
            log.info(f"Kafka consumer created with servers: {KAFKA_BOOTSTRAP_SERVERS}")
        except Exception as e:
            log.error(f"Failed to create Kafka consumer: {e}")
            return

        total_rows_written = 0
        empty_polls = 0

        try:
            while empty_polls < 3 and (max_messages is None or total_rows_written < max_messages):
                # how much to pull this poll
                max_per_poll = BACKLOG_BATCH_SIZE
                if max_messages is not None:
                    max_per_poll = min(max_per_poll, max_messages - total_rows_written)

                log.debug(f"Polling Kafka (max_records={max_per_poll})...")
                try:
                    batch = consumer.poll(timeout_ms=1000, max_records=max_per_poll)
                except Exception as e:
                    log.error(f"Error polling Kafka: {e}")
                    break

                if not batch:
                    empty_polls += 1
                    log.debug(f"No messages polled. Empty polls so far: {empty_polls}")
                    continue

                empty_polls = 0

                # --- Collect rows for this poll, grouped by table ---
                by_table = defaultdict(list)
                msgs_in_poll = 0

                for tp, messages in batch.items():
                    log.debug(f"Polled {len(messages)} messages from partition {tp.partition}")
                    msgs_in_poll += len(messages)
                    for msg in messages:
                        parsed_rows = parse_message_to_row(msg) or []
                        v = msg.value or {}
                        fname = v.get("filename")
                        tbl = _table_from_filename(fname)

                        # attach envelope fields temporarily (routing only)
                        for r in parsed_rows:
                            r["filename"] = fname
                            r["data_format"] = v.get("data_format")

                        by_table[tbl].extend(parsed_rows)

                if not by_table:
                    continue

                # --- Process each table independently (per-table schema inference) ---
                poll_rows_written = 0
                for table_name, tbl_rows in by_table.items():
                    if not tbl_rows:
                        continue

                    # infer columns only for THIS table; exclude envelope fields from data columns
                    all_cols = sorted({
                        k for r in tbl_rows for k in r.keys()
                        if k not in ("filename", "data_format") and k is not None
                    })
                    if not all_cols:
                        log.info(f"[{table_name}] No materialized columns in this poll; skipping.")
                        continue

                    # normalize rows to the table's column set
                    base = {c: None for c in all_cols}
                    rows_norm = [{**base, **{k: v for k, v in r.items() if k in all_cols}} for r in tbl_rows]

                    # build + normalize dataframe
                    df_tbl = build_typed_df_from_rows(spark, rows_norm)
                    # add/normalize timestamps & dates
                    df_tbl = normalize_ingestion_timestamp(df_tbl)
                    df_tbl = normalize_date_columns_dynamic(df_tbl)

                    # cast non-date/timestamp columns to string (consistent with streaming path)
                    select_exprs = [
                        F.col(c) if (t in ("date", "timestamp") or c == "ingestion_timestamp")
                        else F.col(c).cast("string").alias(c)
                        for c, t in df_tbl.dtypes
                    ]
                    df_tbl = df_tbl.select(*select_exprs)

                    # compact tiny partitions to reduce file churn
                    num_parts = df_tbl.rdd.getNumPartitions()
                    df_tbl = df_tbl.coalesce(max(1, min(8, num_parts)))

                    # write to Delta per-table
                    delta_path = f"{DELTA_PATH.rstrip('/')}/{table_name}"
                    n = df_tbl.count()  # trigger once; useful for logging/metrics
                    log.info(f"[{table_name}] Writing {n} rows to Delta path {delta_path}")
                    write_to_delta(df_tbl, delta_path)

                    # Fan-out to RDBMS ONLY when configured
                    if LAKE_TYPE == "rdbms":
                        try:
                            store_to_rdbms(table_name, df_tbl)
                        except Exception as e:
                            log.error(f"store_to_rdbms failed for '{table_name}': {e}")

                    poll_rows_written += n
                    total_rows_written += n

                log.info(f"Bulk fallback wrote {poll_rows_written} rows across {len(by_table)} table(s) in this poll")

        finally:
            # best-effort offset commit and metrics after draining
            try:
                consumer.commit()
                log.debug("Bulk fallback committed offsets")
            except Exception as e:
                log.error(f"Failed to commit offsets after bulk ingestion: {e}")

            try:
                backlog = get_topic_backlog()
                log.info(f"Backlog after bulk ingestion: {backlog} messages")
            except Exception as e:
                log.error(f"Failed to get topic backlog: {e}")

            try:
                consumer.close()
                log.debug("Bulk fallback Kafka consumer closed")
            except Exception:
                pass

        log.info(f"Bulk fallback wrote {total_rows_written} rows to delta-lake")


def streaming_ingest(spark):
    with log_step("Start streaming ingestion"):
        log.debug(f"Kafka topic={KAFKA_TOPIC}, bootstrap={KAFKA_BOOTSTRAP_SERVERS}, group-id={KAFKA_GROUP_ID}")
        try:

            # Kafka message envelope schema
            schema = StructType([
                StructField("filename", StringType()),
                StructField("data_format", StringType()),
                StructField("ingestion_timestamp", StringType()),
                StructField("data", ArrayType(MapType(StringType(), StringType()))),
            ])
            log.debug(f"Streaming schema: {schema.simpleString()}")

            df = (
                spark.readStream.format("kafka")
                .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
                .option("subscribe", KAFKA_TOPIC)
                .option("startingOffsets", KAFKA_STARTING_OFFSETS)
                # group id is optional for Spark streaming; harmless to set:
                .option("kafka.group.id", KAFKA_GROUP_ID)
                .option("maxOffsetsPerTrigger", "5000")
                .option("failOnDataLoss", "false")
                .load()
            )
            log.debug(f"Connected to kafka server {KAFKA_BOOTSTRAP_SERVERS} and topic {KAFKA_TOPIC}")

            parsed = (
                df.selectExpr("partition", "offset", "CAST(value AS STRING) AS json_value")
                .select("partition", "offset", from_json(col("json_value"), schema).alias("msg"))
                .select(
                    col("partition"),
                    col("offset"),
                    col("msg.filename").alias("filename"),
                    col("msg.data_format").alias("data_format"),
                    col("msg.ingestion_timestamp").alias("ingestion_timestamp"),
                    col("msg.data").alias("rows"),
                )
            )

            flattened = (
                parsed.select(
                    "partition",
                    "offset",
                    "filename",
                    "data_format",
                    "ingestion_timestamp",
                    explode_outer(col("rows")).alias("row"),  # one map per output row
                )
            )

            # Feed micro-batches to our per-table writer
            query = (
                flattened.writeStream
                .trigger(processingTime="1 second")
                .foreachBatch(process_batch)
                .outputMode("append")
                .option("checkpointLocation", f"{CHECKPOINT_LOCATION}/by_table")
                .start()
            )
            return query

        except Exception as e:
            log.critical(f"STREAMING FAILURE: {e} — switching to bulk ingestion", exc_info=True)
            bulk_ingest(spark)
            raise


def schedule_bulk(spark):
    with log_step("Scheduling bulk ingestion"):
        scheduler = BackgroundScheduler()
        if SCHEDULE_TYPE == "cron":
            from apscheduler.triggers.cron import CronTrigger

            scheduler.add_job(
                lambda: bulk_ingest(spark), CronTrigger.from_crontab(SCHEDULE_CRON)
            )
            log.info(f"Scheduled bulk ingestion with CRON: {SCHEDULE_CRON}")
        else:
            scheduler.add_job(
                lambda: bulk_ingest(spark), 'interval', hours=SCHEDULE_INTERVAL_HOURS
            )
            log.info(f"Scheduled bulk ingestion every {SCHEDULE_INTERVAL_HOURS} hours")
        scheduler.start()


def main():
    with log_step("Starting Delta Lake Handler"):
        if LAKE_TYPE == "rdbms":
            ensure_postgres_ready()
        spark = get_spark_session()
        sanity_check_kafka()

        if PROCESSING_MODE == 'streaming':
            while True:
                backlog = get_topic_backlog()
                check_and_scale_workers()
                if backlog > 0:
                    log.info(
                        f"Auto consuming backlog of {backlog} messages"
                    )
                    while backlog > 0:
                        to_drain = min(backlog, BACKLOG_BATCH_SIZE)
                        log.info(f"Draining {to_drain} messages from backlog (auto mode)")
                        bulk_ingest(spark, max_messages=to_drain)
                        backlog = get_topic_backlog()
                        if backlog > 0:
                            log.info(f"{backlog} messages remain in backlog")
                    log.info("Re-checking backlog in 5s…")
                    time.sleep(5)
                    continue
                else:
                    break
            while True:
                try:
                    query = streaming_ingest(spark)  # Should return the query object
                    log.info("Streaming ingestion started. Awaiting termination...")
                    query.awaitTermination()
                except Exception:
                    log.error(
                        "STREAMING FAILED – falling back to bulk drain", exc_info=True
                    )
                    bulk_ingest(spark)
                    log.info("Re-starting streaming ingestion in 30s…")
                    time.sleep(30)
        elif PROCESSING_MODE == 'bulk':
            schedule_bulk(spark)
            # keep process alive for scheduled jobs
            while True:
                time.sleep(60)
        else:
            log.critical(
                f"Invalid MODE '{PROCESSING_MODE}' in .env. Use 'streaming' or 'bulk'."
            )


if __name__ == "__main__":
    import threading
    from lake_grpc_service import serve as serve_lake_grpc

    # Event to signal the gRPC server to stop
    grpc_stop_event = threading.Event()

    # Start Lake gRPC server in a background daemon thread
    grpc_thread = threading.Thread(
        target=serve_lake_grpc,
        args=(grpc_stop_event,),
        daemon=True,
        name="lake-grpc-server",
    )
    grpc_thread.start()
    log.info("Lake gRPC server thread started.")


    def _shutdown_handler(signum=None, frame=None):
        log.info("Application stopping, signaling Lake gRPC server and cleaning up Spark workers")

        # Signal the gRPC server to stop
        try:
            grpc_stop_event.set()
        except Exception:
            pass

        # Give the gRPC thread a chance to exit
        try:
            if grpc_thread.is_alive():
                grpc_thread.join(timeout=5)
        except Exception:
            pass

        # Existing cleanup
        cleanup_workers()

        if signum is not None:
            sys.exit(0)


    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, _shutdown_handler)

    # Keep the original atexit registration for Spark worker cleanup
    atexit.register(cleanup_workers)

    # Start the existing ingestion / streaming logic
    main()
