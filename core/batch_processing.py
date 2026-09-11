from __future__ import annotations
from logger import log
from pyspark.sql import functions as F
from config import DELTA_PATH, LAKE_TYPE
from domain.row_batches import date_like_columns, group_rows_by_table, materialized_columns, normalize_rows
from domain.table_naming import delta_target_path
from helper.delta_helper import write_to_delta
from helper.spark_helper import build_typed_df_from_rows

def process_batch(batch_df, batch_id):
    spark = batch_df.sparkSession
    log.debug(f'process_batch: batch_id={batch_id}, incoming columns={batch_df.columns}')
    if 'row' in batch_df.columns and 'data' not in batch_df.columns:
        batch_rows = batch_df
    elif 'data' in batch_df.columns and 'row' not in batch_df.columns:
        batch_rows = batch_df.withColumn('row', F.explode_outer(F.col('data'))).drop('data')
    else:
        raise ValueError(f"process_batch expected either 'row' (map) or 'data' (array<map>). Got columns: {batch_df.columns}")
    rows = batch_rows.withColumn('table_name', F.regexp_replace(F.element_at(F.split(F.regexp_replace(F.col('filename'), '\\\\', '/'), '/'), -1), '\\.[^.]+$', '')).select('table_name', F.col('row')).rdd.map(lambda record: (record['table_name'], record['row'])).collect()
    if not rows:
        log.info('process_batch: no rows in this micro-batch.')
        return
    rows_by_table = group_rows_by_table(rows)
    total_written = 0
    for logical_table_name, table_rows in rows_by_table.items():
        columns = materialized_columns(table_rows)
        if not columns:
            log.info(f"process_batch: table '{logical_table_name}' has no materialized keys in this batch.")
            continue
        local_df = build_typed_df_from_rows(spark, normalize_rows(table_rows, columns))
        for column_name in date_like_columns(columns):
            local_df = local_df.withColumn(column_name, F.when(F.col(column_name).cast('string').rlike('^\\d{4}-\\d{2}-\\d{2}$'), F.to_date(F.col(column_name))).otherwise(F.col(column_name)))
        local_df = local_df.withColumn('ingestion_timestamp', F.current_timestamp())
        final_columns = columns + ['ingestion_timestamp']
        if LAKE_TYPE == 'rdbms':
            dtypes = dict(local_df.dtypes)
            projections = [F.col(column_name) if column_name == 'ingestion_timestamp' or dtypes.get(column_name) in ('date', 'timestamp') else F.col(column_name).cast('string').alias(column_name) for column_name in final_columns]
            local_df = local_df.select(*projections)
        else:
            local_df = local_df.select(*[F.col(column_name) for column_name in final_columns])
        target_path = delta_target_path(DELTA_PATH, logical_table_name, LAKE_TYPE)
        row_count = local_df.count()
        log.info(f'[DLH][process_batch] logical_table={logical_table_name} columns={len(final_columns)} target_path={target_path}')
        write_to_delta(local_df, target_path, table_name=logical_table_name)
        total_written += row_count
    log.info(f'process_batch: batch_id={batch_id} wrote {total_written} total rows across {len(rows_by_table)} table(s).')
__all__ = ['process_batch']
