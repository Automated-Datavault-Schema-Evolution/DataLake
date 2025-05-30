from logger import log


def write_to_delta(spark_df, output_path, mode='append', partition_by=None):
    try:
        writer = spark_df.write.format("delta").mode(mode)
        if partition_by:
            writer = writer.partitionBy(partition_by)

        writer.save(output_path)
        log.info(f"Data written to Delta file {output_path} with mode={mode}")
    except Exception as e:
        log.error(f"Failed to write to Delta file: {e}", exc_info=True)


def write_to_parquet(spark_df, output_path, mode='append', partition_by=None):
    try:
        writer = spark_df.write.format("delta").mode(mode)
        if partition_by:
            writer = writer.partitionBy(partition_by)

        writer.parquet(output_path)
        log.info(f"Data written to Parquet file {output_path} with mode={mode}")
    except Exception as e:
        log.error(f"Failed to write to Parquet file: {e}", exc_info=True)
