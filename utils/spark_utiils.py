import os
import re
from logger import log, log_step
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, DoubleType, LongType, StringType, StructField, StructType
from config import ALLOW_MAVEN, DRIVER_PY, EXEC_PY, SPARK_ADAPTIVE_EXECUTION, SPARK_DRIVER_CORES, SPARK_DRIVER_MEMORY, SPARK_EXECUTOR_CORES, SPARK_EXECUTOR_MEMORY, SPARK_IVY_PATH, SPARK_KRYO_BUFFER_MAX, SPARK_MASTER, SPARK_SERIALIZER, SPARK_SQL_ADAPTIVE_ADVISORY_PARTITION_SIZE, SPARK_SQL_ADAPTIVE_COALESCE_PARTITIONS, SPARK_SQL_SHUFFLE_PARTITIONS
_date_rx = re.compile('^\\d{4}-\\d{2}-\\d{2}$')

def get_spark_session(app_name='Kafka_Consumer_Lake_Handler'):
    with log_step(f"Initializing Spark session '{app_name}'"):
        builder = SparkSession.builder.appName(app_name).master(SPARK_MASTER).config('spark.pyspark.driver.python', DRIVER_PY).config('spark.pyspark.python', EXEC_PY).config('spark.executorEnv.PYSPARK_PYTHON', EXEC_PY).config('spark.ui.showConsoleProgress', 'false').config('spark.jars.ivy', SPARK_IVY_PATH or '/tmp/.ivy2').config('spark.driver.extraJavaOptions', '-Duser.home=/tmp').config('spark.executor.extraJavaOptions', '-Duser.home=/tmp').config('spark.driver.memory', SPARK_DRIVER_MEMORY).config('spark.executor.memory', SPARK_EXECUTOR_MEMORY).config('spark.driver.cores', SPARK_DRIVER_CORES).config('spark.executor.cores', SPARK_EXECUTOR_CORES).config('spark.sql.shuffle.partitions', SPARK_SQL_SHUFFLE_PARTITIONS).config('spark.serializer', SPARK_SERIALIZER).config('spark.kryoserializer.buffer.max', SPARK_KRYO_BUFFER_MAX).config('spark.sql.adaptive.enabled', str(SPARK_ADAPTIVE_EXECUTION).lower()).config('spark.sql.adaptive.coalescePartitions.enabled', str(SPARK_SQL_ADAPTIVE_COALESCE_PARTITIONS).lower()).config('spark.sql.adaptive.advisoryPartitionSizeInBytes', SPARK_SQL_ADAPTIVE_ADVISORY_PARTITION_SIZE).config('spark.sql.streaming.noDataMicroBatches.enabled', 'true').config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension').config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog').config('spark.sql.sources.default', 'delta').config('spark.sql.streaming.kafka.useUninterruptibleThread', 'true').config('spark.ui.enabled', 'true').config('spark.ui.port', '4041').config('spark.ui.reverseProxy', 'true').config('spark.ui.proxyBase', '/spark/dl/app').config('spark.sql.session.timeZone', 'Europe/Vienna')
        requested_jars = ['/opt/delta-jars/delta-spark_2.12-3.3.0.jar', '/opt/delta-jars/delta-storage-3.3.0.jar', '/opt/ext-jars/spark-sql-kafka-0-10_2.12-3.5.6.jar', '/opt/ext-jars/spark-token-provider-kafka-0-10_2.12-3.5.6.jar', '/opt/ext-jars/kafka-clients-3.5.1.jar', '/opt/jdbc-jars/postgresql-42.7.4.jar', '/opt/ext-jars/commons-pool2-2.12.1.jar']
        existing_jars = [path for path in requested_jars if os.path.exists(path)]
        for path in requested_jars:
            log.info(f'[DLH][JAR_CHECK] {path} exists={os.path.exists(path)}')
        if existing_jars:
            builder = builder.config('spark.jars', ','.join(existing_jars))
        else:
            log.warning('[DLH][JAR_CHECK] None of the expected jars were found under /opt/(delta-jars|ext-jars). Spark may try Ivy (Maven) if ALLOW_MAVEN=1.')
        if ALLOW_MAVEN:
            builder = builder.config('spark.jars.packages', ','.join(['org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6', 'org.apache.kafka:kafka-clients:3.5.1', 'org.apache.spark:spark-token-provider-kafka-0-10_2.12:3.5.6', 'org.postgresql:postgresql:42.7.4', 'org.apache.commons:commons:2.12.1']))
        spark = builder.getOrCreate()
        log.debug(f'Spark configuration: {spark.sparkContext.getConf().getAll()}')
        return spark

def normalize_ingestion_timestamp(df):
    if 'ingestion_timestamp' not in df.columns:
        return df
    parsed = F.to_timestamp('ingestion_timestamp')
    return df.withColumn('ingestion_timestamp', F.date_trunc('second', parsed))

def normalize_date_columns_dynamic(df: DataFrame, date_regex: str='^\\d{4}-\\d{2}-\\d{2}$', min_conf: float=0.95, fmt: str='yyyy-MM-dd') -> DataFrame:
    string_cols = [column for column, dtype in df.dtypes if dtype == 'string']
    if not string_cols:
        return df
    out = df
    for column in string_cols:
        stats = out.select(F.sum(F.when(F.col(column).isNotNull(), F.lit(1)).otherwise(F.lit(0))).alias('nn'), F.sum(F.when(F.col(column).rlike(date_regex), F.lit(1)).otherwise(F.lit(0))).alias('hits')).first()
        nn = stats['nn'] or 0
        hits = stats['hits'] or 0
        if nn > 0 and hits / nn >= min_conf:
            out = out.withColumn(column, F.to_date(F.col(column), fmt))
    return out

def build_typed_df_from_rows(spark, rows: list[dict]):
    if not rows:
        return spark.createDataFrame([], StructType([]))
    all_cols = sorted({key for row in rows for key in row.keys()})
    first_vals = {column: next((row.get(column) for row in rows if row.get(column) is not None), None) for column in all_cols}
    date_like_cols = set()
    fields = []
    for column in all_cols:
        value = first_vals[column]
        if isinstance(value, bool):
            fields.append(StructField(column, BooleanType(), True))
        elif isinstance(value, int):
            fields.append(StructField(column, LongType(), True))
        elif isinstance(value, float):
            fields.append(StructField(column, DoubleType(), True))
        elif isinstance(value, str) and _date_rx.match(value):
            date_like_cols.add(column)
            fields.append(StructField(column, StringType(), True))
        else:
            fields.append(StructField(column, StringType(), True))
    schema = StructType(fields)
    normalized_rows = [{**{column: None for column in all_cols}, **row} for row in rows]
    df = spark.createDataFrame(normalized_rows, schema=schema)
    for column in date_like_cols:
        df = df.withColumn(column, F.to_date(F.col(column), 'yyyy-MM-dd'))
    return df
