import os
import re

from logger import log, log_step
from pyspark.sql import SparkSession
from pyspark.sql import functions as F, DataFrame
from pyspark.sql.types import BooleanType, LongType, DoubleType, StringType, DateType, StructType, StructField

from config import (
    SPARK_MASTER,
    SPARK_DRIVER_MEMORY,
    SPARK_EXECUTOR_MEMORY,
    SPARK_DRIVER_CORES,
    SPARK_EXECUTOR_CORES,
    SPARK_SQL_SHUFFLE_PARTITIONS,
    SPARK_SERIALIZER,
    SPARK_KRYO_BUFFER_MAX,
    SPARK_ADAPTIVE_EXECUTION, DRIVER_PY, EXEC_PY, SPARK_IVY_PATH, SPARK_SQL_ADAPTIVE_COALESCE_PARTITIONS,
    SPARK_SQL_ADAPTIVE_ADVISORY_PARTITION_SIZE,
)

_date_rx = re.compile(r"^\d{4}-\d{2}-\d{2}$")  # yyyy-MM-dd


def get_spark_session(app_name="Kafka_Consumer_Lake_Handler"):
    with log_step(f"Initializing Spark session '{app_name}'"):
        spark_master = SPARK_MASTER
        builder = (
            SparkSession.builder
            .appName(app_name)
            .master(spark_master)
            # Python envs
            .config("spark.pyspark.driver.python", DRIVER_PY)
            .config("spark.pyspark.python", EXEC_PY)
            .config("spark.executorEnv.PYSPARK_PYTHON", EXEC_PY)
            .config("spark.ui.showConsoleProgress", "false")
            .config("spark.jars.ivy", SPARK_IVY_PATH)
            .config("spark.driver.extraJavaOptions", "-Duser.home=/tmp")
            .config("spark.executor.extraJavaOptions", "-Duser.home=/tmp")
            .config("spark.driver.memory", SPARK_DRIVER_MEMORY)
            .config("spark.executor.memory", SPARK_EXECUTOR_MEMORY)
            .config("spark.driver.cores", SPARK_DRIVER_CORES)
            .config("spark.executor.cores", SPARK_EXECUTOR_CORES)
            .config("spark.sql.shuffle.partitions", SPARK_SQL_SHUFFLE_PARTITIONS)
            .config("spark.serializer", SPARK_SERIALIZER)
            .config("spark.kryoserializer.buffer.max", SPARK_KRYO_BUFFER_MAX)
            .config("spark.sql.adaptive.enabled", str(SPARK_ADAPTIVE_EXECUTION).lower())
            .config("spark.sql.adaptive.coalescePartitions.enabled",
                    str(SPARK_SQL_ADAPTIVE_COALESCE_PARTITIONS).lower())
            .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", SPARK_SQL_ADAPTIVE_ADVISORY_PARTITION_SIZE)
            .config("spark.sql.streaming.noDataMicroBatches.enabled", "true")
            # Delta (JARs shipped directly)
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.sql.sources.default", "delta")
            # Streaming stability
            .config("spark.sql.streaming.kafka.useUninterruptibleThread", "true")
            # application ui
            .config("spark.ui.enabled", "true")
            .config("spark.ui.port", "4041")
            # timezone
            .config("spark.sql.session.timeZone", "Europe/Vienna")
        )

        # --- Ivy + user.home
        builder = (
            builder
            .config("spark.jars.ivy", SPARK_IVY_PATH or "/tmp/.ivy2")
            .config("spark.driver.extraJavaOptions", "-Duser.home=/tmp")
            .config("spark.executor.extraJavaOptions", "-Duser.home=/tmp")
        )

        # --- Delta + Kafka jars (mounted, offline-friendly) ---
        requested_jars = [
            "/opt/delta-jars/delta-spark_2.12-3.3.0.jar",
            "/opt/delta-jars/delta-storage-3.3.0.jar",
            "/opt/ext-jars/spark-sql-kafka-0-10_2.12-3.5.6.jar",
            "/opt/ext-jars/spark-token-provider-kafka-0-10_2.12-3.5.6.jar",
            "/opt/ext-jars/kafka-clients-3.5.1.jar",
            "/opt/jdbc-jars/postgresql-42.7.4.jar",
            "/opt/ext-jars/commons-pool2-2.12.1.jar"
        ]
        existing_jars = [p for p in requested_jars if os.path.exists(p)]
        for p in requested_jars:
            log.info(f"[JAR_CHECK] {p} exists={os.path.exists(p)}")
        if existing_jars:
            builder = builder.config("spark.jars", ",".join(existing_jars))
        else:
            log.warning("[JAR_CHECK] None of the expected jars were found under /opt/(delta-jars|ext-jars). "
                        "Spark may try Ivy (Maven) if ALLOW_MAVEN=1.")

        # Optional online fallback if ALLOW_MAVEN=1
        if os.getenv("ALLOW_MAVEN") == "1":
            builder = builder.config(
                "spark.jars.packages",
                ",".join([
                    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6",
                    "org.apache.kafka:kafka-clients:3.5.1",
                    "org.apache.spark:spark-token-provider-kafka-0-10_2.12:3.5.6",
                    "org.postgresql:postgresql:42.7.4",
                    "org.apache.commons:commons:2.12.1"
                ])
            )
        spark = builder.getOrCreate()
        log.debug(f"Spark configuration: {spark.sparkContext.getConf().getAll()}")
        return spark


def normalize_ingestion_timestamp(df):
    if "ingestion_timestamp" not in df.columns:
        return df
    # ISO-8601 with offsets parses with the generic to_timestamp(...)
    parsed = F.to_timestamp("ingestion_timestamp")
    return df.withColumn("ingestion_timestamp", F.date_trunc("second", parsed))


def normalize_date_columns_dynamic(
        df: DataFrame,
        date_regex: str = r"^\d{4}-\d{2}-\d{2}$",  # yyyy-MM-dd
        min_conf: float = 0.95,  # cast only if >=95% of non-null values look like dates
        fmt: str = "yyyy-MM-dd"
) -> DataFrame:
    # columns that are strings (skip known control fields)
    string_cols = [c for c, t in df.dtypes if t == "string"]
    if not string_cols:
        return df

    out = df
    for c in string_cols:
        # Compute fraction of non-null values that match the regex
        # (avoid miscasting ID-like strings)
        stats = out.select(
            F.sum(F.when(F.col(c).isNotNull(), F.lit(1)).otherwise(F.lit(0))).alias("nn"),
            F.sum(F.when(F.col(c).rlike(date_regex), F.lit(1)).otherwise(F.lit(0))).alias("hits")
        ).first()
        nn = stats["nn"] or 0
        hits = stats["hits"] or 0

        if nn > 0 and (hits / nn) >= min_conf:
            # Cast column to DateType; non-matching rows become NULL
            out = out.withColumn(c, F.to_date(F.col(c), fmt))
    return out


def _infer_spark_type(py_val):
    if isinstance(py_val, bool): return BooleanType()
    if isinstance(py_val, int):  return LongType()
    if isinstance(py_val, float): return DoubleType()
    if isinstance(py_val, str) and _date_rx.match(py_val): return DateType()
    # everything else -> string
    return StringType()


def build_typed_df_from_rows(spark, rows: list[dict]):
    """
    Build a DataFrame from list[dict] with a safe two-phase typing:
    1) Use Boolean/Long/Double where obvious; keep date-looking columns as STRING.
    2) After createDataFrame, cast date-looking columns to DateType with to_date().
    """
    if not rows:
        return spark.createDataFrame([], StructType([]))

    # union of keys (stable column order)
    all_cols = sorted({k for r in rows for k in r.keys()})
    # first non-null value per column
    first_vals = {c: next((r.get(c) for r in rows if r.get(c) is not None), None) for c in all_cols}

    # Decide schema types (NO DateType here to avoid Python object requirement)
    date_like_cols = set()
    fields = []
    for c in all_cols:
        v = first_vals[c]
        if isinstance(v, bool):
            fields.append(StructField(c, BooleanType(), True))
        elif isinstance(v, int):
            fields.append(StructField(c, LongType(), True))
        elif isinstance(v, float):
            fields.append(StructField(c, DoubleType(), True))
        elif isinstance(v, str) and _date_rx.match(v):
            # remember to cast later
            date_like_cols.add(c)
            fields.append(StructField(c, StringType(), True))
        else:
            fields.append(StructField(c, StringType(), True))

    schema = StructType(fields)

    # Normalize rows so every column exists
    norm_rows = [{**{c: None for c in all_cols}, **r} for r in rows]

    # Create DF with the safe schema
    df = spark.createDataFrame(norm_rows, schema=schema)

    # Now cast date-looking columns to DateType
    for c in date_like_cols:
        df = df.withColumn(c, F.to_date(F.col(c), "yyyy-MM-dd"))

    return df
