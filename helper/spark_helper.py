"""Spark helper facade.

This module is intentionally thin: it centralizes Spark-related utilities
without changing behavior.
"""

from utils.spark_utiils import (
    get_spark_session,
    normalize_ingestion_timestamp,
    normalize_date_columns_dynamic,
    build_typed_df_from_rows,
)


__all__ = [
    "get_spark_session",
    "normalize_ingestion_timestamp",
    "normalize_date_columns_dynamic",
    "build_typed_df_from_rows",
]
