"""Postgres helper facade (thin wrapper over utils.lake_utils)."""

from utils.lake_utils import ensure_postgres_ready, store_to_rdbms


__all__ = [
    "ensure_postgres_ready",
    "store_to_rdbms",
]
