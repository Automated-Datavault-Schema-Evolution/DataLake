"""Delta Lake helper facade."""

from delta.tables import DeltaTable

from utils.lake_utils import write_to_delta


def delta_exists(spark, path: str) -> bool:
    """Return True if the given path is a Delta table.

    Matches the previous best-effort behavior from main.py.
    """

    try:
        return DeltaTable.isDeltaTable(spark, path)
    except Exception:
        return False


__all__ = [
    "write_to_delta",
    "delta_exists",
]
