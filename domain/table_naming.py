"""Pure helpers that map logical dataset names to physical lake identifiers."""

from __future__ import annotations

from dataclasses import dataclass
import os

_PG_IDENTIFIER_MAX = 63


@dataclass(frozen=True)
class TableIdentity:
    """Pair a logical dataset name with the physical storage identifier."""

    logical_name: str
    physical_name: str


def logical_table_name_from_filename(filename: str | None) -> str:
    """Return the basename of a source file without its extension."""
    if not filename:
        return "unknown"
    base = os.path.basename(str(filename))
    stem, _ = os.path.splitext(base)
    return stem or base


def sanitize_table_name(name: str | None) -> str:
    """Normalize a table name into a storage-safe identifier."""
    raw = (name or "").strip()
    if not raw:
        return "unknown"
    if "." in raw:
        raw = raw.split(".", 1)[-1]
    return raw.strip('"').replace('.', '_').replace('-', '_')


def physical_table_name(name: str | None, lake_type: str) -> str:
    """Resolve the physical storage identifier for the configured lake backend."""
    sanitized = sanitize_table_name(name)
    if lake_type == "rdbms":
        return sanitized[:_PG_IDENTIFIER_MAX]
    return sanitized.lower()


def table_identity(name: str | None, lake_type: str) -> TableIdentity:
    """Build the logical and physical table identity in one call."""
    logical = sanitize_table_name(name)
    return TableIdentity(logical_name=logical, physical_name=physical_table_name(logical, lake_type))


def delta_target_path(delta_root: str, table_name: str | None, lake_type: str) -> str:
    """Return the path that should receive writes for the target table."""
    root = str(delta_root).rstrip("/")
    if lake_type == "rdbms":
        return root
    return f"{root}/{physical_table_name(table_name, lake_type)}"
