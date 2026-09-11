from __future__ import annotations
from collections import defaultdict
from typing import Any, Iterable, Mapping
TECHNICAL_COLUMNS = frozenset({'filename', 'data_format'})

def group_rows_by_table(entries: Iterable[tuple[str, Mapping[str, Any] | None]]) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for table_name, payload in entries:
        grouped[str(table_name or 'unknown')].append(dict(payload) if payload is not None else {})
    return dict(grouped)

def materialized_columns(rows: Iterable[Mapping[str, Any]], excluded: Iterable[str]=TECHNICAL_COLUMNS) -> list[str]:
    excluded_set = {str(name) for name in excluded}
    return sorted({key for row in rows for key in row.keys() if key is not None and key not in excluded_set})

def normalize_rows(rows: Iterable[Mapping[str, Any]], column_names: Iterable[str]) -> list[dict[str, Any]]:
    ordered_columns = [str(name) for name in column_names]
    return [{column: None if row.get(column) == '' else row.get(column) for column in ordered_columns} for row in rows]

def date_like_columns(column_names: Iterable[str]) -> list[str]:
    return [column for column in column_names if column.lower().endswith('date') or column.lower() in {'dob', 'dateofbirth'}]
