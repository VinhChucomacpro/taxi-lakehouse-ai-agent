from __future__ import annotations

import time
from pathlib import Path
from typing import Any

import duckdb


class QueryExecutionError(RuntimeError):
    pass


def execute_readonly_query(sql: str, duckdb_path: str) -> tuple[list[str], list[dict[str, Any]], int]:
    started_at = time.perf_counter()
    path = Path(duckdb_path)
    if not path.exists():
        raise QueryExecutionError(f"DuckDB database does not exist: {duckdb_path}")

    try:
        with duckdb.connect(str(path), read_only=True) as connection:
            cursor = connection.execute(sql)
            columns = [column[0] for column in cursor.description or []]
            rows = [dict(zip(columns, row, strict=True)) for row in cursor.fetchall()]
    except duckdb.Error as exc:
        raise QueryExecutionError(str(exc)) from exc

    execution_ms = int((time.perf_counter() - started_at) * 1000)
    return columns, rows, execution_ms
