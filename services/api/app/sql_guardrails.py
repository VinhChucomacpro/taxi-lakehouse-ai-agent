from __future__ import annotations

from dataclasses import dataclass

import sqlglot
from sqlglot import exp

from app.models import SchemaResponse


class SQLValidationError(ValueError):
    pass


@dataclass(frozen=True)
class ValidatedSQL:
    sql: str
    tables: set[str]


def validate_gold_select(sql: str, catalog: SchemaResponse, max_rows: int) -> ValidatedSQL:
    expressions = sqlglot.parse(sql, read="duckdb")
    if len(expressions) != 1:
        raise SQLValidationError("Only one SQL statement is allowed.")

    expression = expressions[0]
    if not isinstance(expression, exp.Select):
        raise SQLValidationError("Only SELECT queries are allowed.")

    forbidden = (exp.Insert, exp.Update, exp.Delete, exp.Drop, exp.Create, exp.Alter, exp.Command)
    if any(expression.find(forbidden_type) for forbidden_type in forbidden):
        raise SQLValidationError("DML, DDL, and command statements are not allowed.")

    allowed_tables = {table.name for table in catalog.tables}
    cte_names = {cte.alias for cte in expression.find_all(exp.CTE) if cte.alias}
    referenced_tables = {
        table.name
        for table in expression.find_all(exp.Table)
        if table.name and table.name not in cte_names
    }

    if not referenced_tables:
        raise SQLValidationError("Query must reference at least one curated Gold table.")

    disallowed_tables = referenced_tables - allowed_tables
    if disallowed_tables:
        disallowed = ", ".join(sorted(disallowed_tables))
        raise SQLValidationError(f"Query references non-Gold or unknown tables: {disallowed}.")

    _apply_limit(expression, max_rows)
    return ValidatedSQL(sql=expression.sql(dialect="duckdb"), tables=referenced_tables)


def _apply_limit(expression: exp.Select, max_rows: int) -> None:
    limit_expression = expression.args.get("limit")
    if limit_expression is None:
        expression.limit(max_rows, copy=False)
        return

    current_limit = limit_expression.expression
    if not isinstance(current_limit, exp.Literal) or current_limit.is_string:
        expression.limit(max_rows, copy=False)
        return

    try:
        current_limit_value = int(current_limit.this)
    except (TypeError, ValueError):
        expression.limit(max_rows, copy=False)
        return

    if current_limit_value > max_rows:
        expression.limit(max_rows, copy=False)
