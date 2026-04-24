from __future__ import annotations

from dataclasses import dataclass

import sqlglot
from sqlglot import exp

from app.models import SchemaResponse, SchemaTable


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

    table_by_name = {table.name: table for table in catalog.tables}
    cataloged_tables = set(table_by_name)
    execution_enabled_tables = {table.name for table in catalog.tables if table.execution_enabled}
    cte_names = {cte.alias for cte in expression.find_all(exp.CTE) if cte.alias}
    referenced_tables = {
        table.name
        for table in expression.find_all(exp.Table)
        if table.name and table.name not in cte_names
    }

    if not referenced_tables:
        raise SQLValidationError("Query must reference at least one curated Gold table.")

    disallowed_tables = referenced_tables - cataloged_tables
    if disallowed_tables:
        disallowed = ", ".join(sorted(disallowed_tables))
        raise SQLValidationError(f"Query references non-Gold or unknown tables: {disallowed}.")

    _validate_wildcards(expression, referenced_tables, table_by_name)

    disabled_tables = referenced_tables - execution_enabled_tables
    if disabled_tables:
        disabled = ", ".join(sorted(disabled_tables))
        raise SQLValidationError(
            f"Query references curated Gold tables that are cataloged but not execution-enabled: {disabled}."
        )

    _validate_joins(expression, referenced_tables, table_by_name, cte_names)
    _validate_columns(expression, referenced_tables, table_by_name, cte_names)

    _apply_limit(expression, max_rows)
    return ValidatedSQL(sql=expression.sql(dialect="duckdb"), tables=referenced_tables)


def _catalog_columns(table_name: str, table_by_name: dict[str, SchemaTable]) -> set[str]:
    table = table_by_name[table_name]
    columns = {field.name for field in table.fields}
    columns.update(table.dimensions)
    columns.update(field.name for field in table.metrics)
    columns.update(table.allowed_filters)
    columns.update(table.primary_key)
    return columns


def _validate_wildcards(
    expression: exp.Select,
    referenced_tables: set[str],
    table_by_name: dict[str, SchemaTable],
) -> None:
    if not list(expression.find_all(exp.Star)):
        return

    wildcard_blocked_tables = {
        table_name
        for table_name in referenced_tables
        if table_by_name[table_name].table_type != "aggregate_mart"
    }
    if wildcard_blocked_tables:
        blocked = ", ".join(sorted(wildcard_blocked_tables))
        raise SQLValidationError(f"Wildcard SELECT is not allowed for detailed Gold tables: {blocked}.")


def _validate_columns(
    expression: exp.Select,
    referenced_tables: set[str],
    table_by_name: dict[str, SchemaTable],
    cte_names: set[str],
) -> None:
    alias_to_table = _table_aliases(expression, referenced_tables, cte_names)
    catalog_columns = {
        table_name: _catalog_columns(table_name, table_by_name)
        for table_name in referenced_tables
    }
    all_catalog_columns = set().union(*catalog_columns.values()) if catalog_columns else set()
    output_aliases = {
        alias.alias
        for alias in expression.find_all(exp.Alias)
        if alias.alias
    }

    for column in expression.find_all(exp.Column):
        column_name = column.name
        if column_name in output_aliases:
            continue

        qualifier = column.table
        if qualifier in cte_names:
            continue

        if qualifier:
            table_name = alias_to_table.get(qualifier)
            if table_name is None:
                raise SQLValidationError(f"Column references unknown table or alias: {qualifier}.")
            if column_name not in catalog_columns[table_name]:
                raise SQLValidationError(
                    f"Query references unknown column {qualifier}.{column_name} for table {table_name}."
                )
            continue

        if column_name not in all_catalog_columns:
            raise SQLValidationError(f"Query references unknown column: {column_name}.")


def _validate_joins(
    expression: exp.Select,
    referenced_tables: set[str],
    table_by_name: dict[str, SchemaTable],
    cte_names: set[str],
) -> None:
    joins = list(expression.find_all(exp.Join))
    if not joins:
        return

    alias_to_table = _table_aliases(expression, referenced_tables, cte_names)
    allowed_joins = _allowed_join_pairs(table_by_name)

    for join in joins:
        kind = str(join.args.get("kind") or "").upper()
        if kind == "CROSS":
            raise SQLValidationError("Cartesian or CROSS JOIN is not allowed.")

        join_table = join.this
        if isinstance(join_table, exp.Table) and join_table.name in cte_names:
            continue

        join_condition = join.args.get("on")
        if join_condition is None:
            raise SQLValidationError("JOIN must include an ON condition.")

        if not _join_condition_matches_allowed_path(join_condition, alias_to_table, allowed_joins):
            raise SQLValidationError(
                "JOIN condition does not match an allowed semantic catalog join path."
            )


def _allowed_join_pairs(table_by_name: dict[str, SchemaTable]) -> set[tuple[str, str, str, str]]:
    allowed: set[tuple[str, str, str, str]] = set()
    for table in table_by_name.values():
        for join in table.allowed_joins:
            left = (join.left_table, join.left_column, join.right_table, join.right_column)
            right = (join.right_table, join.right_column, join.left_table, join.left_column)
            allowed.add(left)
            allowed.add(right)
    return allowed


def _join_condition_matches_allowed_path(
    join_condition: exp.Expression,
    alias_to_table: dict[str, str],
    allowed_joins: set[tuple[str, str, str, str]],
) -> bool:
    equalities = []
    if isinstance(join_condition, exp.EQ):
        equalities.append(join_condition)
    equalities.extend(join_condition.find_all(exp.EQ))

    for equality in equalities:
        left = equality.this
        right = equality.expression
        if not isinstance(left, exp.Column) or not isinstance(right, exp.Column):
            continue

        left_table = _qualified_column_table(left, alias_to_table)
        right_table = _qualified_column_table(right, alias_to_table)
        if left_table is None or right_table is None:
            continue

        join_pair = (left_table, left.name, right_table, right.name)
        if join_pair in allowed_joins:
            return True
    return False


def _qualified_column_table(column: exp.Column, alias_to_table: dict[str, str]) -> str | None:
    qualifier = column.table
    if not qualifier:
        return None
    return alias_to_table.get(qualifier)


def _table_aliases(
    expression: exp.Select,
    referenced_tables: set[str],
    cte_names: set[str],
) -> dict[str, str]:
    aliases: dict[str, str] = {}
    for table in expression.find_all(exp.Table):
        table_name = table.name
        if not table_name or table_name in cte_names or table_name not in referenced_tables:
            continue
        aliases[table_name] = table_name
        alias = table.alias_or_name
        if alias:
            aliases[alias] = table_name
    return aliases


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
