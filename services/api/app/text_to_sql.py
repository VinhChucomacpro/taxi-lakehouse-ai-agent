from __future__ import annotations

import re

from openai import OpenAI

from app.models import SchemaResponse


class SQLGenerationError(RuntimeError):
    pass


def generate_sql_with_openai(
    *,
    question: str,
    catalog: SchemaResponse,
    model: str,
    api_key: str,
    max_rows: int,
) -> str:
    if not api_key or api_key == "replace-me":
        raise SQLGenerationError("OPENAI_API_KEY is not configured.")

    client = OpenAI(api_key=api_key)
    response = client.chat.completions.create(
        model=model,
        messages=[
            {
                "role": "system",
                "content": (
                    "You are a Text-to-SQL generator for DuckDB. "
                    "Return exactly one SELECT statement and no prose. "
                    "Use only the provided execution-enabled curated Gold tables and fields. "
                    "Prefer aggregate marts for daily KPI, service-type trend, and zone-demand questions. "
                    "Use fact and dimension tables only when they are explicitly execution-enabled "
                    "and the question needs vendor, payment type, pickup/dropoff role, or flexible fact/dim analysis. "
                    "Use only cataloged columns and cataloged join paths. "
                    "Do not use SELECT * for detailed fact or dimension tables. "
                    "Do not use DML, DDL, PRAGMA, COPY, ATTACH, or external files. "
                    f"Apply a LIMIT no greater than {max_rows} unless the query is an aggregate."
                ),
            },
            {
                "role": "user",
                "content": (
                    "Semantic catalog:\n"
                    f"{render_catalog_for_prompt(catalog)}\n\n"
                    f"Question: {question}"
                ),
            },
        ],
    )

    content = response.choices[0].message.content or ""
    sql = _extract_sql(content)
    if not sql:
        raise SQLGenerationError("OpenAI did not return SQL.")
    return sql


def render_catalog_for_prompt(catalog: SchemaResponse, *, include_disabled: bool = False) -> str:
    lines: list[str] = []
    prompt_tables = [
        table
        for table in catalog.tables
        if include_disabled or table.execution_enabled
    ]

    lines.extend(
        [
            "Planner policy:",
            "- Use aggregate marts first for common daily KPI, service type, and zone demand questions.",
            "- Use fact/dimension tables only when they are execution-enabled and the question requires star-schema detail.",
            "- Do not reference disabled tables in executable SQL.",
            "- Use only cataloged columns; do not invent columns.",
            "- Use only cataloged allowed joins; do not create cartesian joins.",
            "- Do not use SELECT * on fact or dimension tables.",
            "",
        ]
    )

    _append_table_group(lines, "Aggregate marts", prompt_tables, "aggregate_mart")
    _append_table_group(lines, "Fact tables", prompt_tables, "fact")
    _append_table_group(lines, "Dimensions", prompt_tables, "dimension")
    _append_allowed_joins(lines, prompt_tables)

    return "\n".join(lines).strip()


def _append_table_group(lines: list[str], title: str, tables: list, table_type: str) -> None:
    grouped = [table for table in tables if table.table_type == table_type]
    if not grouped:
        return

    lines.append(f"{title}:")
    for table in grouped:
        _append_table(lines, table)
    lines.append("")


def _append_table(lines: list[str], table) -> None:
    lines.append(f"Table: {table.name}")
    if table.description:
        lines.append(f"Description: {table.description}")
    if table.table_type:
        lines.append(f"Type: {table.table_type}")
    lines.append(f"Execution enabled: {str(table.execution_enabled).lower()}")
    if table.grain:
        lines.append(f"Grain: {table.grain}")
    for field in table.fields:
        description = f" - {field.description}" if field.description else ""
        lines.append(f"Column: {field.name}{description}")
    if table.dimensions:
        lines.append(f"Dimensions: {', '.join(table.dimensions)}")
    for metric in table.metrics:
        description = f" - {metric.description}" if metric.description else ""
        lines.append(f"Metric: {metric.name}{description}")
    if table.allowed_filters:
        lines.append(f"Allowed filters: {', '.join(table.allowed_filters)}")
    if table.primary_key:
        lines.append(f"Primary key: {', '.join(table.primary_key)}")
    for foreign_key in table.foreign_keys:
        lines.append(
            "Foreign key: "
            f"{table.name}.{foreign_key.column} -> "
            f"{foreign_key.references_table}.{foreign_key.references_column}"
        )
    for question in table.preferred_questions:
        lines.append(f"Good for: {question}")


def _append_allowed_joins(lines: list[str], tables: list) -> None:
    joins = [
        join
        for table in tables
        for join in table.allowed_joins
    ]
    if not joins:
        return

    lines.append("Allowed joins:")
    for join in joins:
        lines.append(
            f"- {join.left_table}.{join.left_column} = "
            f"{join.right_table}.{join.right_column}"
        )
    lines.append("")


def _extract_sql(content: str) -> str:
    stripped = content.strip()
    fenced = re.search(r"```(?:sql)?\s*(.*?)```", stripped, flags=re.IGNORECASE | re.DOTALL)
    if fenced:
        stripped = fenced.group(1).strip()
    return stripped.rstrip(";").strip()
