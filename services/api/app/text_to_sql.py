from __future__ import annotations

import re

from openai import OpenAI

from app.catalog import filter_execution_enabled_tables
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


def render_catalog_for_prompt(catalog: SchemaResponse) -> str:
    lines: list[str] = []
    prompt_catalog = filter_execution_enabled_tables(catalog)
    for table in prompt_catalog.tables:
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
        for join in table.allowed_joins:
            lines.append(
                "Allowed join: "
                f"{join.left_table}.{join.left_column} = "
                f"{join.right_table}.{join.right_column}"
            )
        for question in table.preferred_questions:
            lines.append(f"Good for: {question}")
        lines.append("")
    return "\n".join(lines).strip()


def _extract_sql(content: str) -> str:
    stripped = content.strip()
    fenced = re.search(r"```(?:sql)?\s*(.*?)```", stripped, flags=re.IGNORECASE | re.DOTALL)
    if fenced:
        stripped = fenced.group(1).strip()
    return stripped.rstrip(";").strip()
