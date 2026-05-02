from __future__ import annotations

import re
import unicodedata

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
    deterministic_sql = generate_common_mart_sql(question=question, catalog=catalog)
    if deterministic_sql:
        return deterministic_sql

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
                    "Aggregate marts are already denormalized for their dimensions; do not join them "
                    "to dimension tables unless an allowed join is explicitly listed. "
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
            "- Aggregate marts are already denormalized; do not join them to dimensions unless an allowed join is listed.",
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


def generate_common_mart_sql(*, question: str, catalog: SchemaResponse) -> str | None:
    normalized_question = _normalize_question(question)
    if not _is_monthly_service_comparison(normalized_question):
        return None

    if not _has_execution_enabled_columns(
        catalog,
        table_name="gold_daily_kpis",
        columns={"service_type", "pickup_date", "trip_count"},
    ):
        return None

    year = _extract_year(normalized_question)
    where_clause = ""
    if year:
        start_date, end_date = _date_range_for_year(normalized_question, year)
        where_clause = f"\nWHERE pickup_date >= DATE '{start_date}'\n  AND pickup_date < DATE '{end_date}'"

    return (
        "SELECT\n"
        "  strftime(pickup_date, '%Y-%m') AS month,\n"
        "  service_type,\n"
        "  SUM(trip_count) AS trip_count\n"
        "FROM gold_daily_kpis"
        f"{where_clause}\n"
        "GROUP BY 1, 2\n"
        "ORDER BY 1, 2"
    )


def _normalize_question(question: str) -> str:
    question = question.replace("đ", "d").replace("Đ", "D")
    question = question.replace("Ä‘", "d").replace("Ä", "D")
    decomposed = unicodedata.normalize("NFKD", question)
    ascii_question = decomposed.encode("ascii", "ignore").decode("ascii")
    return ascii_question.lower()


def _is_monthly_service_comparison(question: str) -> bool:
    has_monthly_intent = any(token in question for token in ("month", "monthly", "thang"))
    has_trip_intent = any(token in question for token in ("trip", "trips", "chuyen", "luot"))
    has_service_intent = (
        ("yellow" in question and "green" in question)
        or ("vang" in question and "xanh" in question)
        or "service type" in question
    )
    has_comparison_intent = any(
        token in question
        for token in ("compare", "comparison", "so sanh", "by service", "theo loai xe")
    )
    return has_monthly_intent and has_trip_intent and has_service_intent and has_comparison_intent


def _extract_year(question: str) -> int | None:
    match = re.search(r"\b(20\d{2})\b", question)
    if not match:
        return None
    return int(match.group(1))


def _date_range_for_year(question: str, year: int) -> tuple[str, str]:
    if any(token in question for token in ("h1", "first half", "nua dau")):
        return f"{year}-01-01", f"{year}-07-01"
    return f"{year}-01-01", f"{year + 1}-01-01"


def _has_execution_enabled_columns(
    catalog: SchemaResponse,
    *,
    table_name: str,
    columns: set[str],
) -> bool:
    table = next((item for item in catalog.tables if item.name == table_name), None)
    if table is None or not table.execution_enabled:
        return False

    available_columns = {field.name for field in table.fields}
    available_columns.update(table.dimensions)
    available_columns.update(field.name for field in table.metrics)
    available_columns.update(table.allowed_filters)
    return columns <= available_columns


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
