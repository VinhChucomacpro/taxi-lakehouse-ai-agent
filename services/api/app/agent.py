from __future__ import annotations

from dataclasses import dataclass, field
import os
import re
import unicodedata
from typing import Any

from openai import OpenAI

from app.models import AgentStep, QueryRequest, QueryResponse, SchemaResponse, SchemaTable
from app.text_to_sql import (
    SQLGenerationError,
    generate_common_mart_sql,
    generate_sql_with_openai,
    render_catalog_for_prompt,
)


MAX_REPAIR_ATTEMPTS = 1


@dataclass
class QueryPlan:
    intent: str
    surface: str
    selected_tables: list[str]
    reason: str
    expected_groupings: list[str] = field(default_factory=list)


@dataclass
class AgentContext:
    request: QueryRequest
    catalog: SchemaResponse
    model: str
    api_key: str
    duckdb_path: str
    steps: list[AgentStep] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    plan: QueryPlan | None = None

    def add_step(self, name: str, status: str, message: str, **metadata: Any) -> None:
        self.steps.append(
            AgentStep(
                name=name,
                status=status,
                message=message,
                metadata={key: value for key, value in metadata.items() if value is not None},
            )
        )


def run_query_agent(
    *,
    request: QueryRequest,
    catalog: SchemaResponse,
    model: str,
    api_key: str,
    duckdb_path: str,
) -> QueryResponse:
    context = AgentContext(
        request=request,
        catalog=catalog,
        model=model,
        api_key=api_key,
        duckdb_path=duckdb_path,
    )

    normalized_question = normalize_question(request.question)
    clarification = clarification_question(normalized_question)
    if request.sql is None and clarification:
        context.add_step(
            "intent_analysis",
            "needs_clarification",
            "The question is too broad to execute safely.",
        )
        return QueryResponse(
            summary="The agent needs clarification before querying curated Gold data.",
            sql="",
            columns=[],
            rows=[],
            execution_ms=0,
            answer=clarification,
            agent_steps=context.steps,
            warnings=["Question needs clarification before execution."],
            confidence="low",
            requires_clarification=True,
            clarification_question=clarification,
        )

    plan = build_query_plan(normalized_question, catalog)
    context.plan = plan
    context.add_step(
        "intent_analysis",
        "ok",
        f"Detected intent: {plan.intent}.",
        intent=plan.intent,
    )
    context.add_step(
        "planning",
        "ok",
        plan.reason,
        surface=plan.surface,
        selected_tables=plan.selected_tables,
        expected_groupings=plan.expected_groupings,
    )

    candidate_sql = generate_candidate_sql(context)
    validated = validate_candidate_sql(context, candidate_sql)
    from app.query_engine import QueryExecutionError

    try:
        columns, rows, execution_ms = execute_candidate_sql(context, validated.sql)
    except QueryExecutionError as exc:
        if context.request.sql or not has_openai_key(context.api_key):
            raise
        repaired_sql = repair_sql_once(context, validated.sql, str(exc))
        from app.sql_guardrails import validate_gold_select

        validated = validate_gold_select(repaired_sql, context.catalog, context.request.max_rows)
        context.add_step(
            "guardrail_validation",
            "ok",
            "Repaired SQL passed read-only Gold guardrails after execution failure.",
            tables=sorted(validated.tables),
        )
        columns, rows, execution_ms = execute_candidate_sql(context, validated.sql)
    self_check_results(context, rows, columns, request.max_rows)
    answer = synthesize_answer(
        context=context,
        sql=validated.sql,
        columns=columns,
        rows=rows,
    )

    return QueryResponse(
        summary=f"Returned {len(rows)} rows from curated Gold data.",
        sql=validated.sql,
        columns=columns,
        rows=rows,
        execution_ms=execution_ms,
        answer=answer,
        agent_steps=context.steps,
        warnings=context.warnings,
        confidence=confidence_for(rows, context.warnings),
        requires_clarification=False,
        clarification_question=None,
    )


def generate_candidate_sql(context: AgentContext) -> str:
    if context.request.sql:
        context.add_step(
            "sql_generation",
            "provided_sql",
            "Using SQL override supplied by the request.",
        )
        return context.request.sql

    deterministic_sql = deterministic_sql_for_plan(context.request.question, context.plan, context.catalog)
    if deterministic_sql:
        context.add_step(
            "sql_generation",
            "ok",
            "Generated SQL from deterministic agent planner.",
            source="deterministic_planner",
        )
        return deterministic_sql

    candidate_sql = generate_sql_with_openai(
        question=context.request.question,
        catalog=context.catalog,
        model=context.model,
        api_key=context.api_key,
        max_rows=context.request.max_rows,
    )
    context.add_step(
        "sql_generation",
        "ok",
        "Generated SQL with OpenAI using the semantic catalog.",
        source="openai",
    )
    return candidate_sql


def validate_candidate_sql(context: AgentContext, candidate_sql: str):
    from app.sql_guardrails import SQLValidationError, validate_gold_select

    try:
        validated = validate_gold_select(candidate_sql, context.catalog, context.request.max_rows)
        context.add_step(
            "guardrail_validation",
            "ok",
            "SQL passed read-only Gold guardrails.",
            tables=sorted(validated.tables),
        )
        return validated
    except SQLValidationError as exc:
        context.add_step(
            "guardrail_validation",
            "blocked",
            str(exc),
        )
        if context.request.sql or not can_repair_validation_error(str(exc)):
            raise

        repaired_sql = repair_sql_once(context, candidate_sql, str(exc))
        validated = validate_gold_select(repaired_sql, context.catalog, context.request.max_rows)
        context.add_step(
            "guardrail_validation",
            "ok",
            "Repaired SQL passed read-only Gold guardrails.",
            tables=sorted(validated.tables),
        )
        return validated


def execute_candidate_sql(context: AgentContext, sql: str) -> tuple[list[str], list[dict[str, Any]], int]:
    from app.query_engine import execute_readonly_query

    try:
        columns, rows, execution_ms = execute_readonly_query(sql, context.duckdb_path)
        context.add_step(
            "execution",
            "ok",
            f"Executed read-only query and returned {len(rows)} rows.",
            row_count=len(rows),
            execution_ms=execution_ms,
        )
        return columns, rows, execution_ms
    except Exception as exc:
        context.add_step(
            "execution",
            "failed",
            str(exc),
        )
        raise


def self_check_results(
    context: AgentContext,
    rows: list[dict[str, Any]],
    columns: list[str],
    max_rows: int,
) -> None:
    if not rows:
        context.warnings.append("The query returned no rows.")
    if len(rows) >= max_rows:
        context.warnings.append("The result reached the max row limit; increase max rows for more detail.")

    for column in columns:
        values = [row.get(column) for row in rows if row.get(column) is not None]
        numeric_values = [value for value in values if isinstance(value, int | float)]
        if numeric_values and any(value < 0 for value in numeric_values):
            context.warnings.append(f"`{column}` contains negative values.")

    for expected_grouping in context.plan.expected_groupings if context.plan else []:
        if expected_grouping not in columns:
            context.warnings.append(f"Expected grouping `{expected_grouping}` was not present in the result.")

    status = "warning" if context.warnings else "ok"
    message = "Self-check found warnings." if context.warnings else "Self-check passed."
    context.add_step(
        "self_check",
        status,
        message,
        warnings=context.warnings,
    )


def synthesize_answer(
    *,
    context: AgentContext,
    sql: str,
    columns: list[str],
    rows: list[dict[str, Any]],
) -> str:
    deterministic = deterministic_answer(context, columns, rows)
    if not should_use_openai_answer(context.api_key):
        context.add_step(
            "answer",
            "ok",
            "Built deterministic answer from executed rows.",
            source="deterministic",
        )
        return deterministic

    try:
        client = OpenAI(api_key=context.api_key)
        response = client.chat.completions.create(
            model=context.model,
            messages=[
                {
                    "role": "system",
                    "content": (
                        "You summarize already-executed read-only taxi analytics results. "
                        "Do not write SQL. Do not infer facts outside the provided rows. "
                        "Be concise and mention warnings when present."
                    ),
                },
                {
                    "role": "user",
                    "content": (
                        f"Question: {context.request.question}\n"
                        f"SQL: {sql}\n"
                        f"Columns: {columns}\n"
                        f"Rows sample: {rows[:10]}\n"
                        f"Warnings: {context.warnings}\n"
                        f"Fallback summary: {deterministic}"
                    ),
                },
            ],
        )
        answer = (response.choices[0].message.content or "").strip()
        if answer:
            context.add_step(
                "answer",
                "ok",
                "Synthesized natural-language answer from executed rows.",
                source="openai",
            )
            return answer
    except Exception as exc:
        context.warnings.append(f"OpenAI answer synthesis failed; deterministic answer was used: {exc}")

    context.add_step(
        "answer",
        "ok",
        "Built deterministic answer after answer synthesis fallback.",
        source="deterministic",
    )
    return deterministic


def deterministic_answer(context: AgentContext, columns: list[str], rows: list[dict[str, Any]]) -> str:
    if not rows:
        return "No rows matched the question over the curated Gold data."

    row_count = len(rows)
    plan = context.plan
    table_text = ", ".join(plan.selected_tables) if plan and plan.selected_tables else "curated Gold data"
    metric_columns = [
        column
        for column in columns
        if any(token in column.lower() for token in ("count", "amount", "fare", "distance", "avg", "total"))
    ]
    metric_text = ""
    if metric_columns:
        metric = metric_columns[0]
        numeric_rows = [
            row
            for row in rows
            if isinstance(row.get(metric), int | float)
        ]
        if numeric_rows:
            top_row = max(numeric_rows, key=lambda item: item[metric])
            dimension_values = [
                f"{column}={top_row[column]}"
                for column in columns
                if column != metric and column in top_row
            ][:3]
            metric_text = (
                f" Highest `{metric}` in the returned rows is {top_row[metric]} "
                f"({', '.join(dimension_values)})."
            )
    warning_text = f" Warnings: {'; '.join(context.warnings)}" if context.warnings else ""
    return (
        f"The agent queried {table_text} and returned {row_count} rows with "
        f"columns: {', '.join(columns)}.{metric_text}{warning_text}"
    )


def repair_sql_once(context: AgentContext, bad_sql: str, error: str) -> str:
    from app.sql_guardrails import SQLValidationError

    if not has_openai_key(context.api_key):
        raise SQLValidationError(error)

    context.add_step(
        "sql_repair",
        "attempted",
        "Attempting one guarded SQL repair with OpenAI.",
    )
    client = OpenAI(api_key=context.api_key)
    response = client.chat.completions.create(
        model=context.model,
        messages=[
            {
                "role": "system",
                "content": (
                    "Repair one DuckDB SELECT query for a read-only Gold analytics API. "
                    "Return exactly one SELECT statement and no prose. "
                    "Do not use DML, DDL, external files, or uncataloged joins."
                ),
            },
            {
                "role": "user",
                "content": (
                    "Semantic catalog:\n"
                    f"{render_catalog_for_prompt(context.catalog)}\n\n"
                    f"Question: {context.request.question}\n"
                    f"Rejected SQL: {bad_sql}\n"
                    f"Guardrail error: {error}"
                ),
            },
        ],
    )
    content = response.choices[0].message.content or ""
    repaired = extract_sql(content)
    if not repaired:
        raise SQLGenerationError("OpenAI did not return repaired SQL.")
    context.add_step(
        "sql_repair",
        "ok",
        "OpenAI returned a repaired SQL candidate.",
    )
    return repaired


def deterministic_sql_for_plan(
    question: str,
    plan: QueryPlan | None,
    catalog: SchemaResponse,
) -> str | None:
    if plan is None:
        return None

    if plan.intent == "monthly_service_comparison":
        return generate_common_mart_sql(question=question, catalog=catalog)

    normalized = normalize_question(question)
    year = extract_year(normalized)
    date_filter = date_filter_for_question(normalized, year)
    metric = metric_for_question(normalized)
    month_expr = "strftime(pickup_date, '%Y-%m')"
    if plan.intent == "monthly_trip_trend":
        where_clause = f"\nWHERE {date_filter.replace('pickup_date', 'f.pickup_date')}" if date_filter else ""
        return (
            "SELECT\n"
            "  d.year_month,\n"
            "  SUM(f.trip_distance) AS total_trip_distance,\n"
            "  COUNT(*) AS trip_count,\n"
            "  SUM(f.fare_amount) AS total_fare_amount,\n"
            "  AVG(f.trip_distance) AS avg_trip_distance\n"
            "FROM fact_trips AS f\n"
            "JOIN dim_date AS d ON f.pickup_date = d.pickup_date"
            f"{where_clause}\n"
            "GROUP BY d.year_month\n"
            "ORDER BY d.year_month"
        )

    if plan.intent == "monthly_service_kpi":
        where_clause = f"\nWHERE {date_filter}" if date_filter else ""
        metric_sql = "SUM(trip_count) AS trip_count"
        if metric == "avg_trip_distance":
            metric_sql = "AVG(avg_trip_distance) AS avg_trip_distance"
        elif metric == "total_fare_amount":
            metric_sql = "SUM(total_fare_amount) AS total_fare_amount"
        return (
            "SELECT\n"
            f"  {month_expr} AS month,\n"
            "  service_type,\n"
            f"  {metric_sql}\n"
            "FROM gold_daily_kpis"
            f"{where_clause}\n"
            "GROUP BY 1, 2\n"
            "ORDER BY 1, 2"
        )

    if plan.intent == "monthly_service_total_amount":
        where_clause = f"\nWHERE {date_filter.replace('pickup_date', 'pickup_date')}" if date_filter else ""
        return (
            "SELECT\n"
            "  strftime(pickup_date, '%Y-%m') AS month,\n"
            "  service_type,\n"
            "  SUM(total_amount) AS total_amount,\n"
            "  COUNT(*) AS trip_count\n"
            "FROM fact_trips"
            f"{where_clause}\n"
            "GROUP BY 1, 2\n"
            "ORDER BY 1, 2"
        )

    if plan.intent == "zone_demand":
        where_clause = f"\nWHERE {date_filter}" if date_filter else ""
        return (
            "SELECT\n"
            "  zone_name,\n"
            "  borough,\n"
            "  SUM(trip_count) AS trip_count,\n"
            "  SUM(total_amount) AS total_amount\n"
            "FROM gold_zone_demand\n"
            f"{where_clause}\n"
            "GROUP BY zone_name, borough\n"
            "ORDER BY trip_count DESC"
        )

    if plan.intent == "pickup_borough_demand":
        where_clause = f"\nWHERE {date_filter}" if date_filter else ""
        return (
            "SELECT\n"
            "  borough,\n"
            "  SUM(trip_count) AS trip_count,\n"
            "  SUM(total_amount) AS total_amount\n"
            "FROM gold_zone_demand\n"
            f"{where_clause}\n"
            "GROUP BY borough\n"
            "ORDER BY trip_count DESC"
        )

    if plan.intent == "dropoff_borough_demand":
        where_clause = f"\nWHERE {date_filter.replace('pickup_date', 'f.pickup_date')}" if date_filter else ""
        return (
            "SELECT\n"
            "  z.borough,\n"
            "  COUNT(*) AS trip_count,\n"
            "  SUM(f.total_amount) AS total_amount\n"
            "FROM fact_trips AS f\n"
            "JOIN dim_zone AS z ON f.dropoff_zone_id = z.zone_id\n"
            f"{where_clause}\n"
            "GROUP BY z.borough\n"
            "ORDER BY trip_count DESC"
        )

    if plan.intent == "vendor_analysis":
        where_clause = f"\nWHERE {date_filter.replace('pickup_date', 'f.pickup_date')}" if date_filter else ""
        if has_monthly_intent(normalized) or has_trend_intent(normalized):
            return (
                "SELECT\n"
                "  d.year_month,\n"
                "  v.vendor_name,\n"
                "  COUNT(*) AS trip_count,\n"
                "  SUM(f.total_amount) AS total_amount\n"
                "FROM fact_trips AS f\n"
                "JOIN dim_vendor AS v ON f.vendor_id = v.vendor_id\n"
                "JOIN dim_date AS d ON f.pickup_date = d.pickup_date"
                f"{where_clause}\n"
                "GROUP BY d.year_month, v.vendor_name\n"
                "ORDER BY d.year_month, trip_count DESC"
            )
        return (
            "SELECT\n"
            "  v.vendor_name,\n"
            "  COUNT(*) AS trip_count,\n"
            "  SUM(f.total_amount) AS total_amount\n"
            "FROM fact_trips AS f\n"
            "JOIN dim_vendor AS v ON f.vendor_id = v.vendor_id\n"
            f"{where_clause}\n"
            "GROUP BY v.vendor_name\n"
            "ORDER BY trip_count DESC"
        )

    if plan.intent == "payment_analysis":
        where_clause = f"\nWHERE {date_filter.replace('pickup_date', 'f.pickup_date')}" if date_filter else ""
        return (
            "SELECT\n"
            "  p.payment_type_name,\n"
            "  COUNT(*) AS trip_count,\n"
            "  SUM(f.total_amount) AS total_amount\n"
            "FROM fact_trips AS f\n"
            "JOIN dim_payment_type AS p ON f.payment_type = p.payment_type\n"
            f"{where_clause}\n"
            "GROUP BY p.payment_type_name\n"
            "ORDER BY trip_count DESC"
        )

    if plan.intent == "pickup_dropoff_borough_comparison":
        where_clause = f"\nWHERE {date_filter.replace('pickup_date', 'f.pickup_date')}" if date_filter else ""
        return (
            "SELECT\n"
            "  pickup_zone.borough AS pickup_borough,\n"
            "  dropoff_zone.borough AS dropoff_borough,\n"
            "  COUNT(*) AS trip_count,\n"
            "  SUM(f.total_amount) AS total_amount\n"
            "FROM fact_trips AS f\n"
            "JOIN dim_zone AS pickup_zone ON f.pickup_zone_id = pickup_zone.zone_id\n"
            "JOIN dim_zone AS dropoff_zone ON f.dropoff_zone_id = dropoff_zone.zone_id\n"
            f"{where_clause}\n"
            "GROUP BY pickup_zone.borough, dropoff_zone.borough\n"
            "ORDER BY trip_count DESC"
        )

    return None


def build_query_plan(question: str, catalog: SchemaResponse) -> QueryPlan:
    if has_pickup_intent(question) and has_dropoff_intent(question) and has_geography_intent(question):
        return QueryPlan(
            intent="pickup_dropoff_borough_comparison",
            surface="star_schema",
            selected_tables=["fact_trips", "dim_zone"],
            reason="Pickup versus dropoff geography requires fact_trips joined to dim_zone in both roles.",
            expected_groupings=["pickup_borough", "dropoff_borough"],
        )

    if has_monthly_intent(question) and has_service_intent(question):
        metric = metric_for_question(question)
        if metric == "total_amount":
            return QueryPlan(
                intent="monthly_service_total_amount",
                surface="star_schema",
                selected_tables=["fact_trips"],
                reason="Monthly service total amount needs total_amount from the fact table.",
                expected_groupings=["month", "service_type"],
            )
        if metric in {"avg_trip_distance", "total_fare_amount"}:
            return QueryPlan(
                intent="monthly_service_kpi",
                surface="aggregate_mart",
                selected_tables=["gold_daily_kpis"],
                reason="Monthly service KPI trend can use the denormalized daily KPI mart.",
                expected_groupings=["month", "service_type"],
            )
        return QueryPlan(
            intent="monthly_service_comparison",
            surface="aggregate_mart",
            selected_tables=["gold_daily_kpis"],
            reason="Monthly service comparison can use the denormalized daily KPI mart.",
            expected_groupings=["month", "service_type"],
        )

    if has_monthly_intent(question) and has_trip_intent(question):
        return QueryPlan(
            intent="monthly_trip_trend",
            surface="star_schema",
            selected_tables=["fact_trips", "dim_date"],
            reason="Monthly trip trend needs calendar grouping over the trip fact table.",
            expected_groupings=["month"],
        )

    if has_dropoff_intent(question) and has_zone_intent(question):
        return QueryPlan(
            intent="dropoff_borough_demand",
            surface="star_schema",
            selected_tables=["fact_trips", "dim_zone"],
            reason="Dropoff geography requires fact_trips joined to dim_zone through dropoff_zone_id.",
            expected_groupings=["borough"],
        )

    if has_borough_intent(question) and has_pickup_intent(question):
        return QueryPlan(
            intent="pickup_borough_demand",
            surface="aggregate_mart",
            selected_tables=["gold_zone_demand"],
            reason="Pickup borough demand can use the curated pickup zone demand mart.",
            expected_groupings=["borough"],
        )

    if has_zone_intent(question):
        return QueryPlan(
            intent="zone_demand",
            surface="aggregate_mart",
            selected_tables=["gold_zone_demand"],
            reason="Zone demand is already curated in the zone demand mart.",
            expected_groupings=["zone_name"],
        )

    if has_vendor_intent(question):
        return QueryPlan(
            intent="vendor_analysis",
            surface="star_schema",
            selected_tables=["fact_trips", "dim_vendor"],
            reason="Vendor analysis requires the fact table joined to dim_vendor.",
            expected_groupings=["vendor_name"],
        )

    if has_payment_intent(question):
        return QueryPlan(
            intent="payment_analysis",
            surface="star_schema",
            selected_tables=["fact_trips", "dim_payment_type"],
            reason="Payment analysis requires the fact table joined to dim_payment_type.",
            expected_groupings=["payment_type_name"],
        )

    return QueryPlan(
        intent="general_gold_query",
        surface="llm_planned",
        selected_tables=execution_enabled_table_names(catalog),
        reason="No deterministic pattern matched; the LLM will plan from execution-enabled Gold metadata.",
    )


def clarification_question(question: str) -> str | None:
    token_count = len(question.split())
    if token_count <= 2 and has_trip_intent(question):
        return "Which time range, metric, and grouping should I use for the trip analysis?"
    if question in {"compare", "comparison", "so sanh", "show data", "data"}:
        return "What metric, time range, and taxi service or dimension should I compare?"
    return None


def confidence_for(rows: list[dict[str, Any]], warnings: list[str]) -> str:
    if not rows:
        return "low"
    if warnings:
        return "medium"
    return "high"


def can_repair_validation_error(error: str) -> bool:
    lowered = error.lower()
    return "only select" not in lowered and "dml" not in lowered and "ddl" not in lowered


def has_openai_key(api_key: str) -> bool:
    return bool(api_key and api_key != "replace-me")


def should_use_openai_answer(api_key: str) -> bool:
    enabled = os.getenv("OPENAI_ANSWER_SYNTHESIS", "false").lower() in {"1", "true", "yes"}
    return enabled and has_openai_key(api_key)


def execution_enabled_table_names(catalog: SchemaResponse) -> list[str]:
    return [table.name for table in catalog.tables if table.execution_enabled]


def table_has_columns(table: SchemaTable, columns: set[str]) -> bool:
    available = {field.name for field in table.fields}
    available.update(table.dimensions)
    available.update(field.name for field in table.metrics)
    available.update(table.allowed_filters)
    return columns <= available


def normalize_question(question: str) -> str:
    question = question.replace("đ", "d").replace("Đ", "D")
    question = question.replace("Ä‘", "d").replace("Ä", "D")
    decomposed = unicodedata.normalize("NFKD", question)
    ascii_question = decomposed.encode("ascii", "ignore").decode("ascii")
    return ascii_question.lower().strip()


def has_monthly_intent(question: str) -> bool:
    return any(token in question for token in ("month", "monthly", "thang", "year_month"))


def has_trip_intent(question: str) -> bool:
    return any(token in question for token in ("trip", "trips", "chuyen", "luot"))


def has_service_intent(question: str) -> bool:
    return (
        ("yellow" in question and "green" in question)
        or ("vang" in question and "xanh" in question)
        or "service type" in question
    )


def has_zone_intent(question: str) -> bool:
    return any(token in question for token in ("zone", "borough", "pickup zone", "khu vuc", "diem don"))


def has_borough_intent(question: str) -> bool:
    return "borough" in question


def has_pickup_intent(question: str) -> bool:
    return any(token in question for token in ("pickup", "diem don"))


def has_dropoff_intent(question: str) -> bool:
    return any(token in question for token in ("dropoff", "diem tra"))


def has_vendor_intent(question: str) -> bool:
    return any(token in question for token in ("vendor", "provider", "hang xe", "nha cung cap"))


def has_payment_intent(question: str) -> bool:
    return any(token in question for token in ("payment", "cash", "card", "thanh toan"))


def has_trend_intent(question: str) -> bool:
    return any(token in question for token in ("trend", "over time", "by month", "theo thang"))


def has_geography_intent(question: str) -> bool:
    return has_zone_intent(question) or has_borough_intent(question)


def metric_for_question(question: str) -> str:
    if any(token in question for token in ("average distance", "avg distance", "avg trip distance", "trip distance", "khoang cach")):
        return "avg_trip_distance"
    if any(token in question for token in ("total fare", "fare amount", "fare", "cuoc")):
        return "total_fare_amount"
    if any(token in question for token in ("total amount", "revenue", "doanh thu", "tong tien")):
        return "total_amount"
    return "trip_count"


def extract_year(question: str) -> int | None:
    match = re.search(r"\b(20\d{2})\b", question)
    if not match:
        return None
    return int(match.group(1))


def date_filter_for_question(question: str, year: int | None) -> str:
    if year is None:
        return ""
    if any(token in question for token in ("h1", "first half", "nua dau")):
        return f"pickup_date >= DATE '{year}-01-01' AND pickup_date < DATE '{year}-07-01'"
    return f"pickup_date >= DATE '{year}-01-01' AND pickup_date < DATE '{year + 1}-01-01'"


def extract_sql(content: str) -> str:
    stripped = content.strip()
    fenced = re.search(r"```(?:sql)?\s*(.*?)```", stripped, flags=re.IGNORECASE | re.DOTALL)
    if fenced:
        stripped = fenced.group(1).strip()
    return stripped.rstrip(";").strip()
