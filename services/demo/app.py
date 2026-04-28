from __future__ import annotations

import os
from typing import Any

import pandas as pd
import requests
import streamlit as st


API_BASE_URL = os.getenv("API_BASE_URL", "http://localhost:8000").rstrip("/")
DEFAULT_SQL = """select service_type, pickup_date, trip_count
from gold_daily_kpis
where pickup_date between DATE '2024-01-01' and DATE '2024-06-30'
order by pickup_date, service_type"""
STAR_SCHEMA_SQL = """select
    v.vendor_name,
    count(*) as trip_count,
    sum(f.total_amount) as total_amount
from fact_trips as f
join dim_vendor as v
    on f.vendor_id = v.vendor_id
where f.pickup_date between DATE '2024-01-01' and DATE '2024-06-30'
group by v.vendor_name
order by trip_count desc"""
GUARDRAIL_SQL = "select * from silver_trips_unified"
FACT_WILDCARD_SQL = "select * from fact_trips"
AI_HISTORY_KEY = "ai_history"
DEMO_QUESTIONS = (
    "So sánh số chuyến Yellow Taxi và Green Taxi theo tháng trong nửa đầu năm 2024",
    "Top pickup zones by trip count in 2024 H1",
    "Show vendor trip counts in 2024 H1",
    "Payment type distribution in 2024 H1",
    "Pickup borough demand in 2024 H1",
    "Dropoff borough demand in 2024 H1",
    "Average trip distance by service in 2024 H1",
    "trips",
)
DATE_NAME_HINTS = ("date", "_at", "timestamp", "datetime")
DATE_PART_COLUMNS = {"year", "month", "day", "quarter", "day_of_week"}
MONTH_BUCKET_COLUMNS = {"month", "year_month", "pickup_month", "dropoff_month"}
METRIC_HINTS = ("count", "amount", "fare", "distance", "avg", "total", "sum")


st.set_page_config(
    page_title="Taxi Lakehouse AI Demo",
    page_icon="",
    layout="wide",
)


def get_json(path: str) -> tuple[dict[str, Any] | None, str | None]:
    try:
        response = requests.get(f"{API_BASE_URL}{path}", timeout=10)
        response.raise_for_status()
        return response.json(), None
    except requests.RequestException as exc:
        return None, str(exc)


def post_query(payload: dict[str, Any]) -> tuple[dict[str, Any] | None, str | None, int | None]:
    try:
        response = requests.post(f"{API_BASE_URL}/api/v1/query", json=payload, timeout=60)
        if response.status_code >= 400:
            try:
                detail = response.json().get("detail", response.text)
            except ValueError:
                detail = response.text
            return None, str(detail), response.status_code
        return response.json(), None, response.status_code
    except requests.RequestException as exc:
        return None, str(exc), None


def result_key(key_prefix: str) -> str:
    return f"{key_prefix}_result"


def error_key(key_prefix: str) -> str:
    return f"{key_prefix}_error"


def status_key(key_prefix: str) -> str:
    return f"{key_prefix}_status_code"


def store_query_outcome(
    key_prefix: str,
    *,
    result: dict[str, Any] | None,
    error: str | None,
    status_code: int | None,
) -> None:
    st.session_state[result_key(key_prefix)] = result
    st.session_state[error_key(key_prefix)] = error
    st.session_state[status_key(key_prefix)] = status_code
    st.session_state[f"{key_prefix}_show_chart"] = False


def ai_history() -> list[dict[str, Any]]:
    if AI_HISTORY_KEY not in st.session_state:
        st.session_state[AI_HISTORY_KEY] = []
    return st.session_state[AI_HISTORY_KEY]


def ai_history_item(
    *,
    question: str,
    result: dict[str, Any] | None,
    error: str | None,
    status_code: int | None,
) -> dict[str, Any]:
    if error:
        return {
            "question": question,
            "status": "error",
            "message": error,
            "status_code": status_code,
            "sql": "",
            "row_count": 0,
            "execution_ms": None,
            "warnings": [],
        }

    if not result:
        return {
            "question": question,
            "status": "error",
            "message": "No response returned.",
            "status_code": status_code,
            "sql": "",
            "row_count": 0,
            "execution_ms": None,
            "warnings": [],
        }

    requires_clarification = bool(result.get("requires_clarification"))
    message = (
        result.get("clarification_question")
        or result.get("answer")
        or result.get("summary")
        or ""
    )
    return {
        "question": question,
        "status": "clarification" if requires_clarification else "success",
        "message": message,
        "status_code": status_code,
        "sql": result.get("sql", ""),
        "row_count": len(result.get("rows", [])),
        "execution_ms": result.get("execution_ms"),
        "warnings": result.get("warnings") or [],
    }


def append_ai_history(
    *,
    question: str,
    result: dict[str, Any] | None,
    error: str | None,
    status_code: int | None,
) -> None:
    ai_history().append(
        ai_history_item(
            question=question,
            result=result,
            error=error,
            status_code=status_code,
        )
    )


def run_ai_query(question: str, max_rows: int) -> None:
    payload = {"question": question, "max_rows": max_rows}
    with st.spinner("Generating and validating SQL..."):
        result, error, status_code = post_query(payload)
    store_query_outcome(
        "ai",
        result=result,
        error=error,
        status_code=status_code,
    )
    append_ai_history(
        question=question,
        result=result,
        error=error,
        status_code=status_code,
    )


def render_ai_history() -> None:
    history = ai_history()
    if not history:
        return

    st.subheader("Ask AI history")
    if st.button("Clear history"):
        st.session_state[AI_HISTORY_KEY] = []
        return

    for index, item in enumerate(reversed(history), start=1):
        status = item.get("status", "unknown")
        question = item.get("question", "")
        row_count = item.get("row_count", 0)
        execution_ms = item.get("execution_ms")
        title = f"{index}. {status.title()}: {question[:80]}"
        with st.expander(title, expanded=index == 1):
            st.markdown(f"**Question:** {question}")
            message = item.get("message")
            if message:
                if status == "error":
                    st.error(message)
                elif status == "clarification":
                    st.warning(message)
                else:
                    st.info(message)
            metric_col, latency_col, status_col = st.columns(3)
            metric_col.metric("Rows", row_count)
            latency_col.metric(
                "Execution",
                f"{execution_ms} ms" if execution_ms is not None else "n/a",
            )
            status_col.metric("HTTP", item.get("status_code") or "n/a")
            warnings = item.get("warnings") or []
            if warnings:
                for warning in warnings:
                    st.warning(warning)
            sql = item.get("sql")
            if sql:
                st.code(sql, language="sql")


def render_query_state(key_prefix: str, *, error_label: str = "Request failed") -> None:
    error = st.session_state.get(error_key(key_prefix))
    status_code = st.session_state.get(status_key(key_prefix))
    result = st.session_state.get(result_key(key_prefix))

    if error:
        st.error(f"{error_label}{f' ({status_code})' if status_code else ''}: {error}")
        return
    if result:
        render_result(result, key_prefix)


def run_query(key_prefix: str, payload: dict[str, Any], spinner_text: str) -> None:
    with st.spinner(spinner_text):
        result, error, status_code = post_query(payload)
    store_query_outcome(
        key_prefix,
        result=result,
        error=error,
        status_code=status_code,
    )


def prepare_dataframe(rows: list[dict[str, Any]], columns: list[str]) -> pd.DataFrame:
    dataframe = pd.DataFrame(rows, columns=columns)
    for column in dataframe.columns:
        lower_name = column.lower()
        if is_month_bucket_column(lower_name):
            dataframe[column] = format_month_bucket(dataframe[column])
            continue

        numeric = pd.to_numeric(dataframe[column], errors="coerce")
        if numeric.notna().sum() == dataframe[column].notna().sum():
            dataframe[column] = numeric
            continue

        if is_date_like_column(lower_name):
            converted = pd.to_datetime(dataframe[column], errors="coerce")
            if converted.notna().any():
                dataframe[column] = converted
    return dataframe


def format_month_bucket(series: pd.Series) -> pd.Series:
    numeric = pd.to_numeric(series, errors="coerce")
    non_null_count = series.notna().sum()
    if non_null_count and numeric.notna().sum() == non_null_count:
        if numeric.dropna().between(1, 12).all():
            return numeric.astype("Int64").astype(str).str.zfill(2)
        return series.astype(str)

    converted = pd.to_datetime(series, errors="coerce")
    if non_null_count and converted.notna().sum() == non_null_count:
        return converted.dt.strftime("%Y-%m")

    return series.astype(str)


def is_month_bucket_column(column_name: str) -> bool:
    return column_name in MONTH_BUCKET_COLUMNS


def is_date_like_column(column_name: str) -> bool:
    if column_name in DATE_PART_COLUMNS or is_month_bucket_column(column_name):
        return False
    return any(hint in column_name for hint in DATE_NAME_HINTS)


def find_datetime_columns(dataframe: pd.DataFrame) -> list[str]:
    return [
        column
        for column in dataframe.columns
        if pd.api.types.is_datetime64_any_dtype(dataframe[column])
    ]


def find_numeric_columns(dataframe: pd.DataFrame) -> list[str]:
    return [
        column
        for column in dataframe.columns
        if pd.api.types.is_numeric_dtype(dataframe[column])
    ]


def find_category_columns(dataframe: pd.DataFrame) -> list[str]:
    return [
        column
        for column in dataframe.columns
        if column not in find_numeric_columns(dataframe)
        and column not in find_datetime_columns(dataframe)
    ]


def find_time_axis_columns(dataframe: pd.DataFrame) -> list[str]:
    datetime_columns = find_datetime_columns(dataframe)
    month_columns = [
        column
        for column in dataframe.columns
        if is_month_bucket_column(column.lower())
        and not pd.api.types.is_numeric_dtype(dataframe[column])
    ]
    return [*datetime_columns, *[column for column in month_columns if column not in datetime_columns]]


def preferred_metric(numeric_columns: list[str]) -> str | None:
    for hint in METRIC_HINTS:
        for column in numeric_columns:
            if hint in column.lower():
                return column
    return numeric_columns[0] if numeric_columns else None


def result_warnings(dataframe: pd.DataFrame, max_rows: int) -> list[str]:
    warnings: list[str] = []
    if dataframe.empty:
        return ["The query returned no rows."]

    if len(dataframe) >= max_rows:
        warnings.append("The result reached the max row limit; increase max rows for more detail.")

    for column in find_numeric_columns(dataframe):
        if (dataframe[column] < 0).any():
            warnings.append(f"`{column}` contains negative values.")

    for column in find_datetime_columns(dataframe):
        min_date = dataframe[column].min()
        max_date = dataframe[column].max()
        if pd.notna(min_date) and pd.notna(max_date):
            if min_date.year < 2010 or max_date.year > 2030:
                warnings.append(
                    f"`{column}` has an unusual date range: {min_date.date()} to {max_date.date()}."
                )

    return warnings


def preferred_series(category_columns: list[str], x_column: str | None = None) -> str | None:
    candidates = [column for column in category_columns if column != x_column]
    for column in candidates:
        if column.lower() == "service_type":
            return column
    return candidates[0] if candidates else None


def sorted_grouped_series(dataframe: pd.DataFrame, x_column: str, y_column: str) -> pd.Series:
    grouped = dataframe.groupby(x_column, dropna=False)[y_column].sum()
    return grouped.sort_index()


def sorted_pivot(
    dataframe: pd.DataFrame,
    *,
    x_column: str,
    series_column: str,
    y_column: str,
) -> pd.DataFrame:
    pivoted = dataframe.pivot_table(
        index=x_column,
        columns=series_column,
        values=y_column,
        aggfunc="sum",
    )
    return pivoted.sort_index()


def dataframe_to_csv(dataframe: pd.DataFrame) -> bytes:
    return dataframe.to_csv(index=False).encode("utf-8-sig")


def render_agent_timeline(result: dict[str, Any]) -> None:
    steps = result.get("agent_steps") or []
    if not steps:
        return

    with st.expander("Agent timeline", expanded=True):
        for index, step in enumerate(steps, start=1):
            status = step.get("status", "unknown")
            name = step.get("name", "step").replace("_", " ").title()
            message = step.get("message", "")
            st.markdown(f"**{index}. {name}** `{status}`")
            if message:
                st.caption(message)
            metadata = step.get("metadata") or {}
            if metadata:
                st.json(metadata, expanded=False)


def render_agent_checks(dataframe: pd.DataFrame, max_rows: int) -> None:
    warnings = result_warnings(dataframe, max_rows)
    if warnings:
        with st.expander("Agent checks", expanded=True):
            for warning in warnings:
                st.warning(warning)


def render_auto_chart(dataframe: pd.DataFrame, key_prefix: str) -> None:
    time_columns = find_time_axis_columns(dataframe)
    numeric_columns = find_numeric_columns(dataframe)
    category_columns = find_category_columns(dataframe)
    metric = preferred_metric(numeric_columns)

    if dataframe.empty or not metric:
        return

    chart_type_options = ["Auto", "Line", "Bar"]
    chart_type = st.segmented_control(
        "Chart",
        chart_type_options,
        default="Auto",
        key=f"{key_prefix}_chart_type",
    )
    selected_type = "Line" if chart_type == "Auto" and time_columns else chart_type
    if selected_type == "Auto":
        selected_type = "Bar"
    if selected_type == "Line" and not time_columns:
        st.info("Line chart needs a date/month axis; showing a bar chart instead.")
        selected_type = "Bar"

    if selected_type == "Line" and time_columns:
        x_col, y_col, series_col = st.columns(3)
        with x_col:
            x_column = st.selectbox("X axis", time_columns, key=f"{key_prefix}_line_x")
        with y_col:
            y_column = st.selectbox(
                "Y axis",
                numeric_columns,
                index=numeric_columns.index(metric),
                key=f"{key_prefix}_line_y",
            )
        series_default = preferred_series(category_columns, x_column)
        series_options = ["None", *[column for column in category_columns if column != x_column]]
        series_index = series_options.index(series_default) if series_default in series_options else 0
        with series_col:
            series_column = st.selectbox(
                "Series",
                series_options,
                index=series_index,
                key=f"{key_prefix}_line_series",
            )
        if series_column != "None":
            st.line_chart(
                sorted_pivot(
                    dataframe,
                    x_column=x_column,
                    series_column=series_column,
                    y_column=y_column,
                ),
                use_container_width=True,
            )
            return
        st.line_chart(sorted_grouped_series(dataframe, x_column, y_column), use_container_width=True)
        return

    if selected_type == "Bar":
        x_options = [*time_columns, *[column for column in category_columns if column not in time_columns]]
        if not x_options:
            return
        x_col, y_col, series_col = st.columns(3)
        with x_col:
            x_column = st.selectbox("X axis", x_options, key=f"{key_prefix}_bar_x")
        with y_col:
            y_column = st.selectbox(
                "Y axis",
                numeric_columns,
                index=numeric_columns.index(metric),
                key=f"{key_prefix}_bar_y",
            )
        series_default = preferred_series(category_columns, x_column)
        series_options = ["None", *[column for column in category_columns if column != x_column]]
        series_index = series_options.index(series_default) if series_default in series_options else 0
        with series_col:
            series_column = st.selectbox(
                "Series",
                series_options,
                index=series_index,
                key=f"{key_prefix}_bar_series",
            )
        if series_column != "None":
            st.bar_chart(
                sorted_pivot(
                    dataframe,
                    x_column=x_column,
                    series_column=series_column,
                    y_column=y_column,
                ),
                use_container_width=True,
            )
            return
        st.bar_chart(sorted_grouped_series(dataframe, x_column, y_column), use_container_width=True)


def render_result(result: dict[str, Any], key_prefix: str) -> None:
    st.subheader("Result")
    render_agent_timeline(result)

    if result.get("requires_clarification"):
        clarification = result.get("clarification_question") or result.get("answer")
        st.warning(clarification or "The agent needs clarification before running a query.")
        return

    answer = result.get("answer")
    if answer:
        st.info(answer)

    api_warnings = result.get("warnings") or []
    if api_warnings:
        with st.expander("Agent warnings", expanded=True):
            for warning in api_warnings:
                st.warning(warning)

    rows = result.get("rows", [])
    columns = result.get("columns", [])
    row_count = len(rows)
    column_count = len(columns)
    execution_ms = result.get("execution_ms", 0)

    row_metric, column_metric, latency_metric = st.columns(3)
    row_metric.metric("Rows", row_count)
    column_metric.metric("Columns", column_count)
    latency_metric.metric("Execution", f"{execution_ms} ms")

    with st.expander("SQL used", expanded=False):
        st.code(result.get("sql", ""), language="sql")

    if rows:
        dataframe = prepare_dataframe(rows, columns)
        render_agent_checks(dataframe, max_rows)
        export_col, chart_col = st.columns([1, 4])
        with export_col:
            st.download_button(
                "Export CSV",
                data=dataframe_to_csv(dataframe),
                file_name=f"{key_prefix}_query_result.csv",
                mime="text/csv",
                key=f"{key_prefix}_export_csv",
                on_click="ignore",
            )
        with chart_col:
            show_chart = st.toggle("Show chart", value=False, key=f"{key_prefix}_show_chart")
        if show_chart:
            render_auto_chart(dataframe, key_prefix)
        st.dataframe(dataframe, use_container_width=True)
    else:
        st.info("Query completed with no rows.")

    st.caption(result.get("summary", ""))


def render_schema(schema: dict[str, Any] | None) -> None:
    if not schema:
        st.info("Schema is unavailable.")
        return

    for table in schema.get("tables", []):
        with st.expander(table["name"], expanded=True):
            st.write(table.get("description", ""))
            fields = table.get("fields", [])
            if fields:
                st.dataframe(pd.DataFrame(fields), hide_index=True, use_container_width=True)


st.title("Taxi Lakehouse AI Agent")
st.caption("Read-only natural language and SQL demo over curated Gold marts and controlled star schema.")

health, health_error = get_json("/healthz")
schema, schema_error = get_json("/api/v1/schema")

with st.sidebar:
    st.header("Status")
    st.text_input("API base URL", API_BASE_URL, disabled=True)
    if health_error:
        st.error(health_error)
    elif health:
        st.success(health.get("status", "ok"))
        st.write(f"DuckDB: `{health.get('duckdb_path')}`")
        st.write(f"Catalog loaded: `{health.get('semantic_catalog_loaded')}`")

    if schema_error:
        st.error(schema_error)
    elif schema:
        st.write(f"Gold tables: `{len(schema.get('tables', []))}`")

    st.divider()
    max_rows = st.slider("Max rows", min_value=1, max_value=1000, value=25, step=1)


tab_ai, tab_sql, tab_star, tab_guardrails, tab_schema = st.tabs(
    ["Ask AI", "SQL Test", "Star Schema", "Guardrails", "Schema"]
)

with tab_ai:
    selected_question = st.selectbox("Demo prompt", DEMO_QUESTIONS)
    question = st.text_area(
        "Question",
        value=selected_question,
        height=120,
    )
    if st.button("Run AI query", type="primary"):
        run_ai_query(question, max_rows)
    render_query_state("ai")
    render_ai_history()

with tab_sql:
    sql = st.text_area("SQL", value=DEFAULT_SQL, height=180)
    if st.button("Run SQL override", type="primary"):
        payload = {"question": "SQL override demo", "max_rows": max_rows, "sql": sql}
        run_query("sql", payload, "Validating SQL and querying Gold marts...")
    render_query_state("sql")

with tab_star:
    st.write("Controlled fact/dimension query through an allowed semantic join path.")
    star_sql = st.text_area("Star schema SQL", value=STAR_SCHEMA_SQL, height=220)
    if st.button("Run star schema query", type="primary"):
        payload = {"question": "Star schema demo", "max_rows": max_rows, "sql": star_sql}
        run_query("star", payload, "Validating star-schema joins and querying Gold data...")
    render_query_state("star")

with tab_guardrails:
    guardrail_choice = st.selectbox(
        "Blocked query",
        ["Silver access", "Fact wildcard"],
    )
    blocked_sql = GUARDRAIL_SQL if guardrail_choice == "Silver access" else FACT_WILDCARD_SQL
    st.code(blocked_sql, language="sql")
    if st.button("Run blocked query"):
        payload = {"question": "Guardrail demo", "max_rows": max_rows, "sql": blocked_sql}
        run_query("guardrails", payload, "Validating blocked query...")
    render_query_state("guardrails", error_label="Blocked as expected")

with tab_schema:
    render_schema(schema)
