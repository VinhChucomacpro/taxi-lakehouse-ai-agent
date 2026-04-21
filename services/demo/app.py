from __future__ import annotations

import os
from typing import Any

import pandas as pd
import requests
import streamlit as st


API_BASE_URL = os.getenv("API_BASE_URL", "http://localhost:8000").rstrip("/")
DEFAULT_SQL = """select service_type, pickup_date, trip_count
from gold_daily_kpis
order by pickup_date, service_type"""
GUARDRAIL_SQL = "select * from silver_trips_unified"
DATE_HINTS = ("date", "day", "month", "year", "_at")
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


def prepare_dataframe(rows: list[dict[str, Any]], columns: list[str]) -> pd.DataFrame:
    dataframe = pd.DataFrame(rows, columns=columns)
    for column in dataframe.columns:
        lower_name = column.lower()
        if any(hint in lower_name for hint in DATE_HINTS):
            converted = pd.to_datetime(dataframe[column], errors="coerce")
            if converted.notna().any():
                dataframe[column] = converted
        else:
            numeric = pd.to_numeric(dataframe[column], errors="coerce")
            if numeric.notna().sum() == dataframe[column].notna().sum():
                dataframe[column] = numeric
    return dataframe


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


def render_auto_chart(dataframe: pd.DataFrame, max_rows: int) -> None:
    datetime_columns = find_datetime_columns(dataframe)
    numeric_columns = find_numeric_columns(dataframe)
    category_columns = find_category_columns(dataframe)
    metric = preferred_metric(numeric_columns)

    warnings = result_warnings(dataframe, max_rows)
    if warnings:
        with st.expander("Agent checks", expanded=True):
            for warning in warnings:
                st.warning(warning)

    if dataframe.empty or not metric:
        return

    chart_type_options = ["Auto", "Line", "Bar", "Table only"]
    chart_type = st.selectbox("Chart", chart_type_options, index=0)
    selected_type = chart_type
    if chart_type == "Auto":
        selected_type = "Line" if datetime_columns else "Bar"

    if selected_type == "Line" and datetime_columns:
        x_column = st.selectbox("X axis", datetime_columns, key="line_x")
        y_column = st.selectbox("Y axis", numeric_columns, index=numeric_columns.index(metric), key="line_y")
        chart_frame = dataframe.sort_values(x_column)
        if category_columns:
            color_column = st.selectbox("Series", ["None", *category_columns], key="line_series")
            if color_column != "None":
                pivoted = chart_frame.pivot_table(
                    index=x_column,
                    columns=color_column,
                    values=y_column,
                    aggfunc="sum",
                )
                st.line_chart(pivoted, use_container_width=True)
                return
        st.line_chart(chart_frame.set_index(x_column)[[y_column]], use_container_width=True)
        return

    if selected_type == "Bar" and category_columns:
        category_column = st.selectbox("Category", category_columns, key="bar_category")
        y_column = st.selectbox("Metric", numeric_columns, index=numeric_columns.index(metric), key="bar_y")
        chart_frame = (
            dataframe.groupby(category_column, dropna=False)[y_column]
            .sum()
            .sort_values(ascending=False)
            .head(20)
        )
        st.bar_chart(chart_frame, use_container_width=True)


def render_result(result: dict[str, Any]) -> None:
    st.caption(result.get("summary", ""))
    st.code(result.get("sql", ""), language="sql")

    rows = result.get("rows", [])
    columns = result.get("columns", [])
    if rows:
        dataframe = prepare_dataframe(rows, columns)
        render_auto_chart(dataframe, max_rows)
        st.dataframe(dataframe, use_container_width=True)
    else:
        st.info("Query completed with no rows.")

    st.caption(f"Execution time: {result.get('execution_ms', 0)} ms")


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
st.caption("Read-only natural language and SQL demo over curated Gold marts.")

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


tab_ai, tab_sql, tab_guardrails, tab_schema = st.tabs(
    ["Ask AI", "SQL Test", "Guardrails", "Schema"]
)

with tab_ai:
    question = st.text_area(
        "Question",
        value="What are daily trip counts and fare amounts by taxi service?",
        height=120,
    )
    if st.button("Run AI query", type="primary"):
        with st.spinner("Generating and validating SQL..."):
            result, error, status_code = post_query({"question": question, "max_rows": max_rows})
        if error:
            st.error(f"Request failed{f' ({status_code})' if status_code else ''}: {error}")
        elif result:
            render_result(result)

with tab_sql:
    sql = st.text_area("SQL", value=DEFAULT_SQL, height=180)
    if st.button("Run SQL override", type="primary"):
        payload = {"question": "SQL override demo", "max_rows": max_rows, "sql": sql}
        with st.spinner("Validating SQL and querying Gold marts..."):
            result, error, status_code = post_query(payload)
        if error:
            st.error(f"Request failed{f' ({status_code})' if status_code else ''}: {error}")
        elif result:
            render_result(result)

with tab_guardrails:
    st.write("This query should be rejected because it targets Silver instead of Gold.")
    st.code(GUARDRAIL_SQL, language="sql")
    if st.button("Run blocked query"):
        payload = {"question": "Guardrail demo", "max_rows": max_rows, "sql": GUARDRAIL_SQL}
        result, error, status_code = post_query(payload)
        if error:
            st.error(f"Blocked as expected ({status_code}): {error}")
        elif result:
            st.warning("The query was not blocked.")
            render_result(result)

with tab_schema:
    render_schema(schema)
