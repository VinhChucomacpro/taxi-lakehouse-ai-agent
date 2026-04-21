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


def render_result(result: dict[str, Any]) -> None:
    st.caption(result.get("summary", ""))
    st.code(result.get("sql", ""), language="sql")

    rows = result.get("rows", [])
    columns = result.get("columns", [])
    if rows:
        st.dataframe(pd.DataFrame(rows, columns=columns), use_container_width=True)
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
