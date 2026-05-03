# Architecture

## Goal

This project builds a local-first analytics platform on NYC TLC taxi data with a
read-only AI query agent over curated Gold data.

## Main Components

- `Airflow` orchestrates monthly ingestion and downstream transforms.
- `MinIO` stores raw Bronze files as S3-compatible object storage.
- `dbt` models the transformation layers.
- `DuckDB` serves local analytics and query execution.
- `FastAPI` exposes schema and query endpoints and hosts the read-only agent
  workflow.
- `Streamlit` provides a local demo UI.

## Data Flow

1. Airflow builds monthly manifests for Yellow Taxi, Green Taxi, and Taxi Zone
   Lookup.
2. Ingestion downloads each source file to local `data/` through a temporary
   file, validates file size and SHA-256, then atomically promotes it as cache.
3. Ingestion uploads the same file to MinIO under stable Bronze object keys with
   file metadata when available.
4. dbt Bronze models read from MinIO `s3://taxi-lakehouse/...` paths using
   DuckDB `httpfs`.
5. dbt builds Silver normalized trip data and Gold star-schema models/marts in
   DuckDB.
6. Airflow publishes pipeline run metadata JSON locally and under MinIO
   `metadata/pipeline_runs/...`, including ingestion statuses and dbt summaries.
7. FastAPI runs the read-only agent workflow and executes validated SQL against
   curated Gold data through read-only DuckDB access.
8. Streamlit displays agent traces, answers, SQL, tables, charts, and CSV
   exports for local demos.

## Storage Roles

- MinIO is the Bronze source of truth.
- Local `data/` is a download/cache location for ingestion and development
  fallback.
- DuckDB stores transformed Silver and Gold analytical tables in
  `warehouse/analytics.duckdb`.

## Modeling Direction

Gold contains both:

- a star schema: `fact_trips`, `dim_date`, `dim_zone`, `dim_service_type`,
  `dim_vendor`, and `dim_payment_type`
- aggregate marts: `gold_daily_kpis` and `gold_zone_demand`

Aggregate marts are the fast path for common dashboard and agent questions. The
star schema is the flexible analytical foundation and is exposed through
controlled API access after semantic metadata, column guardrails, wildcard
restrictions, and join guardrails validate generated SQL.

## Serving Principle

The read-only agent must not query raw Bronze or partially cleaned Silver data.
It operates over curated Gold tables and semantic metadata, with SQL validated
before execution. Fact/dimension queries must use explicit cataloged columns and
approved semantic join paths.

## Read-Only Agent Workflow

The query layer is a read-only AI query agent rather than a write-capable or
autonomous data agent. Its workflow is implemented directly in the FastAPI
service: intent analysis, query-surface planning, SQL generation, guardrail
validation, DuckDB execution, result self-checks, and answer synthesis. This
keeps the agent behavior visible for demos while preserving the project's
local-first, framework-light architecture.

The agent response includes both data and traceability: final answer,
`agent_steps`, warnings, confidence, optional clarification prompts, validated
SQL, result columns, rows, and execution time. Natural-language answer synthesis
with OpenAI is opt-in; deterministic answers remain the default demo-safe path.
