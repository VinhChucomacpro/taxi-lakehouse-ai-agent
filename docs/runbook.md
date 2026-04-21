# Runbook

## Local Setup

1. Review `.env`
2. Start services with `docker compose up --build`
3. Check:
   - Airflow at `http://localhost:8080`
   - MinIO Console at `http://localhost:9001`
   - API docs at `http://localhost:8000/docs`
   - Streamlit demo at `http://localhost:8501`

## Expected Local Volumes

- `data/` for Bronze and local service data
- `logs/` for Airflow logs
- `warehouse/` for DuckDB database files

## Current Execution Notes

- Bronze ingestion currently starts with Yellow and Green monthly files.
- Taxi Zone Lookup is ingested separately as reference data for enrichment.
- Ingestion downloads each source file to the local data volume and uploads the
  same object key into MinIO bucket `taxi-lakehouse`.
- Airflow runs `dbt build` inside the scheduler/webserver image using `dbt-duckdb`.
- The local `Bronze -> Silver -> Gold` path can be validated with `dbt build`.
- The AI query API validates generated SQL with `sqlglot`, only allows read-only `SELECT`
  statements over curated Gold tables, and executes against DuckDB in read-only mode.

## AI Query Checks

Use `/api/v1/schema` to confirm the semantic catalog before querying.

For deterministic guardrail testing, `/api/v1/query` accepts an optional `sql`
field. When `sql` is omitted, the API uses OpenAI to generate SQL from the
question and then applies the same guardrails before execution.

Example request body:

```json
{
  "question": "Show daily trip counts by service type",
  "max_rows": 10,
  "sql": "select service_type, pickup_date, trip_count from gold_daily_kpis order by pickup_date, service_type"
}
```

## Streamlit Demo

Start the API and demo UI:

```bash
docker compose up --build api demo
```

Open `http://localhost:8501`.

Recommended demo flow:

1. Check the sidebar health status and Gold table count.
2. Open `Schema` to show the curated Gold objects available to the agent.
3. Use `SQL Test` with the default query to show deterministic DuckDB results.
4. Use `Guardrails` to show that Silver access is blocked.
5. Use the auto chart selector and agent checks to show result diagnostics.
6. Use `Ask AI` to generate SQL from a natural-language question when `OPENAI_API_KEY` is configured.

## MinIO Checks

Open `http://localhost:9001` and log in with `MINIO_ROOT_USER` and
`MINIO_ROOT_PASSWORD` from `.env`.

After a successful ingestion run, bucket `taxi-lakehouse` should contain:

- `bronze/yellow_taxi/year=YYYY/month=MM/yellow_tripdata_YYYY-MM.parquet`
- `bronze/green_taxi/year=YYYY/month=MM/green_tripdata_YYYY-MM.parquet`
- `reference/taxi_zone_lookup/taxi_zone_lookup.csv`

## Airflow End-to-End Check

Start Airflow:

```bash
docker compose up -d --build airflow-webserver airflow-scheduler
```

Open `http://localhost:8080` and trigger `taxi_monthly_pipeline` with config:

```json
{
  "year": 2024,
  "month": 1
}
```

For CLI testing from the scheduler container, trigger with a Python dict to avoid
shell-specific JSON escaping issues:

```bash
docker compose exec airflow-scheduler python -c "from airflow.api.common.trigger_dag import trigger_dag; trigger_dag(dag_id='taxi_monthly_pipeline', run_id='e2e_2024_01', conf={'year': 2024, 'month': 1})"
```

Expected results:

- all DAG tasks end in `success`
- MinIO receives Yellow, Green, and Taxi Zone Lookup objects
- `dbt build` completes Bronze, Silver, and Gold models
- Silver only keeps trips whose pickup timestamp falls inside the source file partition month
