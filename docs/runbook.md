# Runbook

## Local Setup

1. Review `.env`
2. Start services with `docker compose up --build`
3. Check:
   - Airflow at `http://localhost:8080`
   - MinIO Console at `http://localhost:9001`
   - API docs at `http://localhost:8000/docs`

## Expected Local Volumes

- `data/` for Bronze and local service data
- `logs/` for Airflow logs
- `warehouse/` for DuckDB database files

## Current Execution Notes

- Bronze ingestion currently starts with Yellow and Green monthly files.
- Taxi Zone Lookup is ingested separately as reference data for enrichment.
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
