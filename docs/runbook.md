# Runbook

## Local Setup

1. Review `.env`
2. Start already-built services with:

   ```bash
   docker compose up -d
   ```

   Use `docker compose up -d --build` only when Dockerfiles, Compose config,
   dependency files such as `requirements.txt`, or image-copied source files
   changed.

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

## Last Verified MVP State

Last local end-to-end verification: `2026-04-22`.

Verified commands and services:

- `python -m pytest -p no:cacheprovider` passed with `7 passed, 2 skipped`.
- `docker compose up -d --build` successfully built and started the stack.
- Airflow, MinIO, API, and Streamlit were reachable on their local ports.
- API health returned `status=ok` and loaded the semantic catalog.
- Airflow DAG `taxi_monthly_pipeline` was triggered with:

  ```json
  {
    "run_id": "verify_2024_01",
    "conf": {
      "year": 2024,
      "month": 1
    }
  }
  ```

Verified results:

- all DAG tasks finished with `success`
- Yellow Taxi, Green Taxi, and Taxi Zone Lookup were ingested to MinIO
- `dbt build` completed Bronze, Silver, and Gold layers
- `gold_daily_kpis` had `124` rows in the local DuckDB warehouse
- `gold_zone_demand` had `20727` rows in the local DuckDB warehouse
- `POST /api/v1/query` returned rows for a valid Gold query
- Silver access and DDL requests returned HTTP `400`
- API docs at `http://localhost:8000/docs` returned HTTP `200`
- Streamlit at `http://localhost:8501` returned HTTP `200`

Notes:

- The local warehouse contained both `2023-12` and `2024-01` data during this
  verification, so Gold row counts reflected more than one month.
- Keep `.env` untracked. If a real `OPENAI_API_KEY` is exposed in terminal output
  or logs, rotate it before sharing the session or repository artifacts.

## Last Verified Dimensional Layer State

Last local Gold dimensional verification: `2026-04-23`.

Implemented models:

- `dim_date`
- `dim_zone`
- `dim_service_type`
- `dim_vendor`
- `dim_payment_type`
- `fact_trips`

Verification:

- services were started with `docker compose up -d`; no rebuild was needed
- `python -m pytest -p no:cacheprovider` passed with `7 passed, 2 skipped`
- full dbt build passed with `PASS=75 WARN=1 ERROR=0 SKIP=0`; the warning was
  the expected warning-only `warn_silver_trip_anomalies` test
- `gold_daily_kpis` now builds from `fact_trips`
- `gold_zone_demand` now builds from `fact_trips` joined to `dim_zone`
- `dim_vendor` and `dim_payment_type` build from `silver_trips_unified`
- `fact_trips` has relationship tests to `dim_vendor` and `dim_payment_type`
- API Gold query smoke test returned rows from `gold_daily_kpis`
- `contracts/semantic_catalog.yaml` was intentionally left unchanged, so AI
  still sees only curated marts

Observed row counts in the local DuckDB warehouse:

- `dim_date`: `62`
- `dim_service_type`: `2`
- `dim_vendor`: `3`
- `dim_payment_type`: `6`
- `dim_zone`: `265`
- `fact_trips`: `6381430`
- `gold_daily_kpis`: `124`
- `gold_zone_demand`: `20727`

## AI Query Checks

Use `/api/v1/schema` to confirm the semantic catalog before querying.

Current AI-visible Gold tables:

- `gold_daily_kpis`
- `gold_zone_demand`

The semantic catalog includes table type, grain, dimensions, metrics, allowed
filters, and preferred question patterns for these aggregate marts. `fact_trips`
and Gold dimensions are intentionally not exposed directly to the AI layer.

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
docker compose up -d api demo
```

Use `--build` only when API/demo image dependencies or image-copied source files
changed.

Open `http://localhost:8501`.

Recommended demo flow:

1. Check the sidebar health status and Gold table count.
2. Open `Schema` to show the curated Gold objects available to the agent.
3. Use `SQL Test` with the default query to show deterministic DuckDB results.
4. Use `Guardrails` to show that Silver access is blocked.
5. Use the auto chart selector and agent checks to show result diagnostics.
6. Use `Ask AI` to generate SQL from a natural-language question when `OPENAI_API_KEY` is configured.

## MVP Verification Checklist

Use this checklist after changing ingestion, dbt models, API guardrails, or demo flow.

1. Run Python tests:

   ```bash
   python -m pytest -p no:cacheprovider
   ```

2. Run dbt build inside the Airflow scheduler container:

   ```bash
   docker compose exec airflow-scheduler python -c "import sys; sys.path.insert(0, '/opt/airflow/dags'); from lib.dbt_runner import run_dbt_build; run_dbt_build()"
   ```

3. Confirm critical dbt tests pass:
   - Bronze Taxi Zone Lookup has unique, non-null `zone_id`.
   - Silver has valid `service_type`, `source_month`, pickup/dropoff zones, and pickup dates.
   - Gold marts are not empty and required metrics are populated.

4. Review warning-only anomaly tests:
   - unusually long trip distances
   - negative total amounts
   - dropoff timestamps before pickup timestamps
   - abnormal Gold metrics

5. Smoke test the API with a Gold query:

   ```json
   {
     "question": "Show daily trip counts by service type",
     "max_rows": 10,
     "sql": "select service_type, pickup_date, trip_count from gold_daily_kpis order by pickup_date, service_type"
   }
   ```

6. Smoke test guardrails by confirming these requests return HTTP `400`:
   - `select * from silver_trips_unified`
   - `drop table gold_daily_kpis`

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
docker compose up -d airflow-webserver airflow-scheduler
```

Use `--build` only when the Airflow image, DAG image dependencies, Compose config,
or image-copied source files changed.

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
