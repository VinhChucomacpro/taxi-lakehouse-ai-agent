# Runbook

## Local Setup

1. Review `.env`
2. Start the already-built Docker services with:

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

## Current Docker Environment

- The local Docker images for Airflow, API, and demo have already been built.
- Prefer `docker compose up -d` for normal startup.
- Use `docker compose up -d --build` only when Dockerfiles, Compose config,
  requirements, dependency installation, or image-copied source files changed.
- Prefer Docker-based API and SQL guardrail checks because the `api` container
  has runtime dependencies such as `sqlglot` and `duckdb`; the host Python
  environment may not.

## Expected Local Volumes

- `data/` for ingestion download/cache files and local service data
- `data/minio/` for MinIO object storage backing files
- `logs/` for Airflow logs
- `warehouse/` for DuckDB database files

## Current Execution Notes

- Bronze ingestion currently starts with Yellow and Green monthly files.
- Taxi Zone Lookup is ingested separately as reference data for enrichment.
- Ingestion downloads each source file to the local data volume and uploads the
  same object key into MinIO bucket `taxi-lakehouse`.
- MinIO is the Bronze source of truth for dbt. Bronze dbt models read
  `s3://taxi-lakehouse/...` paths through DuckDB `httpfs`; local `data/` files
  are cache/fallback files.
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

## Last Verified Agent Workflow State

Last workflow and roadmap update: `2026-04-23`.

Completed documentation state:

- Gold star schema is documented as implemented for the MVP.
- Aggregate marts are documented as a fast/safe path, not a replacement for star
  schema querying.
- The next roadmap step is `Phase 6: Star Schema Semantic Catalog`.
- Agent handoff rules require updating roadmap status, verification notes,
  caveats, and next steps after each meaningful phase or session.

Verification for this docs-focused update:

- Review consistency across `AGENTS.md`, `docs/codex-agent-playbook.md`,
  `docs/development-roadmap.md`, `docs/modeling-decisions.md`, and this runbook.
- `python -m pytest -p no:cacheprovider` passed with `9 passed, 2 skipped`.

## Last Verified Bronze Storage State

Last Bronze storage verification: `2026-04-23`.

Implemented behavior:

- Ingestion still downloads TLC files to local `data/` as a cache before upload.
- Ingestion uploads the files to MinIO bucket `taxi-lakehouse` with stable
  Bronze object keys.
- dbt Bronze models now default to reading from MinIO `s3://taxi-lakehouse/...`
  paths instead of local cache paths.
- dbt config runs a DuckDB `httpfs` setup macro before builds to create MinIO
  S3 access.

Verification for this update:

- `python -m pytest -p no:cacheprovider` passed with `11 passed, 2 skipped`.
- `docker compose up -d minio` started MinIO for dbt S3 reads.
- Full dbt build through `airflow-scheduler` passed with
  `PASS=76 WARN=1 ERROR=0 SKIP=0`; the warning was the expected warning-only
  `warn_silver_trip_anomalies` test.
- The dbt `on-run-start` hook `configure_minio_access` completed successfully,
  confirming DuckDB could configure `httpfs`/S3 access before Bronze model reads.

## Last Verified Semantic Catalog State

Last semantic catalog verification: `2026-04-24`.

Implemented behavior:

- `contracts/semantic_catalog.yaml` now describes all Gold aggregate marts,
  dimensions, and the `fact_trips` table.
- The catalog now includes `execution_enabled`, `primary_key`,
  `foreign_keys`, and `allowed_joins` metadata.
- `/api/v1/schema` returns the full Gold catalog, including cataloged but
  execution-disabled tables.
- `/api/v1/query` and Text-to-SQL prompt rendering now use only
  `execution_enabled: true` tables, so `fact_trips` and `dim_*` remain blocked
  until later guardrail phases.

Verification for this update:

- `python -m pytest -p no:cacheprovider tests/test_semantic_catalog.py` passed
  with `3 passed`.
- `python -m pytest -p no:cacheprovider tests/test_sql_guardrails.py` skipped in
  the current environment because `sqlglot` is unavailable.
- `python -m pytest -p no:cacheprovider tests/test_api_smoke.py` skipped in the
  current environment because optional API test dependencies are unavailable.

Notes:

- The semantic catalog is now both metadata contract and execution-gating
  contract.
- Aggregate marts remain the only current AI-executable Gold surface.

## Last Verified Architecture Review And Guardrail State

Last architecture review and guardrail verification: `2026-04-24`.

Implemented behavior:

- README was refreshed with correct Vietnamese text and current project state.
- `docs/architecture-review.md` was added for thesis-defense architecture
  narrative, known limitations, and next optimization backlog.
- SQL guardrails now validate cataloged columns and table aliases before DuckDB
  execution.
- Wildcard `SELECT *` is rejected for detailed Gold tables such as
  `fact_trips`, while aggregate marts retain wildcard support for existing
  deterministic demo/smoke paths.

Verification:

- `docker compose up -d` started the already-built stack.
- `docker compose ps` showed Postgres, MinIO, API, demo, Airflow scheduler, and
  Airflow webserver running.
- API container dependency check reported `sqlglot 30.6.0` and `duckdb 1.5.2`.
- In-container guardrail smoke check:
  - valid `gold_daily_kpis` query was accepted and limited
  - unknown column query was rejected
  - explicit `fact_trips` query was rejected because it is not execution-enabled
  - `select * from fact_trips` was rejected by wildcard policy
- HTTP API smoke check returned 5 rows for a valid `gold_daily_kpis` query and
  HTTP `400` for an unknown-column query.
- Local API health returned `status=ok` and `semantic_catalog_loaded=True`.
- Streamlit demo returned HTTP `200`.
- Airflow webserver returned HTTP `200`.
- Host-local `python -m pytest -p no:cacheprovider` passed with `11 passed,
  2 skipped`; the skipped tests are dependency-gated in the host Python
  environment, so Docker remains the preferred verification environment for
  API/guardrail behavior.

## Last Verified Join Guardrail State

Last join guardrail verification: `2026-04-24`.

Implemented behavior:

- SQL guardrails now parse joins with `sqlglot`.
- Joins must include explicit `ON` conditions.
- Cartesian joins and `CROSS JOIN` are rejected.
- Join predicates must match semantic catalog `allowed_joins`.
- Pickup and dropoff `dim_zone` roles are both supported through their approved
  fact keys.
- The real semantic catalog still keeps `fact_trips` and all `dim_*` tables
  `execution_enabled: false`; Phase 8 validates the policy before controlled
  exposure.

Verification:

- Host-local syntax compile passed for `services/api/app/sql_guardrails.py` and
  `tests/test_sql_guardrails.py`.
- Host-local `python -m pytest -p no:cacheprovider` passed with `11 passed,
  2 skipped`; Docker remains the preferred environment for SQL guardrail tests.
- `docker compose restart api` restarted the running API service so HTTP traffic
  uses the updated guardrail code.
- API-container smoke tests accepted valid joins:
  - `fact_trips.vendor_id = dim_vendor.vendor_id`
  - `fact_trips.pickup_zone_id = dim_zone.zone_id`
  - `fact_trips.dropoff_zone_id = dim_zone.zone_id`
- API-container smoke tests rejected:
  - wrong join key
  - join without `ON`
  - `CROSS JOIN`
- HTTP smoke check after API restart returned rows for a valid
  `gold_daily_kpis` query and HTTP `400` for direct `fact_trips` access,
  confirming fact/dim execution remains disabled.

## AI Query Checks

Use `/api/v1/schema` to confirm the semantic catalog before querying.

Current AI-visible Gold tables:

- `gold_daily_kpis`
- `gold_zone_demand`

The semantic catalog includes table type, execution flag, grain, dimensions,
metrics, allowed filters, keys, and join metadata. `fact_trips` and Gold
dimensions are intentionally cataloged but not execution-enabled yet, so they
do not appear in the current Text-to-SQL execution surface.

Current SQL guardrails also validate referenced columns and table aliases
against the semantic catalog before execution. Detailed Gold tables such as
`fact_trips` reject wildcard `SELECT *`. Joins must match cataloged
`allowed_joins`; missing-`ON` and cartesian joins are rejected.

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

dbt Bronze reads these objects through S3-compatible paths:

- `s3://taxi-lakehouse/bronze/yellow_taxi/**/*.parquet`
- `s3://taxi-lakehouse/bronze/green_taxi/**/*.parquet`
- `s3://taxi-lakehouse/reference/taxi_zone_lookup/taxi_zone_lookup.csv`

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
