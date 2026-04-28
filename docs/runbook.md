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
- Scheduled ingestion runs on day 15 each month and checks the previous
  `TLC_LOOKBACK_MONTHS` months, default `3`, to handle TLC's delayed
  publication cadence.
- Existing Bronze objects in MinIO are skipped before download. New source files
  are downloaded and uploaded to Bronze. Unpublished TLC source files returning
  HTTP `403` or `404` are recorded as `skipped_source_unavailable` instead of
  failing the whole DAG.
- Manual DAG triggers with explicit `year` and `month` ingest exactly the
  requested month.
- Taxi Zone Lookup is ingested separately as reference data for enrichment.
- Ingestion downloads each source file to the local data volume and uploads the
  same object key into MinIO bucket `taxi-lakehouse`.
- MinIO is the Bronze source of truth for dbt. Bronze dbt models read
  `s3://taxi-lakehouse/...` paths through DuckDB `httpfs`; local `data/` files
  are cache/fallback files.
- Airflow runs `dbt build` inside the scheduler/webserver image using `dbt-duckdb`.
- The local `Bronze -> Silver -> Gold` path can be validated with `dbt build`.
- The read-only agent API plans natural-language questions over curated Gold,
  validates SQL with `sqlglot`, only allows read-only `SELECT` statements, runs
  self-checks, and executes against DuckDB in read-only mode.

## Last Verified Phase 12 Defense Dataset State

Last defense dataset verification: `2026-04-26`.

Selected defense dataset window:

- `2024-01-01` through `2024-06-30`
- Yellow Taxi and Green Taxi monthly files for `2024-01` through `2024-06`
- Taxi Zone Lookup reference dataset for zone enrichment

Verification commands and results:

- `docker compose up -d` started the existing stack without rebuilding images.
- `docker compose ps` showed Postgres, MinIO, API, demo, Airflow scheduler, and
  Airflow webserver running.
- `GET http://localhost:8000/healthz` returned `status=ok`,
  `duckdb_path=/data/warehouse/analytics.duckdb`, and
  `semantic_catalog_loaded=true`.
- Streamlit returned HTTP `200` at `http://localhost:8501`.
- Airflow returned HTTP `200` at `http://localhost:8080/health`.
- MinIO local backing storage contained six Yellow Taxi and six Green Taxi
  Bronze object directories for the selected defense window.
- Taxi Zone Lookup existed at
  `data/minio/taxi-lakehouse/reference/taxi_zone_lookup/taxi_zone_lookup.csv`.
- Airflow DAG `taxi_monthly_pipeline` was triggered with run id
  `phase12_2024_01_20260426` and config `{year: 2024, month: 1}`.
- The Airflow DAG run finished with `success`, from
  `2026-04-26T14:22:52.343988+00:00` to
  `2026-04-26T14:24:37.084520+00:00`.
- dbt build through the Airflow scheduler container completed with
  `PASS=75 WARN=2 ERROR=0 SKIP=0`.
- The warning-only tests were:
  - `warn_silver_trip_anomalies`: `18011` warning rows
  - `warn_gold_metric_anomalies`: `1` warning row

Observed row counts after dbt build:

- `silver_trips_unified`: `98093195`
- `dim_date`: `821`
- `dim_zone`: `265`
- `dim_service_type`: `2`
- `dim_vendor`: `4`
- `dim_payment_type`: `6`
- `fact_trips`: `98093195`
- `gold_daily_kpis`: `1642`
- `gold_zone_demand`: `283947`
- `fact_trips` date range: `2023-12-01` through `2026-02-28`
- Defense window `fact_trips` rows: `20354795`
- Defense window `gold_daily_kpis` rows: `364`
- Defense window `gold_zone_demand` rows: `61154`

API smoke checks for the selected defense window:

- `gold_daily_kpis` query returned daily trip counts for service types starting
  at `2024-01-01`; highest returned row in the sample was Yellow Taxi with
  `79707` trips.
- `fact_trips` joined to `dim_vendor` returned three vendor rows; top vendor in
  the sample was `VeriFone Inc.` with `15390527` trips.
- `gold_zone_demand` top pickup-zone query returned five rows; top zone in the
  sample was `Midtown Center`, `Manhattan`, with `941328` trips.
- Unsafe `drop table gold_daily_kpis` returned HTTP `400`.

Notes and caveats:

- The selected defense window is `2024-H1`, but the local warehouse currently
  contains a broader loaded date range from `2023-12` through `2026-02`.
- Phase 12 intentionally keeps the broader warehouse available; demo,
  evaluation, and performance scenarios should filter to `2024-H1` when they
  need stable defense-window results.
- Warning-only anomaly rows are source-data quality evidence for Phase 13, not
  blocking dbt failures.

## Last Verified Phase 13 Data Quality State

Last data quality verification: `2026-04-26`.

Phase 13 output:

- `docs/data-quality-report.md`

The report uses the Phase 12 defense dataset window, `2024-01-01` through
`2024-06-30`, and documents:

- dbt build result `PASS=75 WARN=2 ERROR=0 SKIP=0`
- Bronze, Silver, Gold fact/dim, and aggregate mart row counts
- Silver validity filtering from raw Bronze into curated Silver
- warning-only anomaly counts for Silver and Gold
- dbt test coverage across Bronze, Silver, Gold star schema, and aggregate marts
- lineage from TLC source files through MinIO, dbt, DuckDB, FastAPI, and the
  read-only agent
- known caveats and out-of-scope areas

## Last Verified Phase 14 Agent Evaluation State

Last agent evaluation verification: `2026-04-26`.

Phase 14 output:

- `docs/agent-evaluation.md`

The runtime API benchmark used `POST /api/v1/query` against the Phase 12 defense
dataset window and verified:

- `21` total evaluation cases
- `21` passed expected behavior
- `10` executed answer cases
- `1` clarification case
- `10` blocked unsafe or invalid SQL cases
- aggregate mart, star-schema, clarification, and blocked query surfaces
- DDL/DML blocking, Bronze/Silver blocking, unknown table/column blocking,
  detailed wildcard blocking, invalid join blocking, missing-`ON` blocking, and
  cartesian join blocking
- deterministic answer grounding without requiring OpenAI answer synthesis

## Last Verified Phase 15 Demo Scenario State

Last demo scenario verification: `2026-04-26`.

Phase 15 outputs:

- `docs/demo-scenarios.md`
- Streamlit demo prompt and default SQL updates in `services/demo/app.py`

Implemented behavior:

- The Ask AI tab exposes stable demo prompts for `2024-H1`, including monthly
  Yellow/Green comparison, top pickup zones, vendor analysis, payment
  distribution, pickup/dropoff borough analysis, average distance, and an
  ambiguous clarification prompt.
- The SQL Test default query is filtered to `2024-01-01` through `2024-06-30`.
- The Star Schema default vendor query is filtered to `2024-01-01` through
  `2024-06-30`.
- The official demo scenario pack defines 12 defense/product demo scenarios.

Verification:

- `docker compose up -d --build demo` rebuilt and restarted the demo. Compose
  also rebuilt/recreated the API image because the demo depends on the API
  service.
- `GET http://localhost:8000/healthz` returned `status=ok`.
- Streamlit returned HTTP `200` at `http://localhost:8501`.
- Demo container inspection confirmed `DEMO_QUESTIONS`, `2024-06-30`, and the
  Vietnamese monthly service prompt are present in `/app/app.py`.
- API smoke check for `Top pickup zones by trip count in 2024 H1` returned rows
  from `gold_zone_demand`; top sample row was `Midtown Center`, `Manhattan`,
  with `941328` trips.

## Last Verified Phase 16 Operational Hardening State

Last operational hardening verification: `2026-04-26`.

Implemented behavior:

- API query audit events are written as JSON Lines.
- Default audit path: `/data/warehouse/query_audit.jsonl`
- Config variable: `QUERY_AUDIT_LOG_PATH`
- `/healthz` reports:
  - `status`
  - `duckdb_path`
  - `semantic_catalog_loaded`
  - `semantic_catalog_path`
  - `duckdb_exists`
  - `duckdb_connectable`
  - `query_audit_log_path`

Audit event fields:

- `timestamp`
- `status`: `success`, `clarification`, `blocked`, `generation_error`, or
  `execution_error`
- `question`
- `has_sql_override`
- `requested_max_rows`
- `sql`
- `execution_ms`
- `warnings`
- `confidence`
- `requires_clarification`
- `clarification_question`
- `agent_step_statuses`
- `error_type`
- `error_detail`

Verification:

- Host-local `tests/test_api_smoke.py` remained dependency-gated and skipped
  because optional API runtime dependencies are unavailable on the host.
- Host-local AST syntax check passed for changed API files and API smoke tests.
- `docker compose up -d --build api demo` rebuilt and restarted API/demo.
- `GET http://localhost:8000/healthz` returned `status=ok`,
  `duckdb_exists=true`, and `duckdb_connectable=true`.
- A successful Gold query wrote a `success` audit event.
- Ambiguous `trips` wrote a `clarification` audit event.
- Unsafe `drop table gold_daily_kpis` returned HTTP `400` and wrote a
  `blocked` audit event with `error_type=SQLValidationError`.

Operational notes:

- Audit logging intentionally does not store result rows.
- Audit logging failure does not fail a read-only query.
- The API request model still enforces `max_rows` between `1` and `1000`.
- Guardrails still enforce read-only, Gold-only, cataloged columns, detailed
  wildcard restrictions, and allowed joins before execution.

## Last Verified Phase 17 Performance Benchmark State

Last Phase 17 verification: `2026-04-27`.

Phase 17 output added:

- `docs/performance-report.md`
- `scripts/benchmark_phase17.py`

Benchmark command:

```bash
python scripts/benchmark_phase17.py --repeats 5 --warmup 1
```

The benchmark uses `/api/v1/query` with SQL override for five representative
queries over the Phase 12 defense window:

- daily KPI trend from `gold_daily_kpis`
- zone demand ranking from `gold_zone_demand`
- vendor aggregation from `fact_trips` joined to `dim_vendor`
- payment-type aggregation from `fact_trips` joined to `dim_payment_type`
- pickup/dropoff borough aggregation from `fact_trips` joined to two
  `dim_zone` roles

Current materialization review:

- Silver defaults to `table`.
- Gold defaults to `view`.
- `gold_daily_kpis` and `gold_zone_demand` are semantic fast paths for planning
  and guardrails, but they are not physically persisted as precomputed DuckDB
  tables in the current dbt config.
- No materialization change was made. The measured latencies are acceptable for
  the local thesis/demo workflow, and persisting `fact_trips` would duplicate
  the largest table without a clear need.

Benchmark results:

- `P01` daily KPI trend over `gold_daily_kpis`: median `962 ms`, min `914 ms`,
  max `2999 ms`, `364` rows.
- `P02` zone demand ranking over `gold_zone_demand`: median `1265 ms`, min
  `1171 ms`, max `1415 ms`, `25` rows.
- `P03` vendor aggregation over `fact_trips` plus `dim_vendor`: median
  `3701 ms`, min `3348 ms`, max `4820 ms`, `3` rows.
- `P04` payment-type aggregation over `fact_trips` plus `dim_payment_type`:
  median `4062 ms`, min `3347 ms`, max `4416 ms`, `6` rows.
- `P05` pickup/dropoff zone joins over `fact_trips` plus two `dim_zone` roles:
  median `1078 ms`, min `1062 ms`, max `1256 ms`, `50` rows.

Verification completed:

- `docker compose up -d` started the existing stack without rebuild.
- `docker compose ps` showed Postgres, MinIO, API, demo, Airflow scheduler, and
  Airflow webserver running.
- `GET http://localhost:8000/healthz` returned `status=ok`,
  `duckdb_exists=true`, and `duckdb_connectable=true`.
- Streamlit returned HTTP `200`.
- Airflow `/health` returned HTTP `200`.
- Phase 17 benchmark completed and wrote
  `docs/performance-benchmark-results.json`.
- Valid API smoke query over `gold_daily_kpis` returned 5 rows.
- DDL smoke query `drop table gold_daily_kpis` returned HTTP `400`.
- AST syntax parse for `scripts/benchmark_phase17.py` passed.
- `python -m pytest -p no:cacheprovider tests/test_semantic_catalog.py
  tests/test_sql_guardrails.py` returned `5 passed, 1 skipped`; SQL guardrail
  tests remained dependency-gated on the host.
- Full host `python -m pytest -p no:cacheprovider` returned `20 passed, 2
  skipped`; API and SQL guardrail tests remained dependency-gated on the host.

Next action:

- Phase 18: add CI/CD and release packaging documentation. Revisit Gold
  materialization only if future benchmark runs show a concrete demo latency
  problem.

## Last Verified Phase 18 Release Packaging State

Last Phase 18 verification: `2026-04-27`.

Phase 18 outputs:

- `.github/workflows/ci.yml`
- `docs/release-checklist.md`
- `scripts/release_check.py`

Implemented behavior:

- CI installs the package with dev dependencies, runs the Python test suite, and
  runs the release consistency check.
- `scripts/release_check.py` verifies required thesis/release docs, required
  `.env.example` keys, known local port notes, release checklist coverage,
  untracked environment secrets, and obvious documentation secret patterns.
- `docs/release-checklist.md` documents pre-defense/final-submission checks,
  startup commands, reset notes, local ports, change-type-specific mandatory
  checks, API smoke checks, official demo scenarios, and final handoff criteria.

Verification:

- `python scripts/release_check.py` passed.
- Syntax parse passed for `scripts/release_check.py` and
  `scripts/benchmark_phase17.py`.
- `python -m pytest -p no:cacheprovider` returned `20 passed, 2 skipped`; API
  and SQL guardrail tests remained dependency-gated on the host.
- `docker compose ps` showed Postgres, MinIO, API, demo, Airflow scheduler, and
  Airflow webserver running.
- API `/healthz` returned `status=ok`, `duckdb_exists=true`, and
  `duckdb_connectable=true`.
- Streamlit returned HTTP `200`.
- Airflow `/health` returned HTTP `200`.
- Release API smoke checks returned:
  - HTTP `200` for a valid `gold_daily_kpis` query
  - HTTP `400` for `drop table gold_daily_kpis`
  - HTTP `400` for `select * from fact_trips`

Next action:

- Phase 19: add security-lite and governance notes. Keep production auth,
  multi-tenant RBAC, write agents, and cloud deployment out of scope unless the
  project scope changes.

## Last Verified Phase 19 Security-Lite State

Last Phase 19 verification: `2026-04-28`.

Phase 19 output:

- `docs/security-notes.md`

Implemented behavior:

- Documented the current read-only query safety boundaries: DuckDB read-only
  execution, `SELECT`-only SQL, curated Gold-only access, semantic catalog table
  and column validation, detailed wildcard restrictions, allowed join paths,
  request row caps, clarification behavior, and deterministic self-checks.
- Documented OpenAI usage: deterministic paths work without an API key, SQL
  generation is still guardrail-validated, and answer synthesis remains opt-in
  through `OPENAI_ANSWER_SYNTHESIS`.
- Documented local secret handling for `.env`, `.env.example`, `OPENAI_API_KEY`,
  MinIO credentials, shared docs, and release artifacts.
- Documented JSONL query audit log contents and local retention expectations.
- Kept API key/basic auth and rate limiting out of the current implementation
  because the release target remains localhost-only. The security notes describe
  the controls required before any non-local deployment.
- Updated `docs/release-checklist.md` and `scripts/release_check.py` so security
  notes are part of final release verification.

Verification:

- `python scripts/release_check.py` passed.
- `python -m pytest -p no:cacheprovider` returned `20 passed, 2 skipped`.
- The skipped tests are the known host dependency-gated API and SQL guardrail
  tests; Docker remains the preferred runtime verification environment for
  those checks.

Next action:

- Phase 20: final thesis/product freeze. Review README and thesis-facing docs
  for final consistency, then run full release checks and Docker smoke checks.

## Last Verified Phase 20 Final Freeze State

Last Phase 20 verification: `2026-04-28`.

Phase 20 output:

- Finalized `README.md` for thesis/product handoff.
- Updated `docs/development-roadmap.md` so Phase 20 is marked completed.
- Updated `docs/release-checklist.md` verification date.

Implemented behavior:

- README now documents the implemented MVP state, architecture, fixed
  `2024-H1` defense dataset window, local setup, demo flow, verification
  commands, security/governance notes, repository layout, and key thesis docs.
- Scope boundaries are frozen for thesis handoff: FHV/HVFHV, streaming,
  write-capable agents, multi-tenant auth, production cloud deployment, and new
  agent frameworks remain out of scope.
- Thesis-facing docs remain aligned with the implemented system: MinIO-backed
  Bronze source of truth, dbt `Bronze -> Silver -> Gold`, Gold star schema plus
  aggregate marts, controlled read-only agent querying over Gold, deterministic
  default answers, optional OpenAI answer synthesis, audit logging, and
  security-lite governance.

Verification:

- `python scripts/release_check.py` passed.
- `python -m pytest -p no:cacheprovider` returned `20 passed, 2 skipped`.
- The skipped tests are the known host dependency-gated API and SQL guardrail
  tests. Use Docker-based API smoke checks from `docs/release-checklist.md`
  before a live defense or after committing the final freeze.
- `docker compose up -d` started the existing stack after Docker Desktop was
  available.
- `docker compose ps` showed Postgres, MinIO, API, demo, Airflow scheduler, and
  Airflow webserver running.
- `GET http://localhost:8000/healthz` returned `status=ok`,
  `duckdb_exists=true`, and `duckdb_connectable=true`.
- Streamlit returned HTTP `200` at `http://localhost:8501`.
- Airflow returned HTTP `200` at `http://localhost:8080/health`.
- Release API smoke checks returned:
  - HTTP `200` for a valid `gold_daily_kpis` query filtered to `2024-H1`
  - HTTP `400` for `drop table gold_daily_kpis`
  - HTTP `400` for `select * from fact_trips`

Final caveats:

- The local warehouse can contain data outside the fixed `2024-H1` defense
  window; official demo queries should keep their defense-window filters.
- Warning-only dbt anomaly tests are source-data caveats, not blocking pipeline
  failures.
- OpenAI answer synthesis remains opt-in and should stay disabled unless it is
  intentionally demonstrated.
- No production auth, multi-tenant RBAC, public deployment hardening, or rate
  limiting is included in this MVP.

Next action:

- Commit the freeze changes and record the final commit hash or tag used for
  thesis submission.

## Last Verified Ask AI History Display

Last verification: `2026-04-28`.

Implemented behavior:

- Streamlit `Ask AI` now stores a session-local `ai_history` list for demo
  display.
- Each Ask AI request appends question, status, answer/clarification/error
  message, SQL when present, row count, execution time, and warnings.
- History renders newest-first under the current Ask AI result.
- `Clear history` removes only the UI session history. It does not call the API,
  clear current query results, or modify the JSONL query audit log.
- History is not sent to `/api/v1/query`; Ask AI requests remain independent and
  are not multi-turn context-aware queries.

Verification:

- Syntax AST parse passed for `services/demo/app.py`.
- `python -m pytest -p no:cacheprovider` returned `20 passed, 2 skipped`.
- `python scripts/release_check.py` passed.
- `docker compose up -d --build demo` rebuilt and restarted the demo. Compose
  also rebuilt/recreated the API service because demo depends on API.
- `GET http://localhost:8000/healthz` returned `status=ok`,
  `duckdb_exists=true`, and `duckdb_connectable=true`.
- Streamlit returned HTTP `200` at `http://localhost:8501`.
- Demo container inspection confirmed `AI_HISTORY_KEY`, `render_ai_history`, and
  `Clear history` are present in `/app/app.py`.
- API smoke query over `gold_daily_kpis` returned HTTP `200`, five rows, and
  `agent_steps`.
- API smoke query `drop table gold_daily_kpis` returned HTTP `400`.
- API smoke query `select * from fact_trips` returned HTTP `400`.

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
- `/api/v1/query` uses the semantic catalog through the read-only agent
  orchestrator. Only `execution_enabled: true` Gold tables can be planned,
  prompted, validated, or executed.

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
- At this historical checkpoint, aggregate marts were the only executable Gold
  surface. This state was superseded by controlled fact/dim exposure and the
  read-only agent workflow verification on `2026-04-24`.

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
- At Phase 8 verification time, the real semantic catalog still kept
  `fact_trips` and all `dim_*` tables `execution_enabled: false`; that was
  superseded by Phase 10 controlled exposure.

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
  `gold_daily_kpis` query and HTTP `400` for direct `fact_trips` access at
  Phase 8 time.

## Read-Only Agent Checks

Use `/api/v1/schema` to confirm the semantic catalog before querying.

Current agent-visible and executable Gold tables:

- `gold_daily_kpis`
- `gold_zone_demand`
- `fact_trips`
- `dim_date`
- `dim_zone`
- `dim_service_type`
- `dim_vendor`
- `dim_payment_type`

The semantic catalog includes table type, execution flag, grain, dimensions,
metrics, allowed filters, keys, and join metadata. Fact/dimension access is
controlled by column validation, wildcard restrictions, and allowed join paths.

Current SQL guardrails also validate referenced columns and table aliases
against the semantic catalog before execution. Detailed Gold tables such as
`fact_trips` reject wildcard `SELECT *`. Joins must match cataloged
`allowed_joins`; missing-`ON` and cartesian joins are rejected.

When Gold views resolve MinIO-backed Bronze reference data, the API DuckDB
connection configures S3 access from `DUCKDB_S3_ENDPOINT` or `MINIO_ENDPOINT`,
plus the MinIO credentials in `.env`.

Current read-only agent workflow:

- Analyze question intent and choose a query surface.
- Plan aggregate mart or star-schema access from the semantic catalog.
- Generate or accept SQL, then validate it with the same SQL guardrails.
- Execute only validated read-only SQL against DuckDB.
- Run deterministic result self-checks.
- Return an answer, warnings, confidence, and an agent step trace.
- Ask for clarification instead of executing when the question is too broad.
- OpenAI answer synthesis is disabled by default for demo safety; set
  `OPENAI_ANSWER_SYNTHESIS=true` to allow LLM-written final answers from
  already-executed rows.

## Last Verified Read-Only Agent Workflow State

Last read-only agent workflow verification: `2026-04-24`.

Implemented behavior:

- `/api/v1/query` now runs through an internal agent orchestrator instead of a
  direct Text-to-SQL path.
- Query responses keep existing fields and add `answer`, `agent_steps`,
  `warnings`, `confidence`, `requires_clarification`, and
  `clarification_question`.
- Agent steps cover intent analysis, planning, SQL generation, guardrail
  validation, execution, self-check, and answer generation.
- Deterministic planning covers monthly service comparison, monthly trip trend,
  zone demand, vendor analysis, and payment analysis.
- Ambiguous questions can return clarification without executing SQL.
- Streamlit `Ask AI` renders an agent timeline and final answer before the
  table/chart/export controls.

Verification:

- Host-local syntax compile passed for changed API and demo files.
- Host-local `python -m pytest -p no:cacheprovider` passed with `20 passed,
  2 skipped`; API smoke tests remain dependency-gated on the host.
- `docker compose up -d --build api demo` rebuilt and restarted the API and
  Streamlit demo.
- HTTP `/api/v1/query` returned agent traces for:
  - monthly Yellow/Green comparison over `gold_daily_kpis`
  - monthly 2024 trip trend over `fact_trips` joined to `dim_date`
  - ambiguous `trips` clarification without execution
  - invalid join blocked by semantic join guardrails
- Streamlit at `http://localhost:8501` returned HTTP `200`.
- Demo container inspection confirmed the running app contains the agent
  timeline UI.

## Last Verified Documentation Alignment State

Last documentation alignment verification: `2026-04-24`.

Updated documentation:

- `README.md` now presents the project as a local-first lakehouse with a
  read-only AI query agent, not a standalone Text-to-SQL demo.
- `docs/architecture.md` and `docs/architecture-review.md` describe the FastAPI
  orchestrator, agent trace, self-checks, clarification behavior, and opt-in
  answer synthesis.
- `docs/data-contracts.md`, `docs/modeling-decisions.md`, and
  `docs/gold-star-schema.md` align Gold, semantic catalog, and controlled
  fact/dim access with the agent contract.
- `AGENTS.md` and `docs/codex-agent-playbook.md` direct future sessions to keep
  Text-to-SQL generation behind `services/api/app/agent.py`.
- `docs/development-roadmap.md` records Phase 11A-11G as completed and keeps the
  handoff rule focused on agent-visible schema and guardrail changes.

Verification:

- Documentation terminology review checked for stale `AI layer`,
  `AI-visible`, `AI-executable`, `Text-to-SQL style`, and `query service`
  wording.
- Remaining Text-to-SQL mentions are historical phase labels or explicit notes
  that prompt generation is a component behind the agent orchestrator.

## Last Verified Controlled Fact/Dim Exposure State

Last controlled fact/dim exposure verification: `2026-04-24`.

Implemented behavior:

- `fact_trips` and Gold dimensions are now `execution_enabled: true` in
  `contracts/semantic_catalog.yaml`.
- Runtime agent planning and Text-to-SQL prompt rendering include aggregate
  marts, fact table, dimensions, and allowed joins.
- API query execution configures DuckDB S3/MinIO settings before executing
  read-only SQL, so `dim_zone` can resolve the MinIO-backed Taxi Zone Lookup
  reference view.
- Streamlit demo includes a `Star Schema` tab with a controlled fact/dim query.
- Guardrails still reject `SELECT *` on `fact_trips`, invalid star-schema joins,
  DML/DDL, and Bronze/Silver access.

Verification:

- Host-local syntax compile passed for API, demo, and test files.
- Host-local `python -m pytest -p no:cacheprovider` passed with `12 passed,
  2 skipped`; Docker remains the preferred environment for API/guardrail tests.
- `docker compose restart api demo` applied API and demo changes.
- HTTP smoke checks passed for:
  - `gold_daily_kpis`
  - `fact_trips` joined to `dim_vendor`
  - `fact_trips` joined to `dim_payment_type`
  - `fact_trips` joined to pickup-role `dim_zone`
  - `fact_trips` joined to dropoff-role `dim_zone`
  - blocked invalid join
  - blocked `select * from fact_trips`
- Streamlit demo returned HTTP `200`.

## Last Verified Monthly Lookback Ingestion State

Last monthly lookback ingestion verification: `2026-04-24`.

Implemented behavior:

- Scheduled monthly ingestion runs at `00:00` on day `15` of each month.
- Each scheduled run checks the previous `TLC_LOOKBACK_MONTHS` months, default
  `3`, for both Yellow and Green Taxi files.
- Existing MinIO Bronze objects are skipped before download.
- New files are downloaded to local cache and uploaded to Bronze MinIO.
- Unpublished TLC source files returning HTTP `403` or `404` are skipped without
  failing the whole DAG.
- Manual DAG triggers with explicit `year` and `month` still ingest the
  requested month exactly.

Verification:

- `python -m pytest -p no:cacheprovider tests/test_tlc_ingestion.py` passed
  with `12 passed`.
- Host-local `python -m pytest -p no:cacheprovider` passed with `19 passed,
  2 skipped`.
- Syntax compile passed for the DAG, ingestion helper, and ingestion tests.
- Airflow-container check confirmed scheduled run date `2026-04-15` prepares
  manifests for `2026-01`, `2026-02`, and `2026-03`, while manual trigger config
  `{year: 2024, month: 1}` prepares only `2024-01`.
- `airflow dags details taxi_monthly_pipeline` confirmed the DAG remains active
  and unpaused with schedule `0 0 15 * *`.

## Last Verified Text-to-SQL Planner State

Last Text-to-SQL planner verification: `2026-04-24`.

This historical state was superseded by the read-only agent workflow verification
on `2026-04-24`. Text-to-SQL prompt rendering remains a component used behind
the agent orchestrator, not the full API behavior.

Implemented behavior:

- Prompt rendering now starts with planner policy: prefer aggregate marts for
  common daily KPI, service type, and zone demand questions.
- The model is instructed to use fact/dimension tables only when they are
  execution-enabled and the question requires star-schema detail.
- Runtime prompt rendering remains limited to execution-enabled tables; after
  Phase 10 this includes aggregate marts, `fact_trips`, and Gold dimensions.
- Planning-context rendering can include disabled fact/dim tables and allowed
  joins when called with `include_disabled=True`; this remains useful if a
  future phase catalogs tables before exposing them.
- Catalog metadata is grouped by aggregate marts, fact tables, dimensions, and
  allowed joins.

Verification:

- Host-local syntax compile passed for `services/api/app/text_to_sql.py` and
  `tests/test_semantic_catalog.py`.
- `python -m pytest -p no:cacheprovider tests/test_semantic_catalog.py` passed
  with `4 passed`.
- Host-local `python -m pytest -p no:cacheprovider` passed with `12 passed,
  2 skipped`; Docker remains the preferred environment for API/guardrail tests.
- API-container prompt renderer smoke check at Phase 9 time confirmed runtime
  prompt excluded `fact_trips`, runtime prompt included aggregate marts, and
  planning context included `fact_trips`, dimensions, and allowed joins.
- This runtime prompt surface was expanded in Phase 10 after controlled
  fact/dim exposure.
- `docker compose restart api` restarted the running API service so the agent
  orchestrator uses the updated prompt code.

## Last Verified Common Demo Query Planner Fix

Last common demo query planner verification: `2026-04-24`.

This historical fix is now part of the deterministic planning behavior inside
the read-only agent orchestrator.

Implemented behavior:

- The agent now routes common monthly Yellow/Green trip comparison questions to
  a deterministic `gold_daily_kpis` plan before calling OpenAI.
- The deterministic query handles Vietnamese phrasing such as
  `so sánh chuyến đi xanh và vàng các tháng trong năm 2023`.
- Prompt policy now states that aggregate marts are already denormalized and
  should not be joined to dimensions unless an allowed join is listed.

Verification:

- Host-local `python -m pytest -p no:cacheprovider tests/test_semantic_catalog.py`
  passed with `5 passed`.
- Host-local `python -m pytest -p no:cacheprovider` passed with `20 passed,
  2 skipped`; the skipped tests remain dependency-gated on the host.
- API-container smoke script generated and validated a no-join query over
  `gold_daily_kpis` for the Vietnamese monthly Yellow/Green comparison.
- HTTP `/api/v1/query` returned rows for the same Vietnamese question after
  `docker compose restart api`.

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

Current visualization behavior:

- Query results are always shown as a table first.
- Charts render only after the user enables `Show chart`.
- Latest results are kept in Streamlit session state per tab, so chart toggles
  and selector changes do not clear the result panel during reruns.
- Successful result panels include `Export CSV` for downloading the currently
  displayed dataframe.
- Monthly buckets named `month`, `year_month`, `pickup_month`, or
  `dropoff_month` are displayed as `YYYY-MM`.
- Numeric `month` values from fact/dim queries are treated as month buckets
  before numeric casting, so line/bar charts can use them as the x-axis.
- If `service_type` is present, it is selected by default as the chart series so
  Yellow and Green Taxi are not connected into one line.
- Line and bar charts aggregate repeated x-axis buckets before rendering.

Recommended demo flow:

1. Check the sidebar health status and Gold table count.
2. Open `Schema` to show the curated Gold objects available to the agent.
3. Use `SQL Test` with the default query to show deterministic DuckDB results.
4. Use `Guardrails` to show that Silver access is blocked.
5. Use the auto chart selector and agent checks to show result diagnostics.
6. Use `Ask AI` to show the agent timeline, answer, SQL, result table, optional
   chart, and export. Deterministic agent paths work without `OPENAI_API_KEY`;
   broader LLM-planned questions still require it.

## Last Verified Demo Visualization State

Last demo visualization verification: `2026-04-24`.

Implemented behavior:

- Streamlit chart rendering now has a human-in-the-loop `Show chart` toggle.
- Result state is persisted per tab, preventing Streamlit reruns from hiding the
  chart/data panel after users change chart controls.
- Monthly result columns are normalized to `YYYY-MM` before chart rendering.
- Numeric `month` values `1` through `12` are now formatted as month buckets
  before chart axis detection.
- Duplicate monthly x-axis buckets are aggregated before line/bar rendering.
- `service_type` is the default series when present, preventing Yellow/Green
  comparison results from being drawn as one zig-zag line.
- Result data can be exported with `Export CSV`.
- API DuckDB S3 setup now attempts `load httpfs`, then `install httpfs`, and
  skips S3 setup if the extension is unavailable so local Gold mart queries do
  not fail after image rebuilds.

Verification:

- Host-local `python -m pytest -p no:cacheprovider` passed with `20 passed,
  2 skipped`.
- `docker compose up -d --build demo` rebuilt the image-copied Streamlit app.
- HTTP `/api/v1/query` returned rows for
  `so sánh chuyến đi xanh và vàng các tháng trong năm 2023`.
- HTTP `/api/v1/query` returned 12 rows for
  `So sánh chuyến đi trong các tháng trong năm 2024`.
- Streamlit at `http://localhost:8501` returned HTTP `200`.
- Demo container inspection confirmed the running image contains the
  session-state chart UI, month-bucket fix, and `Export CSV`.

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

For scheduled runs, Airflow runs on the 15th and checks the previous
`TLC_LOOKBACK_MONTHS` months. With the default `3`, the run at `2026-04-15`
checks January, February, and March 2026 files. Already-ingested objects are
skipped, and not-yet-published files are skipped without failing the pipeline.
