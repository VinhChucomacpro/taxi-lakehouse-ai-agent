# Development Roadmap

This roadmap keeps the lakehouse MVP stable while moving the read-only AI query
agent toward controlled querying over the Gold star schema.

## Principles

- Current sources: Yellow Taxi, Green Taxi, Taxi Zone Lookup.
- Keep the project local-first, repeatable, and testable.
- Defer FHV, HVFHV, streaming, write-capable agents, LangChain/LangGraph, and
  production auth until the current MVP and agent star-schema path are stable.
- Gold is the serving layer. It contains both the star schema and aggregate
  marts.
- MinIO is the Bronze object-storage source of truth. Local `data/` files are
  ingestion cache/fallback files.
- Aggregate marts are the fast path for common questions. They do not replace
  the Gold star schema.
- The agent may query `fact_trips` and dimensions through semantic metadata,
  execution flags, column guardrails, wildcard restrictions, and join guardrails.

## Phase 1: Stabilize MVP Lakehouse

Status: completed for the MVP.

- Harden monthly ingestion for Yellow and Green.
- Keep monthly partition semantics in paths, manifests, and dbt models.
- Use MinIO as the Bronze source of truth for dbt reads.
- Treat Taxi Zone Lookup as enrichment reference data.
- Make the `Bronze -> Silver -> Gold` flow repeatable through Airflow and dbt.

## Phase 2: Data Quality And Repeatability

Status: completed for the MVP; ongoing for anomaly checks.

- Expand dbt tests for Bronze, Silver, and Gold.
- Track important anomalies: pickup dates outside partition month, unusual trip
  distance, negative amounts, and dropoff before pickup.
- Keep the verification checklist in `docs/runbook.md` current.
- Keep the semantic catalog aligned with agent-queryable Gold objects.

## Phase 3: Gold Star Schema

Status: completed for the MVP.

Gold star schema models:

- `fact_trips`: one valid Silver trip per row.
- `dim_date`: date, month, quarter, year, day of week.
- `dim_zone`: Taxi Zone Lookup attributes such as zone, borough, and service
  zone.
- `dim_service_type`: `yellow_taxi`, `green_taxi`.
- `dim_vendor`: TLC vendor code lookup.
- `dim_payment_type`: TLC payment code lookup.

`fact_trips` join keys are `pickup_date`, `service_type`, `vendor_id`,
`payment_type`, `pickup_zone_id`, and `dropoff_zone_id`. Base metrics include
`trip_distance`, `fare_amount`, `total_amount`, and `passenger_count`.

Detailed star-schema structure, columns, and join paths are documented in
`docs/gold-star-schema.md`.

## Phase 4: Aggregate Marts From Star Schema

Status: completed for the current serving marts.

- `gold_daily_kpis` is built from `fact_trips`.
- `gold_zone_demand` is built from `fact_trips` joined to `dim_zone`.
- Aggregate marts remain useful for common dashboard and agent questions, but they
  are not a substitute for the Gold star schema.

## Phase 5: Update Codex Workflow, Project State, And Session Handoff

Status: completed on 2026-04-23.

Completed:

- Updated project guidance so future Codex sessions know the Gold star schema is
  already implemented.
- Clarified that aggregate marts are a fast/safe path, while the next direction
  is controlled agent querying over `fact_trips` and `dim_*`.
- Added session handoff rules: after completing a phase, update the roadmap,
  verification notes, caveats, and next step.

Next step: Phase 6, Star Schema Semantic Catalog.

## Phase 5B: Move Bronze Reads To MinIO

Status: completed on 2026-04-23.

Completed:

- Kept local `data/` as ingestion download/cache storage.
- Made dbt Bronze models read from MinIO `s3://taxi-lakehouse/...` paths by
  default.
- Added DuckDB `httpfs`/S3 setup for MinIO before dbt runs.
- Updated contracts, architecture, runbook, and agent guidance to state that
  MinIO is the Bronze source of truth.
- Verified full dbt build through Airflow scheduler with MinIO started:
  `PASS=76 WARN=1 ERROR=0 SKIP=0`.

Next step: Phase 6, Star Schema Semantic Catalog.

## Phase 6: Star Schema Semantic Catalog

Status: completed on 2026-04-24.

Goal: extend `contracts/semantic_catalog.yaml` so it describes the full Gold
star schema, not only aggregate marts.

Completed:

- Add catalog entries for `fact_trips`, `dim_date`, `dim_zone`,
  `dim_service_type`, `dim_vendor`, and `dim_payment_type`.
- For each table, describe `table_type`, `grain`, `fields`, `metrics`,
  `primary_key`, `foreign_keys`, `allowed_joins`, and `allowed_filters` where
  applicable.
- Keep `gold_daily_kpis` and `gold_zone_demand` as the only
  `execution_enabled: true` tables.
- Add `execution_enabled: false` for `fact_trips` and all `dim_*` tables so the
  full star schema is cataloged without widening the current query surface.
- Update API schema models, catalog loading, prompt rendering, semantic catalog
  tests, SQL guardrail tests, and API smoke tests to support execution gating
  and richer metadata.

Verification:

- `python -m pytest -p no:cacheprovider tests/test_semantic_catalog.py` passed.
- `tests/test_sql_guardrails.py` and `tests/test_api_smoke.py` remain present
  and were updated for Phase 6 behavior, but they still skip in environments
  missing optional dependencies such as `sqlglot`, `duckdb`, or `httpx`.

Next step: Phase 7, Column And Table Guardrails.

## Phase 7A: Architecture Review And Thesis Cleanup

Status: completed on 2026-04-24.

Goal: make the project easier to defend by aligning README, architecture
narrative, and the next optimization backlog with the implemented code.

Completed:

- Replaced the stale, mis-encoded README with a current Vietnamese overview.
- Added `docs/architecture-review.md` to summarize architecture strengths,
  known limitations, defense narrative, and next-phase backlog.
- Clarified that MinIO is the Bronze source of truth, Gold contains star schema
  plus aggregate marts, and AI execution currently remains limited to aggregate
  marts.

## Phase 7B: Column And Table Guardrails

Status: completed on 2026-04-24.

Goal: validate generated SQL against the semantic catalog at table and column
level without widening the executable agent query surface.

Completed:

- Kept current rules: one statement only, `SELECT` only, no DML/DDL, no
  Bronze/Silver, execution-enabled table gating, and enforced `LIMIT`.
- Validate referenced columns against cataloged fields, dimensions, metrics,
  filters, and primary keys for the referenced Gold table or alias.
- Reject unknown columns and unknown table aliases before DuckDB execution.
- Reject wildcard `SELECT *` for detailed Gold tables such as `fact_trips`;
  aggregate marts still allow wildcard queries for existing deterministic smoke
  tests and demos.
- Added guardrail tests for valid cataloged columns, unknown columns, unknown
  aliases, disabled fact access, wildcard fact access, CTE usage, and valid
  aggregate mart queries.

Verification:

- `python -m pytest -p no:cacheprovider` passed locally with `11 passed,
  2 skipped`.
- The skipped tests are dependency-gated in the host Python environment; Docker
  is the preferred API/guardrail verification environment for this repo.
- `docker compose up -d` started the already-built stack.
- API container dependency check confirmed `sqlglot 30.6.0` and `duckdb 1.5.2`.
- In-container guardrail smoke check accepted a valid `gold_daily_kpis` query
  and rejected unknown column, disabled `fact_trips`, and `select * from
  fact_trips` cases.
- HTTP API smoke check returned rows for a valid `gold_daily_kpis` query and
  HTTP `400` for an unknown-column query.
- API health, Streamlit, and Airflow webserver responded locally.

## Phase 8: Join Guardrails For Star Schema

Status: completed on 2026-04-24.

Goal: allow AI to query fact/dim data only through approved star-schema join
paths.

Completed:

- Parse joins with `sqlglot`.
- Require joins between `fact_trips` and dimensions to match cataloged allowed
  joins.
- Reject cartesian joins, joins without `ON`, and joins on the wrong keys.
- Support both `dim_zone` roles:
  - pickup zone through `fact_trips.pickup_zone_id`
  - dropoff zone through `fact_trips.dropoff_zone_id`
- Add tests for valid joins, invalid joins, missing `ON`, and cartesian joins.
- Keep `fact_trips` and all `dim_*` tables `execution_enabled: false` at Phase
  8 time until prompt planning and controlled exposure are ready.

Verification:

- Host-local syntax compile passed for `services/api/app/sql_guardrails.py` and
  `tests/test_sql_guardrails.py`.
- Host-local `python -m pytest -p no:cacheprovider` passed with `11 passed,
  2 skipped`; the skipped tests are dependency-gated in host Python.
- API-container guardrail smoke tests accepted valid joins for `dim_vendor`,
  pickup `dim_zone`, and dropoff `dim_zone`.
- API-container guardrail smoke tests rejected wrong join key, missing `ON`, and
  `CROSS JOIN`.
- `docker compose restart api` restarted the running API service.
- HTTP smoke check after restart returned rows for a valid `gold_daily_kpis`
  query and HTTP `400` for direct `fact_trips` access at Phase 8 time.

## Phase 9: Text-to-SQL Planner For Star Schema

Status: completed on 2026-04-24.

Goal: guide the LLM to choose the right query surface and only generate
catalog-safe SQL.

Completed:

- Prefer aggregate marts for simple daily KPI and zone demand questions.
- Tell the model to use Gold star schema tables only when they are explicitly
  execution-enabled and the question needs vendor, payment type,
  pickup/dropoff role, or flexible fact/dim analysis.
- Render catalog metadata grouped by aggregate marts, fact, dimensions, and
  allowed joins.
- Instruct the model not to use unknown columns, non-cataloged joins, or
  `select *`.
- Keep the runtime prompt limited to execution-enabled tables so Phase 9 AI
  execution still sees only `gold_daily_kpis` and `gold_zone_demand`.
- Add an `include_disabled=True` planning render path that can show cataloged
  fact/dim metadata and allowed joins for tests and future controlled exposure.
- Add prompt rendering tests for runtime aggregate-only prompt behavior and
  star-schema planning context.

Verification:

- Host-local syntax compile passed for `services/api/app/text_to_sql.py` and
  `tests/test_semantic_catalog.py`.
- `python -m pytest -p no:cacheprovider tests/test_semantic_catalog.py` passed
  with `4 passed`.
- Host-local `python -m pytest -p no:cacheprovider` passed with `12 passed,
  2 skipped`; the skipped tests remain dependency-gated in host Python.
- API-container prompt renderer smoke check confirmed:
  - runtime prompt does not include `fact_trips` at Phase 9 time
  - runtime prompt includes the aggregate marts group
  - planning context includes `fact_trips`, dimensions, and allowed joins
- `docker compose restart api` restarted the running API service.

## Phase 10: Controlled Fact/Dim Exposure And Demo Readiness

Status: completed on 2026-04-24.

Goal: officially support controlled API queries over `fact_trips` and
dimensions, then make the demo and thesis docs clear.

Completed:

- Allow `/api/v1/query` to execute valid SQL over `fact_trips` and `dim_*` after
  Phase 6-8 guardrails are in place.
- Add API smoke tests for fact plus `dim_vendor`, `dim_payment_type`, and
  pickup-role `dim_zone`.
- Keep aggregate mart smoke tests.
- Update Streamlit demo with mart query, star-schema query, blocked query, and
  semantic catalog views.
- Configure API DuckDB connections for MinIO/S3 access when Gold views depend on
  Bronze reference data, using `DUCKDB_S3_ENDPOINT` or `MINIO_ENDPOINT`.
- Fix stale or unclear docs so they describe the current implemented state.

Verification:

- Host-local syntax compile passed for changed API, demo, and test files.
- Host-local `python -m pytest -p no:cacheprovider` passed with `12 passed,
  2 skipped`; API tests remain dependency-gated in host Python.
- `docker compose restart api demo` restarted running services after code and
  demo changes.
- HTTP smoke checks passed for:
  - aggregate mart query on `gold_daily_kpis`
  - fact plus `dim_vendor`
  - fact plus `dim_payment_type`
  - fact plus pickup-role `dim_zone`
  - fact plus dropoff-role `dim_zone`
  - blocked invalid star join
  - blocked `select * from fact_trips`
- Streamlit demo returned HTTP `200`.

## Phase 10B: Monthly Lookback Ingestion

Status: completed on 2026-04-24.

Goal: run ingestion automatically on the 15th of each month, check recent TLC
monthly files, and avoid redownloading Bronze objects that already exist.

Completed:

- Changed the DAG schedule to `0 0 15 * *`.
- Added `TLC_LOOKBACK_MONTHS`, default `3`, for scheduled Airflow runs.
- Scheduled runs now prepare Yellow and Green manifests for the previous three
  months.
- Existing MinIO Bronze objects are skipped before download.
- Unpublished TLC source files returning HTTP `403` or `404` are skipped without
  failing the whole DAG.
- Manual DAG triggers with explicit `year` and `month` still ingest the exact
  requested month.
- Updated ingestion tests, `.env.example`, README, runbook, and agent playbook.

Verification:

- `python -m pytest -p no:cacheprovider tests/test_tlc_ingestion.py` passed
  with `12 passed`.
- Host-local `python -m pytest -p no:cacheprovider` passed with `19 passed,
  2 skipped`.
- Airflow-container check confirmed scheduled date `2026-04-15` prepares
  manifests for `2026-01`, `2026-02`, and `2026-03`, while manual
  `{year: 2024, month: 1}` prepares only `2024-01`.
- Airflow DAG details confirmed `taxi_monthly_pipeline` remains active and
  unpaused with schedule `0 0 15 * *`.

## Phase 10C: Demo Visualization Human Review

Status: completed on 2026-04-24.

Goal: make Streamlit result visualization safer for monthly service comparisons
and require an explicit human choice before chart rendering.

Completed:

- Normalize month bucket columns such as `month` and `year_month` to `YYYY-MM`
  labels for display and chart axes.
- Treat numeric `month` values `1` through `12` as month buckets before numeric
  casting, so monthly fact/dim queries can render charts.
- Aggregate duplicate x-axis buckets before charting so multiple service rows
  for the same month do not produce zig-zag line charts.
- Default chart series selection to `service_type` when present, keeping Yellow
  and Green Taxi as separate lines or bars.
- Add a `Show chart` toggle before visualization controls so users can inspect
  SQL/results first and opt in to chart rendering.
- Persist each tab's latest query result in Streamlit session state so toggling
  chart controls does not lose the result panel during reruns.
- Replace the chart type dropdown with a compact segmented control and group
  axis/series selectors into one row for easier scanning.
- Add a CSV export button for successful query results.
- Keep table output always visible after a successful query.
- Add an API `httpfs` fallback so local Gold mart queries continue to run after
  rebuilding the API image even if the DuckDB extension cache is missing.

Verification:

- Host-local `python -m pytest -p no:cacheprovider` passed with `20 passed,
  2 skipped`.
- Rebuilt and restarted the demo with `docker compose up -d --build demo`.
- HTTP `/api/v1/query` returned rows for the Vietnamese monthly Yellow/Green
  comparison after restarting API.
- HTTP `/api/v1/query` returned 12 monthly rows for
  `So sánh chuyến đi trong các tháng trong năm 2024`.
- Streamlit demo returned HTTP `200`.
- Demo container inspection confirmed the session-state chart UI is present in
  `/app/app.py`.

## Phase 11A: Agent Gap And Target Contract

Status: completed on 2026-04-24.

Goal: clarify that the current implementation started as Text-to-SQL plus
guardrails, then define the read-only agent target without widening project
scope.

Completed:

- Document the gap between a SQL generator and a read-only query agent.
- Define agent v1 as a controlled workflow: intent analysis, planning, SQL
  generation, guardrail validation, execution, self-check, and answer synthesis.
- Preserve invariants: Gold-only access, read-only SQL, no DML/DDL, no
  LangChain/LangGraph/Vanna, and no write-capable agent behavior.
- Update docs wording to describe the system as a read-only AI query agent, not
  an autonomous data-writing agent.

Verification:

- README, architecture docs, data contracts, modeling docs, Gold schema docs,
  runbook, playbook, and `AGENTS.md` use consistent read-only agent terminology.

## Phase 11B: Agent Response Contract

Status: completed on 2026-04-24.

Goal: extend `/api/v1/query` responses with agent trace metadata while keeping
existing response fields stable.

Completed:

- Add an `AgentStep` response model with `name`, `status`, `message`, and
  optional metadata.
- Extend `QueryResponse` with optional `answer`, `agent_steps`, `warnings`,
  `confidence`, `requires_clarification`, and `clarification_question`.
- Keep existing fields `summary`, `sql`, `columns`, `rows`, and `execution_ms`
  for backward compatibility.

Verification:

- Existing API tests continue to pass.
- New tests confirm successful query responses include agent steps.
- Host-local full test suite passed with dependency-gated API tests skipped.

## Phase 11C: Agent Orchestrator State Machine

Status: completed on 2026-04-24.

Goal: move `/api/v1/query` through a small internal state machine instead of a
single Text-to-SQL call path.

Completed:

- Add an API-local orchestrator without adding external agent frameworks.
- Run the query workflow through intent analysis, planning, SQL generation,
  validation, execution, and self-check steps.
- Record each step in `agent_steps`.
- Preserve SQL override behavior by marking SQL generation as `provided_sql`.

Verification:

- Valid mart and fact/dim queries return complete traces.
- Invalid SQL still stops before execution.
- HTTP smoke checks confirmed successful responses include full agent traces.

## Phase 11D: Deterministic Intent And Planning

Status: completed on 2026-04-24.

Goal: make the agent choose the intended query surface before asking the LLM to
write SQL.

Completed:

- Add a deterministic classifier for monthly trends, service comparisons, zone
  demand, vendor analysis, payment analysis, and pickup/dropoff analysis.
- Record selected tables, query surface, and planning reason.
- Keep aggregate marts as the fast path for common trend and zone questions.
- Use star-schema planning only when the question requires flexible fact/dim
  analysis.

Verification:

- Monthly Yellow/Green questions plan to `gold_daily_kpis`.
- Zone demand questions plan to `gold_zone_demand`.
- Vendor/payment questions plan to `fact_trips` plus dimensions.
- HTTP smoke checks confirmed monthly service and monthly trend questions use
  deterministic planner paths.

## Phase 11E: Self-Check And Hybrid Answer

Status: completed on 2026-04-24.

Goal: make the agent evaluate query results and return a human-readable answer,
not only rows.

Completed:

- Add deterministic checks for empty results, max-row caps, negative numeric
  metrics, unusual date ranges, and missing expected grouping columns.
- Always return a deterministic answer summary.
- Optionally use OpenAI to synthesize a natural-language answer from the
  already-executed SQL and returned rows only when `OPENAI_ANSWER_SYNTHESIS=true`
  and an API key is configured.
- Do not let answer synthesis generate new SQL or access external data.

Verification:

- Empty and capped results produce warnings.
- Normal results include `answer` and confidence.
- Missing API key still returns a deterministic answer.
- OpenAI answer synthesis is opt-in through `OPENAI_ANSWER_SYNTHESIS` so demos
  default to deterministic answers grounded in returned rows.

## Phase 11F: Safe Clarification And One Retry

Status: completed on 2026-04-24.

Goal: add controlled agent behavior for ambiguity and recoverable generation
errors without weakening guardrails.

Completed:

- Return `requires_clarification=true` when a natural-language question is too
  ambiguous to execute safely.
- Avoid execution when clarification is required.
- Allow at most one LLM repair attempt for generated SQL that fails validation
  or execution.
- Re-validate repaired SQL with the same guardrails.
- Never repair explicit DML/DDL or user-provided SQL into a different query.

Verification:

- Ambiguous questions return clarification.
- Generated SQL can be repaired once.
- DML/DDL and invalid joins remain blocked.
- HTTP smoke check confirmed ambiguous `trips` returns clarification without
  execution.

## Phase 11G: Streamlit Agent Timeline UI

Status: completed on 2026-04-24.

Goal: make the demo visibly show where the agent behavior happens.

Completed:

- Add an agent timeline panel in `Ask AI` for intent, plan, SQL, guardrail,
  execution, self-check, and answer steps.
- Show the final answer above the result table.
- Show clarification prompts instead of table/chart output when the API asks
  for clarification.
- Preserve SQL expander, result table, chart toggle, CSV export, SQL override,
  and guardrail demos.

Verification:

- Streamlit returns HTTP `200`.
- Timeline renders when `agent_steps` are present.
- Existing table, chart, and export flows still work.
- Demo container inspection confirmed the running Streamlit app contains the
  agent timeline UI.

## Phase 12: Defense Dataset And End-To-End Reproducibility

Status: completed on 2026-04-26.

Goal: prove the implemented MVP can be reproduced from source ingestion through
Gold serving and API access using a fixed defense dataset window.

Completed:

- Selected `2024-01-01` through `2024-06-30` as the fixed defense dataset
  window for thesis/demo/evaluation/performance work.
- Verified MinIO Bronze object paths for six Yellow Taxi monthly files and six
  Green Taxi monthly files in the selected window.
- Verified Taxi Zone Lookup is present as the reference dataset used for zone
  enrichment.
- Started the existing Docker stack with `docker compose up -d`.
- Triggered Airflow DAG run `phase12_2024_01_20260426` with manual
  `{year: 2024, month: 1}` config; the run completed successfully.
- Ran dbt build through the Airflow scheduler container.
- Recorded row counts for Silver, Gold dimensions, Gold fact, and aggregate
  marts in `docs/runbook.md`.
- Verified API health, aggregate mart query, controlled fact/dim query, zone
  demand query, and blocked DDL behavior.
- Kept the selected defense dataset window stable for Phase 13-17 work.

Verification target:

- `docker compose ps` showed Postgres, MinIO, API, demo, Airflow scheduler, and
  Airflow webserver running.
- API `/healthz` returned `status=ok`, DuckDB path, and loaded semantic catalog.
- Streamlit returned HTTP `200`; Airflow `/health` returned HTTP `200`.
- Airflow run `phase12_2024_01_20260426` finished with `success`.
- dbt build completed with `PASS=75 WARN=2 ERROR=0 SKIP=0`; warning-only tests
  were `warn_silver_trip_anomalies` and `warn_gold_metric_anomalies`.
- API smoke checks returned rows for `gold_daily_kpis`, `gold_zone_demand`, and
  `fact_trips` joined to `dim_vendor`.
- Unsafe `drop table gold_daily_kpis` returned HTTP `400`.
- Row counts and caveats are recorded in `docs/runbook.md`.

Next step: Phase 13, Data Quality, Lineage, And Trust Evidence.

## Phase 13: Data Quality, Lineage, And Trust Evidence

Status: completed on 2026-04-26.

Goal: make the lakehouse trustworthy enough for a thesis defense by documenting
data quality checks, lineage, and source-data caveats.

Completed:

- Created `docs/data-quality-report.md`.
- Summarized dbt tests for Bronze, Silver, Gold fact/dim tables, and aggregate
  marts.
- Reported null checks, accepted-value checks, relationship checks, partition
  filtering, and anomaly warnings.
- Added defense-window evidence for records filtered by Silver validity rules,
  invalid pickup/dropoff order, unusually long trip distances, negative amounts,
  and Gold metric anomalies.
- Documented lineage from TLC source files to MinIO Bronze, Silver standardized
  trips, Gold star schema, aggregate marts, API, and read-only agent.
- Separated warning-only source-data caveats from blocking pipeline defects.

Verification target:

- dbt build output from Phase 12 is cited with verification date `2026-04-26`.
- Row counts and anomaly counts are consistent with the Phase 12 defense
  dataset window, `2024-01-01` through `2024-06-30`.
- The report explains that dbt completed with `PASS=75 WARN=2 ERROR=0 SKIP=0`
  and that warning-only anomaly tests are not blocking failures.
- `docs/runbook.md` links the Phase 13 report.

Next step: Phase 14, Agent Evaluation And Guardrail Benchmark.

## Phase 14: Agent Evaluation And Guardrail Benchmark

Status: planned.

Goal: evaluate the read-only AI query agent as an engineering component, not
only as a Streamlit demo.

Required completion:

- Create `docs/agent-evaluation.md`.
- Build an evaluation set of 20-30 Vietnamese and English questions covering
  KPI trends, service comparison, zone demand, vendor analysis, payment type,
  pickup/dropoff analysis, ambiguous questions, and unsafe SQL attempts.
- For each case, record expected behavior: executed answer, clarification, or
  rejection.
- Measure planner behavior by query surface: aggregate mart, star schema,
  clarification, or blocked.
- Verify guardrails for DML, DDL, Bronze/Silver access, unknown table, unknown
  column, `select *` on detailed Gold tables, invalid join, missing `ON`, and
  cartesian join.
- Confirm deterministic answers are grounded only in executed SQL rows.
- Confirm OpenAI answer synthesis remains optional and the demo works when
  synthesis is disabled.

Verification target:

- Existing semantic catalog, SQL guardrail, clarification, trace-shape, and API
  smoke tests continue to pass.
- Docker-based API checks cover the evaluation cases that require runtime
  dependencies from the API container.
- The evaluation report includes pass/fail status, generated SQL or block
  reason, and known limitations.

Next step: Phase 15, Demo Scenario Pack And Product Demo UX.

## Phase 15: Demo Scenario Pack And Product Demo UX

Status: planned.

Goal: turn the system into a stable defense and product-style demo with fixed
scenarios instead of improvised prompts.

Required completion:

- Create `docs/demo-scenarios.md`.
- Define 8-12 official demo scenarios with prompt, expected query surface,
  expected result shape, and the main explanation point for defense.
- Cover schema browsing, aggregate mart questions, star-schema fact/dim
  analysis, Vietnamese natural-language questions, clarification behavior,
  blocked unsafe SQL, charting, SQL expander, agent timeline, and CSV export.
- Ensure Streamlit exposes stable example prompts for monthly Yellow/Green
  comparison, top pickup zones, vendor analysis, payment distribution,
  pickup/dropoff borough analysis, ambiguous questions, and blocked SQL.
- Improve user-facing demo polish only where it reduces confusion: clearer
  errors, stable result state, readable timeline labels, and obvious export
  behavior.

Verification target:

- Streamlit returns HTTP `200`.
- Each official scenario works against the Phase 12 defense dataset window.
- Manual review confirms table output, optional charts, SQL expander, agent
  timeline, clarification output, blocked-query output, and CSV export.
- API smoke checks cover the non-UI scenario behavior where practical.

Next step: Phase 16, Operational Hardening.

## Phase 16: Operational Hardening

Status: planned.

Goal: add product-lite operational evidence around the API and agent while
preserving read-only behavior and local-first deployment.

Required completion:

- Add a query audit log for API/agent requests with question, generated or
  provided SQL, validation status, execution status, execution time, warnings,
  confidence, and clarification or block reason when applicable.
- Improve health checks so they report API status, semantic catalog loading,
  DuckDB connectivity, and configured Gold warehouse path.
- Add or document query timeout, max row limit, and error response behavior.
- Improve error messages for blocked queries without exposing unsafe internals
  or suggesting bypasses.
- Update `.env.example` and `docs/runbook.md` with operational configuration
  notes.
- Do not add write-capable behavior or broaden the agent beyond curated Gold
  access.

Verification target:

- API smoke checks confirm successful, clarification, and blocked requests are
  logged.
- Health endpoint checks pass in Docker.
- Guardrail tests still block unsafe SQL after logging and health changes.
- Runbook documents log location, fields, and troubleshooting steps.

Next step: Phase 17, Performance And Materialization Review.

## Phase 17: Performance And Materialization Review

Status: planned.

Goal: make local demo responsiveness measurable and make materialization
choices defensible.

Required completion:

- Create `docs/performance-report.md`.
- Benchmark representative API queries over the Phase 12 dataset window:
  daily KPI trend, zone demand ranking, vendor aggregation, payment-type
  aggregation, and pickup/dropoff zone joins.
- Review dbt materializations for Gold fact, dimensions, and aggregate marts.
- Keep aggregate marts as the fast path for common dashboard and agent
  questions.
- Add persisted tables, indexes, or DuckDB-friendly materializations only when
  benchmarks show a clear benefit.
- Document tradeoffs between freshness, local storage size, build time, and
  query latency.

Verification target:

- The performance report records before/after timings when changes are made.
- dbt build still passes after any materialization change.
- API smoke tests still pass and SQL guardrails are unchanged.
- If no performance change is needed, document that decision with measured
  evidence.

Next step: Phase 18, CI/CD And Release Packaging.

## Phase 18: CI/CD And Release Packaging

Status: planned.

Goal: make the project easier to verify repeatedly and package for thesis
submission or handoff.

Required completion:

- Add CI checks where the repository environment supports them: Python tests,
  semantic catalog tests, SQL guardrail tests, and lightweight docs consistency
  checks.
- Keep Docker-based verification documented for API checks that depend on the
  container runtime dependency set.
- Standardize `.env.example`, startup instructions, reset instructions, and
  known local ports.
- Create `docs/release-checklist.md` for pre-defense and final-submission
  checks.
- Document which checks are mandatory for code changes, dbt changes, API
  changes, docs-only changes, and final release.

Verification target:

- CI or local equivalent checks can be run from documented commands.
- Release checklist includes Docker startup, Airflow, MinIO, dbt, FastAPI,
  Streamlit, official demo scenarios, and known caveats.
- No secrets are committed or printed in release notes.

Next step: Phase 19, Security-Lite And Governance.

## Phase 19: Security-Lite And Governance

Status: planned.

Goal: add enough security and governance for a product-style demo without
turning the project into a production multi-tenant platform.

Required completion:

- Create `docs/security-notes.md`.
- Document current safety boundaries: read-only DuckDB access, Gold-only query
  surface, semantic catalog validation, SQL guardrails, max row limits, and
  optional answer synthesis.
- Add simple API protection only if needed for demo deployment, such as an API
  key or basic auth.
- Add simple rate limiting only if the selected deployment/demo environment
  needs it.
- Document secret handling, `.env` hygiene, OpenAI API key usage, and audit log
  retention.
- Explicitly keep multi-tenant auth, production RBAC, write agents, and cloud
  production deployment out of scope.

Verification target:

- Security notes describe both implemented controls and out-of-scope controls.
- If API protection is added, API and Streamlit calls are updated and tested.
- Guardrail tests still prove DML/DDL, Bronze/Silver access, invalid joins, and
  detailed wildcard queries are blocked.

Next step: Phase 20, Final Thesis/Product Freeze.

## Phase 20: Final Thesis/Product Freeze

Status: planned.

Goal: freeze a complete thesis-ready and product-style MVP with clear evidence,
scope boundaries, and reproducible instructions.

Required completion:

- Update README with final setup, demo flow, architecture overview, defense
  dataset window, and project scope boundaries.
- Update thesis-facing docs: architecture, data contracts, modeling decisions,
  Gold star schema, runbook, data quality report, agent evaluation, demo
  scenarios, performance report, release checklist, and security notes.
- Mark thesis-critical phases as completed or clearly document remaining
  caveats.
- Freeze out-of-scope items: FHV/HVFHV, streaming, write-capable agents,
  multi-tenant auth, production cloud deployment, and new agent frameworks.
- Tag or record the final commit/version used for thesis submission.

Verification target:

- Fresh `docker compose up -d` works from documented instructions.
- Full relevant test suite and Docker smoke checks pass.
- Final demo scenarios work against the Phase 12 defense dataset window.
- Final docs contain verification date, commands, results, pass/fail status,
  caveats, and next-step notes.

Next step: post-thesis extensions only after the thesis/product MVP is frozen.

## Documentation And Handoff Rule

After each meaningful phase or working session, update the durable project
state before stopping:

- `docs/development-roadmap.md`: phase status, date, completed work, remaining
  work, and next step.
- `docs/runbook.md`: commands run, results, caveats, and operational notes.
- `docs/modeling-decisions.md`: modeling decisions or changes to the role of
  star schema and marts.
- `docs/codex-agent-playbook.md`: workflow rule changes for future agents.
- `contracts/semantic_catalog.yaml` and related tests: any agent-visible schema
  or guardrail change.

Use explicit statuses: `completed`, `in progress`, `planned`, or `blocked`.
Do not end a meaningful session with ambiguous project state.

## Verification Defaults

- Python/unit changes: `python -m pytest -p no:cacheprovider`
- dbt model or schema changes: dbt build inside the Airflow scheduler container
  as documented in `docs/runbook.md`
- API guardrail changes: SQL guardrail tests and API smoke tests
- Docs-only changes: review roadmap, modeling, runbook, and playbook terminology
  for consistency
