# Development Roadmap

This roadmap keeps the lakehouse MVP stable while moving the AI layer toward
controlled querying over the Gold star schema.

## Principles

- Current sources: Yellow Taxi, Green Taxi, Taxi Zone Lookup.
- Keep the project local-first, repeatable, and testable.
- Defer FHV, HVFHV, streaming, write-capable agents, LangChain/LangGraph, and
  production auth until the current MVP and AI star-schema path are stable.
- Gold is the serving layer. It contains both the star schema and aggregate
  marts.
- MinIO is the Bronze object-storage source of truth. Local `data/` files are
  ingestion cache/fallback files.
- Aggregate marts are the fast path for common questions. They do not replace
  the Gold star schema.
- AI may query `fact_trips` and dimensions only after semantic metadata and
  guardrails describe allowed tables, columns, metrics, and joins.

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
- Keep the semantic catalog aligned with AI-queryable Gold objects.

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
- Aggregate marts remain useful for common dashboard and AI questions, but they
  are not a substitute for the Gold star schema.

## Phase 5: Update Codex Workflow, Project State, And Session Handoff

Status: completed on 2026-04-23.

Completed:

- Updated project guidance so future Codex sessions know the Gold star schema is
  already implemented.
- Clarified that aggregate marts are a fast/safe path, while the next direction
  is controlled AI querying over `fact_trips` and `dim_*`.
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
level without widening the executable AI query surface.

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
- Keep `fact_trips` and all `dim_*` tables `execution_enabled: false` in the
  real semantic catalog until prompt planning and controlled exposure are ready.

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
  query and HTTP `400` for direct `fact_trips` access, confirming the real
  semantic catalog still keeps fact/dim execution disabled.

## Phase 9: Text-to-SQL Planner For Star Schema

Status: planned.

Goal: guide the LLM to choose the right query surface and only generate
catalog-safe SQL.

Required changes:

- Prefer aggregate marts for simple daily KPI and zone demand questions.
- Use the Gold star schema for vendor, payment type, pickup/dropoff role, and
  flexible fact/dim analysis.
- Render catalog metadata grouped by aggregate marts, fact, dimensions, and
  allowed joins.
- Instruct the model not to use unknown columns, non-cataloged joins, or
  `select *`.
- Add prompt rendering tests and API smoke tests for star-schema questions.

## Phase 10: Controlled Fact/Dim Exposure And Demo Readiness

Status: planned.

Goal: officially support controlled API queries over `fact_trips` and
dimensions, then make the demo and thesis docs clear.

Required changes:

- Allow `/api/v1/query` to execute valid SQL over `fact_trips` and `dim_*` after
  Phase 6-8 guardrails are in place.
- Add API smoke tests for fact plus `dim_vendor`, `dim_payment_type`, and
  pickup-role `dim_zone`.
- Keep aggregate mart smoke tests.
- Update Streamlit demo with mart query, star-schema query, blocked query, and
  semantic catalog views.
- Fix stale or unclear docs, including README and architecture text, so they
  describe the current implemented state.

## Documentation And Handoff Rule

After each meaningful phase or working session, update the durable project
state before stopping:

- `docs/development-roadmap.md`: phase status, date, completed work, remaining
  work, and next step.
- `docs/runbook.md`: commands run, results, caveats, and operational notes.
- `docs/modeling-decisions.md`: modeling decisions or changes to the role of
  star schema and marts.
- `docs/codex-agent-playbook.md`: workflow rule changes for future agents.
- `contracts/semantic_catalog.yaml` and related tests: any AI-visible schema or
  guardrail change.

Use explicit statuses: `completed`, `in progress`, `planned`, or `blocked`.
Do not end a meaningful session with ambiguous project state.

## Verification Defaults

- Python/unit changes: `python -m pytest -p no:cacheprovider`
- dbt model or schema changes: dbt build inside the Airflow scheduler container
  as documented in `docs/runbook.md`
- API guardrail changes: SQL guardrail tests and API smoke tests
- Docs-only changes: review roadmap, modeling, runbook, and playbook terminology
  for consistency
