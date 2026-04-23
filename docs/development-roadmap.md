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
- Aggregate marts are the fast path for common questions. They do not replace
  the Gold star schema.
- AI may query `fact_trips` and dimensions only after semantic metadata and
  guardrails describe allowed tables, columns, metrics, and joins.

## Phase 1: Stabilize MVP Lakehouse

Status: completed for the MVP.

- Harden monthly ingestion for Yellow and Green.
- Keep monthly partition semantics in paths, manifests, and dbt models.
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

## Phase 6: Star Schema Semantic Catalog

Status: next.

Goal: extend `contracts/semantic_catalog.yaml` so it describes the full Gold
star schema, not only aggregate marts.

Required changes:

- Add catalog entries for `fact_trips`, `dim_date`, `dim_zone`,
  `dim_service_type`, `dim_vendor`, and `dim_payment_type`.
- For each table, describe `table_type`, `grain`, `fields`, `metrics`,
  `primary_key`, `foreign_keys`, and `allowed_filters` where applicable.
- For `fact_trips`, describe allowed joins:
  - `fact_trips.pickup_date = dim_date.pickup_date`
  - `fact_trips.service_type = dim_service_type.service_type`
  - `fact_trips.vendor_id = dim_vendor.vendor_id`
  - `fact_trips.payment_type = dim_payment_type.payment_type`
  - `fact_trips.pickup_zone_id = dim_zone.zone_id`
  - `fact_trips.dropoff_zone_id = dim_zone.zone_id`
- Update semantic catalog tests and prompt rendering tests.
- Update this roadmap and `docs/runbook.md` after verification.

## Phase 7: Column And Table Guardrails

Status: planned.

Goal: validate generated SQL against the semantic catalog at table and column
level.

Required changes:

- Validate referenced tables are cataloged.
- Validate referenced columns are cataloged for the table or alias being used.
- Reject unknown columns.
- Reject `select * from fact_trips`.
- Keep current rules: one statement only, `SELECT` only, no DML/DDL, no
  Bronze/Silver, enforced `LIMIT`.
- Add guardrail tests for unknown table, unknown column, `select * from
  fact_trips`, and valid aggregate mart queries.

## Phase 8: Join Guardrails For Star Schema

Status: planned.

Goal: allow AI to query fact/dim data only through approved star-schema join
paths.

Required changes:

- Parse joins with `sqlglot`.
- Require joins between `fact_trips` and dimensions to match cataloged allowed
  joins.
- Reject cartesian joins, joins without `ON`, and joins on the wrong keys.
- Support both `dim_zone` roles:
  - pickup zone through `fact_trips.pickup_zone_id`
  - dropoff zone through `fact_trips.dropoff_zone_id`
- Add tests for valid joins, invalid joins, missing `ON`, and cartesian joins.

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
