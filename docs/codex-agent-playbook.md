# Codex Agent Playbook

Use this file to reduce guesswork before editing the repo. Preserve the project
story: lakehouse first, read-only AI agent on top.

## Read First

1. `AGENTS.md` for scope and repository rules.
2. `README.md` for MVP status and local commands.
3. `docs/modeling-decisions.md` for Gold, marts, dim/fact, or semantic-layer work.
4. `docs/development-roadmap.md` for phased direction.
5. `docs/runbook.md` for Airflow, dbt, operations, and verification.
6. `contracts/semantic_catalog.yaml` before any AI query or guardrail work.

## Quick File Reference

Use this table to go directly to the likely source of truth instead of scanning
the repo first.

| Task or question | Primary file(s) | Notes |
| --- | --- | --- |
| project scope, allowed sources, architecture constraints | `AGENTS.md` | Read first before major changes |
| current roadmap phase and next step | `docs/development-roadmap.md` | Phase status only, not detailed schema |
| modeling decisions and why the current approach exists | `docs/modeling-decisions.md` | Use before changing Gold design |
| detailed Gold star schema columns, grain, join paths | `docs/gold-star-schema.md` | Canonical doc for dim/fact structure |
| operational commands, dbt build flow, Docker and MinIO run steps | `docs/runbook.md` | Source of truth for verification commands |
| Bronze, Silver, Gold model tests and docs metadata | `dbt/models/schema.yml` | Update in the same change as model edits |
| Gold dim/fact SQL models | `dbt/models/gold/` | Includes `fact_trips`, `dim_*`, and marts |
| ingestion DAG orchestration | `airflow/dags/` | Use for schedule and pipeline flow changes |
| ingestion implementation and tests | `tests/test_tlc_ingestion.py` | Start here for ingestion regressions |
| AI-visible semantic metadata | `contracts/semantic_catalog.yaml` | Required before exposing new AI-queryable tables |
| SQL safety and query restrictions | `services/api/app/sql_guardrails.py` | Main guardrail logic |
| semantic catalog loading in API | `services/api/app/catalog.py` | Reads and validates catalog for app usage |
| prompt assembly for Text-to-SQL | `services/api/app/text_to_sql.py` | Main LLM prompt rendering path |
| API entrypoints and query execution path | `services/api/app/` | Inspect alongside guardrails and catalog |
| SQL guardrail tests | `tests/test_sql_guardrails.py` | Update when SQL policy changes |
| semantic catalog tests | `tests/test_semantic_catalog.py` | Update when catalog shape or validation changes |
| API smoke tests | `tests/test_api_smoke.py` | Use for end-to-end query behavior checks |
| repo-specific agent workflow guidance | `docs/codex-agent-playbook.md` | Update when agent workflow changes |

## Current Project State

- Gold star schema is implemented for the MVP.
- Star schema models are `fact_trips`, `dim_date`, `dim_zone`,
  `dim_service_type`, `dim_vendor`, and `dim_payment_type`.
- Aggregate marts `gold_daily_kpis` and `gold_zone_demand` are built from the
  star schema. They are a fast/safe path for common questions, not a replacement
  for the star schema.
- The current roadmap direction is controlled AI querying over the Gold star
  schema, starting with semantic metadata and then column/table and join
  guardrails.

## Session Closeout

Before ending a meaningful session, leave a durable note in docs when the work
changes project state or operational knowledge:

- update `docs/runbook.md` when verification, Docker, Airflow, dbt, MinIO, API,
  or demo behavior was tested
- update `docs/development-roadmap.md` when a roadmap phase changes status or
  the next priority changes
- update `docs/modeling-decisions.md` when a modeling decision is made or
  revised
- update `AGENTS.md` or this playbook when an agent workflow rule changes

The note should include what was completed, what was verified, known caveats,
and the recommended next step. Do not include secrets from `.env`.

When a phase is completed, explicitly record `completed`, `in progress`,
`planned`, or `blocked` in `docs/development-roadmap.md`. Include verification
commands and results in `docs/runbook.md` when tests, dbt, Docker, API, or demo
behavior were checked.

## Architecture Rules

- Keep Airflow, dbt, DuckDB, MinIO, FastAPI, OpenAI API, and `sqlglot` unless the
  user explicitly changes direction.
- Do not add FHV, HVFHV, streaming, write-capable agents, or production auth
  before the MVP lakehouse is stable.
- Do not let AI query Bronze or Silver.
- Do not add DML or DDL capability to the AI query path.
- Prefer explicit semantic metadata over business inference from column names.

## Docker Workflow

- Prefer `docker compose up -d` when images are already built.
- Use `docker compose up -d --build` only when Dockerfiles, Compose config,
  `requirements.txt`, dependency installation, or image-copied source files
  changed.
- If only mounted code, docs, `.env`, local data, or DuckDB content changed, do
  not rebuild unless a container restart fails to pick up the change.
- Use `docker compose ps` before starting services to avoid unnecessary rebuilds
  or restarts.
- Use targeted services when possible, for example `docker compose up -d api demo`
  for API/demo checks.

## Layer Rules

### Bronze

- Keep source files as raw as practical.
- Preserve `year=YYYY/month=MM` paths for Yellow and Green.
- Treat Taxi Zone Lookup as reference data, not a fact source.
- If manifests or object keys change, update the runbook and ingestion tests.

### Silver

- Silver is normalized trip-level data for Yellow and Green.
- Preserve `service_type`, `source_year`, and `source_month` for lineage.
- Filter trips whose `pickup_at` falls outside the source file partition month.
- Do not put business aggregates in Silver.

### Gold

- MVP serving marts are `gold_daily_kpis` and `gold_zone_demand`.
- Gold star schema exists: `fact_trips`, `dim_date`, `dim_zone`,
  `dim_service_type`, `dim_vendor`, and `dim_payment_type`.
- `fact_trips` grain should be one valid Silver trip per row.
- Keep aggregate marts as the fast path for common dashboard and AI questions.
- If adding a Gold model, update `dbt/models/schema.yml`, docs, and
  `contracts/semantic_catalog.yaml` when AI may query it.

## Dim/Fact Layer

Use this direction when maintaining dimensional models:

- `dim_date`: date, month, quarter, year, day of week.
- `dim_zone`: from Taxi Zone Lookup, with `zone_id`, `borough`, `zone_name`, and
  `service_zone` when available.
- `dim_service_type`: small catalog for `yellow_taxi` and `green_taxi`.
- `fact_trips`: from `silver_trips_unified`, with base metrics such as
  `trip_distance`, `fare_amount`, `total_amount`, and `passenger_count`.

Do not assume `fact_trips` should be avoided forever. The intended next step is
controlled AI querying over the star schema after semantic metadata, column
guardrails, and join guardrails are in place. If a question becomes very common,
a Gold aggregate mart can still be added as a fast path.

## AI Query Layer

- `contracts/semantic_catalog.yaml` is the AI-visible Gold catalog.
- `execution_enabled` in the semantic catalog controls whether a cataloged Gold
  table is currently allowed in prompt generation and `/api/v1/query`.
- `services/api/app/sql_guardrails.py` blocks unsafe SQL.
- `services/api/app/text_to_sql.py` renders semantic metadata into the LLM prompt.
- `services/api/app/catalog.py` loads catalog metadata for the API and prompt.
- When exposing new tables to AI, add clear field descriptions.
- Before exposing `fact_trips` or any `dim_*` table, catalog table type, grain,
  fields, metrics, keys, allowed filters, and allowed joins, then add guardrail
  and API tests.

## Verification By Change Type

- Ingestion: `tests/test_tlc_ingestion.py`
- dbt models/schema: dbt build from `docs/runbook.md`
- SQL guardrails: `tests/test_sql_guardrails.py`
- API behavior: `tests/test_api_smoke.py`
- Semantic catalog: `tests/test_semantic_catalog.py`
- Docs only: check links and terms across `AGENTS.md`, `README.md`, and docs.

General command:

```bash
python -m pytest -p no:cacheprovider
```

## Defaults When Details Are Missing

- Architecture: follow `AGENTS.md`.
- Modeling: keep Silver trip-level; keep Gold centered on the star schema with
  aggregate marts as the fast path.
- AI safety: choose the more restrictive query surface.
- New features: prefer small, testable increments over new sources/frameworks.
- Local stack: start existing images with `docker compose up -d`; rebuild only
  when the changed files require it.
