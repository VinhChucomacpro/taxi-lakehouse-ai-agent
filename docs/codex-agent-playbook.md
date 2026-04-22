# Codex Agent Playbook

Use this file to reduce guesswork before editing the repo. Preserve the project
story: lakehouse first, read-only AI agent on top.

## Read First

1. `AGENTS.md` for scope and repository rules.
2. `README.md` for MVP status and local commands.
3. `docs/modeling-decisions.md` for Gold, marts, dim/fact, or semantic-layer work.
4. `docs/development-roadmap.md` for phased direction.
5. `docs/runbook.md` for Airflow, dbt, operations, and verification.

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
- Next-phase dimensional models may include `dim_date`, `dim_zone`,
  `dim_service_type`, and `fact_trips`.
- `fact_trips` grain should be one valid Silver trip per row.
- Keep aggregate marts as the preferred surface for common dashboard and AI
  questions.
- If adding a Gold model, update `dbt/models/schema.yml`, docs, and
  `contracts/semantic_catalog.yaml` when AI may query it.

## Next-Phase Dim/Fact

Use this direction when implementing dimensional models:

- `dim_date`: date, month, quarter, year, day of week.
- `dim_zone`: from Taxi Zone Lookup, with `zone_id`, `borough`, `zone_name`, and
  `service_zone` when available.
- `dim_service_type`: small catalog for `yellow_taxi` and `green_taxi`.
- `fact_trips`: from `silver_trips_unified`, with base metrics such as
  `trip_distance`, `fare_amount`, `total_amount`, and `passenger_count`.

Do not make AI join facts for every recurring question. If a question becomes
common, create a Gold mart and expose that mart through the semantic catalog.

## AI Query Layer

- `contracts/semantic_catalog.yaml` is the AI-visible Gold catalog.
- `services/api/app/sql_guardrails.py` blocks unsafe SQL.
- When exposing new tables to AI, add clear field descriptions.
- For future facts/dims, catalog table type, grain, metrics, and join keys before
  allowing AI access.

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
- Modeling: keep Silver trip-level and Gold serving-oriented.
- AI safety: choose the more restrictive query surface.
- New features: prefer small, testable increments over new sources/frameworks.
- Local stack: start existing images with `docker compose up -d`; rebuild only
  when the changed files require it.
