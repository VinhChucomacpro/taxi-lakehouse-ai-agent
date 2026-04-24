# AGENTS.md

This repository is a graduation-project codebase for a local-first taxi data
lakehouse with a read-only AI query agent.

## Project Goal

Build an end-to-end data platform for NYC TLC taxi data that:

- ingests monthly `Yellow Taxi` and `Green Taxi` trip data
- organizes data into `Bronze -> Silver -> Gold`
- serves curated analytics from `Gold`
- exposes a read-only `Text-to-SQL` style API over curated tables

## Current Scope

Only these sources are in scope for the current phase:

- `Yellow Taxi`
- `Green Taxi`
- `Taxi Zone Lookup` as a reference dataset only

The following are out of scope until the MVP lakehouse is stable:

- extra reference datasets beyond Taxi Zone Lookup unless the user explicitly reintroduces them
- `FHV`
- `HVFHV`
- streaming ingestion
- write-capable AI agents
- multi-tenant auth or production-grade access control

## Technology Decisions

Prefer these technologies unless the user explicitly changes direction:

- `Airflow` for orchestration
- `dbt` for transformations and tests
- `DuckDB` for local analytics serving
- `MinIO` for object storage
- `FastAPI` for the query API
- `OpenAI API` for the AI query layer
- `sqlglot` for SQL validation and guardrails

Do not switch the core architecture to Vanna, LangChain, LangGraph, or another
agent framework unless the user explicitly asks for that change.

## Current Project State

- The MVP Gold star schema is implemented.
- Gold star schema models are `fact_trips`, `dim_date`, `dim_zone`,
  `dim_service_type`, `dim_vendor`, and `dim_payment_type`.
- Aggregate marts `gold_daily_kpis` and `gold_zone_demand` are built from the
  star schema and remain useful as a fast/safe path for common dashboard and AI
  questions.
- The semantic catalog covers the full Gold star schema, but only aggregate
  marts are currently `execution_enabled`.
- Column/table guardrails validate cataloged tables, execution-enabled tables,
  known columns, table aliases, and wildcard restrictions for detailed Gold
  tables.
- Join guardrails validate explicit `ON` joins against semantic catalog
  `allowed_joins` and reject missing-`ON` or cartesian joins.
- Text-to-SQL prompt planning prefers aggregate marts for common questions and
  keeps runtime prompts limited to execution-enabled tables.
- The next major direction is controlled AI querying over the Gold star schema:
  controlled fact/dim API exposure.

## Local Environment Notes

- Docker images have already been built for this project environment.
- Prefer starting the existing stack with `docker compose up -d`.
- Do not install project runtime dependencies into the host Python environment
  just because local pytest skips dependency-gated tests. The API container
  already has runtime dependencies such as `sqlglot` and `duckdb`.
- For SQL guardrail/API verification, prefer running checks inside Docker,
  especially the `api` container, unless the user explicitly asks for host-local
  setup.
- Use `docker compose up -d --build` only after Dockerfile, image dependency,
  requirements, or compose changes, or when a rebuild is needed to pick up code
  copied into an image.

## Data Modeling Rules

- Bronze stores raw source files with minimal mutation.
- MinIO is the Bronze object-storage source of truth. Local `data/` files are
  download/cache files for ingestion and development fallback, not the primary
  dbt Bronze source.
- Silver standardizes schema across Yellow and Green datasets.
- Gold contains the star schema and curated aggregate marts for BI and AI
  querying.
- The AI layer must only query `Gold` tables or views.
- Prefer adding `service_type` to marts where Yellow and Green are combined.
- Keep monthly partition semantics visible in paths and pipeline manifests.
- Keep Yellow and Green as the primary fact sources; Taxi Zone Lookup is only for enrichment.
- For the current MVP, keep `gold_daily_kpis` and `gold_zone_demand` as curated
  serving marts.
- Do not treat star schema as missing work; it already exists in Gold.
- Do not expose `fact_trips` broadly to the AI layer until the semantic catalog
  describes its grain, metrics, columns, keys, and allowed join paths.
- Keep aggregate marts as the fast path for common questions, not as a
  replacement for controlled fact/dim querying.

## Coding Priorities

When editing this repo, prioritize work in this order:

1. ingestion reliability for Yellow and Green TLC data
2. stable Bronze to Silver to Gold transforms
3. data quality checks and repeatability
4. AI query safety and schema-aware answers
5. dashboard and user-facing polish

## Guardrails For The AI Query Layer

- read-only behavior only
- no DML or DDL
- no access outside curated `Gold` objects
- validate generated SQL before execution
- apply limits to ad hoc queries
- enforce allowed semantic join paths before fact/dim execution is enabled
- prefer explicit semantic metadata over inferring business meaning from names

## Repo Navigation

- `airflow/dags/` contains orchestration code
- `dbt/` contains Bronze, Silver, and Gold models
- `services/api/` contains the query API
- `contracts/` contains semantic metadata for the AI layer
- `docs/` contains project context for humans and agents
- `docs/development-roadmap.md` contains the phased roadmap
- `docs/modeling-decisions.md` explains the current Gold star schema and why
  aggregate marts remain as a fast path
- `docs/codex-agent-playbook.md` contains the repo-specific agent workflow

## Working Style For Agents

- keep changes incremental and scoped
- do not introduce extra frameworks without clear value
- prefer local-first, testable implementations
- preserve the project narrative: lakehouse first, AI agent on top
- at the end of a meaningful working session, update the relevant docs with what
  was completed, verified, and left as the next step so future sessions do not
  have to rediscover it
- after completing a roadmap phase, update `docs/development-roadmap.md` with
  explicit status, verification date when available, caveats, and the next step
- before changing architecture, read `docs/modeling-decisions.md`
- before adding tables available to AI, update `contracts/semantic_catalog.yaml`
  and corresponding guardrail/API tests
- before exposing `fact_trips` or any `dim_*` table to AI, add semantic metadata
  and guardrail tests for allowed columns and joins
- when changing dbt models, update `dbt/models/schema.yml` tests and docs in the
  same change
- when changing ingestion paths or manifests, update `docs/runbook.md` and
  ingestion tests
- when changing Bronze storage or dbt read paths, update `docs/data-contracts.md`,
  `docs/runbook.md`, and tests that assert the expected storage source
- prefer Docker-based verification for API guardrails and services because the
  built images contain the runtime dependency set used by the app

## Suggested Verification

Choose the smallest verification that covers the changed area:

- Python/unit changes: `python -m pytest -p no:cacheprovider`
- dbt model changes: run dbt build through the Airflow scheduler container as
  documented in `docs/runbook.md`
- API guardrail changes: run SQL guardrail and API smoke tests
- docs-only changes: review Markdown links and terminology consistency
