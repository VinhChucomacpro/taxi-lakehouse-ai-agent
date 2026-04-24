# Architecture Review For Thesis Defense

This review summarizes the current architecture and the optimization backlog for
the next phases. It is written for thesis defense preparation: explain what is
implemented, why the design is reasonable, and which limitations are known.

## Current Architecture Assessment

The current architecture is coherent for a local-first analytical lakehouse:

- Airflow owns monthly orchestration and keeps partition semantics visible.
- MinIO is the Bronze object-storage source of truth.
- Local `data/` files are ingestion cache and development fallback only.
- dbt owns `Bronze -> Silver -> Gold` transformations and data tests.
- DuckDB is the local analytical warehouse for Gold serving.
- FastAPI exposes read-only schema/query endpoints and hosts the internal agent
  workflow.
- The read-only agent is constrained by semantic metadata and SQL guardrails
  before DuckDB execution, then returns agent steps, warnings, confidence, and
  an answer grounded in executed rows.

This shape is appropriate for the project goal because it separates ingestion,
modeling, serving, and AI safety. It also keeps the stack explainable for a
graduation project without adding unnecessary agent frameworks.

## Design Strengths

- The lakehouse story is clear: raw source files land in Bronze, Silver
  standardizes Yellow and Green trips, and Gold serves analytics and AI.
- Gold now has both a dimensional star schema and aggregate marts. This gives a
  fast path for common questions while preserving a flexible analytical base.
- `contracts/semantic_catalog.yaml` is an explicit agent-facing contract
  instead of relying on table names alone.
- `execution_enabled` separates cataloged tables from executable tables, so
  `fact_trips` and dimensions can be documented before they are exposed.
- DuckDB is opened in read-only mode and generated SQL is parsed with
  `sqlglot`, which makes the agent query surface safer and easier to defend.
- The read-only agent workflow is explicit and framework-light: intent,
  planning, SQL generation, validation, execution, self-check, and answer.
- The scope is controlled: Yellow Taxi, Green Taxi, and Taxi Zone Lookup only.

## Known Limitations

- SQL guardrails validate tables, execution status, cataloged columns, and
  approved semantic join paths. The agent planner prefers aggregate marts for
  common questions, while controlled fact/dim API exposure supports vendor,
  payment type, pickup zone, and dropoff zone analysis.
- The semantic catalog, dbt schema docs, and human documentation are separate
  sources that can drift. A future consistency test should compare Gold model
  names and catalog entries.
- `fact_trips` currently has a clear grain but no explicit surrogate key. This
  is acceptable for the current serving layer, but a `trip_key` should be
  considered if lineage, row-level drilldown, or stronger uniqueness tests are
  needed.
- Gold models are configured as views. This is simple and transparent, but the
  project should evaluate materializing large Gold objects if demo latency or
  repeated queries become a problem.
- The Airflow DAG has a `publish_metadata` placeholder. It should either publish
  useful run metadata or be removed from the defense narrative.
- Some API and guardrail tests can skip when optional dependencies are missing.
  A defense verification environment should install the full project dependency
  set and record non-skipped test results.

## Defense Narrative

Use this narrative when explaining the system:

1. The platform first solves reliable local lakehouse ingestion and modeling.
2. MinIO is the durable Bronze source of truth; local files are only cache.
3. dbt makes transformation logic repeatable and testable.
4. Gold has two serving surfaces: aggregate marts for safe common questions and
   star schema for controlled flexible analysis.
5. The read-only agent is intentionally constrained. It does not query Bronze or
   Silver, cannot write data, and only executes SQL after semantic and syntactic
   validation.
6. The agent workflow records intent, plan, guardrail, execution, self-check,
   and answer steps for demo and defense transparency.
7. `fact_trips` and dimensions are queryable only through semantic metadata,
   explicit columns, and approved joins. This is the controlled star-schema
   path.

## Next Optimization Backlog

Priority 1: Guardrail hardening

- Keep table execution gating.
- Validate referenced columns and aliases against the semantic catalog.
- Reject wildcard access on detailed Gold tables.
- Add guardrail tests for unknown table, unknown column, disabled fact access,
  valid mart query, and CTE usage.

Priority 2: Join guardrails

- Enforce `allowed_joins` from the semantic catalog.
- Reject cartesian joins, joins without `ON`, and joins on wrong keys.
- Support both pickup and dropoff `dim_zone` roles.

Status: implemented for guardrail validation, prompt planning, and controlled
fact/dim execution.

Priority 3: Catalog and documentation consistency

- Add tests to detect drift between dbt Gold models and semantic catalog tables.
- Keep README, architecture docs, data contracts, and roadmap aligned after each
  phase.
- Decide whether `fact_trips` needs a surrogate key for stronger explanation and
  testing.

Priority 4: Demo and defense readiness

- Use a stable demo flow: schema view, Ask AI with agent timeline, valid mart
  query, controlled star-schema query, blocked unsafe query, chart, and CSV
  export.
- Run full tests in an environment where `sqlglot`, `duckdb`, `httpx`, and API
  dependencies are installed.
- Record verification date, command output, caveats, and next step in
  `docs/runbook.md`.
