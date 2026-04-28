# Security Notes

Verification date: `2026-04-28`

This document records the current security and governance posture for the
local-first thesis/demo MVP. It is intentionally security-lite: enough to make
the demo boundaries explicit without turning the project into a production
multi-tenant platform.

## Scope

Current security scope:

- Protect the read-only query path from unsafe SQL.
- Restrict the API agent to curated Gold objects.
- Keep secrets out of git and release notes.
- Preserve local audit evidence for successful, clarified, blocked, and failed
  query attempts.
- Make optional OpenAI usage explicit and grounded in executed results.

Out of scope for this MVP:

- Multi-tenant authentication.
- Production RBAC.
- Public internet deployment.
- Write-capable agents.
- Cloud secret managers.
- Production-grade rate limiting, abuse detection, or WAF controls.

## Implemented Controls

The API query path has these implemented controls:

- DuckDB is opened in read-only mode for query execution.
- SQL guardrails allow `SELECT` statements only.
- DML and DDL are rejected before execution.
- Queries must reference curated Gold tables from
  `contracts/semantic_catalog.yaml`.
- Bronze and Silver tables are not part of the agent query surface.
- `execution_enabled` controls which cataloged Gold tables can be queried.
- Referenced tables, aliases, and columns are validated against the semantic
  catalog.
- Detailed Gold tables such as `fact_trips` reject wildcard `SELECT *`.
- Joins must use explicit `ON` predicates and match cataloged allowed join
  paths.
- Missing-`ON` joins, cartesian joins, and invalid star-schema joins are
  blocked.
- API requests cap `max_rows` at `1000`.
- Ambiguous natural-language questions can return clarification instead of
  executing a broad query.
- Agent self-checks add warnings for empty results, capped results, suspicious
  numeric values, unusual date ranges, and missing expected grouping columns.

## OpenAI Usage

OpenAI is used only in the API agent path and only when configured locally:

- `OPENAI_API_KEY=replace-me` keeps deterministic demo paths available without
  calling OpenAI.
- Natural-language SQL generation requires a configured API key when no
  deterministic planner path applies.
- `OPENAI_ANSWER_SYNTHESIS=false` is the default.
- When answer synthesis is enabled, the model receives only the executed SQL and
  returned rows. It is not allowed to generate new SQL or access external data
  during answer synthesis.
- Generated or repaired SQL is always validated by the same guardrails before
  execution.

## Secret Handling

Expected local secret handling:

- `.env` is local-only and must not be committed.
- `.env.example` contains placeholders or non-sensitive local defaults.
- Do not copy real `OPENAI_API_KEY`, MinIO passwords, or other secrets into
  documentation, screenshots, release notes, issue text, or terminal transcripts
  intended for sharing.
- If a real API key or password is exposed, rotate it before sharing the repo or
  defense artifacts.
- `scripts/release_check.py` checks that `.env` is not tracked and scans
  README/docs for obvious OpenAI secret patterns.

## Audit Logging

API query audit logging is enabled through `QUERY_AUDIT_LOG_PATH`.

Current default:

- `/data/warehouse/query_audit.jsonl`

Audit events include:

- timestamp
- status
- question
- SQL override flag
- requested max rows
- final or provided SQL
- execution time
- warnings
- confidence
- clarification fields
- agent step statuses
- error type and detail when applicable

Audit logging intentionally does not store result rows. Audit log write failures
do not fail read-only API queries.

Retention for the local MVP:

- Keep the audit log for defense/debugging evidence while the local environment
  is active.
- Before sharing artifacts, review the audit log for user-entered prompts that
  may contain sensitive text.
- For a clean demo reset, archive or delete the local audit log only after any
  needed verification evidence has been recorded in docs.

## API Protection Decision

No API key, basic auth, or rate limiting was added in Phase 19.

Reason:

- The current release target is a local-first thesis/product demo on localhost.
- The documented scope explicitly excludes production access control.
- Adding authentication would require Streamlit/API wiring and extra operator
  steps without improving the local defense workflow.

If the demo is exposed outside localhost later, add these before deployment:

- A simple API key or basic auth for FastAPI.
- Matching Streamlit request headers or login configuration.
- Basic per-client rate limiting.
- Secret injection through the deployment environment rather than committed
  files.
- Docker/API smoke tests proving unauthenticated requests are rejected and
  authenticated demo requests still work.

## Governance Boundaries

Data governance boundaries:

- Current sources are Yellow Taxi, Green Taxi, and Taxi Zone Lookup only.
- Taxi Zone Lookup is reference enrichment data, not a new fact source.
- FHV, HVFHV, streaming ingestion, and extra reference datasets remain out of
  scope until explicitly reintroduced.
- MinIO is the Bronze source of truth. Local `data/` files are cache/fallback
  files.
- Gold is the only serving surface for the API agent.
- Aggregate marts remain the fast path for common dashboard and agent questions.
- Fact/dimension queries are allowed only through semantic metadata, cataloged
  columns, wildcard restrictions, and allowed joins.

Operational governance boundaries:

- Use `docker compose up -d` for the existing local stack.
- Use `docker compose up -d --build` only when Dockerfiles, dependency files,
  Compose config, or image-copied source files change.
- Prefer Docker-based API and guardrail verification because the API container
  has the runtime dependency set used by the app.
- Keep verification evidence in `docs/runbook.md`,
  `docs/development-roadmap.md`, and the phase-specific reports.

## Verification

Phase 19 verification target:

- `python scripts/release_check.py` passes.
- `python -m pytest -p no:cacheprovider` passes or reports only known
  dependency-gated skips.
- Security notes are linked from the release checklist and roadmap.
- Guardrail evidence remains covered by Phase 14, Phase 16, and Phase 18
  verification: valid Gold queries execute, DML/DDL is blocked, Bronze/Silver
  access is blocked, invalid joins are blocked, and detailed wildcard queries
  are blocked.
