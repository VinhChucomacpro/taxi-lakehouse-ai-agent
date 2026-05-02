# Release Checklist

Verification date: `2026-04-28`

Use this checklist before a thesis defense, final submission, or handoff. It is
designed for the current local-first MVP, not for production cloud deployment.

## Release Scope

Included:

- Yellow Taxi and Green Taxi monthly trip data.
- Taxi Zone Lookup as the reference enrichment dataset.
- MinIO-backed Bronze source of truth.
- dbt `Bronze -> Silver -> Gold` transformations.
- Gold star schema and aggregate marts.
- FastAPI read-only query agent over curated Gold objects.
- Streamlit demo with official demo scenarios.

Out of scope:

- FHV and HVFHV sources.
- Streaming ingestion.
- Write-capable agents.
- Multi-tenant auth or production RBAC.
- Production cloud deployment.
- LangChain, LangGraph, Vanna, or another agent framework.

## Environment

Before release:

- `.env.example` is present and contains all required configuration keys.
- `.env` exists only locally and is not committed.
- `OPENAI_API_KEY` is either `replace-me` for deterministic demo mode or a real
  local secret kept outside git.
- `OPENAI_ANSWER_SYNTHESIS=false` unless the demo explicitly needs opt-in answer
  synthesis from executed rows.
- Docker Desktop is running before Docker-based checks.
- `docs/security-notes.md` is current for the release scope, implemented
  guardrails, secret handling, OpenAI usage, and audit log retention.

Known local ports:

| Service | URL |
| --- | --- |
| Airflow | `http://localhost:8080` |
| MinIO Console | `http://localhost:9001` |
| FastAPI docs | `http://localhost:8000/docs` |
| Streamlit demo | `http://localhost:8501` |

## Standard Verification

Run Python tests:

```bash
python -m pytest -p no:cacheprovider
```

Run release consistency checks:

```bash
python scripts/release_check.py
```

Start the existing Docker stack:

```bash
docker compose up -d
```

Check services:

```bash
docker compose ps
curl http://localhost:8000/healthz
curl http://localhost:8080/health
```

Open or check the Streamlit demo:

```text
http://localhost:8501
```

Run dbt build through the Airflow scheduler container:

```bash
docker compose exec airflow-scheduler python -c "import sys; sys.path.insert(0, '/opt/airflow/dags'); from lib.dbt_runner import run_dbt_build; run_dbt_build()"
```

## Required Checks By Change Type

| Change type | Mandatory checks |
| --- | --- |
| Python/unit changes | `python -m pytest -p no:cacheprovider` |
| Ingestion changes | `python -m pytest -p no:cacheprovider tests/test_tlc_ingestion.py`; review MinIO object paths |
| dbt model or schema changes | dbt build through `airflow-scheduler`; review warning-only anomaly tests |
| Semantic catalog changes | `python -m pytest -p no:cacheprovider tests/test_semantic_catalog.py`; API schema smoke check |
| SQL guardrail changes | SQL guardrail tests; Docker API blocked-query smoke checks |
| API agent changes | API smoke checks for answer, clarification, and blocked SQL |
| Demo changes | Streamlit HTTP `200`; official demo scenario spot checks |
| Docs-only changes | `python scripts/release_check.py`; terminology consistency review |
| Final release | All standard verification checks plus official demo scenarios |

## Security Review

Before a defense or handoff, review `docs/security-notes.md` and confirm:

- `.env` is local-only and not tracked.
- Shared docs do not contain real `OPENAI_API_KEY` values or MinIO passwords.
- `OPENAI_ANSWER_SYNTHESIS=false` unless opt-in answer synthesis is part of the
  demo.
- API audit logs do not contain sensitive user-entered prompt text before
  sharing artifacts.
- DML/DDL, Bronze/Silver access, invalid joins, and detailed wildcard queries
  are still blocked by API smoke checks.
- The API remains localhost-only unless simple API protection and matching demo
  wiring are added and tested.

## API Smoke Checks

Valid Gold query:

```json
{
  "question": "Release smoke valid Gold query",
  "max_rows": 5,
  "sql": "select service_type, pickup_date, trip_count from gold_daily_kpis where pickup_date between date '2024-01-01' and date '2024-06-30' order by pickup_date, service_type"
}
```

Expected result:

- HTTP `200`
- rows returned from `gold_daily_kpis`
- `agent_steps` present

Blocked DDL query:

```json
{
  "question": "Release smoke blocked DDL",
  "max_rows": 5,
  "sql": "drop table gold_daily_kpis"
}
```

Expected result:

- HTTP `400`
- no DuckDB write occurs

Blocked detailed wildcard query:

```json
{
  "question": "Release smoke blocked detailed wildcard",
  "max_rows": 5,
  "sql": "select * from fact_trips"
}
```

Expected result:

- HTTP `400`
- wildcard access to detailed Gold fact is rejected

## Official Demo Scenarios

Use `docs/demo-scenarios.md` as the source of truth. The final release demo
should include:

- schema browsing
- default SQL query over `gold_daily_kpis`
- top pickup zones from `gold_zone_demand`
- vendor analysis from `fact_trips` plus `dim_vendor`
- payment distribution from `fact_trips` plus `dim_payment_type`
- pickup/dropoff borough analysis with `dim_zone`
- Vietnamese natural-language monthly comparison
- clarification for ambiguous questions
- blocked unsafe SQL
- chart toggle and CSV export
- agent timeline

## Performance Check

For final release evidence, rerun Phase 17 benchmarks when data or model
materialization changes:

```bash
python scripts/benchmark_phase17.py --repeats 5 --warmup 1
```

Record results in `docs/performance-report.md`.

## Reset Notes

Use reset only when the local environment is intentionally being rebuilt for a
fresh verification. Do not delete local data casually during normal development.

Local state directories:

- `data/`
- `warehouse/`
- `logs/`

After a reset, rebuild the lakehouse through Airflow/dbt and re-run API and demo
smoke checks before presenting the project.

## Final Handoff

Before handoff:

- Final handoff snapshot tag: `thesis-final-handoff-2026-05-02`.
- `docs/development-roadmap.md` has explicit phase statuses.
- `docs/runbook.md` records the latest verification results.
- `docs/data-quality-report.md`, `docs/agent-evaluation.md`,
  `docs/demo-scenarios.md`, `docs/performance-report.md`, and
  `docs/security-notes.md` are current.
- No real secrets are committed or copied into release notes.
- Known caveats are stated clearly, especially warning-only data anomalies and
  host-local dependency-gated tests.
