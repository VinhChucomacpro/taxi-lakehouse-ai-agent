# Release Checklist

Verification date: `2026-05-03`

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
| Pipeline metadata changes | `python -m pytest -p no:cacheprovider tests/test_pipeline_metadata.py tests/test_dbt_runner.py tests/test_tlc_ingestion.py`; confirm MinIO `metadata/pipeline_runs/...` after Docker/Airflow run |
| Semantic catalog changes | `python -m pytest -p no:cacheprovider tests/test_semantic_catalog.py`; API schema smoke check |
| Gold model exposure changes | `python scripts/release_check.py`; confirm every `dbt/models/gold/*.sql` model has one matching `contracts/semantic_catalog.yaml` table and vice versa |
| SQL guardrail changes | SQL guardrail tests; Docker API blocked-query smoke checks |
| API agent changes | API smoke checks for answer, clarification, and blocked SQL |
| Demo changes | Streamlit HTTP `200`; official demo scenario spot checks |
| Docs-only changes | `python scripts/release_check.py`; terminology consistency review |
| Final release | All standard verification checks plus official demo scenarios |

## Post-Thesis Extension Gate

The Phase 20 thesis MVP and Phase 21 handoff tag remain the stable baseline.
Before implementing post-thesis work, confirm `docs/development-roadmap.md`
records exactly one selected extension direction.

Phase 24 selected extension direction:

- Agent extension: improve deterministic planner and evaluation coverage while
  preserving read-only, Gold-only, framework-light behavior.

Current active next phase:

- Phase 30 decision gate: choose exactly one next extension direction after
  Phase 25-29 pipeline, quality gate, planner, evaluation, and rehearsal work.
  Default to public demo hardening only if the app must leave localhost.

Phase 25 pipeline evidence should include:

- atomic local downloads before Bronze upload
- checksum and file-size metadata on new Bronze objects
- explicit statuses for verified/unverified existing objects
- explicit statuses for recent publication lag versus historical missing source
- dbt `run_results.json` summaries in pipeline metadata
- MinIO JSON metadata under `metadata/pipeline_runs/taxi_monthly_pipeline/...`
- release check confirming generated `dbt/target` artifacts are not tracked

Phase 25 was verified on `2026-05-06` with Airflow run
`phase25_2024_01_20260506`. Use this command to validate the local metadata
copy for that run:

```bash
python scripts/check_pipeline_run.py --run-id phase25_2024_01_20260506 --local-only
```

The host may not be able to read MinIO backing `xl.meta` files directly on
Windows. In that case, verify the MinIO copy through the S3 API from the Airflow
scheduler container.

Phase 28 agent evaluation can be rerun with:

```bash
python scripts/agent_eval.py --base-url http://localhost:8000 --window 2024-H1 --output docs/agent-evaluation-results.json
```

Do not mix this with public deployment hardening, performance materialization,
or new data-source work in the same phase. Any API agent change still needs
tests plus API smoke checks for success, clarification, and blocked SQL.

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
