# taxi-lakehouse-ai-agent

Local-first lakehouse cho dữ liệu NYC TLC Taxi, kèm read-only AI query agent để
hỏi dữ liệu Gold bằng ngôn ngữ tự nhiên. Đây là MVP phục vụ thesis defense và
product-style demo, không phải production cloud system.

## Trạng Thái MVP

Đã hoàn thành:

- Ingestion monthly cho Yellow Taxi và Green Taxi.
- Taxi Zone Lookup làm reference enrichment dataset.
- MinIO làm Bronze object-storage source of truth.
- dbt transformation `Bronze -> Silver -> Gold`.
- Phase 25 pipeline hardening: atomic downloads, checksum-aware Bronze
  idempotency, source availability classification, dbt run summaries, and
  pipeline run metadata under MinIO `metadata/pipeline_runs/...`.
- Gold star schema: `fact_trips`, `dim_date`, `dim_zone`,
  `dim_service_type`, `dim_vendor`, `dim_payment_type`.
- Gold aggregate marts: `gold_daily_kpis`, `gold_zone_demand`.
- FastAPI read-only query agent với intent analysis, planning, SQL generation,
  semantic guardrails, DuckDB execution, self-checks, answer, and trace output.
- Streamlit demo với schema browsing, Ask AI, SQL override, guardrail demos,
  chart toggle, CSV export, agent timeline, and session-local Ask AI history.

Defense dataset window cố định:

- `2024-01-01` through `2024-06-30`
- Yellow Taxi và Green Taxi monthly files từ `2024-01` đến `2024-06`
- Taxi Zone Lookup reference file

Khi cần kết quả demo ổn định, filter truy vấn về `2024-H1`.

## Kiến Trúc

Core stack:

- `Airflow` điều phối ingestion và dbt build.
- `MinIO` lưu Bronze files theo S3-compatible object paths.
- `dbt` chuẩn hóa Silver và xây Gold star schema/marts.
- `DuckDB` phục vụ local analytics trong `warehouse/analytics.duckdb`.
- `FastAPI` cung cấp health, schema, and query API.
- `OpenAI API` sinh SQL khi có `OPENAI_API_KEY` và deterministic planner không
  đủ.
- `sqlglot` validate SQL guardrails trước khi DuckDB execute.
- `Streamlit` cung cấp demo UI.

Data flow:

1. Airflow tạo manifest cho Yellow, Green, and Taxi Zone Lookup.
2. Source files được download vào local `data/` như cache qua temp file, sau đó
   validate size/SHA-256 và atomic promote.
3. Ingestion upload cùng object key vào MinIO bucket `taxi-lakehouse`, kèm file
   metadata khi có.
4. dbt Bronze đọc từ MinIO qua DuckDB `httpfs`.
5. dbt xây Silver unified trips và Gold serving models trong DuckDB.
6. Airflow publish pipeline run metadata JSON vào
   `metadata/pipeline_runs/taxi_monthly_pipeline/...`.
7. FastAPI chỉ execute validated read-only SQL trên curated Gold objects.
8. Streamlit hiển thị answer, trace, SQL, table, chart, and export.

Bronze object paths:

```text
s3://taxi-lakehouse/bronze/yellow_taxi/year=YYYY/month=MM/yellow_tripdata_YYYY-MM.parquet
s3://taxi-lakehouse/bronze/green_taxi/year=YYYY/month=MM/green_tripdata_YYYY-MM.parquet
s3://taxi-lakehouse/reference/taxi_zone_lookup/taxi_zone_lookup.csv
s3://taxi-lakehouse/metadata/pipeline_runs/taxi_monthly_pipeline/...
```

## Phạm Vi

In scope:

- Yellow Taxi monthly trip data.
- Green Taxi monthly trip data.
- Taxi Zone Lookup as reference data only.
- Local-first lakehouse and read-only Gold query agent.
- Thesis/demo reproducibility and verification evidence.

Out of scope for this MVP:

- FHV and HVFHV.
- Streaming ingestion.
- Write-capable agents.
- Multi-tenant auth or production RBAC.
- Production cloud deployment.
- LangChain, LangGraph, Vanna, or another agent framework.

## Read-Only AI Query Agent

The API agent is read-only. It is implemented directly in
`services/api/app/agent.py`, with Text-to-SQL prompt rendering kept behind the
orchestrator.

Workflow:

1. Analyze intent.
2. Choose aggregate mart or star-schema query surface.
3. Generate SQL or accept SQL override.
4. Validate SQL with semantic guardrails.
5. Execute against DuckDB read-only connection.
6. Run deterministic self-checks.
7. Return answer, warnings, confidence, SQL, rows, and `agent_steps`.

Guardrails:

- one statement only
- `SELECT` only
- no DML or DDL
- no Bronze/Silver/system/external table access
- only cataloged Gold objects in `contracts/semantic_catalog.yaml`
- only `execution_enabled` tables
- known table aliases and columns only
- no wildcard `SELECT *` on detailed Gold tables such as `fact_trips`
- explicit `ON` joins only
- joins must match semantic catalog `allowed_joins`
- enforced `max_rows` limit

Current execution-enabled Gold objects:

- `gold_daily_kpis`
- `gold_zone_demand`
- `fact_trips`
- `dim_date`
- `dim_zone`
- `dim_service_type`
- `dim_vendor`
- `dim_payment_type`

OpenAI answer synthesis is disabled by default. Set
`OPENAI_ANSWER_SYNTHESIS=true` only when the demo explicitly needs natural
language answer synthesis from already executed SQL rows.

## Local Setup

1. Copy `.env.example` to `.env`.
2. Keep `.env` local-only. Do not commit real `OPENAI_API_KEY` or MinIO
   passwords.
3. Start the existing stack:

   ```bash
   docker compose up -d
   ```

Use rebuild only when Dockerfiles, Compose config, dependency files, or
image-copied source files changed:

```bash
docker compose up -d --build
```

Local services:

| Service | URL |
| --- | --- |
| Airflow | `http://localhost:8080` |
| MinIO Console | `http://localhost:9001` |
| FastAPI docs | `http://localhost:8000/docs` |
| Streamlit demo | `http://localhost:8501` |

## Demo Flow

Use `docs/demo-scenarios.md` as the official scenario pack.

Recommended defense flow:

1. Start with `docker compose up -d`.
2. Open Streamlit at `http://localhost:8501`.
3. Check sidebar health and semantic catalog status.
4. Open `Schema` to show Gold-only queryable objects.
5. Run default SQL over `gold_daily_kpis`.
6. Run one Ask AI scenario, for example:
   `So sánh số chuyến Yellow Taxi và Green Taxi theo tháng trong nửa đầu năm 2024`.
7. Show Ask AI history as a local display log. Each request is still independent;
   history is not sent back to the API as multi-turn memory.
8. Show a controlled star-schema query such as vendor or payment distribution.
9. Show guardrails blocking DDL, Silver access, or detailed wildcard access.
10. Enable `Show chart` only after reviewing table results.
11. Export CSV from a successful result.

## Verification

Host-local checks:

```bash
python -m pytest -p no:cacheprovider
python scripts/release_check.py
```

Operational/evaluation checks after the Docker stack is running:

```bash
python scripts/check_pipeline_run.py --run-id phase25_2024_01_20260506 --local-only
python scripts/agent_eval.py --base-url http://localhost:8000 --window 2024-H1 --output docs/agent-evaluation-results.json
```

dbt build through the Airflow scheduler container:

```bash
docker compose exec airflow-scheduler python -c "import sys; sys.path.insert(0, '/opt/airflow/dags'); from lib.dbt_runner import run_dbt_build; run_dbt_build()"
```

API smoke request:

```json
{
  "question": "Show daily trip counts by service type",
  "max_rows": 10,
  "sql": "select service_type, pickup_date, trip_count from gold_daily_kpis where pickup_date between date '2024-01-01' and date '2024-06-30' order by pickup_date, service_type"
}
```

Expected blocked queries:

- `drop table gold_daily_kpis`
- `select * from silver_trips_unified`
- `select * from fact_trips`

Latest verification results and caveats are recorded in `docs/runbook.md`.

## Security And Governance

This MVP is localhost-first. API key/basic auth and rate limiting are not added
for the current thesis/demo target. Before exposing the API outside localhost,
add simple API protection, matching Streamlit wiring, and deployment-managed
secrets.

Security notes are documented in `docs/security-notes.md`.

Important local rules:

- Keep `.env` untracked.
- Keep real API keys and passwords out of docs and release notes.
- Use `OPENAI_API_KEY=replace-me` for deterministic demo paths when OpenAI is
  not needed.
- Leave `OPENAI_ANSWER_SYNTHESIS=false` unless answer synthesis is explicitly
  demonstrated.
- Review `QUERY_AUDIT_LOG_PATH` before sharing artifacts because audit logs can
  contain user-entered prompts.

## Repository Layout

```text
.
|-- airflow/dags/          # Airflow DAG and ingestion/dbt helpers
|-- contracts/             # Semantic catalog for the read-only agent
|-- dbt/models/            # Bronze, Silver, and Gold dbt models
|-- docs/                  # Architecture, roadmap, runbook, reports, checklist
|-- scripts/               # Benchmark and release verification scripts
|-- services/api/          # FastAPI read-only query agent
|-- services/demo/         # Streamlit demo
|-- tests/                 # Unit, catalog, ingestion, guardrail, smoke tests
`-- docker-compose.yml
```

## Key Documents

- `docs/architecture.md`: system architecture.
- `docs/data-contracts.md`: Bronze/Silver/Gold and agent query contracts.
- `docs/gold-star-schema.md`: Gold dimensional model.
- `docs/modeling-decisions.md`: modeling tradeoffs and decisions.
- `docs/data-quality-report.md`: data quality and lineage evidence.
- `docs/agent-evaluation.md`: agent and guardrail benchmark.
- `docs/demo-scenarios.md`: official defense demo scenarios.
- `docs/performance-report.md`: API latency and materialization review.
- `docs/release-checklist.md`: final release and handoff checks.
- `docs/security-notes.md`: security-lite and governance notes.
- `docs/runbook.md`: operational commands and latest verification notes.
- `docs/development-roadmap.md`: phase status and next steps.
