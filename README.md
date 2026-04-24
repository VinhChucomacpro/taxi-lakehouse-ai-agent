# taxi-lakehouse-ai-agent

Repo đồ án xây dựng một nền tảng lakehouse local-first cho dữ liệu NYC TLC
Taxi, kèm API AI query read-only để hỏi dữ liệu Gold bằng ngôn ngữ tự nhiên.

## Kiến Trúc

- `MinIO` là Bronze object-storage source of truth.
- `Airflow` điều phối pipeline ingest và transform theo tháng.
- `dbt` xây dựng các lớp `Bronze -> Silver -> Gold` và chạy data tests.
- `DuckDB` lưu warehouse local tại `warehouse/analytics.duckdb`.
- `FastAPI` cung cấp health, schema và query API read-only.
- `OpenAI API` sinh SQL từ câu hỏi tự nhiên khi có `OPENAI_API_KEY`.
- `sqlglot` kiểm tra SQL guardrails trước khi DuckDB thực thi.
- `Streamlit` cung cấp demo UI cho schema, SQL test, guardrails và Ask AI.

## Phạm Vi Hiện Tại

Đang tập trung MVP với:

- `Yellow Taxi` monthly trip parquet
- `Green Taxi` monthly trip parquet
- `Taxi Zone Lookup` làm reference dataset
- pipeline `Bronze -> Silver -> Gold`
- Gold star schema và aggregate marts
- API Text-to-SQL read-only trên các Gold objects được bật trong semantic catalog

Tạm thời chưa đưa vào scope:

- `FHV`, `HVFHV`
- streaming ingestion
- write-capable agents
- multi-tenant auth hoặc production-grade access control
- LangChain/LangGraph/Vanna hoặc agent framework khác

## Cấu Trúc Repo

```text
.
|-- airflow/dags/          # Airflow DAG và ingestion/dbt helpers
|-- contracts/             # Semantic catalog cho AI layer
|-- dbt/models/            # Bronze, Silver, Gold dbt models
|-- docs/                  # Kiến trúc, roadmap, runbook, data contracts
|-- services/api/          # FastAPI query service
|-- services/demo/         # Streamlit demo
|-- tests/                 # Unit, guardrail, catalog, smoke tests
`-- docker-compose.yml
```

## Pipeline Dữ Liệu

Airflow DAG chính: `taxi_monthly_pipeline`.

Luồng xử lý:

1. Tạo manifest cho Yellow, Green và Taxi Zone Lookup.
2. Download source file vào local `data/` làm cache phát triển.
3. Upload cùng object key vào MinIO bucket `taxi-lakehouse`.
4. dbt Bronze đọc từ MinIO qua DuckDB `httpfs`.
5. dbt chuẩn hóa Silver và xây Gold trong DuckDB.
6. API và demo chỉ phục vụ dữ liệu Gold đã curated.

Object MinIO kỳ vọng:

```text
s3://taxi-lakehouse/bronze/yellow_taxi/year=YYYY/month=MM/yellow_tripdata_YYYY-MM.parquet
s3://taxi-lakehouse/bronze/green_taxi/year=YYYY/month=MM/green_tripdata_YYYY-MM.parquet
s3://taxi-lakehouse/reference/taxi_zone_lookup/taxi_zone_lookup.csv
```

## Mô Hình Dữ Liệu

Gold hiện có star schema:

- `fact_trips`
- `dim_date`
- `dim_zone`
- `dim_service_type`
- `dim_vendor`
- `dim_payment_type`

Gold aggregate marts:

- `gold_daily_kpis`
- `gold_zone_demand`

Aggregate marts là fast/safe path cho dashboard và câu hỏi AI phổ biến. Star
schema là nền tảng phân tích linh hoạt hơn, nhưng `fact_trips` và `dim_*` chỉ
được mở cho AI execution sau khi column guardrails và join guardrails hoàn tất.

## AI Query Agent

API chính:

- `GET /healthz`
- `GET /api/v1/schema`
- `POST /api/v1/query`

Guardrails hiện tại:

- chỉ cho một câu lệnh `SELECT`
- chặn DML, DDL và command statements
- chỉ cho truy cập Gold objects có trong `contracts/semantic_catalog.yaml`
- chỉ execute các bảng có `execution_enabled: true`
- validate referenced columns theo semantic catalog
- chặn wildcard `SELECT *` trên detailed Gold tables như `fact_trips`
- ép `LIMIT <= max_rows`
- chạy DuckDB bằng read-only connection

Hiện tại AI execution surface chỉ gồm:

- `gold_daily_kpis`
- `gold_zone_demand`

`fact_trips` và `dim_*` đã có semantic metadata nhưng vẫn
`execution_enabled: false` cho đến khi join guardrails được triển khai.

## Chạy Local

1. Copy `.env.example` thành `.env` và cập nhật secret nếu cần.
2. Start stack đã build:

   ```bash
   docker compose up -d
   ```

   Dùng `docker compose up -d --build` khi Dockerfile, dependency, compose config
   hoặc source được copy vào image thay đổi.

3. Mở các service:

   - Airflow: `http://localhost:8080`
   - MinIO Console: `http://localhost:9001`
   - API docs: `http://localhost:8000/docs`
   - Streamlit demo: `http://localhost:8501`

## Kiểm Thử

Chạy unit tests:

```bash
python -m pytest -p no:cacheprovider
```

Chạy dbt build trong Airflow scheduler container:

```bash
docker compose exec airflow-scheduler python -c "import sys; sys.path.insert(0, '/opt/airflow/dags'); from lib.dbt_runner import run_dbt_build; run_dbt_build()"
```

Smoke test API bằng SQL override:

```json
{
  "question": "Show daily trip counts by service type",
  "max_rows": 10,
  "sql": "select service_type, pickup_date, trip_count from gold_daily_kpis order by pickup_date, service_type"
}
```

Trạng thái verify và caveats gần nhất nằm trong `docs/runbook.md`.

## Tài Liệu Thêm

- `docs/architecture.md`: kiến trúc tổng quan
- `docs/architecture-review.md`: rà soát kiến trúc, tradeoff và backlog bảo vệ
- `docs/data-contracts.md`: hợp đồng dữ liệu
- `docs/development-roadmap.md`: roadmap theo phase
- `docs/gold-star-schema.md`: cấu trúc Gold star schema
- `docs/modeling-decisions.md`: quyết định mô hình dữ liệu
- `docs/runbook.md`: hướng dẫn vận hành local và verification
