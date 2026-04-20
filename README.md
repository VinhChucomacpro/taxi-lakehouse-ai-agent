# taxi-lakehouse-ai-agent

Taxi Lakehouse AI Agent is a graduation-project style repository for building a
local-first data lakehouse on taxi trip data and exposing a read-only AI query
agent over the curated analytics layer.

## Architecture

- `MinIO` stores raw and curated objects.
- `Airflow` orchestrates monthly ELT ingestion and transformation jobs.
- `dbt` models the `Bronze -> Silver -> Gold` layers.
- `DuckDB` serves the analytics layer consumed by BI and the AI agent.
- `FastAPI` exposes health, schema, and query endpoints for the AI query agent.

## Repository Layout

```text
.
|-- docs/
|-- airflow/
|   `-- dags/
|-- dbt/
|   `-- models/
|-- services/
|   `-- api/
|-- contracts/
|-- infra/
|-- tests/
`-- docker-compose.yml
```

## Project Context

Additional repo context for contributors and coding agents lives in:

- `AGENTS.md`
- `docs/architecture.md`
- `docs/data-contracts.md`
- `docs/source-notes.md`
- `docs/runbook.md`

## Quick Start

1. Copy `.env.example` to `.env` and update secrets.
2. Review `docker-compose.yml`.
3. Start the local stack:

   ```bash
   docker compose up --build
   ```

4. Open:
   - Airflow: `http://localhost:8080`
   - MinIO Console: `http://localhost:9001`
   - API docs: `http://localhost:8000/docs`

## Initial Scope

- Ingest monthly `Yellow Taxi` and `Green Taxi` TLC trip data into `Bronze`
- Ingest `Taxi Zone Lookup` as reference data for enrichment
- Normalize and test data in `Silver`
- Publish curated marts in `Gold`
- Expose a read-only `Text-to-SQL` API over `Gold`

## TLC Data Sources

- Yellow Taxi trip parquet: monthly source files published by TLC
- Green Taxi trip parquet: monthly source files published by TLC
- Taxi Zone Lookup CSV: reference dataset for zone and borough enrichment

The pipeline intentionally starts with Yellow and Green only. `FHV` and
`HVFHV` stay out of scope until the core ELT flow is stable.

## Current Bronze Ingestion

- Airflow prepares one monthly manifest for `yellow_tripdata_YYYY-MM.parquet`
  and one for `green_tripdata_YYYY-MM.parquet`
- Airflow also prepares one reference manifest for `taxi_zone_lookup.csv`
- Each file is downloaded from the TLC CDN into the mounted local data volume
- Downloaded files land under:
  - `data/bronze/yellow_taxi/year=YYYY/month=MM/`
  - `data/bronze/green_taxi/year=YYYY/month=MM/`
  - `data/reference/taxi_zone_lookup/taxi_zone_lookup.csv`

## Current Transform Execution

- Airflow now runs `dbt build` for Bronze and Silver after ingestion completes
- Gold marts are built in a separate downstream task
- DuckDB warehouse files are persisted under `warehouse/`

## Next Steps

- Join Taxi Zone Lookup into more Silver and Gold marts where location names are needed
- Add SQL validation and LLM integration in `services/api/app`
- Add integration tests once the first runnable pipeline is in place
