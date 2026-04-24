# Data Contracts

## Sources In Scope

- `yellow_tripdata_YYYY-MM.parquet`
- `green_tripdata_YYYY-MM.parquet`
- `taxi_zone_lookup.csv` as a reference dataset

## Bronze Contract

- Preserve source files with minimal mutation.
- Store source files in MinIO as the Bronze object-storage source of truth.
- Keep local `data/` files only as ingestion download/cache files or development
  fallback, not as the primary dbt Bronze source.
- Store files in partitioned paths by service type, year, and month.
- Keep naming close to the original TLC source names.

Expected MinIO object paths:

- `s3://taxi-lakehouse/bronze/yellow_taxi/year=YYYY/month=MM/...`
- `s3://taxi-lakehouse/bronze/green_taxi/year=YYYY/month=MM/...`
- `s3://taxi-lakehouse/reference/taxi_zone_lookup/taxi_zone_lookup.csv`

Expected local cache paths:

- `data/bronze/yellow_taxi/year=YYYY/month=MM/...`
- `data/bronze/green_taxi/year=YYYY/month=MM/...`
- `data/reference/taxi_zone_lookup/taxi_zone_lookup.csv`

## Silver Contract

Unified trip rows should include at least:

- `service_type`
- `source_year`
- `source_month`
- `pickup_date`
- `pickup_at`
- `dropoff_at`
- `pickup_zone_id`
- `dropoff_zone_id`
- `trip_distance`
- `fare_amount`
- `total_amount`

Silver should normalize Yellow and Green into the same semantic shape.
Taxi Zone Lookup may be joined as reference data, but it does not change the
fact-source scope of the project.
Silver filters out records whose pickup timestamp falls outside the source file
partition month.

## Gold Contract

Gold is the serving layer for analytics and AI.

Current marts:

- `gold_daily_kpis`
- `gold_zone_demand`

Current dimensional models:

- `dim_date`
- `dim_zone`
- `dim_service_type`
- `dim_vendor`
- `dim_payment_type`
- `fact_trips`

Rules:

- prefer explicit metrics and dimensions
- keep `service_type` when combining Yellow and Green
- use only business-safe, curated columns for AI access
- `fact_trips` grain is one valid Silver trip per row
- keep aggregate marts as the fast path for common BI and AI questions
- do not expose `fact_trips` directly to AI until semantic metadata describes
  grain, metrics, columns, keys, and safe join paths
- expose aggregate Gold marts to AI through `contracts/semantic_catalog.yaml`
  with table type, grain, dimensions, metrics, filters, and preferred questions

## AI Query Contract

- The API may only execute `SELECT` statements.
- SQL must reference at least one curated Gold table from `contracts/semantic_catalog.yaml`.
- References to Bronze, Silver, system tables, external files, DML, and DDL are rejected.
- Referenced columns must be present in the semantic catalog for the referenced
  Gold table or table alias.
- Wildcard `SELECT *` is allowed for aggregate marts, but is rejected for
  detailed Gold tables such as `fact_trips`.
- Joins must use explicit `ON` conditions and match an allowed join path from
  the semantic catalog. Cartesian joins and `CROSS JOIN` are rejected.
- The API enforces the caller's `max_rows` limit before execution.
- DuckDB is opened in read-only mode for query execution.
