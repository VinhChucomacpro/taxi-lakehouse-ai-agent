# Data Quality, Lineage, And Trust Report

Last verified: `2026-04-26`

Defense dataset window: `2024-01-01` through `2024-06-30`

This report summarizes the evidence used to trust the local-first taxi
lakehouse for thesis defense. It is scoped to the current MVP sources: Yellow
Taxi, Green Taxi, and Taxi Zone Lookup.

## Verification Summary

Phase 12 established the fixed defense window and verified the full stack:

- Docker services started with `docker compose up -d`.
- Airflow DAG run `phase12_2024_01_20260426` completed with `success`.
- dbt build through the Airflow scheduler completed with
  `PASS=75 WARN=2 ERROR=0 SKIP=0`.
- API health and representative Gold queries succeeded.
- Unsafe DDL was blocked with HTTP `400`.

The two dbt warnings are intentional warning-only anomaly tests:

- `warn_silver_trip_anomalies`: `18011` rows in the full loaded warehouse.
- `warn_gold_metric_anomalies`: `1` row in the full loaded warehouse.

These warnings are treated as source-data quality evidence for investigation,
not as blocking pipeline failures.

## Layer Row Counts

The local warehouse currently contains a broader date range,
`2023-12-01` through `2026-02-28`. Defense and evaluation work should filter to
`2024-H1` for stable results.

| Object | Full loaded rows | Defense window rows |
| --- | ---: | ---: |
| `bronze_yellow_trips` | `100393644` | `20332093` |
| `bronze_green_trips` | `1393453` | `339807` |
| `silver_trips_unified` | `98093195` | `20354795` |
| `fact_trips` | `98093195` | `20354795` |
| `gold_daily_kpis` | `1642` | `364` |
| `gold_zone_demand` | `283947` | `61154` |
| `dim_date` | `821` | derived from loaded pickup dates |
| `dim_zone` | `265` | reference dimension |
| `dim_service_type` | `2` | reference dimension |
| `dim_vendor` | `4` | derived dimension |
| `dim_payment_type` | `6` | derived dimension |

Defense window service split:

| Layer | Service type | Rows |
| --- | --- | ---: |
| Bronze | `yellow_taxi` | `20332093` |
| Bronze | `green_taxi` | `339807` |
| Silver | `yellow_taxi` | `20016200` |
| Silver | `green_taxi` | `338595` |

## Bronze To Silver Filtering

Silver applies the MVP validity rules before exposing trip records downstream:

- required pickup and dropoff timestamps
- required pickup and dropoff zone ids
- non-negative trip distance
- non-negative fare amount
- pickup timestamp inside the source file partition month

For the `2024-H1` defense window:

| Check | Rows |
| --- | ---: |
| Raw Bronze rows | `20671900` |
| Rows passing Silver filters | `20354795` |
| Rows failing at least one Silver filter | `317105` |
| Null pickup timestamp | `0` |
| Null dropoff timestamp | `0` |
| Null pickup zone | `0` |
| Null dropoff zone | `0` |
| Negative trip distance | `0` |
| Negative fare amount | `316916` |
| Pickup outside source month | `193` |

Some failure categories can overlap, so category counts should be interpreted as
diagnostic evidence rather than mutually exclusive buckets.

## dbt Test Coverage

Bronze:

- `bronze_taxi_zone_lookup.zone_id` is non-null and unique.
- Yellow and Green trip Bronze models preserve raw monthly source columns and
  partition metadata from the source path.

Silver:

- `service_type` is non-null and accepted as `yellow_taxi` or `green_taxi`.
- `source_year`, `source_month`, `pickup_date`, `pickup_zone_id`, and
  `dropoff_zone_id` are non-null.
- `source_month` is accepted in `1` through `12`.
- Warning-only anomaly checks track impossible dropoff order, unusually long
  distances, and negative total amounts after Silver validity filters.

Gold star schema:

- `dim_date.pickup_date` is non-null and unique.
- `dim_zone.zone_id` is non-null and unique; borough and zone name are non-null.
- `dim_service_type.service_type` is non-null, unique, and accepted.
- `dim_vendor.vendor_id` and `dim_payment_type.payment_type` are non-null and
  unique.
- `fact_trips` required dimensions and measures are non-null.
- `fact_trips` has relationship tests to `dim_date`, `dim_service_type`,
  `dim_vendor`, `dim_payment_type`, and pickup/dropoff `dim_zone`.

Gold aggregate marts:

- `gold_daily_kpis` and `gold_zone_demand` are tested as non-empty.
- Required dimensions and metrics are non-null.
- Service type values are accepted.
- Zone demand has a relationship test from `zone_id` to Taxi Zone Lookup.
- Warning-only Gold metric checks track non-positive trip counts, negative fare
  totals, and invalid average trip distance.

## Anomaly Evidence

Silver anomaly counts:

| Check | Full loaded warehouse | Defense window |
| --- | ---: | ---: |
| Dropoff before pickup | `5113` | `300` |
| Trip distance greater than `200` | `3730` | `659` |
| Negative total amount | `9168` | `965` |

Gold daily KPI anomaly counts:

| Check | Full loaded warehouse | Defense window |
| --- | ---: | ---: |
| Non-positive trip count | `0` | `0` |
| Negative total fare amount | `0` | `0` |
| Average trip distance below `0` or above `200` | `1` | `0` |

Interpretation:

- The Silver anomaly rows are retained as warning evidence because they can be
  useful for source-data analysis, but they do not break the curated Gold build.
- The selected defense window has no Gold daily KPI anomaly rows.
- The single full-warehouse Gold anomaly is outside the defense-window
  evaluation scope and should be investigated in Phase 13 follow-up work only if
  the defense/demo window changes.

## Lineage

1. TLC monthly Yellow and Green parquet files are ingested by Airflow.
2. Files are downloaded to local cache and uploaded into MinIO Bronze under
   `bronze/<service>/year=YYYY/month=MM/...`.
3. Taxi Zone Lookup is ingested as a reference file under
   `reference/taxi_zone_lookup/taxi_zone_lookup.csv`.
4. dbt Bronze models read from MinIO S3-compatible paths through DuckDB
   `httpfs`.
5. `silver_trips_unified` standardizes Yellow and Green into one trip-level
   schema and applies validity filters.
6. Gold star schema models expose `fact_trips` and dimensions.
7. Gold marts `gold_daily_kpis` and `gold_zone_demand` provide fast, curated
   aggregate query surfaces.
8. `contracts/semantic_catalog.yaml` defines which Gold tables, fields, metrics,
   filters, and joins are visible to the read-only agent.
9. FastAPI validates SQL with guardrails and executes read-only queries against
   DuckDB.
10. Streamlit presents schema, SQL tests, guardrail demos, natural-language
    answers, agent trace, charts, and CSV export.

## Known Caveats

- The warehouse contains more months than the defense dataset window. Defense
  queries must filter to `2024-H1` when stable results are required.
- Warning-only anomaly tests intentionally do not fail the pipeline. They are
  evidence for data quality discussion, not proof of corrupted transformations.
- Taxi Zone Lookup is the only reference dataset in scope.
- FHV, HVFHV, streaming ingestion, write-capable agents, multi-tenant auth, and
  production cloud deployment remain out of scope for the thesis-ready MVP.
