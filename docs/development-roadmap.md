# Development Roadmap

This roadmap keeps the MVP stable first, then adds dim/fact modeling as the next
warehouse step.

## Principles

- Current sources: Yellow Taxi, Green Taxi, Taxi Zone Lookup.
- Keep the project local-first, repeatable, and testable.
- Defer FHV, HVFHV, streaming, write-capable agents, and production auth until
  the MVP lakehouse is stable.
- The AI query layer may query only curated Gold data.

## Phase 1: Stabilize MVP Lakehouse

- Harden monthly ingestion for Yellow and Green.
- Keep monthly partition semantics in paths, manifests, and dbt models.
- Treat Taxi Zone Lookup as enrichment reference data.
- Make the `Bronze -> Silver -> Gold` flow repeatable through Airflow and dbt.

## Phase 2: Data Quality And Repeatability

- Expand dbt tests for Bronze, Silver, and Gold.
- Track important anomalies: pickup dates outside partition month, unusual trip
  distance, negative amounts, and dropoff before pickup.
- Keep the verification checklist in `docs/runbook.md` current.
- Keep the semantic catalog aligned with AI-queryable Gold objects.

## Phase 3: Add Dimensional Layer

Status: implemented for the MVP Gold layer.

Gold dim/fact models:

- `dim_date`: date, month, quarter, year, day of week.
- `dim_zone`: Taxi Zone Lookup attributes such as zone, borough, and service
  zone.
- `dim_service_type`: `yellow_taxi`, `green_taxi`.
- `dim_vendor`: TLC vendor code lookup.
- `dim_payment_type`: TLC payment code lookup.
- `fact_trips`: trip-level fact from `silver_trips_unified`.

`fact_trips` grain should be one valid Silver trip per row. Main join keys are
`pickup_date`, `service_type`, `vendor_id`, `payment_type`, `pickup_zone_id`,
and `dropoff_zone_id`. Base metrics include `trip_distance`, `fare_amount`,
`total_amount`, and `passenger_count`.

## Phase 4: Build Marts From Dim/Fact

- Status: implemented for the existing serving marts.
- Keep `gold_daily_kpis` and `gold_zone_demand` as serving marts built from
  `fact_trips` and related dimensions.
- Add new marts for repeated BI or AI questions instead of making AI join facts
  directly every time.

## Phase 5: Improve AI Querying

- Status: in progress.
- `contracts/semantic_catalog.yaml` now describes AI-visible aggregate marts with
  table type, grain, dimensions, metrics, allowed filters, and preferred
  question patterns.
- Prefer aggregate marts for common questions.
- Allow `fact_trips` only after cataloging grain, metrics, and safe join paths.
- Keep guardrails: `SELECT` only, Gold only, no DML/DDL, enforced `LIMIT`,
  DuckDB read-only.
