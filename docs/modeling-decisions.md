# Data Modeling Decisions

## Context

The project currently targets a local-first lakehouse for Yellow Taxi and Green
Taxi data. Taxi Zone Lookup is reference data for location enrichment.

Current flow:

1. `Bronze` keeps source data as raw as practical.
2. `Silver` normalizes Yellow and Green into one trip-level schema.
3. `Gold` serves BI, dashboards, and the read-only AI query API.

## Current Decision

The MVP now has two Gold serving layers:

- Gold star schema: `fact_trips`, `dim_date`, `dim_zone`, `dim_service_type`,
  `dim_vendor`, `dim_payment_type`
- curated aggregate marts: `gold_daily_kpis`, `gold_zone_demand`

The aggregate marts remain a fast and safe serving surface:

- `gold_daily_kpis`
- `gold_zone_demand`

This matches common question patterns: daily KPIs, service type splits, and
zone-level demand. Aggregate marts keep simple AI queries fast and reduce joins,
but they do not replace the Gold star schema.

## Why Dim/Fact Was Added After MVP Verification

Dim/fact modeling was deferred until the basic MVP flow was verified because it
adds early complexity:

- More grain, key, and join-policy decisions.
- Higher risk of Text-to-SQL generating invalid or overly detailed joins.
- More guardrail complexity than a small set of curated Gold marts.

After ingestion, transforms, data quality checks, and safe AI querying were
verified, the Gold dimensional layer was added. The marts still exist so common
queries do not need to touch fact-level data directly.

## Implemented Dimensional Layer

Implemented Gold dimensional models:

- `dim_date`
- `dim_zone`
- `dim_service_type`
- `dim_vendor`
- `dim_payment_type`
- `fact_trips`

Detailed table structure, columns, and approved join paths are documented in
`docs/gold-star-schema.md`.

`fact_trips` has one row per valid Silver trip. It is the base for better marts
and drill-down analysis, with join keys for date, service type, vendor, payment
type, pickup zone, and dropoff zone. It does not replace aggregate marts.

`gold_daily_kpis` and `gold_zone_demand` are serving marts built from
`fact_trips` and related dimensions.

## AI Query Direction

- AI may query only Gold objects listed in the semantic catalog.
- Prefer aggregate marts for simple common questions.
- Use controlled star-schema querying for vendor, payment type, pickup/dropoff
  role, and flexible fact/dim analysis after semantic metadata and join
  guardrails are implemented.
- Keep `fact_trips` out of executable AI access until its grain, metrics,
  columns, keys, and safe join paths are cataloged and tested.
- Never expose Bronze or Silver to AI.
- Never allow DML, DDL, or external file access.
