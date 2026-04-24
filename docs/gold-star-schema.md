# Gold Star Schema

This document describes the implemented Gold star schema in the MVP lakehouse.
It is the detailed structural companion to `docs/development-roadmap.md` and
`docs/modeling-decisions.md`.

## Scope

Gold star schema models:

- `fact_trips`
- `dim_date`
- `dim_zone`
- `dim_service_type`
- `dim_vendor`
- `dim_payment_type`

These models are implemented in dbt Gold and are the dimensional base for
aggregate marts and controlled read-only agent querying.

## Table Structure

### `dim_date`

Grain: one row per `pickup_date`.

Columns:

- `pickup_date`
- `year`
- `quarter`
- `month`
- `day`
- `day_name`
- `day_of_week`
- `year_month`

Source logic: derived from distinct valid `pickup_date` values in
`silver_trips_unified`.

### `dim_service_type`

Grain: one row per `service_type`.

Columns:

- `service_type`
- `service_type_name`

Current values:

- `yellow_taxi`
- `green_taxi`

### `dim_vendor`

Grain: one row per `vendor_id`.

Columns:

- `vendor_id`
- `vendor_name`

Current mappings:

- `1` -> `Creative Mobile Technologies, LLC`
- `2` -> `VeriFone Inc.`
- other values -> `Unknown vendor`

### `dim_payment_type`

Grain: one row per `payment_type`.

Columns:

- `payment_type`
- `payment_type_name`

Current mappings:

- `1` -> `Credit card`
- `2` -> `Cash`
- `3` -> `No charge`
- `4` -> `Dispute`
- `5` -> `Unknown`
- `6` -> `Voided trip`
- other values -> `Other`

### `dim_zone`

Grain: one row per `zone_id`.

Columns:

- `zone_id`
- `borough`
- `zone_name`
- `service_zone`

Source: Taxi Zone Lookup reference data.

### `fact_trips`

Grain: one row per valid trip from `silver_trips_unified`.

Columns:

- `service_type`
- `source_year`
- `source_month`
- `pickup_date`
- `pickup_at`
- `dropoff_at`
- `vendor_id`
- `pickup_zone_id`
- `dropoff_zone_id`
- `passenger_count`
- `trip_distance`
- `fare_amount`
- `total_amount`
- `payment_type`

Base metrics available from the fact:

- `trip_distance`
- `fare_amount`
- `total_amount`
- `passenger_count`

## Join Paths

Approved star-schema join paths:

- `fact_trips.pickup_date = dim_date.pickup_date`
- `fact_trips.service_type = dim_service_type.service_type`
- `fact_trips.vendor_id = dim_vendor.vendor_id`
- `fact_trips.payment_type = dim_payment_type.payment_type`
- `fact_trips.pickup_zone_id = dim_zone.zone_id`
- `fact_trips.dropoff_zone_id = dim_zone.zone_id`

These are the intended join paths to be reflected in
`contracts/semantic_catalog.yaml` and enforced by SQL guardrails.

## Notes

- Aggregate marts such as `gold_daily_kpis` and `gold_zone_demand` remain the
  fast path for common dashboard and agent questions.
- The star schema does not replace the aggregate marts.
- `fact_trips` is exposed to the read-only agent only through semantic metadata,
  explicit columns, wildcard restrictions, and approved join paths.
