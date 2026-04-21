with unified_trips as (
    select
        service_type,
        cast(year as integer) as source_year,
        cast(month as integer) as source_month,
        cast(VendorID as varchar) as vendor_id,
        cast(tpep_pickup_datetime as timestamp) as pickup_at,
        cast(tpep_dropoff_datetime as timestamp) as dropoff_at,
        cast(PULocationID as integer) as pickup_zone_id,
        cast(DOLocationID as integer) as dropoff_zone_id,
        cast(passenger_count as integer) as passenger_count,
        cast(trip_distance as double) as trip_distance,
        cast(fare_amount as double) as fare_amount,
        cast(total_amount as double) as total_amount,
        cast(payment_type as varchar) as payment_type
    from {{ ref('bronze_yellow_trips') }}

    union all

    select
        service_type,
        cast(year as integer) as source_year,
        cast(month as integer) as source_month,
        cast(VendorID as varchar) as vendor_id,
        cast(lpep_pickup_datetime as timestamp) as pickup_at,
        cast(lpep_dropoff_datetime as timestamp) as dropoff_at,
        cast(PULocationID as integer) as pickup_zone_id,
        cast(DOLocationID as integer) as dropoff_zone_id,
        cast(passenger_count as integer) as passenger_count,
        cast(trip_distance as double) as trip_distance,
        cast(fare_amount as double) as fare_amount,
        cast(total_amount as double) as total_amount,
        cast(payment_type as varchar) as payment_type
    from {{ ref('bronze_green_trips') }}
)

select
    service_type,
    source_year,
    source_month,
    cast(date_trunc('day', pickup_at) as date) as pickup_date,
    pickup_at,
    dropoff_at,
    vendor_id,
    pickup_zone_id,
    dropoff_zone_id,
    passenger_count,
    trip_distance,
    fare_amount,
    total_amount,
    payment_type
from unified_trips
where pickup_at is not null
  and dropoff_at is not null
  and pickup_zone_id is not null
  and dropoff_zone_id is not null
  and trip_distance >= 0
  and fare_amount >= 0
  and pickup_at >= make_date(source_year, source_month, 1)
  and pickup_at < make_date(source_year, source_month, 1) + interval '1 month'
