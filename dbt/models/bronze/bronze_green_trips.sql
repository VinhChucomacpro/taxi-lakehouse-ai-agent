select
    'green_taxi' as service_type,
    *
from read_parquet(
    '{{ var("green_tripdata_path", env_var("LOCAL_DATA_ROOT", "../data") ~ "/bronze/green_taxi/**/*.parquet") }}',
    union_by_name = true
)
