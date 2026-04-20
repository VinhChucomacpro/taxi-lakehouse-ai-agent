select
    'yellow_taxi' as service_type,
    *
from read_parquet(
    '{{ var("yellow_tripdata_path", env_var("LOCAL_DATA_ROOT", "../data") ~ "/bronze/yellow_taxi/**/*.parquet") }}',
    union_by_name = true
)
