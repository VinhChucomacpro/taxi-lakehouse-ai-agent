from datetime import datetime
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path
import sys


def load_tlc_ingestion_module():
    module_path = Path("airflow/dags/lib/tlc_ingestion.py")
    spec = spec_from_file_location("tlc_ingestion", module_path)
    module = module_from_spec(spec)
    assert spec is not None
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_build_tripdata_url_for_yellow() -> None:
    module = load_tlc_ingestion_module()
    url = module.build_tripdata_url("yellow", 2024, 1)

    assert url.endswith("/trip-data/yellow_tripdata_2024-01.parquet")


def test_build_trip_manifest_for_green() -> None:
    module = load_tlc_ingestion_module()
    manifest = module.build_trip_manifest("green", datetime(2024, 2, 1)).to_dict()

    assert manifest["service_type"] == "green_taxi"
    assert manifest["month"] == "02"
    assert manifest["local_relative_path"].endswith(
        "green_taxi/year=2024/month=02/green_tripdata_2024-02.parquet"
    )


def test_build_lookup_manifest() -> None:
    module = load_tlc_ingestion_module()
    manifest = module.build_lookup_manifest().to_dict()

    assert manifest["dataset"] == "taxi_zone_lookup"
    assert manifest["source_url"].endswith("/misc/taxi_zone_lookup.csv")
    assert manifest["local_relative_path"] == "reference/taxi_zone_lookup/taxi_zone_lookup.csv"
