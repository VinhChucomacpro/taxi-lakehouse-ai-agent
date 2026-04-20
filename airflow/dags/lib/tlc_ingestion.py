from __future__ import annotations

from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
from shutil import copyfileobj
from urllib.request import Request, urlopen

TLC_CLOUDFRONT_BASE_URL = "https://d37ci6vzurychx.cloudfront.net"
TRIP_DATASETS = {
    "yellow": "yellow_taxi",
    "green": "green_taxi",
}
DEFAULT_TIMEOUT_SECONDS = 300


@dataclass(frozen=True)
class TripDataManifest:
    dataset: str
    service_type: str
    year: str
    month: str
    source_url: str
    bronze_object_key: str
    local_relative_path: str

    def to_dict(self) -> dict[str, str]:
        return asdict(self)


@dataclass(frozen=True)
class ReferenceDataManifest:
    dataset: str
    service_type: str
    source_url: str
    bronze_object_key: str
    local_relative_path: str

    def to_dict(self) -> dict[str, str]:
        return asdict(self)


def build_tripdata_url(dataset: str, year: int, month: int) -> str:
    if dataset not in TRIP_DATASETS:
        raise ValueError(f"Unsupported dataset: {dataset}")

    return (
        f"{TLC_CLOUDFRONT_BASE_URL}/trip-data/"
        f"{dataset}_tripdata_{year:04d}-{month:02d}.parquet"
    )


def build_lookup_url() -> str:
    return f"{TLC_CLOUDFRONT_BASE_URL}/misc/taxi_zone_lookup.csv"


def build_trip_manifest(dataset: str, run_date: datetime) -> TripDataManifest:
    if dataset not in TRIP_DATASETS:
        raise ValueError(f"Unsupported dataset: {dataset}")

    service_type = TRIP_DATASETS[dataset]
    year = run_date.year
    month = run_date.month
    file_name = f"{dataset}_tripdata_{year:04d}-{month:02d}.parquet"
    local_relative_path = (
        f"bronze/{service_type}/year={year:04d}/month={month:02d}/{file_name}"
    )

    return TripDataManifest(
        dataset=dataset,
        service_type=service_type,
        year=f"{year:04d}",
        month=f"{month:02d}",
        source_url=build_tripdata_url(dataset, year, month),
        bronze_object_key=local_relative_path,
        local_relative_path=local_relative_path,
    )


def build_lookup_manifest() -> ReferenceDataManifest:
    local_relative_path = "reference/taxi_zone_lookup/taxi_zone_lookup.csv"
    return ReferenceDataManifest(
        dataset="taxi_zone_lookup",
        service_type="reference_data",
        source_url=build_lookup_url(),
        bronze_object_key=local_relative_path,
        local_relative_path=local_relative_path,
    )


def resolve_local_path(local_data_root: str, local_relative_path: str) -> Path:
    return Path(local_data_root).joinpath(local_relative_path)


def download_file_to_local(
    manifest: dict[str, str],
    local_data_root: str,
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
) -> dict[str, str]:
    destination_path = resolve_local_path(local_data_root, manifest["local_relative_path"])
    destination_path.parent.mkdir(parents=True, exist_ok=True)

    request = Request(
        manifest["source_url"],
        headers={"User-Agent": "taxi-lakehouse-ai-agent/0.1"},
    )

    with urlopen(request, timeout=timeout_seconds) as response:
        with destination_path.open("wb") as destination_file:
            copyfileobj(response, destination_file)

    return {
        **manifest,
        "local_path": str(destination_path),
    }


def download_tripdata_to_local(
    manifest: dict[str, str],
    local_data_root: str,
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
) -> dict[str, str]:
    return download_file_to_local(
        manifest=manifest,
        local_data_root=local_data_root,
        timeout_seconds=timeout_seconds,
    )
