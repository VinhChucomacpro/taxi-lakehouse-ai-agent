from __future__ import annotations

import logging
import hashlib
import os
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path
from shutil import copyfileobj
from urllib.error import HTTPError
from urllib.request import Request, urlopen

TLC_CLOUDFRONT_BASE_URL = "https://d37ci6vzurychx.cloudfront.net"
TRIP_DATASETS = {
    "yellow": "yellow_taxi",
    "green": "green_taxi",
}
DEFAULT_TIMEOUT_SECONDS = 300
DEFAULT_PUBLICATION_GRACE_MONTHS = 3
LOGGER = logging.getLogger(__name__)


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


def month_start_with_lag(run_date: datetime, lag_months: int) -> datetime:
    if lag_months < 0:
        raise ValueError("lag_months must be non-negative")

    month_index = run_date.year * 12 + run_date.month - 1 - lag_months
    year = month_index // 12
    month = month_index % 12 + 1
    return datetime(year, month, 1)


def previous_month_starts(run_date: datetime, count: int) -> list[datetime]:
    if count <= 0:
        raise ValueError("count must be positive")

    return [
        month_start_with_lag(run_date, lag_months=lag)
        for lag in range(count, 0, -1)
    ]


def months_between(later: datetime, earlier_year: int, earlier_month: int) -> int:
    return later.year * 12 + later.month - (earlier_year * 12 + earlier_month)


def is_historical_missing_source(
    manifest: dict[str, str],
    checked_at: datetime,
    publication_grace_months: int = DEFAULT_PUBLICATION_GRACE_MONTHS,
) -> bool:
    if "year" not in manifest or "month" not in manifest:
        return False
    age_months = months_between(
        checked_at,
        int(manifest["year"]),
        int(manifest["month"]),
    )
    return age_months > publication_grace_months


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


def compute_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source_file:
        for chunk in iter(lambda: source_file.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def describe_local_file(path: Path) -> dict[str, str]:
    file_size_bytes = path.stat().st_size
    if file_size_bytes <= 0:
        raise ValueError(f"Downloaded file is empty: {path}")

    return {
        "local_path": str(path),
        "file_size_bytes": str(file_size_bytes),
        "sha256": compute_sha256(path),
    }


def download_file_to_local(
    manifest: dict[str, str],
    local_data_root: str,
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
) -> dict[str, str]:
    destination_path = resolve_local_path(local_data_root, manifest["local_relative_path"])
    temporary_path = destination_path.with_name(f".{destination_path.name}.tmp")
    destination_path.parent.mkdir(parents=True, exist_ok=True)
    LOGGER.info(
        "Downloading dataset=%s from %s to %s",
        manifest["dataset"],
        manifest["source_url"],
        destination_path,
    )

    request = Request(
        manifest["source_url"],
        headers={"User-Agent": "taxi-lakehouse-ai-agent/0.1"},
    )

    try:
        if temporary_path.exists():
            temporary_path.unlink()
        with urlopen(request, timeout=timeout_seconds) as response:
            with temporary_path.open("wb") as destination_file:
                copyfileobj(response, destination_file)
        describe_local_file(temporary_path)
        os.replace(temporary_path, destination_path)
    except Exception:
        if temporary_path.exists():
            temporary_path.unlink()
        raise

    file_metadata = describe_local_file(destination_path)
    LOGGER.info(
        "Downloaded dataset=%s to %s (%s bytes, sha256=%s)",
        manifest["dataset"],
        destination_path,
        file_metadata["file_size_bytes"],
        file_metadata["sha256"],
    )

    return {
        **manifest,
        **file_metadata,
        "downloaded_at_utc": datetime.now(timezone.utc).isoformat(),
    }


def build_object_metadata(manifest: dict[str, str]) -> dict[str, str]:
    metadata = {
        "sha256": str(manifest.get("sha256", "")),
        "file_size_bytes": str(manifest.get("file_size_bytes", "")),
        "source_url": str(manifest.get("source_url", "")),
        "dataset": str(manifest.get("dataset", "")),
        "ingested_at_utc": datetime.now(timezone.utc).isoformat(),
    }
    for key in ("service_type", "year", "month"):
        if manifest.get(key) is not None:
            metadata[key] = str(manifest[key])
    return {key: value for key, value in metadata.items() if value}


def upload_local_file_to_minio(
    manifest: dict[str, str],
    minio_endpoint: str,
    minio_bucket: str,
    minio_access_key: str,
    minio_secret_key: str,
) -> dict[str, str]:
    local_path = Path(manifest["local_path"])
    if not local_path.exists():
        raise FileNotFoundError(f"Local file does not exist: {local_path}")
    file_metadata = describe_local_file(local_path)

    import boto3
    from botocore.client import Config

    object_key = manifest["bronze_object_key"]
    client = boto3.client(
        "s3",
        endpoint_url=minio_endpoint,
        aws_access_key_id=minio_access_key,
        aws_secret_access_key=minio_secret_key,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )

    existing_buckets = {
        bucket["Name"]
        for bucket in client.list_buckets().get("Buckets", [])
    }
    if minio_bucket not in existing_buckets:
        LOGGER.info("Creating MinIO bucket=%s", minio_bucket)
        client.create_bucket(Bucket=minio_bucket)

    LOGGER.info(
        "Uploading dataset=%s from %s to s3://%s/%s",
        manifest["dataset"],
        local_path,
        minio_bucket,
        object_key,
    )
    object_metadata = build_object_metadata(file_metadata | manifest)
    client.upload_file(
        str(local_path),
        minio_bucket,
        object_key,
        ExtraArgs={"Metadata": object_metadata},
    )
    LOGGER.info(
        "Uploaded dataset=%s to s3://%s/%s (%s bytes, sha256=%s)",
        manifest["dataset"],
        minio_bucket,
        object_key,
        file_metadata["file_size_bytes"],
        file_metadata["sha256"],
    )

    return {
        **manifest,
        **file_metadata,
        "minio_bucket": minio_bucket,
        "minio_endpoint": minio_endpoint,
        "minio_object_key": object_key,
        "minio_uri": f"s3://{minio_bucket}/{object_key}",
        "ingested_at_utc": datetime.now(timezone.utc).isoformat(),
    }


def ingest_file_to_minio(
    manifest: dict[str, str],
    local_data_root: str,
    minio_endpoint: str,
    minio_bucket: str,
    minio_access_key: str,
    minio_secret_key: str,
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
    publication_grace_months: int = DEFAULT_PUBLICATION_GRACE_MONTHS,
) -> dict[str, str]:
    existing_object = describe_minio_object(
        minio_endpoint=minio_endpoint,
        minio_bucket=minio_bucket,
        object_key=manifest["bronze_object_key"],
        minio_access_key=minio_access_key,
        minio_secret_key=minio_secret_key,
    )
    if existing_object["exists"]:
        metadata = existing_object.get("metadata", {})
        status = "skipped_existing_unverified"
        metadata_size = metadata.get("file_size_bytes")
        source_url = metadata.get("source_url")
        if metadata_size:
            actual_size = str(existing_object.get("content_length", ""))
            if metadata_size != actual_size:
                raise ValueError(
                    "Existing MinIO object metadata size does not match content length: "
                    f"s3://{minio_bucket}/{manifest['bronze_object_key']}"
                )
        if source_url and source_url != manifest.get("source_url"):
            raise ValueError(
                "Existing MinIO object metadata source URL does not match manifest: "
                f"s3://{minio_bucket}/{manifest['bronze_object_key']}"
            )
        if metadata.get("sha256") and metadata_size:
            status = "skipped_existing_verified"
        LOGGER.info(
            "Skipping dataset=%s because s3://%s/%s already exists with status=%s",
            manifest["dataset"],
            minio_bucket,
            manifest["bronze_object_key"],
            status,
        )
        return {
            **manifest,
            "minio_bucket": minio_bucket,
            "minio_endpoint": minio_endpoint,
            "minio_object_key": manifest["bronze_object_key"],
            "minio_uri": f"s3://{minio_bucket}/{manifest['bronze_object_key']}",
            "status": status,
            "file_size_bytes": str(existing_object.get("content_length", "")),
            "sha256": str(metadata.get("sha256", "")),
            "checked_at_utc": datetime.now(timezone.utc).isoformat(),
        }

    try:
        downloaded_manifest = download_file_to_local(
            manifest=manifest,
            local_data_root=local_data_root,
            timeout_seconds=timeout_seconds,
        )
    except HTTPError as exc:
        if exc.code in {403, 404}:
            checked_at = datetime.now(timezone.utc)
            if is_historical_missing_source(
                manifest,
                checked_at,
                publication_grace_months=publication_grace_months,
            ):
                LOGGER.warning(
                    "Historical source is missing for dataset=%s: %s",
                    manifest["dataset"],
                    manifest["source_url"],
                )
                return {
                    **manifest,
                    "status": "failed_source_missing_historical",
                    "http_status": str(exc.code),
                    "checked_at_utc": checked_at.isoformat(),
                }
            LOGGER.info(
                "Skipping dataset=%s because source is not available yet: %s",
                manifest["dataset"],
                manifest["source_url"],
            )
            return {
                **manifest,
                "status": "skipped_source_unavailable_recent",
                "http_status": str(exc.code),
                "checked_at_utc": checked_at.isoformat(),
            }
        raise

    return upload_local_file_to_minio(
        manifest=downloaded_manifest,
        minio_endpoint=minio_endpoint,
        minio_bucket=minio_bucket,
        minio_access_key=minio_access_key,
        minio_secret_key=minio_secret_key,
    )


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


def minio_object_exists(
    minio_endpoint: str,
    minio_bucket: str,
    object_key: str,
    minio_access_key: str,
    minio_secret_key: str,
) -> bool:
    return bool(
        describe_minio_object(
            minio_endpoint=minio_endpoint,
            minio_bucket=minio_bucket,
            object_key=object_key,
            minio_access_key=minio_access_key,
            minio_secret_key=minio_secret_key,
        )["exists"]
    )


def describe_minio_object(
    minio_endpoint: str,
    minio_bucket: str,
    object_key: str,
    minio_access_key: str,
    minio_secret_key: str,
) -> dict[str, object]:
    import boto3
    from botocore.client import Config
    from botocore.exceptions import ClientError

    client = boto3.client(
        "s3",
        endpoint_url=minio_endpoint,
        aws_access_key_id=minio_access_key,
        aws_secret_access_key=minio_secret_key,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )

    try:
        response = client.head_object(Bucket=minio_bucket, Key=object_key)
    except ClientError as exc:
        error_code = str(exc.response.get("Error", {}).get("Code", ""))
        if error_code in {"404", "NoSuchKey", "NoSuchBucket", "NotFound"}:
            return {"exists": False}
        raise
    return {
        "exists": True,
        "content_length": response.get("ContentLength"),
        "metadata": response.get("Metadata", {}),
        "etag": response.get("ETag"),
        "last_modified": str(response.get("LastModified", "")),
    }
