from datetime import datetime
import hashlib
import io
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path
import sys
import types
from urllib.error import HTTPError


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


def test_month_start_with_lag_handles_same_year() -> None:
    module = load_tlc_ingestion_module()

    run_date = module.month_start_with_lag(datetime(2026, 4, 24), lag_months=2)

    assert run_date == datetime(2026, 2, 1)


def test_month_start_with_lag_handles_year_boundary() -> None:
    module = load_tlc_ingestion_module()

    run_date = module.month_start_with_lag(datetime(2026, 1, 1), lag_months=2)

    assert run_date == datetime(2025, 11, 1)


def test_month_start_with_lag_rejects_negative_lag() -> None:
    module = load_tlc_ingestion_module()

    try:
        module.month_start_with_lag(datetime(2026, 4, 1), lag_months=-1)
    except ValueError as exc:
        assert "non-negative" in str(exc)
    else:
        raise AssertionError("Expected negative lag to be rejected")


def test_previous_month_starts_returns_oldest_to_newest() -> None:
    module = load_tlc_ingestion_module()

    run_dates = module.previous_month_starts(datetime(2026, 4, 15), count=3)

    assert run_dates == [
        datetime(2026, 1, 1),
        datetime(2026, 2, 1),
        datetime(2026, 3, 1),
    ]


def test_previous_month_starts_rejects_non_positive_count() -> None:
    module = load_tlc_ingestion_module()

    try:
        module.previous_month_starts(datetime(2026, 4, 15), count=0)
    except ValueError as exc:
        assert "positive" in str(exc)
    else:
        raise AssertionError("Expected non-positive count to be rejected")


def test_is_historical_missing_source_uses_publication_grace() -> None:
    module = load_tlc_ingestion_module()

    assert module.is_historical_missing_source(
        {"year": "2024", "month": "01"},
        datetime(2024, 5, 1),
        publication_grace_months=3,
    )
    assert not module.is_historical_missing_source(
        {"year": "2024", "month": "03"},
        datetime(2024, 5, 1),
        publication_grace_months=3,
    )


def test_build_lookup_manifest() -> None:
    module = load_tlc_ingestion_module()
    manifest = module.build_lookup_manifest().to_dict()

    assert manifest["dataset"] == "taxi_zone_lookup"
    assert manifest["source_url"].endswith("/misc/taxi_zone_lookup.csv")
    assert manifest["local_relative_path"] == "reference/taxi_zone_lookup/taxi_zone_lookup.csv"


def test_download_file_to_local_uses_atomic_temp_file(monkeypatch, tmp_path) -> None:
    module = load_tlc_ingestion_module()

    class FakeResponse(io.BytesIO):
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, traceback):
            return False

    def fake_urlopen(*args, **kwargs):
        return FakeResponse(b"sample")

    monkeypatch.setattr(module, "urlopen", fake_urlopen)

    result = module.download_file_to_local(
        manifest={
            "dataset": "yellow",
            "source_url": "https://example.test/yellow.parquet",
            "local_relative_path": "bronze/yellow_taxi/year=2024/month=01/yellow.parquet",
        },
        local_data_root=str(tmp_path),
    )

    final_path = tmp_path / "bronze/yellow_taxi/year=2024/month=01/yellow.parquet"
    temp_path = final_path.with_name(f".{final_path.name}.tmp")

    assert final_path.read_bytes() == b"sample"
    assert not temp_path.exists()
    assert result["sha256"] == hashlib.sha256(b"sample").hexdigest()


def test_download_file_to_local_preserves_existing_file_on_failure(monkeypatch, tmp_path) -> None:
    module = load_tlc_ingestion_module()
    final_path = tmp_path / "bronze/yellow_taxi/year=2024/month=01/yellow.parquet"
    final_path.parent.mkdir(parents=True)
    final_path.write_bytes(b"existing")

    def failing_urlopen(*args, **kwargs):
        raise RuntimeError("download failed")

    monkeypatch.setattr(module, "urlopen", failing_urlopen)

    try:
        module.download_file_to_local(
            manifest={
                "dataset": "yellow",
                "source_url": "https://example.test/yellow.parquet",
                "local_relative_path": "bronze/yellow_taxi/year=2024/month=01/yellow.parquet",
            },
            local_data_root=str(tmp_path),
        )
    except RuntimeError as exc:
        assert "download failed" in str(exc)
    else:
        raise AssertionError("Expected download failure")

    assert final_path.read_bytes() == b"existing"


def test_upload_local_file_to_minio_creates_bucket_and_uploads(tmp_path) -> None:
    calls = []
    fake_boto3 = types.ModuleType("boto3")
    fake_botocore = types.ModuleType("botocore")
    fake_botocore_client = types.ModuleType("botocore.client")

    class FakeConfig:
        def __init__(self, **kwargs):
            self.kwargs = kwargs

    fake_botocore_client.Config = FakeConfig
    sys.modules["boto3"] = fake_boto3
    sys.modules["botocore"] = fake_botocore
    sys.modules["botocore.client"] = fake_botocore_client

    module = load_tlc_ingestion_module()
    local_file = tmp_path / "sample.parquet"
    local_file.write_bytes(b"sample")

    class FakeClient:
        def list_buckets(self):
            calls.append(("list_buckets",))
            return {"Buckets": []}

        def create_bucket(self, Bucket):
            calls.append(("create_bucket", Bucket))

        def upload_file(self, Filename, Bucket, Key, ExtraArgs=None):
            calls.append(("upload_file", Filename, Bucket, Key, ExtraArgs))

    def fake_client(*args, **kwargs):
        calls.append(("client", args, kwargs))
        return FakeClient()

    fake_boto3.client = fake_client

    result = module.upload_local_file_to_minio(
        manifest={
            "dataset": "yellow",
            "local_path": str(local_file),
            "bronze_object_key": "bronze/yellow_taxi/year=2024/month=01/sample.parquet",
        },
        minio_endpoint="http://minio:9000",
        minio_bucket="taxi-lakehouse",
        minio_access_key="minioadmin",
        minio_secret_key="minioadmin123",
    )

    assert ("create_bucket", "taxi-lakehouse") in calls
    upload_call = [call for call in calls if call[0] == "upload_file"][0]
    assert upload_call[1:4] == (
        str(local_file),
        "taxi-lakehouse",
        "bronze/yellow_taxi/year=2024/month=01/sample.parquet",
    )
    object_metadata = upload_call[4]["Metadata"]
    assert object_metadata["sha256"] == hashlib.sha256(b"sample").hexdigest()
    assert object_metadata["file_size_bytes"] == "6"
    assert object_metadata["dataset"] == "yellow"
    assert "ingested_at_utc" in object_metadata
    assert result["minio_uri"] == (
        "s3://taxi-lakehouse/bronze/yellow_taxi/year=2024/month=01/sample.parquet"
    )
    assert result["file_size_bytes"] == "6"
    assert result["sha256"] == hashlib.sha256(b"sample").hexdigest()
    assert "ingested_at_utc" in result


def test_upload_local_file_to_minio_rejects_empty_file(tmp_path) -> None:
    module = load_tlc_ingestion_module()
    local_file = tmp_path / "empty.parquet"
    local_file.write_bytes(b"")

    try:
        module.upload_local_file_to_minio(
            manifest={
                "dataset": "yellow",
                "local_path": str(local_file),
                "bronze_object_key": "bronze/yellow_taxi/year=2024/month=01/empty.parquet",
            },
            minio_endpoint="http://minio:9000",
            minio_bucket="taxi-lakehouse",
            minio_access_key="minioadmin",
            minio_secret_key="minioadmin123",
        )
    except ValueError as exc:
        assert "empty" in str(exc)
    else:
        raise AssertionError("Expected empty files to be rejected")


def test_ingest_file_to_minio_skips_existing_object(monkeypatch) -> None:
    module = load_tlc_ingestion_module()

    def existing_object(**kwargs):
        return {
            "exists": True,
            "content_length": 123,
            "metadata": {
                "sha256": "abc",
                "file_size_bytes": "123",
                "source_url": "https://example.test/yellow.parquet",
            },
        }

    def fail_download(*args, **kwargs):
        raise AssertionError("Existing MinIO objects should not be downloaded again")

    monkeypatch.setattr(module, "describe_minio_object", existing_object)
    monkeypatch.setattr(module, "download_file_to_local", fail_download)

    result = module.ingest_file_to_minio(
        manifest={
            "dataset": "yellow",
            "source_url": "https://example.test/yellow.parquet",
            "bronze_object_key": "bronze/yellow_taxi/year=2026/month=02/yellow.parquet",
            "local_relative_path": "bronze/yellow_taxi/year=2026/month=02/yellow.parquet",
        },
        local_data_root="/tmp/data",
        minio_endpoint="http://minio:9000",
        minio_bucket="taxi-lakehouse",
        minio_access_key="minioadmin",
        minio_secret_key="minioadmin123",
    )

    assert result["status"] == "skipped_existing_verified"
    assert result["minio_uri"] == (
        "s3://taxi-lakehouse/bronze/yellow_taxi/year=2026/month=02/yellow.parquet"
    )
    assert result["sha256"] == "abc"


def test_ingest_file_to_minio_skips_existing_object_without_metadata(monkeypatch) -> None:
    module = load_tlc_ingestion_module()

    def existing_object(**kwargs):
        return {"exists": True, "content_length": 123, "metadata": {}}

    def fail_download(*args, **kwargs):
        raise AssertionError("Existing MinIO objects should not be downloaded again")

    monkeypatch.setattr(module, "describe_minio_object", existing_object)
    monkeypatch.setattr(module, "download_file_to_local", fail_download)

    result = module.ingest_file_to_minio(
        manifest={
            "dataset": "yellow",
            "source_url": "https://example.test/yellow.parquet",
            "bronze_object_key": "bronze/yellow_taxi/year=2026/month=02/yellow.parquet",
            "local_relative_path": "bronze/yellow_taxi/year=2026/month=02/yellow.parquet",
        },
        local_data_root="/tmp/data",
        minio_endpoint="http://minio:9000",
        minio_bucket="taxi-lakehouse",
        minio_access_key="minioadmin",
        minio_secret_key="minioadmin123",
    )

    assert result["status"] == "skipped_existing_unverified"


def test_ingest_file_to_minio_rejects_existing_metadata_size_mismatch(monkeypatch) -> None:
    module = load_tlc_ingestion_module()

    def existing_object(**kwargs):
        return {
            "exists": True,
            "content_length": 123,
            "metadata": {"sha256": "abc", "file_size_bytes": "999"},
        }

    monkeypatch.setattr(module, "describe_minio_object", existing_object)

    try:
        module.ingest_file_to_minio(
            manifest={
                "dataset": "yellow",
                "source_url": "https://example.test/yellow.parquet",
                "bronze_object_key": "bronze/yellow_taxi/year=2026/month=02/yellow.parquet",
                "local_relative_path": "bronze/yellow_taxi/year=2026/month=02/yellow.parquet",
            },
            local_data_root="/tmp/data",
            minio_endpoint="http://minio:9000",
            minio_bucket="taxi-lakehouse",
            minio_access_key="minioadmin",
            minio_secret_key="minioadmin123",
        )
    except ValueError as exc:
        assert "metadata size" in str(exc)
    else:
        raise AssertionError("Expected existing metadata mismatch to be rejected")


def test_ingest_file_to_minio_skips_unpublished_source(monkeypatch) -> None:
    module = load_tlc_ingestion_module()

    def missing_object(**kwargs):
        return {"exists": False}

    def unavailable_source(*args, **kwargs):
        raise HTTPError(
            url="https://example.test/missing.parquet",
            code=404,
            msg="Not Found",
            hdrs=None,
            fp=None,
        )

    monkeypatch.setattr(module, "describe_minio_object", missing_object)
    monkeypatch.setattr(module, "download_file_to_local", unavailable_source)

    result = module.ingest_file_to_minio(
        manifest={
            "dataset": "green",
            "source_url": "https://example.test/missing.parquet",
            "bronze_object_key": "bronze/green_taxi/year=2026/month=03/green.parquet",
            "local_relative_path": "bronze/green_taxi/year=2026/month=03/green.parquet",
        },
        local_data_root="/tmp/data",
        minio_endpoint="http://minio:9000",
        minio_bucket="taxi-lakehouse",
        minio_access_key="minioadmin",
        minio_secret_key="minioadmin123",
    )

    assert result["status"] == "skipped_source_unavailable_recent"
    assert result["http_status"] == "404"


def test_ingest_file_to_minio_marks_historical_missing_source(monkeypatch) -> None:
    module = load_tlc_ingestion_module()

    def missing_object(**kwargs):
        return {"exists": False}

    def unavailable_source(*args, **kwargs):
        raise HTTPError(
            url="https://example.test/missing.parquet",
            code=404,
            msg="Not Found",
            hdrs=None,
            fp=None,
        )

    monkeypatch.setattr(module, "describe_minio_object", missing_object)
    monkeypatch.setattr(module, "download_file_to_local", unavailable_source)

    result = module.ingest_file_to_minio(
        manifest={
            "dataset": "green",
            "source_url": "https://example.test/missing.parquet",
            "bronze_object_key": "bronze/green_taxi/year=2020/month=03/green.parquet",
            "local_relative_path": "bronze/green_taxi/year=2020/month=03/green.parquet",
            "year": "2020",
            "month": "03",
        },
        local_data_root="/tmp/data",
        minio_endpoint="http://minio:9000",
        minio_bucket="taxi-lakehouse",
        minio_access_key="minioadmin",
        minio_secret_key="minioadmin123",
    )

    assert result["status"] == "failed_source_missing_historical"
    assert result["http_status"] == "404"
