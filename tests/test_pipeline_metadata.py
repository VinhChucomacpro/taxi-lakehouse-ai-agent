from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path
import json
import sys
import types


def load_pipeline_metadata_module():
    module_path = Path("airflow/dags/lib/pipeline_metadata.py")
    spec = spec_from_file_location("pipeline_metadata", module_path)
    module = module_from_spec(spec)
    assert spec is not None
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_pipeline_run_metadata_key_is_stable() -> None:
    module = load_pipeline_metadata_module()

    key = module.pipeline_run_metadata_key(
        "taxi_monthly_pipeline",
        "manual__2026-05-03T00:00:00+00:00",
        "2026-05-03T00:00:00+00:00",
    )

    assert key == (
        "metadata/pipeline_runs/taxi_monthly_pipeline/2026-05-03/"
        "manual__2026-05-03T00_00_00_00_00.json"
    )


def test_build_pipeline_run_summary_is_json_serializable() -> None:
    module = load_pipeline_metadata_module()

    summary = module.build_pipeline_run_summary(
        dag_id="taxi_monthly_pipeline",
        run_id="manual__2026-05-03",
        run_mode="manual",
        logical_date="2026-05-03T00:00:00+00:00",
        target_months=["2024-01"],
        ingestion_results=[
            {
                "dataset": "yellow",
                "status": "uploaded",
                "minio_uri": "s3://taxi-lakehouse/bronze/yellow.parquet",
                "file_size_bytes": "6",
                "sha256": "abc",
            }
        ],
        dbt_results=[
            {
                "status": "success",
                "counts": {"pass": 4, "warn": 0, "error": 0, "skip": 0},
            }
        ],
        created_at_utc="2026-05-03T00:00:00+00:00",
    )

    encoded = json.dumps(summary)

    assert "taxi_monthly_pipeline" in encoded
    assert summary["quality_gate"]["status"] == "passed"
    assert summary["target_months"] == ["2024-01"]


def test_quality_gate_marks_warnings_for_review() -> None:
    module = load_pipeline_metadata_module()

    gate = module.evaluate_quality_gate(
        ingestion_results=[],
        dbt_results=[
            {"counts": {"pass": 75, "warn": 2, "error": 0, "skip": 0}},
        ],
    )

    assert gate["status"] == "passed_with_warnings"
    assert gate["dbt_counts"]["warn"] == 2


def test_quality_gate_marks_blocking_ingestion_failure() -> None:
    module = load_pipeline_metadata_module()

    gate = module.evaluate_quality_gate(
        ingestion_results=[
            {
                "dataset": "yellow",
                "status": "failed_existing_metadata_mismatch",
                "source_url": "https://example.test/yellow.parquet",
            }
        ],
        dbt_results=[],
    )

    assert gate["status"] == "failed_blocking"
    assert gate["blocking_ingestion_statuses"][0]["dataset"] == "yellow"


def test_write_pipeline_run_summary_local(tmp_path) -> None:
    module = load_pipeline_metadata_module()
    summary = {"dag_id": "taxi_monthly_pipeline", "quality_gate": {"status": "passed"}}

    path = module.write_pipeline_run_summary_local(
        summary,
        str(tmp_path),
        "metadata/pipeline_runs/taxi_monthly_pipeline/run.json",
    )

    assert path.exists()
    assert json.loads(path.read_text(encoding="utf-8"))["dag_id"] == "taxi_monthly_pipeline"


def test_upload_pipeline_run_summary_to_minio_puts_json_object() -> None:
    calls = []
    fake_boto3 = types.ModuleType("boto3")
    fake_botocore = types.ModuleType("botocore")
    fake_botocore_client = types.ModuleType("botocore.client")

    class FakeConfig:
        def __init__(self, **kwargs):
            self.kwargs = kwargs

    class FakeClient:
        def put_object(self, **kwargs):
            calls.append(("put_object", kwargs))

    def fake_client(*args, **kwargs):
        calls.append(("client", args, kwargs))
        return FakeClient()

    fake_botocore_client.Config = FakeConfig
    fake_boto3.client = fake_client
    sys.modules["boto3"] = fake_boto3
    sys.modules["botocore"] = fake_botocore
    sys.modules["botocore.client"] = fake_botocore_client

    module = load_pipeline_metadata_module()
    result = module.upload_pipeline_run_summary_to_minio(
        summary={
            "dag_id": "taxi_monthly_pipeline",
            "run_id": "manual__test",
            "quality_gate": {"status": "passed"},
        },
        metadata_key="metadata/pipeline_runs/taxi_monthly_pipeline/run.json",
        minio_endpoint="http://minio:9000",
        minio_bucket="taxi-lakehouse",
        minio_access_key="minioadmin",
        minio_secret_key="minioadmin123",
    )

    put_call = [call for call in calls if call[0] == "put_object"][0][1]
    assert put_call["Bucket"] == "taxi-lakehouse"
    assert put_call["ContentType"] == "application/json"
    assert put_call["Metadata"]["quality_gate"] == "passed"
    assert result["metadata_uri"].endswith("/metadata/pipeline_runs/taxi_monthly_pipeline/run.json")
