from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


QUALITY_PASSED = "passed"
QUALITY_PASSED_WITH_WARNINGS = "passed_with_warnings"
QUALITY_FAILED_BLOCKING = "failed_blocking"

BLOCKING_INGESTION_STATUSES = {
    "failed_existing_metadata_mismatch",
    "failed_source_missing_historical",
}


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def safe_key_part(value: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9_.=-]+", "_", value.strip())
    return cleaned.strip("_") or "unknown"


def pipeline_run_metadata_key(
    dag_id: str,
    run_id: str,
    logical_date: str | None = None,
) -> str:
    parts = ["metadata", "pipeline_runs", safe_key_part(dag_id)]
    if logical_date:
        parts.append(safe_key_part(logical_date[:10]))
    parts.append(f"{safe_key_part(run_id)}.json")
    return "/".join(parts)


def dbt_status_counts(dbt_results: list[dict[str, Any]]) -> dict[str, int]:
    totals = {"pass": 0, "warn": 0, "error": 0, "skip": 0}
    for result in dbt_results:
        counts = result.get("counts", {})
        for key in totals:
            totals[key] += int(counts.get(key, 0) or 0)
    return totals


def evaluate_quality_gate(
    ingestion_results: list[dict[str, Any]],
    dbt_results: list[dict[str, Any]],
) -> dict[str, Any]:
    blocking_ingestion = [
        result
        for result in ingestion_results
        if result.get("status") in BLOCKING_INGESTION_STATUSES
    ]
    counts = dbt_status_counts(dbt_results)

    if blocking_ingestion or counts["error"] > 0:
        status = QUALITY_FAILED_BLOCKING
    elif counts["warn"] > 0:
        status = QUALITY_PASSED_WITH_WARNINGS
    else:
        status = QUALITY_PASSED

    return {
        "status": status,
        "dbt_counts": counts,
        "blocking_ingestion_statuses": [
            {
                "dataset": result.get("dataset"),
                "status": result.get("status"),
                "source_url": result.get("source_url"),
                "bronze_object_key": result.get("bronze_object_key"),
            }
            for result in blocking_ingestion
        ],
    }


def build_pipeline_run_summary(
    *,
    dag_id: str,
    run_id: str,
    run_mode: str,
    logical_date: str | None,
    target_months: list[str],
    ingestion_results: list[dict[str, Any]],
    dbt_results: list[dict[str, Any]],
    created_at_utc: str | None = None,
) -> dict[str, Any]:
    created_at = created_at_utc or utc_now_iso()
    quality_gate = evaluate_quality_gate(ingestion_results, dbt_results)
    return {
        "schema_version": "1.0",
        "dag_id": dag_id,
        "run_id": run_id,
        "run_mode": run_mode,
        "logical_date": logical_date,
        "target_months": target_months,
        "ingestion_results": ingestion_results,
        "dbt_results": dbt_results,
        "quality_gate": quality_gate,
        "created_at_utc": created_at,
    }


def write_pipeline_run_summary_local(
    summary: dict[str, Any],
    local_data_root: str,
    metadata_key: str,
) -> Path:
    path = Path(local_data_root).joinpath(metadata_key)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(summary, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    return path


def upload_pipeline_run_summary_to_minio(
    *,
    summary: dict[str, Any],
    metadata_key: str,
    minio_endpoint: str,
    minio_bucket: str,
    minio_access_key: str,
    minio_secret_key: str,
) -> dict[str, str]:
    import boto3
    from botocore.client import Config

    body = json.dumps(summary, indent=2, sort_keys=True).encode("utf-8")
    client = boto3.client(
        "s3",
        endpoint_url=minio_endpoint,
        aws_access_key_id=minio_access_key,
        aws_secret_access_key=minio_secret_key,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )
    client.put_object(
        Bucket=minio_bucket,
        Key=metadata_key,
        Body=body,
        ContentType="application/json",
        Metadata={
            "dag_id": str(summary.get("dag_id", "")),
            "run_id": str(summary.get("run_id", "")),
            "quality_gate": str(summary.get("quality_gate", {}).get("status", "")),
        },
    )
    return {
        "metadata_key": metadata_key,
        "metadata_uri": f"s3://{minio_bucket}/{metadata_key}",
        "metadata_size_bytes": str(len(body)),
    }
