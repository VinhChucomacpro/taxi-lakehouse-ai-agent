from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any


DEFAULT_DAG_ID = "taxi_monthly_pipeline"
DEFAULT_BUCKET = "taxi-lakehouse"

REQUIRED_TOP_LEVEL_FIELDS = {
    "schema_version",
    "dag_id",
    "run_id",
    "run_mode",
    "target_months",
    "ingestion_results",
    "dbt_results",
    "quality_gate",
    "created_at_utc",
}
REQUIRED_QUALITY_FIELDS = {"status", "dbt_counts", "blocking_ingestion_statuses"}
REQUIRED_DBT_COUNTS = {"pass", "warn", "error", "skip"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Validate durable pipeline run metadata written by Airflow."
    )
    parser.add_argument("--run-id", required=True, help="Airflow DAG run id to validate.")
    parser.add_argument("--dag-id", default=DEFAULT_DAG_ID, help="Airflow DAG id.")
    parser.add_argument(
        "--data-root",
        default="data",
        help="Local data root containing metadata/pipeline_runs.",
    )
    parser.add_argument(
        "--minio-root",
        default=f"data/minio/{DEFAULT_BUCKET}",
        help="Local MinIO bucket backing directory.",
    )
    parser.add_argument(
        "--local-only",
        action="store_true",
        help="Validate only the local metadata copy, not the MinIO-backed copy.",
    )
    return parser.parse_args()


def metadata_candidates(root: Path, dag_id: str, run_id: str) -> list[Path]:
    metadata_root = root / "metadata" / "pipeline_runs" / dag_id
    if not metadata_root.exists():
        return []
    return sorted(
        [
            path
            for path in metadata_root.rglob("*.json")
            if path.is_file()
            if run_id in path.name or run_id == _read_run_id_safely(path)
        ],
        key=lambda item: item.stat().st_mtime,
        reverse=True,
    )


def load_summary(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def validate_summary(summary: dict[str, Any], *, expected_run_id: str, expected_dag_id: str) -> list[str]:
    failures: list[str] = []
    missing = sorted(REQUIRED_TOP_LEVEL_FIELDS - set(summary))
    if missing:
        failures.append("Missing top-level fields: " + ", ".join(missing))

    if summary.get("dag_id") != expected_dag_id:
        failures.append(f"Expected dag_id={expected_dag_id}, found {summary.get('dag_id')}")
    if summary.get("run_id") != expected_run_id:
        failures.append(f"Expected run_id={expected_run_id}, found {summary.get('run_id')}")
    if summary.get("run_mode") not in {"manual", "scheduled"}:
        failures.append("run_mode must be manual or scheduled")
    if not isinstance(summary.get("target_months"), list) or not summary.get("target_months"):
        failures.append("target_months must be a non-empty list")

    ingestion_results = summary.get("ingestion_results")
    if not isinstance(ingestion_results, list) or not ingestion_results:
        failures.append("ingestion_results must be a non-empty list")
    else:
        failures.extend(validate_ingestion_results(ingestion_results))

    dbt_results = summary.get("dbt_results")
    if not isinstance(dbt_results, list) or not dbt_results:
        failures.append("dbt_results must be a non-empty list")

    quality_gate = summary.get("quality_gate")
    if not isinstance(quality_gate, dict):
        failures.append("quality_gate must be an object")
    else:
        failures.extend(validate_quality_gate(quality_gate))

    return failures


def validate_ingestion_results(results: list[dict[str, Any]]) -> list[str]:
    failures: list[str] = []
    for index, result in enumerate(results):
        prefix = f"ingestion_results[{index}]"
        for field in ("dataset", "status", "source_url"):
            if not result.get(field):
                failures.append(f"{prefix} missing {field}")
        object_key = result.get("bronze_object_key") or result.get("object_key")
        if not object_key and "source_unavailable" not in str(result.get("status", "")):
            failures.append(f"{prefix} missing bronze_object_key/object_key")
        if result.get("status") in {"uploaded", "skipped_existing_verified"}:
            if not (result.get("sha256") or result.get("checksum_sha256")):
                failures.append(f"{prefix} missing checksum for verified/uploaded object")
            if not (result.get("file_size_bytes") or result.get("size_bytes")):
                failures.append(f"{prefix} missing file size for verified/uploaded object")
    return failures


def validate_quality_gate(quality_gate: dict[str, Any]) -> list[str]:
    failures: list[str] = []
    missing = sorted(REQUIRED_QUALITY_FIELDS - set(quality_gate))
    if missing:
        failures.append("quality_gate missing fields: " + ", ".join(missing))
    if quality_gate.get("status") not in {"passed", "passed_with_warnings", "failed_blocking"}:
        failures.append("quality_gate.status has an unexpected value")

    counts = quality_gate.get("dbt_counts")
    if not isinstance(counts, dict):
        failures.append("quality_gate.dbt_counts must be an object")
    else:
        missing_counts = sorted(REQUIRED_DBT_COUNTS - set(counts))
        if missing_counts:
            failures.append("quality_gate.dbt_counts missing: " + ", ".join(missing_counts))
    return failures


def validate_metadata_copy(
    *,
    root: Path,
    label: str,
    dag_id: str,
    run_id: str,
) -> tuple[Path | None, dict[str, Any] | None, list[str]]:
    candidates = metadata_candidates(root, dag_id, run_id)
    if not candidates:
        return None, None, [f"{label}: no metadata JSON found for run_id={run_id}"]

    path = candidates[0]
    try:
        summary = load_summary(path)
    except OSError as exc:
        return path, None, [f"{label}: could not read {path}: {exc}"]
    except json.JSONDecodeError as exc:
        return path, None, [f"{label}: invalid JSON in {path}: {exc}"]

    failures = [f"{label}: {failure}" for failure in validate_summary(summary, expected_run_id=run_id, expected_dag_id=dag_id)]
    return path, summary, failures


def _read_run_id_safely(path: Path) -> str:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return ""
    return str(payload.get("run_id", ""))


def main() -> int:
    args = parse_args()
    checks = [
        validate_metadata_copy(
            root=Path(args.data_root),
            label="local",
            dag_id=args.dag_id,
            run_id=args.run_id,
        )
    ]
    if not args.local_only:
        checks.append(
            validate_metadata_copy(
                root=Path(args.minio_root),
                label="minio",
                dag_id=args.dag_id,
                run_id=args.run_id,
            )
        )

    failures = [failure for _, _, check_failures in checks for failure in check_failures]
    if failures:
        print("Pipeline metadata check failed:")
        for failure in failures:
            print(f"- {failure}")
        return 1

    print("Pipeline metadata check passed.")
    for path, summary, _ in checks:
        if summary is None:
            continue
        gate = summary.get("quality_gate", {})
        counts = gate.get("dbt_counts", {})
        print(
            f"- {path}: mode={summary.get('run_mode')} months={summary.get('target_months')} "
            f"quality={gate.get('status')} dbt={counts}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
