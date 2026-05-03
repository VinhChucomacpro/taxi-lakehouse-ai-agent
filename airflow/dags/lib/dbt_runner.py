from __future__ import annotations

import logging
import os
import json
import subprocess
from pathlib import Path
from typing import Any


DBT_PROJECT_DIR = Path(os.getenv("DBT_PROJECT_DIR", "/opt/airflow/dbt"))
DBT_PROFILES_DIR = Path(os.getenv("DBT_PROFILES_DIR", "/opt/airflow/.dbt"))
DBT_TARGET_PATH = Path(os.getenv("DBT_DUCKDB_PATH", "/opt/airflow/warehouse/analytics.duckdb"))
LOGGER = logging.getLogger(__name__)


def ensure_dbt_profile() -> Path:
    DBT_PROFILES_DIR.mkdir(parents=True, exist_ok=True)
    profile_path = DBT_PROFILES_DIR / "profiles.yml"
    profile_path.write_text(
        (
            "taxi_lakehouse:\n"
            "  target: dev\n"
            "  outputs:\n"
            "    dev:\n"
            "      type: duckdb\n"
            f"      path: {DBT_TARGET_PATH.as_posix()}\n"
            "      threads: 4\n"
        ),
        encoding="utf-8",
    )
    return profile_path


def summarize_run_results(run_results_path: Path) -> dict[str, Any]:
    if not run_results_path.is_file():
        raise FileNotFoundError(f"dbt run results artifact was not found: {run_results_path}")

    payload = json.loads(run_results_path.read_text(encoding="utf-8"))
    counts = {"pass": 0, "warn": 0, "error": 0, "skip": 0}
    status_map = {
        "success": "pass",
        "pass": "pass",
        "warn": "warn",
        "error": "error",
        "fail": "error",
        "skipped": "skip",
        "skip": "skip",
    }
    for result in payload.get("results", []):
        raw_status = str(result.get("status", "")).lower()
        mapped_status = status_map.get(raw_status, "error")
        counts[mapped_status] += 1

    return {
        "status": "success" if counts["error"] == 0 else "error",
        "counts": counts,
        "invocation_id": payload.get("metadata", {}).get("invocation_id"),
        "generated_at": payload.get("metadata", {}).get("generated_at"),
        "run_results_path": str(run_results_path),
    }


def run_dbt_build(select: str | None = None) -> dict[str, Any]:
    profile_path = ensure_dbt_profile()
    command = [
        "dbt",
        "build",
        "--project-dir",
        str(DBT_PROJECT_DIR),
        "--profiles-dir",
        str(DBT_PROFILES_DIR),
    ]
    if select:
        command.extend(["--select", select])

    LOGGER.info("Running dbt command: %s", " ".join(command))
    LOGGER.info("Using dbt profile at %s", profile_path)
    subprocess.run(command, check=True)
    summary = summarize_run_results(DBT_PROJECT_DIR / "target" / "run_results.json")
    summary["command"] = command
    summary["select"] = select
    return summary
