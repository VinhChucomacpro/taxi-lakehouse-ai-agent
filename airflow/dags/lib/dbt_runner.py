from __future__ import annotations

import logging
import os
import subprocess
from pathlib import Path


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


def run_dbt_build(select: str | None = None) -> None:
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
