from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path
import json
import sys


def load_dbt_runner_module():
    module_path = Path("airflow/dags/lib/dbt_runner.py")
    spec = spec_from_file_location("dbt_runner", module_path)
    module = module_from_spec(spec)
    assert spec is not None
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_ensure_dbt_profile_writes_expected_profile(tmp_path) -> None:
    module = load_dbt_runner_module()
    module.DBT_PROFILES_DIR = tmp_path / ".dbt"
    module.DBT_TARGET_PATH = tmp_path / "warehouse" / "analytics.duckdb"

    profile_path = module.ensure_dbt_profile()

    assert profile_path.exists()
    content = profile_path.read_text(encoding="utf-8")
    assert "taxi_lakehouse:" in content
    assert "type: duckdb" in content


def test_bronze_models_default_to_minio_paths() -> None:
    bronze_dir = Path("dbt/models/bronze")

    yellow_sql = (bronze_dir / "bronze_yellow_trips.sql").read_text(encoding="utf-8")
    green_sql = (bronze_dir / "bronze_green_trips.sql").read_text(encoding="utf-8")
    lookup_sql = (bronze_dir / "bronze_taxi_zone_lookup.sql").read_text(encoding="utf-8")

    assert 's3://" ~ env_var("MINIO_BUCKET", "taxi-lakehouse")' in yellow_sql
    assert "/bronze/yellow_taxi/**/*.parquet" in yellow_sql
    assert 's3://" ~ env_var("MINIO_BUCKET", "taxi-lakehouse")' in green_sql
    assert "/bronze/green_taxi/**/*.parquet" in green_sql
    assert 's3://" ~ env_var("MINIO_BUCKET", "taxi-lakehouse")' in lookup_sql
    assert "/reference/taxi_zone_lookup/taxi_zone_lookup.csv" in lookup_sql


def test_dbt_project_configures_minio_access_on_run_start() -> None:
    project = Path("dbt/dbt_project.yml").read_text(encoding="utf-8")
    macro = Path("dbt/macros/configure_minio_access.sql").read_text(encoding="utf-8")

    assert "configure_minio_access" in project
    assert "install httpfs" in macro.lower()
    assert "create or replace secret minio_bronze" in macro.lower()
    assert "endpoint" in macro.lower()


def test_summarize_run_results_counts_statuses(tmp_path) -> None:
    module = load_dbt_runner_module()
    run_results_path = tmp_path / "run_results.json"
    run_results_path.write_text(
        json.dumps(
            {
                "metadata": {
                    "invocation_id": "abc",
                    "generated_at": "2026-05-03T00:00:00Z",
                },
                "results": [
                    {"status": "success"},
                    {"status": "warn"},
                    {"status": "error"},
                    {"status": "skipped"},
                ],
            }
        ),
        encoding="utf-8",
    )

    summary = module.summarize_run_results(run_results_path)

    assert summary["status"] == "error"
    assert summary["counts"] == {"pass": 1, "warn": 1, "error": 1, "skip": 1}
    assert summary["invocation_id"] == "abc"


def test_run_dbt_build_returns_summary(monkeypatch, tmp_path) -> None:
    module = load_dbt_runner_module()
    module.DBT_PROJECT_DIR = tmp_path / "dbt"
    module.DBT_PROFILES_DIR = tmp_path / ".dbt"
    module.DBT_TARGET_PATH = tmp_path / "warehouse" / "analytics.duckdb"
    target_dir = module.DBT_PROJECT_DIR / "target"
    target_dir.mkdir(parents=True)
    (target_dir / "run_results.json").write_text(
        json.dumps(
            {
                "metadata": {"invocation_id": "abc"},
                "results": [{"status": "success"}, {"status": "warn"}],
            }
        ),
        encoding="utf-8",
    )
    calls = []

    def fake_run(command, check):
        calls.append((command, check))

    monkeypatch.setattr(module.subprocess, "run", fake_run)

    summary = module.run_dbt_build("path:models/gold")

    assert calls[0][1] is True
    assert "--select" in calls[0][0]
    assert summary["counts"] == {"pass": 1, "warn": 1, "error": 0, "skip": 0}
    assert summary["select"] == "path:models/gold"
