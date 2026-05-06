from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path
import json
import sys


def load_script(name: str):
    module_path = Path("scripts") / f"{name}.py"
    spec = spec_from_file_location(name, module_path)
    module = module_from_spec(spec)
    assert spec is not None
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def sample_pipeline_summary() -> dict:
    return {
        "schema_version": "1.0",
        "dag_id": "taxi_monthly_pipeline",
        "run_id": "manual__phase25_2024_01",
        "run_mode": "manual",
        "logical_date": "2026-05-06T00:00:00+00:00",
        "target_months": ["2024-01"],
        "ingestion_results": [
            {
                "dataset": "yellow",
                "status": "uploaded",
                "source_url": "https://example.test/yellow.parquet",
                "bronze_object_key": "bronze/yellow_taxi/year=2024/month=01/yellow.parquet",
                "file_size_bytes": "10",
                "sha256": "abc",
            }
        ],
        "dbt_results": [
            {
                "layer": "gold",
                "status": "success",
                "counts": {"pass": 75, "warn": 2, "error": 0, "skip": 0},
            }
        ],
        "quality_gate": {
            "status": "passed_with_warnings",
            "dbt_counts": {"pass": 75, "warn": 2, "error": 0, "skip": 0},
            "blocking_ingestion_statuses": [],
        },
        "created_at_utc": "2026-05-06T00:00:00+00:00",
    }


def test_check_pipeline_run_validates_summary() -> None:
    script = load_script("check_pipeline_run")

    failures = script.validate_summary(
        sample_pipeline_summary(),
        expected_run_id="manual__phase25_2024_01",
        expected_dag_id="taxi_monthly_pipeline",
    )

    assert failures == []


def test_check_pipeline_run_detects_missing_quality_counts() -> None:
    script = load_script("check_pipeline_run")
    summary = sample_pipeline_summary()
    summary["quality_gate"]["dbt_counts"] = {"pass": 1}

    failures = script.validate_summary(
        summary,
        expected_run_id="manual__phase25_2024_01",
        expected_dag_id="taxi_monthly_pipeline",
    )

    assert any("dbt_counts missing" in failure for failure in failures)


def test_check_pipeline_run_finds_metadata_by_run_id(tmp_path) -> None:
    script = load_script("check_pipeline_run")
    root = tmp_path / "data"
    path = (
        root
        / "metadata"
        / "pipeline_runs"
        / "taxi_monthly_pipeline"
        / "2026-05-06"
        / "manual__phase25_2024_01.json"
    )
    path.parent.mkdir(parents=True)
    path.write_text(json.dumps(sample_pipeline_summary()), encoding="utf-8")

    candidates = script.metadata_candidates(
        root,
        "taxi_monthly_pipeline",
        "manual__phase25_2024_01",
    )

    assert candidates == [path]


def test_agent_eval_detects_expected_planning_payload() -> None:
    script = load_script("agent_eval")
    case = script.EvalCase(
        case_id="A",
        question="Average trip distance by service",
        expected_surface="aggregate_mart",
        expected_tables={"gold_daily_kpis"},
        sql_contains=("gold_daily_kpis", "avg_trip_distance"),
    )
    payload = {
        "requires_clarification": False,
        "sql": "SELECT avg_trip_distance FROM gold_daily_kpis",
        "rows": [{"avg_trip_distance": 2.5}],
        "agent_steps": [
            {
                "name": "planning",
                "metadata": {
                    "surface": "aggregate_mart",
                    "selected_tables": ["gold_daily_kpis"],
                },
            }
        ],
    }

    assert script.evaluate_payload(case, 200, payload) == []


def test_agent_eval_cases_include_bilingual_and_guardrail_coverage() -> None:
    script = load_script("agent_eval")
    cases = script.evaluation_cases("2024-H1")
    case_ids = {case.case_id for case in cases}

    assert {"A01", "A05", "A08", "C01", "B01", "B02"} <= case_ids
    assert any("nửa đầu năm 2024" in case.question for case in cases)
