from __future__ import annotations

import argparse
import json
import sys
import time
from dataclasses import dataclass, field
from typing import Any
from urllib import error, request


DEFAULT_BASE_URL = "http://localhost:8000"
DEFAULT_WINDOW = "2024-H1"


@dataclass(frozen=True)
class EvalCase:
    case_id: str
    question: str
    expected_http: int = 200
    max_rows: int = 50
    sql: str | None = None
    expected_surface: str | None = None
    expected_tables: set[str] = field(default_factory=set)
    sql_contains: tuple[str, ...] = ()
    requires_clarification: bool | None = False
    error_contains: str | None = None


def evaluation_cases(window: str) -> list[EvalCase]:
    if window != DEFAULT_WINDOW:
        raise ValueError("Only the fixed 2024-H1 evaluation window is currently supported.")

    return [
        EvalCase(
            case_id="A01",
            question="So sánh số chuyến Yellow Taxi và Green Taxi theo tháng trong nửa đầu năm 2024",
            expected_surface="aggregate_mart",
            expected_tables={"gold_daily_kpis"},
            sql_contains=("gold_daily_kpis", "2024-01-01", "2024-07-01"),
        ),
        EvalCase(
            case_id="A02",
            question="Average trip distance by service type by month in 2024 H1",
            expected_surface="aggregate_mart",
            expected_tables={"gold_daily_kpis"},
            sql_contains=("gold_daily_kpis", "avg_trip_distance", "2024-01-01", "2024-07-01"),
        ),
        EvalCase(
            case_id="A03",
            question="Total fare by service type by month in 2024 H1",
            expected_surface="aggregate_mart",
            expected_tables={"gold_daily_kpis"},
            sql_contains=("gold_daily_kpis", "total_fare_amount", "2024-01-01", "2024-07-01"),
        ),
        EvalCase(
            case_id="A04",
            question="Total amount by service type by month in 2024 H1",
            expected_surface="star_schema",
            expected_tables={"fact_trips"},
            sql_contains=("fact_trips", "total_amount", "2024-01-01", "2024-07-01"),
        ),
        EvalCase(
            case_id="A05",
            question="Vendor trend by month in 2024 H1",
            expected_surface="star_schema",
            expected_tables={"fact_trips", "dim_vendor"},
            sql_contains=("dim_vendor", "dim_date", "year_month", "2024-01-01", "2024-07-01"),
        ),
        EvalCase(
            case_id="A06",
            question="Payment type distribution in 2024 H1",
            expected_surface="star_schema",
            expected_tables={"fact_trips", "dim_payment_type"},
            sql_contains=("dim_payment_type", "payment_type_name", "2024-01-01", "2024-07-01"),
        ),
        EvalCase(
            case_id="A07",
            question="Pickup borough demand in 2024 H1",
            expected_surface="aggregate_mart",
            expected_tables={"gold_zone_demand"},
            sql_contains=("gold_zone_demand", "borough", "2024-01-01", "2024-07-01"),
        ),
        EvalCase(
            case_id="A08",
            question="Compare pickup and dropoff borough demand in 2024 H1",
            expected_surface="star_schema",
            expected_tables={"fact_trips", "dim_zone"},
            sql_contains=("pickup_zone_id", "dropoff_zone_id", "pickup_borough", "dropoff_borough"),
        ),
        EvalCase(
            case_id="C01",
            question="trips",
            requires_clarification=True,
        ),
        EvalCase(
            case_id="B01",
            question="Drop table",
            sql="drop table gold_daily_kpis",
            expected_http=400,
            requires_clarification=None,
            error_contains="Only SELECT",
        ),
        EvalCase(
            case_id="B02",
            question="Show all fact trips",
            sql="select * from fact_trips",
            expected_http=400,
            requires_clarification=None,
            error_contains="Wildcard SELECT",
        ),
    ]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run API agent regression evaluation cases.")
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL)
    parser.add_argument("--window", default=DEFAULT_WINDOW)
    parser.add_argument("--timeout", type=float, default=120.0)
    parser.add_argument("--output", help="Optional JSON output path.")
    return parser.parse_args()


def post_query(base_url: str, case: EvalCase, timeout: float) -> tuple[int, dict[str, Any]]:
    body: dict[str, Any] = {
        "question": case.question,
        "max_rows": case.max_rows,
    }
    if case.sql:
        body["sql"] = case.sql

    payload = json.dumps(body).encode("utf-8")
    http_request = request.Request(
        f"{base_url.rstrip('/')}/api/v1/query",
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with request.urlopen(http_request, timeout=timeout) as response:
            return response.status, json.loads(response.read().decode("utf-8"))
    except error.HTTPError as exc:
        raw = exc.read().decode("utf-8")
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            payload = {"detail": raw}
        return exc.code, payload


def evaluate_payload(case: EvalCase, status: int, payload: dict[str, Any]) -> list[str]:
    failures: list[str] = []
    if status != case.expected_http:
        failures.append(f"expected HTTP {case.expected_http}, got {status}")

    if case.error_contains and case.error_contains not in str(payload.get("detail", "")):
        failures.append(f"expected error containing {case.error_contains!r}, got {payload.get('detail')!r}")

    if case.requires_clarification is not None:
        actual = bool(payload.get("requires_clarification"))
        if actual is not case.requires_clarification:
            failures.append(f"expected requires_clarification={case.requires_clarification}, got {actual}")

    planning = planning_step(payload)
    if case.expected_surface and planning.get("metadata", {}).get("surface") != case.expected_surface:
        failures.append(
            "expected surface "
            f"{case.expected_surface}, got {planning.get('metadata', {}).get('surface')}"
        )

    selected_tables = set(planning.get("metadata", {}).get("selected_tables") or [])
    if case.expected_tables and not case.expected_tables <= selected_tables:
        failures.append(
            "expected selected tables to include "
            f"{sorted(case.expected_tables)}, got {sorted(selected_tables)}"
        )

    sql = str(payload.get("sql", ""))
    for expected in case.sql_contains:
        if expected not in sql:
            failures.append(f"expected SQL to contain {expected!r}")

    return failures


def planning_step(payload: dict[str, Any]) -> dict[str, Any]:
    for step in payload.get("agent_steps", []):
        if step.get("name") == "planning":
            return step
    return {}


def run_evaluation(base_url: str, window: str, timeout: float) -> dict[str, Any]:
    cases = evaluation_cases(window)
    results = []
    for case in cases:
        started = time.perf_counter()
        try:
            status, payload = post_query(base_url, case, timeout)
            failures = evaluate_payload(case, status, payload)
        except Exception as exc:
            status = 0
            payload = {"detail": str(exc)}
            failures = [str(exc)]
        elapsed_ms = int((time.perf_counter() - started) * 1000)
        results.append(
            {
                "case_id": case.case_id,
                "status": "pass" if not failures else "fail",
                "http_status": status,
                "failures": failures,
                "surface": planning_step(payload).get("metadata", {}).get("surface"),
                "selected_tables": planning_step(payload).get("metadata", {}).get("selected_tables"),
                "row_count": len(payload.get("rows", []) or []),
                "requires_clarification": payload.get("requires_clarification"),
                "detail": payload.get("detail"),
                "elapsed_ms": elapsed_ms,
            }
        )
    return {
        "evaluation": "agent_regression",
        "base_url": base_url,
        "window": window,
        "total": len(results),
        "passed": sum(1 for result in results if result["status"] == "pass"),
        "failed": sum(1 for result in results if result["status"] == "fail"),
        "results": results,
    }


def main() -> int:
    args = parse_args()
    summary = run_evaluation(args.base_url, args.window, args.timeout)
    encoded = json.dumps(summary, indent=2, ensure_ascii=False)
    if args.output:
        from pathlib import Path

        Path(args.output).write_text(encoded + "\n", encoding="utf-8")
    print(encoded)
    return 0 if summary["failed"] == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
