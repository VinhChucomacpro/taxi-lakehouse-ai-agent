from pathlib import Path
import sys
from types import SimpleNamespace

import pytest

duckdb = pytest.importorskip("duckdb")
pytest.importorskip("httpx")

sys.path.insert(0, str(Path("services/api")))

from fastapi.testclient import TestClient  # noqa: E402


def build_test_client(tmp_path, monkeypatch) -> TestClient:
    catalog_path = tmp_path / "semantic_catalog.yaml"
    catalog_path.write_text(
        """
tables:
  - name: gold_daily_kpis
    description: Daily KPIs
    table_type: aggregate_mart
    execution_enabled: true
    grain: One row per service_type and pickup_date.
    fields:
      - name: service_type
        description: Taxi service type.
      - name: pickup_date
        description: Pickup date.
      - name: trip_count
        description: Trip count.
    dimensions:
      - service_type
      - pickup_date
    metrics:
      - name: trip_count
        description: Trip count.
    allowed_filters:
      - service_type
      - pickup_date
      - trip_count
    primary_key:
      - service_type
      - pickup_date
    foreign_keys: []
    allowed_joins: []
  - name: fact_trips
    description: Trip fact
    table_type: fact
    execution_enabled: false
    grain: One row per trip.
    fields:
      - name: pickup_date
        description: Pickup date.
      - name: trip_distance
        description: Trip distance.
    dimensions:
      - pickup_date
    metrics:
      - name: trip_distance
        description: Trip distance.
    allowed_filters:
      - pickup_date
      - trip_distance
    primary_key: []
    foreign_keys: []
    allowed_joins: []
""".strip(),
        encoding="utf-8",
    )

    duckdb_path = tmp_path / "analytics.duckdb"
    with duckdb.connect(str(duckdb_path)) as connection:
        connection.execute(
            """
            create table gold_daily_kpis (
                service_type varchar,
                pickup_date date,
                trip_count integer
            )
            """
        )
        connection.execute(
            "insert into gold_daily_kpis values ('yellow_taxi', date '2024-01-01', 10)"
        )

    from app import main

    monkeypatch.setattr(
        main,
        "get_settings",
        lambda: SimpleNamespace(
            duckdb_path=str(duckdb_path),
            semantic_catalog=catalog_path,
            openai_api_key="replace-me",
            openai_model="gpt-4.1-mini",
        ),
    )
    return TestClient(main.app)


def test_query_endpoint_allows_gold_select_with_sql_override(tmp_path, monkeypatch) -> None:
    client = build_test_client(tmp_path, monkeypatch)

    response = client.post(
        "/api/v1/query",
        json={
            "question": "Show daily trips",
            "max_rows": 10,
            "sql": "select service_type, pickup_date, trip_count from gold_daily_kpis",
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["columns"] == ["service_type", "pickup_date", "trip_count"]
    assert payload["rows"] == [
        {"service_type": "yellow_taxi", "pickup_date": "2024-01-01", "trip_count": 10}
    ]
    assert payload["sql"].endswith("LIMIT 10")


def test_schema_endpoint_returns_full_catalog_metadata(tmp_path, monkeypatch) -> None:
    client = build_test_client(tmp_path, monkeypatch)

    response = client.get("/api/v1/schema")

    assert response.status_code == 200
    payload = response.json()
    assert {table["name"] for table in payload["tables"]} == {"gold_daily_kpis", "fact_trips"}
    mart = next(table for table in payload["tables"] if table["name"] == "gold_daily_kpis")
    fact = next(table for table in payload["tables"] if table["name"] == "fact_trips")
    assert mart["execution_enabled"] is True
    assert mart["primary_key"] == ["service_type", "pickup_date"]
    assert fact["execution_enabled"] is False
    assert fact["allowed_joins"] == []


def test_query_endpoint_rejects_non_gold_sql(tmp_path, monkeypatch) -> None:
    client = build_test_client(tmp_path, monkeypatch)

    response = client.post(
        "/api/v1/query",
        json={
            "question": "Show silver trips",
            "max_rows": 10,
            "sql": "select * from silver_trips_unified",
        },
    )

    assert response.status_code == 400
    assert "non-Gold" in response.json()["detail"]


def test_query_endpoint_rejects_disabled_fact_sql(tmp_path, monkeypatch) -> None:
    client = build_test_client(tmp_path, monkeypatch)

    response = client.post(
        "/api/v1/query",
        json={
            "question": "Show trip facts",
            "max_rows": 10,
            "sql": "select * from fact_trips",
        },
    )

    assert response.status_code == 400
    assert "not execution-enabled" in response.json()["detail"]


def test_query_endpoint_rejects_ddl(tmp_path, monkeypatch) -> None:
    client = build_test_client(tmp_path, monkeypatch)

    response = client.post(
        "/api/v1/query",
        json={
            "question": "Drop a table",
            "max_rows": 10,
            "sql": "drop table gold_daily_kpis",
        },
    )

    assert response.status_code == 400
    assert "Only SELECT" in response.json()["detail"]
