from pathlib import Path
import sys

import yaml

sys.path.insert(0, str(Path("services/api")))

from app.catalog import load_schema_catalog  # noqa: E402
from app.agent import build_query_plan, deterministic_sql_for_plan, normalize_question  # noqa: E402
from app.text_to_sql import generate_sql_with_openai, render_catalog_for_prompt  # noqa: E402


def test_semantic_catalog_has_tables() -> None:
    catalog_path = Path("contracts/semantic_catalog.yaml")
    payload = yaml.safe_load(catalog_path.read_text(encoding="utf-8"))

    assert "tables" in payload
    assert payload["tables"]


def test_semantic_catalog_describes_gold_star_schema_and_execution_surface() -> None:
    catalog_path = Path("contracts/semantic_catalog.yaml")
    payload = yaml.safe_load(catalog_path.read_text(encoding="utf-8"))

    table_names = {table["name"] for table in payload["tables"]}
    assert table_names == {
        "gold_daily_kpis",
        "gold_zone_demand",
        "fact_trips",
        "dim_date",
        "dim_zone",
        "dim_service_type",
        "dim_vendor",
        "dim_payment_type",
    }

    execution_enabled_tables = {
        table["name"] for table in payload["tables"] if table.get("execution_enabled")
    }
    assert execution_enabled_tables == table_names

    table_by_name = {table["name"]: table for table in payload["tables"]}

    for table in payload["tables"]:
        assert table["grain"]
        assert table["fields"]
        assert table["allowed_filters"]
        assert "primary_key" in table
        assert "foreign_keys" in table
        assert "allowed_joins" in table
        assert table["dimensions"] is not None
        assert table["metrics"] is not None

    assert table_by_name["gold_daily_kpis"]["table_type"] == "aggregate_mart"
    assert table_by_name["fact_trips"]["table_type"] == "fact"
    assert table_by_name["dim_zone"]["table_type"] == "dimension"
    assert table_by_name["fact_trips"]["primary_key"] == []
    assert len(table_by_name["fact_trips"]["foreign_keys"]) == 6
    assert len(table_by_name["fact_trips"]["allowed_joins"]) == 6
    assert len(table_by_name["fact_trips"]["metrics"]) == 4
    assert table_by_name["dim_date"]["primary_key"] == ["pickup_date"]


def test_catalog_loader_and_prompt_include_semantic_metadata() -> None:
    catalog = load_schema_catalog(Path("contracts/semantic_catalog.yaml"))
    rendered = render_catalog_for_prompt(catalog)

    assert len(catalog.tables) == 8
    assert sum(1 for table in catalog.tables if table.execution_enabled) == 8
    fact_trips = next(table for table in catalog.tables if table.name == "fact_trips")
    assert len(fact_trips.foreign_keys) == 6
    assert len(fact_trips.allowed_joins) == 6
    assert "Planner policy:" in rendered
    assert "Aggregate marts:" in rendered
    assert "Aggregate marts are already denormalized" in rendered
    assert "Grain:" in rendered
    assert "Metric: trip_count" in rendered
    assert "Allowed filters:" in rendered
    assert "Primary key: service_type, pickup_date" in rendered
    assert "Execution enabled: true" in rendered
    assert "Table: fact_trips" in rendered
    assert "Fact tables:" in rendered
    assert "Dimensions:" in rendered
    assert "Allowed joins:" in rendered
    assert "fact_trips.vendor_id = dim_vendor.vendor_id" in rendered


def test_prompt_can_render_star_schema_planning_context() -> None:
    catalog = load_schema_catalog(Path("contracts/semantic_catalog.yaml"))
    rendered = render_catalog_for_prompt(catalog, include_disabled=True)

    assert "Aggregate marts:" in rendered
    assert "Fact tables:" in rendered
    assert "Dimensions:" in rendered
    assert "Table: fact_trips" in rendered
    assert "Execution enabled: true" in rendered
    assert "Table: dim_vendor" in rendered
    assert "Table: dim_payment_type" in rendered
    assert "Allowed joins:" in rendered
    assert "fact_trips.vendor_id = dim_vendor.vendor_id" in rendered
    assert "fact_trips.payment_type = dim_payment_type.payment_type" in rendered
    assert "fact_trips.pickup_zone_id = dim_zone.zone_id" in rendered
    assert "fact_trips.dropoff_zone_id = dim_zone.zone_id" in rendered
    assert "Do not reference disabled tables in executable SQL." in rendered


def test_common_vietnamese_monthly_service_comparison_uses_daily_kpi_mart() -> None:
    catalog = load_schema_catalog(Path("contracts/semantic_catalog.yaml"))

    sql = generate_sql_with_openai(
        question="so sánh chuyến đi xanh và vàng các tháng trong năm 2023",
        catalog=catalog,
        model="gpt-4.1-mini",
        api_key="replace-me",
        max_rows=100,
    )

    assert "FROM gold_daily_kpis" in sql
    assert "JOIN" not in sql.upper()
    assert "2023-01-01" in sql
    assert "2024-01-01" in sql


def test_vietnamese_h1_demo_prompt_uses_daily_kpi_mart() -> None:
    catalog = load_schema_catalog(Path("contracts/semantic_catalog.yaml"))

    sql = generate_sql_with_openai(
        question="So sánh số chuyến Yellow Taxi và Green Taxi theo tháng trong nửa đầu năm 2024",
        catalog=catalog,
        model="gpt-4.1-mini",
        api_key="replace-me",
        max_rows=100,
    )

    assert "FROM gold_daily_kpis" in sql
    assert "JOIN" not in sql.upper()
    assert "2024-01-01" in sql
    assert "2024-07-01" in sql


def test_planner_generates_monthly_service_distance_from_daily_kpis() -> None:
    catalog = load_schema_catalog(Path("contracts/semantic_catalog.yaml"))
    question = "Average trip distance by service type by month in 2024 H1"
    normalized = normalize_question(question)

    plan = build_query_plan(normalized, catalog)
    sql = deterministic_sql_for_plan(question, plan, catalog)

    assert plan.intent == "monthly_service_kpi"
    assert plan.surface == "aggregate_mart"
    assert plan.selected_tables == ["gold_daily_kpis"]
    assert sql is not None
    assert "FROM gold_daily_kpis" in sql
    assert "avg_trip_distance" in sql
    assert "2024-01-01" in sql
    assert "2024-07-01" in sql


def test_planner_generates_monthly_service_total_amount_from_fact() -> None:
    catalog = load_schema_catalog(Path("contracts/semantic_catalog.yaml"))
    question = "Total amount by service type by month in 2024 H1"
    normalized = normalize_question(question)

    plan = build_query_plan(normalized, catalog)
    sql = deterministic_sql_for_plan(question, plan, catalog)

    assert plan.intent == "monthly_service_total_amount"
    assert plan.surface == "star_schema"
    assert plan.selected_tables == ["fact_trips"]
    assert sql is not None
    assert "FROM fact_trips" in sql
    assert "SUM(total_amount) AS total_amount" in sql
    assert "2024-01-01" in sql
    assert "2024-07-01" in sql


def test_planner_generates_vendor_monthly_trend_with_allowed_joins() -> None:
    catalog = load_schema_catalog(Path("contracts/semantic_catalog.yaml"))
    question = "Vendor trend by month in 2024 H1"
    normalized = normalize_question(question)

    plan = build_query_plan(normalized, catalog)
    sql = deterministic_sql_for_plan(question, plan, catalog)

    assert plan.intent == "vendor_analysis"
    assert plan.surface == "star_schema"
    assert plan.selected_tables == ["fact_trips", "dim_vendor"]
    assert sql is not None
    assert "JOIN dim_vendor AS v ON f.vendor_id = v.vendor_id" in sql
    assert "JOIN dim_date AS d ON f.pickup_date = d.pickup_date" in sql
    assert "d.year_month" in sql


def test_planner_generates_pickup_dropoff_borough_comparison() -> None:
    catalog = load_schema_catalog(Path("contracts/semantic_catalog.yaml"))
    question = "Compare pickup and dropoff borough demand in 2024 H1"
    normalized = normalize_question(question)

    plan = build_query_plan(normalized, catalog)
    sql = deterministic_sql_for_plan(question, plan, catalog)

    assert plan.intent == "pickup_dropoff_borough_comparison"
    assert plan.surface == "star_schema"
    assert plan.selected_tables == ["fact_trips", "dim_zone"]
    assert sql is not None
    assert "f.pickup_zone_id = pickup_zone.zone_id" in sql
    assert "f.dropoff_zone_id = dropoff_zone.zone_id" in sql
    assert "pickup_borough" in sql
    assert "dropoff_borough" in sql
