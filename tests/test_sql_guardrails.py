from pathlib import Path
import sys

import pytest

pytest.importorskip("sqlglot")

sys.path.insert(0, str(Path("services/api")))

from app.models import SchemaField, SchemaResponse, SchemaTable  # noqa: E402
from app.sql_guardrails import SQLValidationError, validate_gold_select  # noqa: E402


def catalog() -> SchemaResponse:
    return SchemaResponse(
        tables=[
            SchemaTable(
                name="gold_daily_kpis",
                description="Daily KPIs",
                fields=[SchemaField(name="pickup_date", description="Pickup date")],
            ),
            SchemaTable(
                name="gold_zone_demand",
                description="Zone demand",
                fields=[SchemaField(name="zone_id", description="Zone ID")],
            ),
        ]
    )


def test_validate_gold_select_adds_limit() -> None:
    result = validate_gold_select("select * from gold_daily_kpis", catalog(), max_rows=25)

    assert result.sql == "SELECT * FROM gold_daily_kpis LIMIT 25"
    assert result.tables == {"gold_daily_kpis"}


def test_validate_gold_select_caps_existing_limit() -> None:
    result = validate_gold_select("select * from gold_daily_kpis limit 1000", catalog(), max_rows=50)

    assert result.sql == "SELECT * FROM gold_daily_kpis LIMIT 50"


def test_validate_gold_select_rejects_non_gold_table() -> None:
    with pytest.raises(SQLValidationError, match="non-Gold"):
        validate_gold_select("select * from silver_trips_unified", catalog(), max_rows=100)


def test_validate_gold_select_rejects_ddl() -> None:
    with pytest.raises(SQLValidationError, match="Only SELECT"):
        validate_gold_select("drop table gold_daily_kpis", catalog(), max_rows=100)


def test_validate_gold_select_allows_cte_over_gold_table() -> None:
    result = validate_gold_select(
        "with daily as (select * from gold_daily_kpis) select * from daily",
        catalog(),
        max_rows=10,
    )

    assert result.tables == {"gold_daily_kpis"}
    assert result.sql.endswith("LIMIT 10")
