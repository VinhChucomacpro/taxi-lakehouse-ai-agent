from pathlib import Path
import sys

import pytest

pytest.importorskip("sqlglot")

sys.path.insert(0, str(Path("services/api")))

from app.models import SchemaField, SchemaJoin, SchemaResponse, SchemaTable  # noqa: E402
from app.sql_guardrails import SQLValidationError, validate_gold_select  # noqa: E402


def catalog() -> SchemaResponse:
    return SchemaResponse(
        tables=[
            SchemaTable(
                name="gold_daily_kpis",
                description="Daily KPIs",
                execution_enabled=True,
                fields=[
                    SchemaField(name="pickup_date", description="Pickup date"),
                    SchemaField(name="trip_count", description="Trip count"),
                    SchemaField(name="service_type", description="Service type"),
                ],
            ),
            SchemaTable(
                name="gold_zone_demand",
                description="Zone demand",
                execution_enabled=True,
                fields=[
                    SchemaField(name="zone_id", description="Zone ID"),
                    SchemaField(name="trip_count", description="Trip count"),
                ],
            ),
            SchemaTable(
                name="fact_trips",
                description="Trip fact",
                table_type="fact",
                execution_enabled=False,
                fields=[
                    SchemaField(name="pickup_date", description="Pickup date"),
                    SchemaField(name="trip_distance", description="Trip distance"),
                ],
            ),
        ]
    )


def star_catalog() -> SchemaResponse:
    return SchemaResponse(
        tables=[
            SchemaTable(
                name="fact_trips",
                description="Trip fact",
                table_type="fact",
                execution_enabled=True,
                fields=[
                    SchemaField(name="pickup_date", description="Pickup date"),
                    SchemaField(name="vendor_id", description="Vendor ID"),
                    SchemaField(name="pickup_zone_id", description="Pickup zone ID"),
                    SchemaField(name="dropoff_zone_id", description="Dropoff zone ID"),
                    SchemaField(name="trip_distance", description="Trip distance"),
                ],
                allowed_joins=[
                    SchemaJoin(
                        left_table="fact_trips",
                        left_column="vendor_id",
                        right_table="dim_vendor",
                        right_column="vendor_id",
                    ),
                    SchemaJoin(
                        left_table="fact_trips",
                        left_column="pickup_zone_id",
                        right_table="dim_zone",
                        right_column="zone_id",
                    ),
                    SchemaJoin(
                        left_table="fact_trips",
                        left_column="dropoff_zone_id",
                        right_table="dim_zone",
                        right_column="zone_id",
                    ),
                ],
            ),
            SchemaTable(
                name="dim_vendor",
                description="Vendor dimension",
                table_type="dimension",
                execution_enabled=True,
                fields=[
                    SchemaField(name="vendor_id", description="Vendor ID"),
                    SchemaField(name="vendor_name", description="Vendor name"),
                ],
            ),
            SchemaTable(
                name="dim_zone",
                description="Zone dimension",
                table_type="dimension",
                execution_enabled=True,
                fields=[
                    SchemaField(name="zone_id", description="Zone ID"),
                    SchemaField(name="zone_name", description="Zone name"),
                ],
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


def test_validate_gold_select_allows_cataloged_columns() -> None:
    result = validate_gold_select(
        "select k.pickup_date, sum(k.trip_count) as trips "
        "from gold_daily_kpis as k group by k.pickup_date order by trips desc",
        catalog(),
        max_rows=50,
    )

    assert result.tables == {"gold_daily_kpis"}
    assert "SUM(k.trip_count) AS trips" in result.sql


def test_validate_gold_select_rejects_non_gold_table() -> None:
    with pytest.raises(SQLValidationError, match="non-Gold"):
        validate_gold_select("select * from silver_trips_unified", catalog(), max_rows=100)


def test_validate_gold_select_rejects_unknown_column() -> None:
    with pytest.raises(SQLValidationError, match="unknown column"):
        validate_gold_select("select pickup_date, fake_metric from gold_daily_kpis", catalog(), max_rows=100)


def test_validate_gold_select_rejects_unknown_qualified_column() -> None:
    with pytest.raises(SQLValidationError, match="unknown column"):
        validate_gold_select("select k.fake_metric from gold_daily_kpis as k", catalog(), max_rows=100)


def test_validate_gold_select_rejects_unknown_alias() -> None:
    with pytest.raises(SQLValidationError, match="unknown table or alias"):
        validate_gold_select("select bad.pickup_date from gold_daily_kpis as k", catalog(), max_rows=100)


def test_validate_gold_select_rejects_cataloged_but_disabled_table() -> None:
    with pytest.raises(SQLValidationError, match="not execution-enabled"):
        validate_gold_select("select pickup_date from fact_trips", catalog(), max_rows=100)


def test_validate_gold_select_rejects_wildcard_on_fact_table() -> None:
    with pytest.raises(SQLValidationError, match="Wildcard SELECT"):
        validate_gold_select("select * from fact_trips", catalog(), max_rows=100)


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


def test_validate_gold_select_allows_valid_fact_vendor_join() -> None:
    result = validate_gold_select(
        "select f.pickup_date, v.vendor_name, sum(f.trip_distance) as total_distance "
        "from fact_trips as f "
        "join dim_vendor as v on f.vendor_id = v.vendor_id "
        "group by f.pickup_date, v.vendor_name",
        star_catalog(),
        max_rows=100,
    )

    assert result.tables == {"fact_trips", "dim_vendor"}
    assert result.sql.endswith("LIMIT 100")


def test_validate_gold_select_allows_valid_pickup_zone_join() -> None:
    result = validate_gold_select(
        "select z.zone_name, sum(f.trip_distance) as total_distance "
        "from fact_trips f "
        "join dim_zone z on f.pickup_zone_id = z.zone_id "
        "group by z.zone_name",
        star_catalog(),
        max_rows=50,
    )

    assert result.tables == {"fact_trips", "dim_zone"}


def test_validate_gold_select_allows_valid_dropoff_zone_join() -> None:
    result = validate_gold_select(
        "select z.zone_name, sum(f.trip_distance) as total_distance "
        "from fact_trips f "
        "join dim_zone z on f.dropoff_zone_id = z.zone_id "
        "group by z.zone_name",
        star_catalog(),
        max_rows=50,
    )

    assert result.tables == {"fact_trips", "dim_zone"}


def test_validate_gold_select_rejects_wrong_join_key() -> None:
    with pytest.raises(SQLValidationError, match="allowed semantic catalog join path"):
        validate_gold_select(
            "select f.pickup_date, v.vendor_name "
            "from fact_trips f "
            "join dim_vendor v on f.pickup_zone_id = v.vendor_id",
            star_catalog(),
            max_rows=100,
        )


def test_validate_gold_select_rejects_join_without_on() -> None:
    with pytest.raises(SQLValidationError, match="JOIN must include an ON"):
        validate_gold_select(
            "select f.pickup_date, v.vendor_name from fact_trips f join dim_vendor v",
            star_catalog(),
            max_rows=100,
        )


def test_validate_gold_select_rejects_cross_join() -> None:
    with pytest.raises(SQLValidationError, match="CROSS JOIN"):
        validate_gold_select(
            "select f.pickup_date, v.vendor_name from fact_trips f cross join dim_vendor v",
            star_catalog(),
            max_rows=100,
        )
