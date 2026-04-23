from pathlib import Path
import sys

import yaml

sys.path.insert(0, str(Path("services/api")))

from app.catalog import load_schema_catalog  # noqa: E402
from app.text_to_sql import render_catalog_for_prompt  # noqa: E402


def test_semantic_catalog_has_tables() -> None:
    catalog_path = Path("contracts/semantic_catalog.yaml")
    payload = yaml.safe_load(catalog_path.read_text(encoding="utf-8"))

    assert "tables" in payload
    assert payload["tables"]


def test_semantic_catalog_describes_ai_serving_marts() -> None:
    catalog_path = Path("contracts/semantic_catalog.yaml")
    payload = yaml.safe_load(catalog_path.read_text(encoding="utf-8"))

    table_names = {table["name"] for table in payload["tables"]}
    assert table_names == {"gold_daily_kpis", "gold_zone_demand"}
    assert "fact_trips" not in table_names

    for table in payload["tables"]:
        assert table["table_type"] == "aggregate_mart"
        assert table["grain"]
        assert table["fields"]
        assert table["dimensions"]
        assert table["metrics"]
        assert table["allowed_filters"]
        assert table["preferred_questions"]


def test_catalog_loader_and_prompt_include_semantic_metadata() -> None:
    catalog = load_schema_catalog(Path("contracts/semantic_catalog.yaml"))
    rendered = render_catalog_for_prompt(catalog)

    assert catalog.tables[0].grain
    assert catalog.tables[0].metrics
    assert "Grain:" in rendered
    assert "Metric: trip_count" in rendered
    assert "Allowed filters:" in rendered
