from pathlib import Path

import yaml

from app.models import SchemaField, SchemaResponse, SchemaTable


def _load_fields(items: list[dict]) -> list[SchemaField]:
    return [
        SchemaField(name=item["name"], description=item.get("description", ""))
        for item in items
    ]


def load_schema_catalog(path: Path) -> SchemaResponse:
    payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    tables = []
    for item in payload.get("tables", []):
        tables.append(
            SchemaTable(
                name=item["name"],
                description=item.get("description", ""),
                table_type=item.get("table_type", "aggregate_mart"),
                grain=item.get("grain", ""),
                fields=_load_fields(item.get("fields", [])),
                dimensions=item.get("dimensions", []),
                metrics=_load_fields(item.get("metrics", [])),
                allowed_filters=item.get("allowed_filters", []),
                preferred_questions=item.get("preferred_questions", []),
            )
        )
    return SchemaResponse(tables=tables)
