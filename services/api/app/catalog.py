from pathlib import Path

import yaml

from app.models import SchemaField, SchemaForeignKey, SchemaJoin, SchemaResponse, SchemaTable


def _load_fields(items: list[dict]) -> list[SchemaField]:
    return [
        SchemaField(name=item["name"], description=item.get("description", ""))
        for item in items
    ]


def _load_foreign_keys(items: list[dict]) -> list[SchemaForeignKey]:
    return [
        SchemaForeignKey(
            column=item["column"],
            references_table=item["references_table"],
            references_column=item["references_column"],
        )
        for item in items
    ]


def _load_allowed_joins(items: list[dict]) -> list[SchemaJoin]:
    return [
        SchemaJoin(
            left_table=item["left_table"],
            left_column=item["left_column"],
            right_table=item["right_table"],
            right_column=item["right_column"],
        )
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
                execution_enabled=item.get("execution_enabled", False),
                grain=item.get("grain", ""),
                fields=_load_fields(item.get("fields", [])),
                dimensions=item.get("dimensions", []),
                metrics=_load_fields(item.get("metrics", [])),
                allowed_filters=item.get("allowed_filters", []),
                primary_key=item.get("primary_key", []),
                foreign_keys=_load_foreign_keys(item.get("foreign_keys", [])),
                allowed_joins=_load_allowed_joins(item.get("allowed_joins", [])),
                preferred_questions=item.get("preferred_questions", []),
            )
        )
    return SchemaResponse(tables=tables)


def filter_execution_enabled_tables(catalog: SchemaResponse) -> SchemaResponse:
    return SchemaResponse(tables=[table for table in catalog.tables if table.execution_enabled])
