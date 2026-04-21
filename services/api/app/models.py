from typing import Any

from pydantic import BaseModel, Field


class HealthResponse(BaseModel):
    status: str
    duckdb_path: str
    semantic_catalog_loaded: bool


class SchemaField(BaseModel):
    name: str
    description: str


class SchemaTable(BaseModel):
    name: str
    description: str
    fields: list[SchemaField] = Field(default_factory=list)


class SchemaResponse(BaseModel):
    tables: list[SchemaTable]


class QueryRequest(BaseModel):
    question: str = Field(..., min_length=3)
    max_rows: int = Field(default=100, ge=1, le=1000)
    sql: str | None = Field(
        default=None,
        description="Optional SQL override for deterministic read-only testing.",
    )


class QueryResponse(BaseModel):
    summary: str
    sql: str
    columns: list[str]
    rows: list[dict[str, Any]]
    execution_ms: int
