from __future__ import annotations

import logging
import os
from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator

from lib.dbt_runner import run_dbt_build
from lib.tlc_ingestion import build_lookup_manifest, build_trip_manifest, download_file_to_local

LOCAL_DATA_ROOT = os.getenv("LOCAL_DATA_ROOT", "/opt/airflow/data")
TLC_DOWNLOAD_TIMEOUT_SECONDS = int(os.getenv("TLC_DOWNLOAD_TIMEOUT_SECONDS", "300"))
LOGGER = logging.getLogger(__name__)


def resolve_run_date(data_interval_start, dag_run=None) -> datetime:
    if dag_run and dag_run.conf:
        year = dag_run.conf.get("year")
        month = dag_run.conf.get("month")
        if year and month:
            return datetime(int(year), int(month), 1)
    return data_interval_start


with DAG(
    dag_id="taxi_monthly_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule="@monthly",
    catchup=False,
    render_template_as_native_obj=True,
    tags=["taxi", "lakehouse", "elt", "yellow", "green"],
) as dag:
    start = EmptyOperator(task_id="start")

    @task
    def prepare_yellow_manifest(data_interval_start=None, dag_run=None) -> dict[str, str]:
        run_date = resolve_run_date(data_interval_start, dag_run)
        manifest = build_trip_manifest("yellow", run_date).to_dict()
        LOGGER.info("Prepared yellow manifest: %s", manifest)
        return manifest

    @task
    def prepare_green_manifest(data_interval_start=None, dag_run=None) -> dict[str, str]:
        run_date = resolve_run_date(data_interval_start, dag_run)
        manifest = build_trip_manifest("green", run_date).to_dict()
        LOGGER.info("Prepared green manifest: %s", manifest)
        return manifest

    @task
    def prepare_lookup_reference() -> dict[str, str]:
        manifest = build_lookup_manifest().to_dict()
        LOGGER.info("Prepared lookup manifest: %s", manifest)
        return manifest

    @task
    def ingest_to_local(manifest: dict[str, str]) -> dict[str, str]:
        LOGGER.info(
            "Starting local ingestion for dataset=%s into root=%s",
            manifest["dataset"],
            LOCAL_DATA_ROOT,
        )
        result = download_file_to_local(
            manifest=manifest,
            local_data_root=LOCAL_DATA_ROOT,
            timeout_seconds=TLC_DOWNLOAD_TIMEOUT_SECONDS,
        )
        LOGGER.info("Finished local ingestion for dataset=%s: %s", manifest["dataset"], result)
        return result

    @task
    def build_silver_layer() -> None:
        LOGGER.info("Starting dbt build for Bronze and Silver layers")
        run_dbt_build("path:models/bronze path:models/silver")
        LOGGER.info("Completed dbt build for Bronze and Silver layers")

    @task
    def build_gold_layer() -> None:
        LOGGER.info("Starting dbt build for Gold layer")
        run_dbt_build("path:models/gold")
        LOGGER.info("Completed dbt build for Gold layer")

    publish_metadata = EmptyOperator(task_id="publish_metadata")
    done = EmptyOperator(task_id="done")

    yellow_manifest = prepare_yellow_manifest()
    green_manifest = prepare_green_manifest()
    lookup_manifest = prepare_lookup_reference()
    yellow_bronze = ingest_to_local.override(task_id="ingest_yellow_bronze")(yellow_manifest)
    green_bronze = ingest_to_local.override(task_id="ingest_green_bronze")(green_manifest)
    lookup_reference = ingest_to_local.override(task_id="ingest_taxi_zone_lookup")(lookup_manifest)
    build_silver = build_silver_layer()
    build_gold = build_gold_layer()

    start >> [yellow_manifest, green_manifest, lookup_manifest]
    yellow_manifest >> yellow_bronze
    green_manifest >> green_bronze
    lookup_manifest >> lookup_reference
    [yellow_bronze, green_bronze, lookup_reference] >> build_silver
    build_silver >> build_gold >> publish_metadata >> done
