from __future__ import annotations

import logging
import os
from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator

from lib.dbt_runner import run_dbt_build
from lib.pipeline_metadata import (
    build_pipeline_run_summary,
    pipeline_run_metadata_key,
    upload_pipeline_run_summary_to_minio,
    write_pipeline_run_summary_local,
)
from lib.tlc_ingestion import (
    build_lookup_manifest,
    build_trip_manifest,
    ingest_file_to_minio,
    previous_month_starts,
)

LOCAL_DATA_ROOT = os.getenv("LOCAL_DATA_ROOT", "/opt/airflow/data")
TLC_DOWNLOAD_TIMEOUT_SECONDS = int(os.getenv("TLC_DOWNLOAD_TIMEOUT_SECONDS", "300"))
TLC_LOOKBACK_MONTHS = int(os.getenv("TLC_LOOKBACK_MONTHS", "3"))
TLC_PUBLICATION_GRACE_MONTHS = int(os.getenv("TLC_PUBLICATION_GRACE_MONTHS", "3"))
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "taxi-lakehouse")
MINIO_ROOT_USER = os.getenv("MINIO_ROOT_USER", "minioadmin")
MINIO_ROOT_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin123")
LOGGER = logging.getLogger(__name__)


def resolve_run_dates(data_interval_start, dag_run=None) -> list[datetime]:
    if dag_run and dag_run.conf:
        year = dag_run.conf.get("year")
        month = dag_run.conf.get("month")
        if year and month:
            return [datetime(int(year), int(month), 1)]
    return previous_month_starts(data_interval_start, TLC_LOOKBACK_MONTHS)


with DAG(
    dag_id="taxi_monthly_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule="0 0 15 * *",
    catchup=False,
    render_template_as_native_obj=True,
    tags=["taxi", "lakehouse", "elt", "yellow", "green"],
) as dag:
    start = EmptyOperator(task_id="start")

    @task
    def prepare_trip_manifests(data_interval_start=None, dag_run=None) -> list[dict[str, str]]:
        run_dates = resolve_run_dates(data_interval_start, dag_run)
        manifests = [
            build_trip_manifest(dataset, run_date).to_dict()
            for run_date in run_dates
            for dataset in ("yellow", "green")
        ]
        LOGGER.info("Prepared trip manifests: %s", manifests)
        return manifests

    @task
    def prepare_lookup_reference() -> dict[str, str]:
        manifest = build_lookup_manifest().to_dict()
        LOGGER.info("Prepared lookup manifest: %s", manifest)
        return manifest

    @task
    def ingest_to_bronze(manifest: dict[str, str]) -> dict[str, str]:
        LOGGER.info(
            "Starting Bronze ingestion for dataset=%s into root=%s and bucket=%s",
            manifest["dataset"],
            LOCAL_DATA_ROOT,
            MINIO_BUCKET,
        )
        result = ingest_file_to_minio(
            manifest=manifest,
            local_data_root=LOCAL_DATA_ROOT,
            minio_endpoint=MINIO_ENDPOINT,
            minio_bucket=MINIO_BUCKET,
            minio_access_key=MINIO_ROOT_USER,
            minio_secret_key=MINIO_ROOT_PASSWORD,
            timeout_seconds=TLC_DOWNLOAD_TIMEOUT_SECONDS,
            publication_grace_months=TLC_PUBLICATION_GRACE_MONTHS,
        )
        LOGGER.info("Finished Bronze ingestion for dataset=%s: %s", manifest["dataset"], result)
        return result

    @task
    def build_silver_layer() -> dict:
        LOGGER.info("Starting dbt build for Bronze and Silver layers")
        result = run_dbt_build("path:models/bronze path:models/silver")
        LOGGER.info("Completed dbt build for Bronze and Silver layers")
        return {"layer": "bronze_silver", **result}

    @task
    def build_gold_layer() -> dict:
        LOGGER.info("Starting dbt build for Gold layer")
        result = run_dbt_build("path:models/gold")
        LOGGER.info("Completed dbt build for Gold layer")
        return {"layer": "gold", **result}

    @task
    def publish_metadata(
        trip_results: list[dict],
        lookup_result: dict,
        silver_result: dict,
        gold_result: dict,
        data_interval_start=None,
        dag_run=None,
    ) -> dict:
        dag_id = "taxi_monthly_pipeline"
        run_id = getattr(dag_run, "run_id", "unknown")
        conf = getattr(dag_run, "conf", {}) or {}
        run_mode = "manual" if conf.get("year") and conf.get("month") else "scheduled"
        logical_date = (
            data_interval_start.isoformat()
            if hasattr(data_interval_start, "isoformat")
            else str(data_interval_start)
        )
        target_months = sorted(
            {
                f"{result.get('year')}-{result.get('month')}"
                for result in trip_results
                if result.get("year") and result.get("month")
            }
        )
        ingestion_results = [*trip_results, lookup_result]
        dbt_results = [silver_result, gold_result]
        summary = build_pipeline_run_summary(
            dag_id=dag_id,
            run_id=run_id,
            run_mode=run_mode,
            logical_date=logical_date,
            target_months=target_months,
            ingestion_results=ingestion_results,
            dbt_results=dbt_results,
        )
        metadata_key = pipeline_run_metadata_key(dag_id, run_id, logical_date)
        local_path = write_pipeline_run_summary_local(
            summary,
            LOCAL_DATA_ROOT,
            metadata_key,
        )
        upload_result = upload_pipeline_run_summary_to_minio(
            summary=summary,
            metadata_key=metadata_key,
            minio_endpoint=MINIO_ENDPOINT,
            minio_bucket=MINIO_BUCKET,
            minio_access_key=MINIO_ROOT_USER,
            minio_secret_key=MINIO_ROOT_PASSWORD,
        )
        result = {**upload_result, "local_metadata_path": str(local_path)}
        LOGGER.info("Published pipeline metadata: %s", result)
        return result

    done = EmptyOperator(task_id="done")

    trip_manifests = prepare_trip_manifests()
    lookup_manifest = prepare_lookup_reference()
    trip_bronze = ingest_to_bronze.override(task_id="ingest_trip_bronze").expand(
        manifest=trip_manifests
    )
    lookup_reference = ingest_to_bronze.override(task_id="ingest_taxi_zone_lookup")(lookup_manifest)
    build_silver = build_silver_layer()
    build_gold = build_gold_layer()
    metadata = publish_metadata(trip_bronze, lookup_reference, build_silver, build_gold)

    start >> [trip_manifests, lookup_manifest]
    lookup_manifest >> lookup_reference
    trip_bronze >> build_silver
    lookup_reference >> build_silver
    build_silver >> build_gold >> metadata >> done
