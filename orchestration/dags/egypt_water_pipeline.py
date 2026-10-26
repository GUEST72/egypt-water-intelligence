"""Amr Yousri's orchestration: bounded tasks around the complete two-lane pipeline."""
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor

from water_pipeline import runtime

DEFAULTS = {"owner": "Amr Yousri", "retries": 1, "retry_delay": timedelta(seconds=30),
            "execution_timeout": timedelta(minutes=30)}

with DAG(
    "egypt_water_pipeline", description="Generate → stream/archive → Spark/Iceberg → Trino",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc), schedule=None, catchup=False,
    max_active_runs=1, default_args=DEFAULTS,
    params={
        "config": Param("smoke", enum=["smoke", "pilot"], description="Generator dataset"),
        "batch_job": Param("handoff_smoke", enum=["handoff_smoke"],
                           description="Infrastructure smoke job; Amr AbdEl-Monem adds production transformations here"),
    },
    tags=["water", "whole-pipeline"],
    doc_md="""Owned by Amr Yousri. Compose boots the service processes; this DAG owns pipeline
    dependencies and work. Flink jobs and the archiver remain running after the DAG succeeds.
    `handoff_smoke` proves the batch integration; the analytical transformations belong to
    Amr AbdEl-Monem. See docs/AMR_ABDEL_MONEM_HANDOFF.md and docs/ORCHESTRATION.md.
    """,
) as dag:
    initialize = PythonOperator(task_id="initialize_topics_and_buckets", python_callable=runtime.initialize)
    generate = PythonOperator(task_id="generate", python_callable=runtime.generate)
    validate = PythonOperator(task_id="validate", python_callable=runtime.validate)
    archive = PythonOperator(task_id="archive_raw_to_minio", python_callable=runtime.archive_raw)
    claim = PythonOperator(task_id="claim_stream_namespace", python_callable=runtime.claim_stream)
    archiver = PythonOperator(task_id="ensure_archiver", python_callable=runtime.ensure_archiver)
    flink = PythonOperator(task_id="ensure_flink_jobs", python_callable=runtime.ensure_flink_jobs)
    ready = PythonSensor(task_id="wait_for_flink", python_callable=runtime.flink_running,
                         mode="reschedule", poke_interval=15, timeout=600)
    # Publication cannot be automatically replayed after an ambiguous failure.
    publish = PythonOperator(task_id="publish_delivery_to_kafka", python_callable=runtime.publish, retries=0)
    drained = PythonSensor(task_id="wait_for_stream_and_archive", python_callable=runtime.pipeline_drained,
                           mode="reschedule", poke_interval=30, timeout=1800)
    batch = PythonOperator(task_id="spark_batch", python_callable=runtime.spark_batch)
    query = PythonOperator(task_id="verify_iceberg_through_trino", python_callable=runtime.verify_trino)

    initialize >> generate >> validate >> archive >> claim >> archiver >> flink >> ready >> publish >> drained >> batch >> query

with DAG(
    "egypt_water_health", description="Monitor the services that keep running between pipeline runs",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc), schedule="*/5 * * * *", catchup=False,
    max_active_runs=1, default_args={**DEFAULTS, "execution_timeout": timedelta(minutes=3)},
    tags=["water", "monitoring"],
) as health_dag:
    PythonOperator(task_id="check_pipeline_services", python_callable=runtime.monitor)
