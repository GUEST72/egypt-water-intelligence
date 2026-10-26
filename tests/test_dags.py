from pathlib import Path
from unittest.mock import patch

import pytest

pytest.importorskip("airflow.models", reason="Install the pinned Airflow runtime for real DAG parsing")
from airflow.models import DagBag


def load_dags():
    path = Path(__file__).resolve().parents[1] / "orchestration/dags"
    # A DAG parse must remain possible while the service network is unavailable.
    with patch("urllib.request.urlopen", side_effect=AssertionError("network I/O at parse time")):
        bag = DagBag(dag_folder=str(path), include_examples=False, safe_mode=False)
    assert bag.import_errors == {}
    return bag


def test_whole_pipeline_dependencies_and_replay_policy():
    dag = load_dags().dags["egypt_water_pipeline"]
    assert dag.schedule_interval is None
    assert dag.max_active_runs == 1
    chain = ["initialize_topics_and_buckets", "generate", "validate", "archive_raw_to_minio",
             "claim_stream_namespace", "ensure_archiver", "ensure_flink_jobs", "wait_for_flink",
             "publish_delivery_to_kafka", "wait_for_stream_and_archive", "spark_batch",
             "verify_iceberg_through_trino"]
    assert set(dag.task_ids) == set(chain)
    for before, after in zip(chain, chain[1:]):
        assert dag.get_task(before).downstream_task_ids == {after}
    assert dag.get_task("publish_delivery_to_kafka").retries == 0
    for sensor in ("wait_for_flink", "wait_for_stream_and_archive"):
        assert dag.get_task(sensor).mode == "reschedule"
    assert dag.params["config"] == "smoke"
    assert dag.params["batch_job"] == "handoff_smoke"


def test_continuous_services_have_a_scheduled_health_check():
    dag = load_dags().dags["egypt_water_health"]
    assert dag.schedule_interval == "*/5 * * * *"
    assert dag.task_ids == ["check_pipeline_services"]
