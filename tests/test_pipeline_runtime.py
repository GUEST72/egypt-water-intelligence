import json
from contextlib import contextmanager
from unittest.mock import Mock

import pytest

from water_pipeline import runtime as rt

RID = "run-0123456789abcdef"


@pytest.fixture
def ti():
    result = Mock()
    result.xcom_pull.return_value = {"run_id": RID, "manifest": {
        "delivery_district_records": 360, "delivery_meter_records": 48,
    }}
    return result


@pytest.mark.parametrize("value", ["../../tmp/x", "run-1", "run-' OR true", None])
def test_run_ids_reject_invalid_paths_and_sql(value):
    with pytest.raises(ValueError):
        rt.valid_run(value)


def test_container_failure_propagates_and_removes_container(monkeypatch):
    container = Mock()
    container.logs.return_value = [b"job error\n"]
    container.wait.return_value = {"StatusCode": 2}
    client = Mock()
    client.containers.run.return_value = container
    @contextmanager
    def fake_client():
        yield client
    # Avoid requiring Docker's package for pure failure-path testing.
    import sys, types
    docker_types = types.ModuleType("docker.types")
    docker_types.Mount = Mock()
    monkeypatch.setitem(sys.modules, "docker.types", docker_types)
    monkeypatch.setattr(rt, "docker_client", fake_client)
    monkeypatch.setenv("EWI_NETWORK", "test_default")
    monkeypatch.setenv("EWI_PROJECT", "test")
    with pytest.raises(RuntimeError, match="exited 2"):
        rt.run_container("test", ["--test"])
    container.remove.assert_called_once_with(force=True)


def test_task_cancellation_removes_running_container(monkeypatch):
    import sys, types
    module = types.ModuleType("docker.types")
    module.Mount = Mock()
    monkeypatch.setitem(sys.modules, "docker.types", module)
    container = Mock()
    container.logs.side_effect = TimeoutError("Airflow task timeout")
    client = Mock()
    client.containers.run.return_value = container
    @contextmanager
    def fake_client():
        yield client
    monkeypatch.setattr(rt, "docker_client", fake_client)
    monkeypatch.setenv("EWI_NETWORK", "test_default")
    monkeypatch.setenv("EWI_PROJECT", "test")
    with pytest.raises(TimeoutError):
        rt.run_container("test", [])
    container.remove.assert_called_once_with(force=True)


def test_job_env_excludes_airflow_secrets(monkeypatch):
    monkeypatch.setenv("AIRFLOW__DATABASE__SQL_ALCHEMY_CONN", "secret")
    monkeypatch.setenv("MINIO_ACCESS_KEY", "key")
    assert "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN" not in rt.env()
    assert rt.env()["MINIO_ACCESS_KEY"] == "key"


@pytest.mark.parametrize("state", [
    {"run_id": "run-ffffffffffffffff", "status": "published"},
    {"run_id": RID, "status": "publishing"},
])
def test_claim_refuses_foreign_run_or_interrupted_publication(monkeypatch, ti, state):
    monkeypatch.setattr(rt, "read_state", lambda: state)
    with pytest.raises(RuntimeError):
        rt.claim_stream(ti)


def test_claim_refuses_unmanaged_kafka_records(monkeypatch, ti):
    monkeypatch.setattr(rt, "read_state", lambda: None)
    @contextmanager
    def fake_consumer():
        yield Mock()
    monkeypatch.setattr(rt, "kafka_consumer", fake_consumer)
    monkeypatch.setattr(rt, "ends", lambda *_: {"topic:0": 100})
    write = Mock()
    monkeypatch.setattr(rt, "write_state", write)
    with pytest.raises(RuntimeError, match="Unmanaged Kafka data"):
        rt.claim_stream(ti)
    write.assert_not_called()


def test_completed_publication_is_not_replayed(monkeypatch, ti):
    state = {"run_id": RID, "status": "published", "offsets": {"topic:0": 360}}
    monkeypatch.setattr(rt, "read_state", lambda: state)
    generate = Mock()
    monkeypatch.setattr(rt, "generator", generate)
    assert rt.publish(ti) == state["offsets"]
    generate.assert_not_called()


def test_failed_publication_retains_ambiguous_state(monkeypatch, ti):
    monkeypatch.setattr(rt, "read_state", lambda: {"run_id": RID, "status": "prepared"})
    saved = []
    monkeypatch.setattr(rt, "write_state", lambda state: saved.append(json.loads(json.dumps(state))))
    monkeypatch.setattr(rt, "generator", Mock(side_effect=RuntimeError("producer died")))
    with pytest.raises(RuntimeError, match="producer died"):
        rt.publish(ti)
    assert saved == [{"run_id": RID, "status": "publishing"}]


def test_flink_reuses_existing_jobs_without_submission(monkeypatch):
    monkeypatch.setattr(rt, "read_state", lambda: None)
    jobs = [{"name": f"ewi-{key}", "jid": str(i), "state": "RUNNING"}
            for i, (key, _, _) in enumerate(rt.JOBS)]
    monkeypatch.setattr(rt, "http_json", lambda _: {"jobs": jobs})
    docker = Mock(side_effect=AssertionError("must not submit again"))
    monkeypatch.setattr(rt, "docker_client", docker)
    assert len(rt.ensure_flink_jobs()) == 4
    docker.assert_not_called()


def test_flink_does_not_resubmit_a_failed_job_without_state(monkeypatch):
    monkeypatch.setattr(rt, "read_state", lambda: None)
    monkeypatch.setattr(rt, "http_json", lambda _: {"jobs": [
        {"name": "ewi-validation", "jid": "1", "state": "FAILED"}]})
    with pytest.raises(RuntimeError, match="savepoint"):
        rt.ensure_flink_jobs()


def test_flink_rejects_competing_manual_jobs(monkeypatch):
    monkeypatch.setattr(rt, "read_state", lambda: None)
    monkeypatch.setattr(rt, "http_json", lambda _: {"jobs": [
        {"name": "manual-job", "jid": "1", "state": "RUNNING"}]})
    with pytest.raises(RuntimeError, match="Unmanaged Flink jobs"):
        rt.ensure_flink_jobs()


def test_total_cluster_loss_requires_recovery_after_publication(monkeypatch):
    monkeypatch.setattr(rt, "read_state", lambda: {"run_id": RID, "status": "published"})
    monkeypatch.setattr(rt, "http_json", lambda _: {"jobs": []})
    with pytest.raises(RuntimeError, match="savepoint"):
        rt.ensure_flink_jobs()


def test_trino_reads_all_pages_and_reports_late_errors(monkeypatch):
    pages = iter([{"nextUri": "http://trino:8080/next", "data": [[1]]},
                  {"error": {"message": "missing Iceberg object"}}])
    @contextmanager
    def response(*args, **kwargs):
        import io
        yield io.StringIO(json.dumps(next(pages)))
    monkeypatch.setattr(rt, "urlopen", response)
    with pytest.raises(RuntimeError, match="missing Iceberg object"):
        rt.trino_query("SELECT 1")


def test_trino_mismatched_counts_fail_verification(monkeypatch, ti):
    monkeypatch.setattr(rt, "trino_query", lambda _: [[360, 0]])
    with pytest.raises(RuntimeError, match="count mismatch"):
        rt.verify_trino(ti)


def test_trino_matches_actual_delivery_counts(monkeypatch, ti):
    monkeypatch.setattr(rt, "trino_query", lambda _: [[360, 48]])
    assert rt.verify_trino(ti)["trino_verified"] is True
