"""Bounded Airflow tasks; network calls happen at task execution, never DAG import."""
from __future__ import annotations

import io
import json
import logging
import os
import re
from collections import deque
from contextlib import contextmanager
from urllib.request import Request, urlopen

LOG = logging.getLogger(__name__)
RAW_TOPICS = ("water.district-sensors.v1", "water.meter-readings.v1")
JOBS = (
    ("validation", "job2_district_validation.py", RAW_TOPICS[0]),
    ("features", "job3_district_features.py", "water.district-sensors.clean.v1"),
    ("alerts", "job3b_district_alerts.py", "water.district-features.15m.v1"),
    ("level_shift", "job3c_level_shift.py", "water.district-relative.15m.v1"),
)
REGISTRY = "orchestration/stream.json"


def env():
    """Explicit allowlist keeps Airflow metadata credentials out of job containers."""
    names = (
        "KAFKA_BOOTSTRAP_SERVERS", "MINIO_ENDPOINT", "MINIO_ACCESS_KEY",
        "MINIO_SECRET_KEY", "MINIO_RAW_BUCKET", "MINIO_RESTRICTED_BUCKET",
        "MINIO_EVENT_BUCKET", "MINIO_WAREHOUSE_BUCKET", "CATALOG_DB_PASSWORD", "AWS_REGION",
    )
    return {name: os.environ[name] for name in names if name in os.environ}


def valid_run(rid):
    if not isinstance(rid, str) or not re.fullmatch(r"run-[0-9a-f]{16}", rid):
        raise ValueError(f"Invalid generator run ID: {rid!r}")
    return rid


def receipt(ti):
    result = ti.xcom_pull(task_ids="generate")
    valid_run(result["run_id"])
    return result


def minio_client():
    from minio import Minio
    return Minio(os.environ["MINIO_ENDPOINT"], access_key=os.environ["MINIO_ACCESS_KEY"],
                 secret_key=os.environ["MINIO_SECRET_KEY"], secure=False)


def read_state():
    from minio.error import S3Error
    client = minio_client()
    try:
        response = client.get_object(os.environ["MINIO_EVENT_BUCKET"], REGISTRY)
    except S3Error as exc:
        if exc.code in {"NoSuchKey", "NoSuchObject", "NoSuchBucket"}:
            return None
        raise
    try:
        return json.load(response)
    finally:
        response.close()
        response.release_conn()


def write_state(state):
    payload = json.dumps(state, sort_keys=True).encode()
    minio_client().put_object(os.environ["MINIO_EVENT_BUCKET"], REGISTRY,
                             io.BytesIO(payload), len(payload), content_type="application/json")


def http_json(path, method="GET"):
    request = Request(f"http://flink-jobmanager:8081{path}", method=method)
    with urlopen(request, timeout=15) as response:
        return json.load(response)


@contextmanager
def docker_client():
    import docker
    client = docker.from_env(timeout=30)
    try:
        yield client
    finally:
        client.close()


def service(client, name):
    matches = client.containers.list(all=True, filters={"label": [
        f"com.docker.compose.project={os.environ['EWI_PROJECT']}",
        f"com.docker.compose.service={name}", "com.docker.compose.oneoff=False",
    ]})
    if len(matches) != 1:
        raise RuntimeError(f"Expected one Compose {name} service; run scripts/start-pipeline.sh first")
    return matches[0]


def run_container(image, command, entrypoint=None, output=False):
    """Stream logs, propagate failure, remove the task container even on Airflow timeout."""
    from docker.types import Mount
    mounts = [Mount(target="/app/output", source=os.environ["EWI_OUTPUT_VOLUME"], type="volume")] if output else []
    with docker_client() as client:
        container = client.containers.run(
            image, command=command, entrypoint=entrypoint, detach=True,
            network=os.environ["EWI_NETWORK"], environment=env(), mounts=mounts,
            labels={"ewi.managed": "airflow-task", "ewi.project": os.environ["EWI_PROJECT"]},
        )
        tail = deque(maxlen=40)
        try:
            for chunk in container.logs(stream=True, follow=True):
                line = chunk.decode("utf-8", errors="replace").rstrip()
                LOG.info("%s", line)
                tail.append(line)
            status = container.wait(timeout=30)["StatusCode"]
            if status:
                raise RuntimeError(f"Pipeline container exited {status}; inspect task logs")
            return "\n".join(tail)
        finally:
            container.remove(force=True)


def generator(command, entrypoint=None):
    return run_container(os.environ["EWI_GENERATOR_IMAGE"], command, entrypoint, output=True)


def initialize():
    run_container(os.environ["EWI_GENERATOR_IMAGE"], ["scripts/init_services.py"], entrypoint="python")


def generate(params, **_):
    if params["config"] not in {"smoke", "pilot"}:
        raise ValueError("config must be smoke or pilot")
    result = generator(["scripts/generate_pipeline_run.py", "--config", params["config"]], entrypoint="python")
    data = json.loads(result.splitlines()[-1])
    valid_run(data["run_id"])
    return data


def validate(ti, **_):
    generator(["validate", "--run-dir", f"/app/output/{receipt(ti)['run_id']}"])


def archive_raw(ti, **_):
    generator(["archive-minio", "--run-dir", f"/app/output/{receipt(ti)['run_id']}"])


@contextmanager
def kafka_consumer():
    from kafka import KafkaConsumer
    consumer = KafkaConsumer(bootstrap_servers=os.environ["KAFKA_BOOTSTRAP_SERVERS"],
                             enable_auto_commit=False, request_timeout_ms=30000,
                             api_version_auto_timeout_ms=10000)
    try:
        yield consumer
    finally:
        consumer.close()


def ends(consumer, topics):
    from kafka import TopicPartition
    partitions = []
    for topic in topics:
        ids = consumer.partitions_for_topic(topic)
        if ids is None:
            raise RuntimeError(f"Missing Kafka topic {topic}; initialize must run first")
        partitions.extend(TopicPartition(topic, p) for p in ids)
    return consumer.end_offsets(partitions)


def claim_stream(ti, **_):
    """Existing Flink outputs omit run_id, so one Kafka namespace holds one dataset."""
    rid = receipt(ti)["run_id"]
    state = read_state()
    if state:
        if state["run_id"] != rid:
            raise RuntimeError("Streaming namespace belongs to another run. Use a separate COMPOSE_PROJECT_NAME/stack.")
        if state["status"] not in {"prepared", "published"}:
            raise RuntimeError("Publication was interrupted. Inspect stream state; automatic replay could duplicate alerts.")
        return state
    # Refuse to silently adopt manual or previously processed topics.
    topics = list(RAW_TOPICS) + [j[2] for j in JOBS[1:]] + [
        "water.district-alerts.v1", "water.dead-letter.district.v1"]
    with kafka_consumer() as consumer:
        if any(ends(consumer, topics).values()):
            raise RuntimeError("Unmanaged Kafka data exists. Use a fresh COMPOSE_PROJECT_NAME for Airflow.")
    state = {"run_id": rid, "status": "prepared"}
    write_state(state)
    return state


def ensure_archiver():
    with docker_client() as client:
        container = service(client, "archiver")
        if container.status != "running":
            container.start()
        container.reload()
        if container.status != "running":
            raise RuntimeError("Kafka archiver failed to start; inspect Compose logs")


def ensure_flink_jobs():
    """Reattach existing jobs by stable pipeline name; never restart lost state silently."""
    result = {}
    stream_state = read_state()
    managed_names = {f"ewi-{key}" for key, _, _ in JOBS}
    for job in http_json("/jobs/overview")["jobs"]:
        if job["state"] in {"RUNNING", "CREATED", "RESTARTING", "INITIALIZING"} and job["name"] not in managed_names:
            raise RuntimeError("Unmanaged Flink jobs are running; use a separate stack for manual experiments")
    for key, filename, _ in JOBS:
        name = f"ewi-{key}"
        matching = [j for j in http_json("/jobs/overview")["jobs"] if j["name"] == name]
        active = [j for j in matching if j["state"] in {"RUNNING", "CREATED", "RESTARTING", "INITIALIZING"}]
        if len(active) > 1:
            raise RuntimeError(f"Duplicate managed Flink jobs: {name}")
        if active:
            result[key] = active[0]["jid"]
            continue
        if matching or (stream_state and stream_state["status"] == "published"):
            raise RuntimeError(f"{name} lost its running state. Restore its savepoint before retrying.")
        with docker_client() as client:
            manager = service(client, "flink-jobmanager")
            status, output = manager.exec_run([
                "/opt/flink/bin/flink", "run", "-d", "-p", "1", f"-Dpipeline.name={name}",
                "-py", f"/opt/flink/jobs/{filename}", "--group-id", f"ewi-{key}",
            ])
            LOG.info("%s", output.decode(errors="replace"))
            if status:
                raise RuntimeError(f"Flink submission failed for {name}")
        ids = re.findall(r"Job has been submitted with JobID ([0-9a-f]{32})", output.decode())
        if len(ids) != 1:
            raise RuntimeError(f"Could not determine job ID for {name}; inspect Flink before retrying")
        result[key] = ids[0]
    return result


def flink_running(ti=None, **_):
    ids = ti.xcom_pull(task_ids="ensure_flink_jobs") if ti else None
    if ids is None:
        jobs = http_json("/jobs/overview")["jobs"]
        ids = {key: next((j["jid"] for j in jobs if j["name"] == f"ewi-{key}"), None)
               for key, _, _ in JOBS}
    ready = True
    for key, jid in ids.items():
        if not jid:
            raise RuntimeError(f"Missing Flink job {key}")
        state = http_json(f"/jobs/{jid}")["state"]
        if state in {"FAILED", "CANCELED", "FINISHED", "SUSPENDED"}:
            raise RuntimeError(f"Flink job {key} is {state}; inspect exceptions/savepoints")
        ready = ready and state == "RUNNING"
    return ready


def publish(ti, **_):
    state = read_state()
    rid = receipt(ti)["run_id"]
    if not state or state["run_id"] != rid:
        raise RuntimeError("Stream ownership changed")
    if state["status"] == "published":
        LOG.info("Already published %s; reusing saved offsets", rid)
        return state["offsets"]
    if state["status"] != "prepared":
        raise RuntimeError("Ambiguous previous publication; inspect stream registry")
    state["status"] = "publishing"
    write_state(state)
    generator(["publish-kafka", "--run-dir", f"/app/output/{rid}", "--input-set", "delivery", "--speed", "0"])
    with kafka_consumer() as consumer:
        state["offsets"] = {f"{tp.topic}:{tp.partition}": offset for tp, offset in ends(consumer, RAW_TOPICS).items()}
    state["status"] = "published"
    write_state(state)
    return state["offsets"]


def groups_drained():
    """Checkpoint-committed offsets, rather than quiet logs, prove archive/stream progress."""
    from kafka import KafkaConsumer
    groups = [("water-event-archiver", RAW_TOPICS)] + [(f"ewi-{key}", (topic,)) for key, _, topic in JOBS]
    for group, topics in groups:
        consumer = KafkaConsumer(bootstrap_servers=os.environ["KAFKA_BOOTSTRAP_SERVERS"],
                                 group_id=group, enable_auto_commit=False)
        try:
            for tp, target in ends(consumer, topics).items():
                committed = consumer.committed(tp)
                if target > 0 and (committed is None or committed < target):
                    LOG.info("%s: %s committed=%s target=%s", group, tp, committed, target)
                    return False
        finally:
            consumer.close()
    return True


def pipeline_drained(ti, **_):
    if not flink_running(ti):
        return False
    with docker_client() as client:
        if service(client, "archiver").status != "running":
            raise RuntimeError("Archiver stopped before consuming all events")
    for jid in ti.xcom_pull(task_ids="ensure_flink_jobs").values():
        if http_json(f"/jobs/{jid}/checkpoints")["counts"]["completed"] < 1:
            return False
    return groups_drained()


def spark_batch(ti, params, **_):
    # Add approved entrypoints here as Amr AbdEl-Monem's transformations land.
    jobs = {"handoff_smoke": "/opt/spark/work-dir/jobs/handoff_smoke.py"}
    job = jobs[params["batch_job"]]
    run_container(os.environ["EWI_SPARK_IMAGE"], [job, "--run-id", receipt(ti)["run_id"]])


def trino_query(sql):
    """Consume the entire Trino protocol response, including errors on later pages."""
    request = Request("http://trino:8080/v1/statement", data=sql.encode(),
                      headers={"X-Trino-User": "airflow"}, method="POST")
    rows = []
    while True:
        with urlopen(request, timeout=30) as response:
            page = json.load(response)
        if "error" in page:
            raise RuntimeError(f"Trino query failed: {page['error'].get('message', page['error'])}")
        rows.extend(page.get("data", []))
        if not page.get("nextUri"):
            return rows
        request = Request(page["nextUri"], headers={"X-Trino-User": "airflow"})


def verify_trino(ti, **_):
    data = receipt(ti)
    rid = data["run_id"]
    expected = data["manifest"]
    rows = trino_query(f"SELECT district_records, meter_records FROM water.handoff.pipeline_runs WHERE run_id = '{rid}'")
    if rows != [[expected["delivery_district_records"], expected["delivery_meter_records"]]]:
        raise RuntimeError(f"Spark/Trino count mismatch for {rid}: {rows}")
    return {"run_id": rid, "trino_verified": True, "batch_job": "handoff_smoke"}


def monitor():
    state = read_state()
    if state is None:
        return "Pipeline has not started"
    if state["status"] == "prepared":
        return "Pipeline startup is in progress"
    if not flink_running():
        raise RuntimeError("A managed Flink job is restarting")
    with docker_client() as client:
        if service(client, "archiver").status != "running":
            raise RuntimeError("Archiver is not running")
    trino_query("SELECT 1")
    return "Streaming jobs, archiver, MinIO and Trino reachable"
