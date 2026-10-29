"""Live demo: replay a small run through the same four Flink jobs in a separate, resettable `demo.` namespace.

The main run's topics, archive, registry and tables are never touched. Only demo.* topics are deleted and
recreated, so the demo can be repeated as often as needed. The Grafana board (stream = demo) shows the
detections arriving while the replay runs.
"""
# No `from __future__ import annotations` here: Dagster resolves op annotations at runtime.
import re
import time

from dagster import Config, Failure, OpExecutionContext, job, op

from . import checks
from .assets.source import config_run_id
from .assets.streaming import ALL_TOPICS, FLINK_JOBS, RAW_TOPICS
from .resources import ComposeResource, FlinkResource, KafkaResource
from .settings import PATHS

PREFIX = "demo."
SAVEPOINTS = "file:///opt/flink/state/savepoints"
# Topic arguments of each Flink job script (flink/jobs/*.py), so the demo jobs read and write demo.* topics.
JOB_TOPIC_ARGS = {
    "validation": {"--input-topic": "water.district-sensors.v1", "--clean-topic": "water.district-sensors.clean.v1",
                   "--dead-letter-topic": "water.dead-letter.district.v1"},
    "features": {"--input-topic": "water.district-sensors.clean.v1", "--features-topic": "water.district-features.15m.v1"},
    "alerts": {"--features-topic": "water.district-features.15m.v1", "--alerts-topic": "water.district-alerts.v1",
               "--relative-topic": "water.district-relative.15m.v1"},
    "level_shift": {"--relative-topic": "water.district-relative.15m.v1", "--alerts-topic": "water.district-alerts.v1"},
}


class DemoConfig(Config):
    config_file: str = "demo-live.yml"  # in configs/
    speed: float = 480.0                # simulated seconds per wall-clock second: 2 days in about 6 minutes
    stop_main_jobs: bool = False        # stop the main run's Flink jobs with savepoints to free task slots


def _flink_request(flink: FlinkResource, method: str, path: str, **kwargs):
    import requests
    response = requests.request(method, flink.url + path, timeout=30, **kwargs)
    response.raise_for_status()
    return response.json() if response.content else {}


@op
def prepare_demo(context: OpExecutionContext, config: DemoConfig, compose: ComposeResource,
                 kafka: KafkaResource, flink: FlinkResource) -> str:
    run_id = config_run_id(compose, config.config_file)
    if not (PATHS.run_dir(run_id) / "_RUN_COMPLETE").is_file():
        compose.run("generator", ["generate", "--config", f"/app/pipeline-configs/{config.config_file}"],
                    log=context.log.info)
    checks.read_manifest(PATHS.run_dir(run_id))

    jobs = [j for j in flink.jobs() if j["state"] == "RUNNING"]
    for j in jobs:
        if j["name"].startswith("ewi-demo-"):
            _flink_request(flink, "PATCH", f"/jobs/{j['jid']}?mode=cancel")
            context.log.info(f"cancelled previous demo job {j['name']}")
        elif j["name"].startswith("ewi-") and config.stop_main_jobs:
            trigger = _flink_request(flink, "POST", f"/jobs/{j['jid']}/stop",
                                     json={"targetDirectory": SAVEPOINTS, "drain": False})
            context.log.info(f"stopping {j['name']} with a savepoint (trigger {trigger.get('request-id')})")

    deadline = time.monotonic() + 180
    while _flink_request(flink, "GET", "/overview")["slots-available"] < len(FLINK_JOBS):
        if time.monotonic() > deadline:
            raise Failure(description=(
                f"Flink needs {len(FLINK_JOBS)} free task slots for the demo jobs. Stop the main run's jobs "
                "(run with stop_main_jobs: true; they are stopped with savepoints) or add task slots."))
        time.sleep(3)
    for j in flink.jobs():
        if j["name"].startswith("ewi-") and not j["name"].startswith("ewi-demo-") and j["state"] == "FINISHED":
            context.log.info(f"{j['name']} stopped; savepoints are listed under /opt/flink/state/savepoints")

    # Reset ONLY the demo namespace.
    demo_topics = [PREFIX + t for t in ALL_TOPICS]
    from kafka.admin import KafkaAdminClient
    admin = KafkaAdminClient(bootstrap_servers=kafka.bootstrap, client_id="ewi-demo")
    try:
        existing = [t for t in demo_topics if t in set(admin.list_topics())]
        if existing:
            admin.delete_topics(existing)
            while set(existing) & set(admin.list_topics()):
                time.sleep(1)
    finally:
        admin.close()
    time.sleep(3)
    kafka.ensure_topics(demo_topics)
    compose.exec("catalog-db", ["psql", "-U", "iceberg", "-d", "iceberg", "-c",
                                "DELETE FROM live.relative WHERE ns='demo'; DELETE FROM live.features WHERE ns='demo'; "
                                "DELETE FROM live.alerts WHERE ns='demo'; DELETE FROM live.throughput WHERE ns='demo';"])
    context.log.info(f"demo namespace reset: {len(demo_topics)} topics recreated, live demo rows cleared")

    ids = {}
    for key, script, _ in FLINK_JOBS:
        name = f"ewi-demo-{key}"
        args = [a for flag, topic in JOB_TOPIC_ARGS[key].items() for a in (flag, PREFIX + topic)]
        output = compose.exec("flink-jobmanager", [
            "flink", "run", "-d", "-p", "1", f"-Dpipeline.name={name}", "-py", f"/opt/flink/jobs/{script}",
            *args, "--group-id", name, "--startup-mode", "earliest-offset"], log=context.log.info)
        ids[name] = re.findall(r"[0-9a-f]{32}", output)[-1]
    deadline = time.monotonic() + 300
    while not all(all(v == "RUNNING" for v in flink.vertex_states(jid)) for jid in ids.values()):
        if time.monotonic() > deadline:
            raise Failure(description="demo Flink jobs did not reach RUNNING within 5 minutes")
        time.sleep(5)
    context.log.info(f"demo Flink jobs running: {sorted(ids)}")
    return run_id


@op
def replay_demo(context: OpExecutionContext, config: DemoConfig, compose: ComposeResource, run_id: str) -> None:
    start, end = "2026-02-01T00:00:00.000Z", "2026-02-03T00:00:00.000Z"
    url = f"http://localhost:3001/d/water-live-ops?orgId=1&var-ns=demo&from={start}&to={end}&refresh=5s"
    context.log.info(f"[SCREENSHOT] Open {url} NOW and keep it on screen: detections appear while the replay runs")
    compose.run("generator", [
        "publish-kafka", "--run-dir", f"/app/output/{run_id}", "--input-set", "delivery",
        "--bootstrap-servers", "kafka:9092", "--speed", str(config.speed),
        "--district-topic", PREFIX + RAW_TOPICS["district"], "--meter-topic", PREFIX + RAW_TOPICS["meter"]],
        log=context.log.info)
    context.log.info("replay finished; the last 15-minute window stays open until more data arrives (finite replay)")


@job(description=(
    "Live demo: reset only the demo.* namespace, start the four Flink jobs on it, and replay a small run at "
    "accelerated speed while Grafana (http://localhost:3001, stream = demo) shows detections arriving."))
def live_demo():
    replay_demo(prepare_demo())
