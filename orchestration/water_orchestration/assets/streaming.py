"""Streaming stage: publish a run to Kafka, prove the raw archive is complete, prove Flink processed it.

State that must survive Dagster restarts lives in MinIO, next to the data it describes:
  water-events/_orchestration/stream.json      which runs were published, with their offset ranges
  water-events/_manifests/<run>/archive-manifest.json   the archive-completion proof Spark reads
"""
# No `from __future__ import annotations` here: Dagster resolves op/asset annotations at runtime.
import gzip
import hashlib
import re
import time
from datetime import datetime, timezone

from dagster import AssetExecutionContext, Config, Failure, MaterializeResult, MetadataValue, asset

from .. import checks
from ..archive import coverage
from ..resources import ComposeResource, FlinkResource, KafkaResource, MinioResource
from ..screenshots import prompt
from ..settings import PATHS, setting
from .source import generator_run, generator_runs

RAW_TOPICS = {"district": "water.district-sensors.v1", "meter": "water.meter-readings.v1"}
CLEAN_TOPIC = "water.district-sensors.clean.v1"
DEAD_LETTER_TOPIC = "water.dead-letter.district.v1"
FEATURES_TOPIC = "water.district-features.15m.v1"
RELATIVE_TOPIC = "water.district-relative.15m.v1"
ALERTS_TOPIC = "water.district-alerts.v1"
ALL_TOPICS = [*RAW_TOPICS.values(), CLEAN_TOPIC, DEAD_LETTER_TOPIC, FEATURES_TOPIC, RELATIVE_TOPIC, ALERTS_TOPIC]

# The four pipeline jobs from flink/jobs, in dependency order: (key, script, input topic).
FLINK_JOBS = [
    ("validation", "job2_district_validation.py", RAW_TOPICS["district"]),
    ("features", "job3_district_features.py", CLEAN_TOPIC),
    ("alerts", "job3b_district_alerts.py", FEATURES_TOPIC),
    ("level_shift", "job3c_level_shift.py", RELATIVE_TOPIC),
]
ARCHIVER_GROUP = "water-event-archiver"
REGISTRY_KEY = "_orchestration/stream.json"


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _service_running(compose: ComposeResource, service: str) -> bool:
    return service in compose.call(["ps", "--status", "running", "--services"]).split()


def _object_lines(minio: MinioResource, bucket: str, key: str) -> list[bytes]:
    response = minio.client().get_object(bucket, key)
    try:
        return gzip.decompress(response.read()).splitlines()
    finally:
        response.close()
        response.release_conn()


# ---------------------------------------------------------------- Flink jobs

def _managed_jobs(flink: FlinkResource) -> dict[str, dict]:
    """Latest job per managed name (ewi-<key>)."""
    jobs: dict[str, dict] = {}
    for job in sorted(flink.jobs(), key=lambda j: j["start-time"]):
        if job["name"].startswith("ewi-"):
            jobs[job["name"]] = job
    return jobs


def ensure_flink_jobs(context, compose: ComposeResource, flink: FlinkResource, may_submit: bool) -> dict[str, str]:
    """Submit missing jobs (only before anything was published), never duplicates. Returns name -> job id."""
    existing = _managed_jobs(flink)
    ids = {}
    for key, script, _ in FLINK_JOBS:
        name = f"ewi-{key}"
        job = existing.get(name)
        if job and job["state"] in {"RUNNING", "CREATED", "INITIALIZING", "RESTARTING"}:
            ids[name] = job["jid"]
            continue
        if not may_submit:
            raise Failure(description=(
                f"Flink job {name} is not running (last state: {job['state'] if job else 'never submitted'}) "
                "but data was already published. Restarting it from scratch would re-emit duplicates. "
                "Restore it from its savepoint (docs/runbook.md) before continuing."))
        output = compose.exec("flink-jobmanager", [
            "flink", "run", "-d", "-p", "1", f"-Dpipeline.name={name}",
            "-py", f"/opt/flink/jobs/{script}", "--group-id", name], log=context.log.info)
        found = re.findall(r"[0-9a-f]{32}", output)
        if not found:
            raise Failure(description=f"could not read the job id of {name} from the Flink CLI output")
        ids[name] = found[-1]
        context.log.info(f"submitted {name} as {ids[name]}")

    deadline = time.monotonic() + 300
    while True:
        states = {name: flink.vertex_states(jid) for name, jid in ids.items()}
        if all(s and all(v == "RUNNING" for v in s) for s in states.values()):
            return ids
        if time.monotonic() > deadline:
            raise Failure(description=f"Flink jobs not RUNNING after 5 minutes: {states}")
        time.sleep(5)


# ---------------------------------------------------------------- publication registry

def _registry(minio: MinioResource) -> dict:
    return minio.read_json(setting("MINIO_EVENT_BUCKET"), REGISTRY_KEY) or {"streaming_owner": None, "runs": {}}


def _save_registry(minio: MinioResource, registry: dict) -> None:
    minio.write_json(setting("MINIO_EVENT_BUCKET"), REGISTRY_KEY, registry)


def _topic_ends(kafka: KafkaResource) -> dict[str, dict[str, int]]:
    return {stream: {str(p): end for p, (_, end) in kafka.offsets(topic).items()} for stream, topic in RAW_TOPICS.items()}


class PublishConfig(Config):
    speed: float = 0.0  # simulated seconds per wall-clock second; 0 publishes without pauses
    # Continue an interrupted publication after proving Kafka holds an exact prefix of the replay order.
    resume_interrupted: bool = False


def _replay_module():
    """The generator's own replay ordering (Generator/.../replay.py, standard library only), loaded unchanged."""
    import importlib.util
    path = PATHS.repo / "Generator" / "src" / "egypt_water_intelligence" / "generator" / "replay.py"
    spec = importlib.util.spec_from_file_location("ewi_generator_replay", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _last_key(kafka: KafkaResource, topic: str, end: int) -> str | None:
    from kafka import KafkaConsumer, TopicPartition
    if end == 0:
        return None
    consumer = KafkaConsumer(bootstrap_servers=kafka.bootstrap, group_id=None, enable_auto_commit=False,
                             consumer_timeout_ms=10000)
    try:
        tp = TopicPartition(topic, 0)
        consumer.assign([tp])
        consumer.seek(tp, end - 1)
        message = next(iter(consumer))
        return message.key.decode("utf-8")
    finally:
        consumer.close()


def resume_publication(context, kafka: KafkaResource, run_id: str, entry: dict) -> int:
    """Send the records an interrupted publication did not deliver, without duplicating any.

    The publisher sends one record at a time in a deterministic order and waits for each acknowledgement,
    so Kafka must hold an exact prefix of that order. This proves it (counts per topic and the identity of
    the last record in each topic), then sends the rest with an ordered, patient producer.
    """
    from kafka import KafkaProducer
    replay = _replay_module()
    done = {stream: sum(end - entry["start_offsets"][stream].get(p, 0)
                        for p, (_, end) in kafka.offsets(topic).items())
            for stream, topic in RAW_TOPICS.items()}
    stream_of = {topic: stream for stream, topic in RAW_TOPICS.items()}
    events = replay.iter_events(PATHS.run_dir(run_id), input_set="delivery")
    seen = {"district": 0, "meter": 0}
    last_seen: dict[str, str] = {}
    for _ in range(sum(done.values())):
        topic, record, _ = next(events)
        stream = stream_of[topic]
        seen[stream] += 1
        last_seen[stream] = record["event_id"]
    for stream, topic in RAW_TOPICS.items():
        end = sum(e for _, e in kafka.offsets(topic).values())
        if seen[stream] != done[stream] or last_seen.get(stream) != _last_key(kafka, topic, end):
            raise Failure(description=(
                f"Kafka does not hold an exact prefix of the replay order for {stream} "
                f"(prefix count {seen[stream]} vs Kafka {done[stream]}); refusing to resume."))
    context.log.info(f"verified prefix: {done}; resuming after record {sum(done.values()):,}")

    producer = KafkaProducer(bootstrap_servers=kafka.bootstrap, client_id="ewi-resume", acks="all",
                             retries=1_000_000, retry_backoff_ms=500, max_in_flight_requests_per_connection=1,
                             linger_ms=20, request_timeout_ms=60000)
    sent = 0
    try:
        for topic, record, payload in events:
            producer.send(topic, key=record["event_id"].encode("utf-8"), value=payload)
            sent += 1
            if sent % 100_000 == 0:
                producer.flush()
                context.log.info(f"resumed {sent:,} records")
        producer.flush()
    finally:
        producer.close()
    return sent


def _finish_publication(minio, registry, run_id, entry, manifest, kafka, published):
    """Mark a run published only if Kafka gained exactly the manifest's delivery counts."""
    end_offsets = _topic_ends(kafka)
    added = {stream: sum(end_offsets[stream][p] - entry["start_offsets"][stream].get(p, 0) for p in end_offsets[stream])
             for stream in RAW_TOPICS}
    for stream, count in added.items():
        if count != manifest[f"delivery_{stream}_records"]:
            raise Failure(description=f"{stream}: {count} records added to Kafka, manifest says "
                                      f"{manifest[f'delivery_{stream}_records']} (run stays 'publishing')")
    entry.update(state="published", published_at=_now(), end_offsets=end_offsets,
                 published_records=published if published is not None else sum(added.values()))
    registry["runs"][run_id] = entry
    _save_registry(minio, registry)


@asset(
    group_name="streaming",
    partitions_def=generator_runs,
    deps=[generator_run],
    description=(
        "The run's delivery events in the two raw Kafka topics. Before publishing: topics exist, the archiver runs, "
        "and the four Flink jobs run. A run is published at most once (registry in water-events/_orchestration)."
    ),
)
def kafka_delivery_streams(context: AssetExecutionContext, config: PublishConfig, compose: ComposeResource,
                           kafka: KafkaResource, flink: FlinkResource, minio: MinioResource) -> MaterializeResult:
    run_id = checks.require_run_id(context.partition_key)
    manifest = checks.read_manifest(PATHS.run_dir(run_id))
    registry = _registry(minio)
    entry = registry["runs"].get(run_id)

    created = kafka.ensure_topics(ALL_TOPICS)
    if created:
        context.log.info(f"created missing topics: {created}")
    if not _service_running(compose, "archiver"):
        raise Failure(description="The archiver service is not running: `docker compose up -d archiver`")

    if entry and entry["state"] == "published":
        context.log.info(f"{run_id} was already published at {entry['published_at']}; not publishing again.")
        # Once Flink's outputs for this run are verified and frozen, its jobs are no longer needed for it
        # (they may have been stopped with savepoints, or the cluster restarted).
        if registry["streaming_owner"] == run_id and "flink_output_end_offsets" not in entry:
            ensure_flink_jobs(context, compose, flink, may_submit=False)
    elif entry and entry["state"] == "publishing":
        if not config.resume_interrupted:
            raise Failure(description=(
                f"A previous publication of {run_id} was interrupted. Some records are already in Kafka; publishing "
                "again would duplicate them. Re-run with config resume_interrupted: true to verify the delivered "
                "prefix and send only the rest."))
        if registry["streaming_owner"] == run_id:
            ensure_flink_jobs(context, compose, flink, may_submit=False)
        resumed = resume_publication(context, kafka, run_id, entry)
        entry.update(resumed_at=_now(), resumed_records=resumed)
        _finish_publication(minio, registry, run_id, entry, manifest, kafka, published=None)
    else:
        if not registry["runs"]:
            unregistered = {t: kafka.count(t) for t in ALL_TOPICS if kafka.count(t)}
            if unregistered:
                raise Failure(description=f"Topics already hold records not published by this pipeline: {unregistered}")
            registry["streaming_owner"] = run_id
        owner = registry["streaming_owner"] == run_id
        ensure_flink_jobs(context, compose, flink, may_submit=owner and not registry["runs"])
        if not owner:
            context.log.warning(
                f"Kafka streaming outputs belong to {registry['streaming_owner']}. Flink rows carry no run_id, so "
                f"{run_id} is published for archiving and batch processing only.")

        entry = {"state": "publishing", "started_at": _now(), "start_offsets": _topic_ends(kafka), "speed": config.speed}
        registry["runs"][run_id] = entry
        _save_registry(minio, registry)

        output = compose.run("generator", [
            "publish-kafka", "--run-dir", f"/app/output/{run_id}", "--input-set", "delivery",
            "--bootstrap-servers", "kafka:9092", "--speed", str(config.speed)], log=context.log.info)
        _finish_publication(minio, registry, run_id, entry, manifest, kafka,
                            published=int(output.strip().splitlines()[-1]))

    return MaterializeResult(metadata={
        "streaming_owner": registry["streaming_owner"],
        "published_at": entry["published_at"],
        "published_records": entry["published_records"],
        "start_offsets": MetadataValue.json(entry["start_offsets"]),
        "end_offsets": MetadataValue.json(entry["end_offsets"]),
        **prompt(context, "http://localhost:8080/ui/clusters/water/all-topics",
                 "Kafka UI topic list with message counts of both raw topics"),
    })


# ---------------------------------------------------------------- raw archive

class ArchiveConfig(Config):
    stall_minutes: int = 15     # fail if the archiver's committed offset stops moving while records remain
    max_wait_minutes: int = 240
    verify_content: bool = True  # compare every archived line with the local delivery file


def _wait_for_group(context, kafka: KafkaResource, group: str, topic: str, targets: dict[str, int],
                    stall_minutes: int, max_wait_minutes: int, alive=lambda: True) -> None:
    """Progress-based wait: committed offsets must reach `targets`. Silence alone never means done."""
    started = last_progress = time.monotonic()
    best = -1
    while True:
        committed = kafka.committed(group, topic)
        remaining = sum(max(0, end - (committed.get(int(p)) or 0)) for p, end in targets.items())
        if remaining == 0:
            return
        done = sum(committed.get(int(p)) or 0 for p in targets)
        if done > best:
            best, last_progress = done, time.monotonic()
            context.log.info(f"{group} on {topic}: {remaining} records still to commit")
        if not alive():
            raise Failure(description=f"{group} has {remaining} records left on {topic} but its process is not running")
        if time.monotonic() - last_progress > stall_minutes * 60:
            raise Failure(description=f"{group} made no progress on {topic} for {stall_minutes} minutes ({remaining} left)")
        if time.monotonic() - started > max_wait_minutes * 60:
            raise Failure(description=f"{group} did not finish {topic} within {max_wait_minutes} minutes ({remaining} left)")
        time.sleep(15)


def _line_digest(lines) -> tuple[int, int]:
    """Order-independent multiset digest: (count, sum of per-line SHA-256 prefixes mod 2^64)."""
    count = total = 0
    for line in lines:
        line = line.rstrip(b"\r\n")
        if line:
            count += 1
            total = (total + int.from_bytes(hashlib.sha256(line).digest()[:8], "big")) % (1 << 64)
    return count, total


@asset(
    group_name="streaming",
    partitions_def=generator_runs,
    deps=[kafka_delivery_streams],
    description=(
        "Raw events of the run archived from Kafka to water-events by the archiver service. Complete when the archiver "
        "committed past the published end offsets and archive objects tile the published offset range."
    ),
)
def raw_event_archive(context: AssetExecutionContext, config: ArchiveConfig, compose: ComposeResource,
                      kafka: KafkaResource, minio: MinioResource) -> MaterializeResult:
    run_id = checks.require_run_id(context.partition_key)
    bucket = setting("MINIO_EVENT_BUCKET")
    manifest_key = f"_manifests/{run_id}/archive-manifest.json"
    entry = _registry(minio)["runs"].get(run_id)
    if not entry or entry["state"] != "published":
        raise Failure(description=f"{run_id} has no completed publication; materialize kafka_delivery_streams first")

    existing = minio.read_json(bucket, manifest_key)
    if existing and existing.get("status") == "complete":
        # Immutable once written: the batch source fingerprint includes this object.
        context.log.info(f"{manifest_key} already exists; re-checking the listed objects without rewriting it.")
        missing = [k for t in existing["topics"].values() for k in t["objects"] if minio.size(bucket, k) is None]
        if missing:
            raise Failure(description=f"archive manifest lists objects that no longer exist: {missing[:5]}")
        archive = existing
    else:
        topics = {}
        for stream, topic in RAW_TOPICS.items():
            targets = entry["end_offsets"][stream]
            _wait_for_group(context, kafka, ARCHIVER_GROUP, topic, targets, config.stall_minutes,
                            config.max_wait_minutes, alive=lambda: _service_running(compose, "archiver"))
            ranges = {int(p): (entry["start_offsets"][stream].get(p, 0), end) for p, end in targets.items()}
            result = coverage(minio.list_keys(bucket, f"events/{topic}/{run_id}/"), ranges)
            if not result["complete"]:
                raise Failure(description=f"{topic}: archive does not cover the published offsets",
                              metadata={"problems": "\n".join(result["problems"])})
            expected = checks.read_manifest(PATHS.run_dir(run_id))[f"delivery_{stream}_records"]
            if result["records"] != expected:
                raise Failure(description=f"{topic}: archive holds {result['records']} records, manifest says {expected}")
            topics[stream] = {"topic": topic, "offset_ranges": {str(p): list(r) for p, r in ranges.items()}, **result}

        if config.verify_content:
            for stream, info in topics.items():
                archived = _line_digest(line for key in info["objects"] for line in _object_lines(minio, bucket, key))
                local_path = PATHS.run_dir(run_id) / "delivery" / f"{'district-sensors' if stream == 'district' else 'meter-readings'}.ndjson"
                with local_path.open("rb") as handle:
                    local = _line_digest(handle)
                if archived != local:
                    raise Failure(description=f"{stream}: archived lines differ from the delivery file "
                                              f"(archive {archived[0]} lines, file {local[0]} lines)")
                info["content_matches_delivery_file"] = True
                context.log.info(f"{stream}: {archived[0]} archived lines are identical to the delivery file (multiset)")

        archive = {"run_id": run_id, "status": "complete", "created_at": _now(), "archiver_group": ARCHIVER_GROUP,
                   "topics": topics}
        minio.write_json(bucket, manifest_key, archive)
        context.log.info(f"wrote {bucket}/{manifest_key}")

    return MaterializeResult(metadata={
        "archive_manifest": f"s3://{bucket}/{manifest_key}",
        **{f"{s}_records": t["records"] for s, t in archive["topics"].items()},
        **{f"{s}_objects": len(t["objects"]) for s, t in archive["topics"].items()},
        "overlapping_objects_ignored": sum(len(t["overlapping_objects_ignored"]) for t in archive["topics"].values()),
        **prompt(context, f"http://localhost:9001/browser/{bucket}/events%2F",
                 "MinIO water-events/events/<topic>/<run>/ archive objects, and _manifests/<run>/archive-manifest.json"),
    })


# ---------------------------------------------------------------- Flink outputs

class FlinkOutputsConfig(Config):
    stall_minutes: int = 15
    max_wait_minutes: int = 240


@asset(
    group_name="streaming",
    partitions_def=generator_runs,
    deps=[kafka_delivery_streams],
    description=(
        "Flink-derived Kafka topics (clean, dead-letter, 15-min features, relative values, alerts) after the four jobs "
        "consumed their inputs to the end. Only for the run that owns the streaming namespace."
    ),
)
def flink_district_outputs(context: AssetExecutionContext, config: FlinkOutputsConfig, compose: ComposeResource,
                           kafka: KafkaResource, flink: FlinkResource, minio: MinioResource) -> MaterializeResult:
    run_id = checks.require_run_id(context.partition_key)
    registry = _registry(minio)
    if registry["streaming_owner"] != run_id:
        raise Failure(description=(
            f"Streaming outputs belong to {registry['streaming_owner']}; Flink rows have no run_id, so they cannot be "
            f"attributed to {run_id}."))
    manifest = checks.read_manifest(PATHS.run_dir(run_id))
    output_topics = [CLEAN_TOPIC, DEAD_LETTER_TOPIC, FEATURES_TOPIC, RELATIVE_TOPIC, ALERTS_TOPIC]
    entry = registry["runs"][run_id]
    checkpoints = {}
    if "flink_output_end_offsets" in entry:
        # Already verified and frozen; the jobs may since have been stopped with savepoints.
        context.log.info(f"Flink outputs of {run_id} were verified at {entry['flink_outputs_verified_at']}; using the frozen offsets.")
    else:
        ids = ensure_flink_jobs(context, compose, flink, may_submit=False)
        # Each job must have committed (on checkpoint) past the end of its input topic, in pipeline order.
        for key, _, input_topic in FLINK_JOBS:
            name = f"ewi-{key}"
            targets = {str(p): end for p, (_, end) in kafka.offsets(input_topic).items()}
            _wait_for_group(context, kafka, name, input_topic, targets, config.stall_minutes, config.max_wait_minutes,
                            alive=lambda jid=ids[name]: all(s == "RUNNING" for s in flink.vertex_states(jid)))
        # Freeze where this run's outputs end. Flink rows carry no run_id, so anything a later publication adds
        # after these offsets must not be attributed to this run (load_streaming.py reads only up to them).
        entry["flink_output_end_offsets"] = {t: {str(p): end for p, (_, end) in kafka.offsets(t).items()} for t in output_topics}
        entry["flink_output_counts"] = {t: kafka.count(t) for t in output_topics}
        entry["flink_outputs_verified_at"] = _now()
        checkpoints = {name: flink.completed_checkpoints(jid) for name, jid in ids.items()}
        entry["flink_completed_checkpoints"] = checkpoints
        _save_registry(minio, registry)

    counts = entry["flink_output_counts"]
    checkpoints = checkpoints or entry.get("flink_completed_checkpoints", {})
    expected_clean = manifest["expected_district_records"]  # every logical district event, once
    windows = manifest["logical_interval_count"] // 15
    districts = expected_clean // manifest["logical_interval_count"]
    expected_features = districts * (windows - 1)  # the last 15-minute window never closes in a finite replay
    if counts[CLEAN_TOPIC] != expected_clean:
        raise Failure(description=f"clean topic has {counts[CLEAN_TOPIC]} rows, expected {expected_clean} "
                                  "(one per logical district event after deduplication)")
    return MaterializeResult(metadata={
        **{topic: count for topic, count in counts.items()},
        "frozen_output_offsets": MetadataValue.json(entry["flink_output_end_offsets"]),
        "expected_clean": expected_clean,
        "expected_features_finite_replay": expected_features,
        "features_match_expectation": counts[FEATURES_TOPIC] == expected_features,
        "completed_checkpoints": MetadataValue.json(checkpoints),
        **prompt(context, "http://localhost:8081/#/job/running",
                 "Flink running jobs: ewi-validation, ewi-features, ewi-alerts, ewi-level_shift (and a job's Checkpoints tab)"),
    })
