"""Incremental intake: register every completed local generator run as a partition."""
from __future__ import annotations

from dagster import DefaultSensorStatus, RunRequest, SensorEvaluationContext, SensorResult, SkipReason, sensor

from . import checks
from .assets.source import generator_runs
from .resources import MinioResource
from .settings import PATHS, setting


@sensor(
    minimum_interval_seconds=60,
    default_status=DefaultSensorStatus.STOPPED,
    description="Adds a partition for each Generator/output/run-* folder that finished with a current manifest.",
)
def generator_run_sensor(context: SensorEvaluationContext):
    known = set(context.instance.get_dynamic_partitions(generator_runs.name))
    new = []
    for run_dir in sorted(PATHS.generator_output.glob("run-*")):
        if run_dir.name in known or not checks.RUN_ID_PATTERN.fullmatch(run_dir.name):
            continue
        try:
            checks.read_manifest(run_dir)
        except checks.CheckFailed as exc:  # unfinished or stale run: not eligible
            context.log.info(f"skipping {run_dir.name}: {exc}")
            continue
        new.append(run_dir.name)
    if not new:
        return SkipReason("no new completed generator runs")
    return SensorResult(dynamic_partitions_requests=[generator_runs.build_add_request(new)])


@sensor(
    job_name="batch_run",
    minimum_interval_seconds=120,
    default_status=DefaultSensorStatus.STOPPED,
    description=(
        "Incremental batch: requests the batch job for each run whose archive manifest exists (archive proven "
        "complete). One request per run (run_key); partial or unarchived runs are never eligible."
    ),
)
def archive_complete_sensor(context: SensorEvaluationContext, minio: MinioResource):
    bucket = setting("MINIO_EVENT_BUCKET")
    requests = []
    for run_id in context.instance.get_dynamic_partitions(generator_runs.name):
        archive = minio.read_json(bucket, f"_manifests/{run_id}/archive-manifest.json")
        if archive and archive.get("status") == "complete":
            requests.append(RunRequest(run_key=f"batch-{run_id}", partition_key=run_id))
    return requests or SkipReason("no run has a complete archive manifest yet")
