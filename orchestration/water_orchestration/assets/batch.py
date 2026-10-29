"""Batch stage: the Spark jobs in batch/jobs, run in the spark Compose service for one archived run."""
# No `from __future__ import annotations` here: Dagster resolves op/asset annotations at runtime.
import json

from dagster import AssetExecutionContext, Config, Failure, MaterializeResult, MetadataValue, asset

from .. import checks
from ..resources import ComposeResource
from ..screenshots import prompt
from .source import generator_runs, raw_run_upload
from .streaming import raw_event_archive

JOBS = "/opt/spark/work-dir/jobs"
# One Spark container at a time (the executor limits this tag): they share the Spark UI port and the memory budget.
SPARK = {"resource": "spark"}


def _spark(context, compose: ComposeResource, script: str, args: list[str]) -> dict:
    """Run one Spark job and return the JSON report it prints last."""
    prompt(context, "http://localhost:4040", f"live Spark UI while {script} runs (jobs and stages)")
    output = compose.run("spark", [f"{JOBS}/{script}", *args], log=context.log.info, flags=["--service-ports"])
    reports = [line for line in output.splitlines() if line.startswith("{")]
    if not reports:
        raise Failure(description=f"{script} printed no JSON report")
    return json.loads(reports[-1])


class BuildConfig(Config):
    reprocess: bool = False  # rebuild this run even if its source fingerprint was already completed


@asset(
    group_name="batch",
    op_tags=SPARK,
    partitions_def=generator_runs,
    deps=[raw_run_upload, raw_event_archive],
    description=(
        "Silver (water.analytics) and Gold (water.gold) Iceberg tables for one run, built by build_batch.py from the "
        "archived Kafka events. An unchanged, completed run is skipped; other runs are never touched."
    ),
)
def batch_silver_gold(context: AssetExecutionContext, config: BuildConfig, compose: ComposeResource) -> MaterializeResult:
    run_id = checks.require_run_id(context.partition_key)
    report = _spark(context, compose, "build_batch.py", ["--run-id", run_id, *(["--reprocess"] if config.reprocess else [])])
    if report.get("status") not in {"completed", "skipped"}:
        raise Failure(description=f"build_batch did not complete: {report}")
    metadata = {"status": report["status"], "namespaces": f"water.{report['namespace']} / water.{report['gold_namespace']}"}
    if report["status"] == "skipped":
        context.log.info(f"{run_id}: {report.get('reason')}")
    for row in report.get("counts", []):
        for field in ("raw_records", "accepted_records", "rejected_records", "duplicate_records"):
            metadata[f"{row['stream']}_{field}"] = row[field]
    if report.get("source_fingerprint"):
        metadata["source_fingerprint"] = report["source_fingerprint"]
    return MaterializeResult(metadata={
        **metadata,
        **prompt(context, "http://localhost:18080", "Spark History: the completed water batch application"),
    })


@asset(
    group_name="batch",
    op_tags=SPARK,
    partitions_def=generator_runs,
    deps=[batch_silver_gold],
    description="validate_batch.py: keys, accounting, references, water balance, star schema and Trino row counts.",
)
def batch_validation(context: AssetExecutionContext, compose: ComposeResource) -> MaterializeResult:
    run_id = checks.require_run_id(context.partition_key)
    report = _spark(context, compose, "validate_batch.py", ["--run-id", run_id])
    if report.get("status") != "passed":
        raise Failure(description=f"batch validation did not pass: {report}")
    tables = report["tables"]
    return MaterializeResult(metadata={
        "status": "passed",
        "tables": MetadataValue.json(tables),
        **{f"{name}_records": t["records"] for name, t in tables.items()},
        **prompt(context, "http://localhost:8082/ui/", "Trino UI query list showing the validator's count queries"),
    })
