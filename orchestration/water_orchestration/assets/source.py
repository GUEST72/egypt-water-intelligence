"""Source stage: an existing generator run is validated and uploaded to MinIO as an immutable batch."""
# No `from __future__ import annotations` here: Dagster resolves op/asset annotations at runtime.
import io
import json

from dagster import (
    AssetExecutionContext,
    AssetRecordsFilter,
    Config,
    DynamicPartitionsDefinition,
    Failure,
    MaterializeResult,
    MetadataValue,
    OpExecutionContext,
    asset,
    job,
    op,
)

from .. import checks
from ..resources import ComposeResource, MinioResource
from ..screenshots import prompt
from ..settings import PATHS, setting

generator_runs = DynamicPartitionsDefinition(name="generator_run")


class GenerateConfig(Config):
    config_file: str = "pipeline-large.yml"  # a file in the repository's configs/ folder


def config_run_id(compose: ComposeResource, config_file: str) -> str:
    """The generator's own config-derived run ID, computed inside the generator image."""
    path = f"/app/pipeline-configs/{config_file}"
    output = compose.run("generator", [
        "-c", "from egypt_water_intelligence.generator.config import load_config;"
              "from egypt_water_intelligence.generator.outputs import run_id;"
              f"print(run_id(load_config('{path}')))"], flags=["--entrypoint", "python"])
    return checks.require_run_id(output.strip().splitlines()[-1])


@op
def generate_dataset(context: OpExecutionContext, config: GenerateConfig, compose: ComposeResource) -> str:
    """Generate the configured dataset unless that run already exists, then register its partition."""
    container_config = f"/app/pipeline-configs/{config.config_file}"
    run_id = config_run_id(compose, config.config_file)
    if (PATHS.run_dir(run_id) / "_RUN_COMPLETE").is_file():
        context.log.info(f"{run_id} already exists; generation is deterministic, so it is reused.")
    else:
        compose.run("generator", ["generate", "--config", container_config], log=context.log.info)
    context.instance.add_dynamic_partitions(generator_runs.name, [run_id])
    context.log.info(f"Registered partition {run_id}. Materialize the pipeline for it in the Dagster UI.")
    return run_id


@job(description="Run the generator for a config in configs/ and register the run as a partition.")
def generate_dataset_job():
    generate_dataset()


class ValidateConfig(Config):
    force_validation: bool = False  # re-run the full schema validation even if the files are unchanged


@asset(
    group_name="source",
    partitions_def=generator_runs,
    description=(
        "A completed local generator run, validated by the generator and against its manifest. The full schema "
        "validation (minutes for millions of records) is skipped when the files are unchanged since it last passed."
    ),
)
def generator_run(context: AssetExecutionContext, config: ValidateConfig, compose: ComposeResource) -> MaterializeResult:
    run_id = checks.require_run_id(context.partition_key)
    run_dir = PATHS.run_dir(run_id)
    manifest = checks.read_manifest(run_dir)
    signature = checks.files_signature(run_dir)

    records = context.instance.fetch_materializations(
        AssetRecordsFilter(asset_key=context.asset_key, asset_partitions=[run_id]), limit=1).records
    previous_meta = records[0].asset_materialization.metadata if records and records[0].asset_materialization else {}
    unchanged = previous_meta.get("files_signature") is not None and previous_meta["files_signature"].value == signature
    if unchanged and not config.force_validation:
        context.log.info(f"{run_id}: files unchanged since the last passed validation; skipping the schema re-check.")
        report = {"district_records": previous_meta["public_district_records"].value,
                  "meter_records": previous_meta["public_meter_records"].value}
    else:
        output = compose.run("generator", ["validate", "--run-dir", f"/app/output/{run_id}"])
        report = json.loads(output[output.index("{"):])
        if report["status"] != "passed":
            raise Failure(description=f"generator validation failed: {report['errors'][:5]}")

    counts = checks.check_delivery_counts(run_dir, manifest)
    context.log.info(f"{run_id}: validation passed; delivery district={counts['district']} meter={counts['meter']}")
    return MaterializeResult(metadata={
        "run_dir": str(run_dir),
        "files_signature": signature,
        "schema_validation": "skipped (files unchanged)" if unchanged and not config.force_validation else "ran",
        "public_district_records": report["district_records"],
        "public_meter_records": report["meter_records"],
        "delivery_district_records": counts["district"],
        "delivery_meter_records": counts["meter"],
        "scenario_count": manifest.get("scenario_count"),
        "fault_count": manifest.get("fault_count"),
        "logical_interval_count": manifest["logical_interval_count"],
    })


@asset(
    group_name="source",
    partitions_def=generator_runs,
    deps=[generator_run],
    description=(
        "Reference artifacts (manifest, validation report, districts, meters, network) in water-raw, truth files in "
        "water-restricted. No event data: events reach MinIO only via Kafka and the archiver. "
        "_RUN_COMPLETE is written last; a completed run is never re-uploaded (it is immutable)."
    ),
)
def raw_run_upload(context: AssetExecutionContext, minio: MinioResource) -> MaterializeResult:
    run_id = checks.require_run_id(context.partition_key)
    run_dir = PATHS.run_dir(run_id)
    raw_bucket, restricted_bucket = setting("MINIO_RAW_BUCKET"), setting("MINIO_RESTRICTED_BUCKET")
    local_sizes = {key: (run_dir / source).stat().st_size for key, source in checks.LOCAL_SOURCES.items()}

    def remote_sizes() -> dict[str, int | None]:
        return {key: minio.size(restricted_bucket if key.startswith("restricted/") else raw_bucket, f"{run_id}/{key}")
                for key in local_sizes}

    marker = f"{run_id}/_RUN_COMPLETE"
    already_complete = minio.size(raw_bucket, marker) is not None
    if already_complete:
        # Re-uploading would change object timestamps and the batch source fingerprint.
        context.log.info(f"{marker} exists: verifying the completed upload without rewriting it.")
    else:
        # Uploaded here rather than with `water-generator archive-minio`, which also copies the delivery
        # event files into water-raw. Events must reach MinIO only through Kafka and the archiver.
        client = minio.client()
        for key, source in checks.LOCAL_SOURCES.items():
            bucket = restricted_bucket if key.startswith("restricted/") else raw_bucket
            client.fput_object(bucket, f"{run_id}/{key}", str(run_dir / source))
            context.log.info(f"uploaded {bucket}/{run_id}/{key}")

    problems = checks.compare_uploaded(local_sizes, remote_sizes())
    if problems:
        raise Failure(description="Uploaded run does not match the local files", metadata={"problems": "\n".join(problems)})

    if not already_complete:
        minio.client().put_object(raw_bucket, marker, io.BytesIO(b"complete\n"), length=9, content_type="text/plain")
        context.log.info(f"Wrote {raw_bucket}/{marker} after all {len(local_sizes)} objects were verified.")

    return MaterializeResult(metadata={
        "status": "verified existing upload" if already_complete else "uploaded",
        "raw_prefix": f"s3://{raw_bucket}/{run_id}/",
        "restricted_prefix": f"s3://{restricted_bucket}/{run_id}/",
        "objects_verified": len(local_sizes),
        "uploaded_bytes": MetadataValue.int(sum(local_sizes.values())),
        **prompt(context, f"http://localhost:9001/browser/{raw_bucket}/{run_id}%2F",
                 f"MinIO {raw_bucket}/{run_id}/ showing artifacts/ and _RUN_COMPLETE"),
    })
