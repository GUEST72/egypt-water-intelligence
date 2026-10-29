"""Dagster definitions for the Egypt Water Intelligence pipeline."""
from dagster import AssetSelection, Definitions, define_asset_job, load_assets_from_modules, multiprocess_executor

from .assets import batch, lakehouse, source, streaming
from .demo import live_demo
from .resources import ComposeResource, FlinkResource, KafkaResource, MinioResource, TrinoResource
from .sensors import archive_complete_sensor, generator_run_sensor

all_assets = load_assets_from_modules([source, streaming, batch, lakehouse])


def _job(name: str, selection, description: str):
    return define_asset_job(name, selection=selection, partitions_def=source.generator_runs, description=description)


ingest_source_job = _job("ingest_source", AssetSelection.groups("source"),
                         "Validate a generator run and upload it to MinIO.")
streaming_job = _job("stream_run", AssetSelection.groups("streaming"),
                     "Publish a run to Kafka (once), then prove the raw archive and the Flink outputs are complete.")
batch_job = _job("batch_run", AssetSelection.groups("batch"),
                 "Build and validate the Silver/Gold Iceberg tables of one archived run.")
lakehouse_job = _job("streaming_lakehouse", AssetSelection.groups("lakehouse", "evaluation"),
                     "Load Flink outputs into water.streaming, then evaluate alerts against restricted truth.")
marts_job = _job("build_marts", AssetSelection.groups("marts", "dashboard"),
                 "Rebuild this run's dashboard marts and the Power BI Parquet exports.")

# Independent steps run in parallel, but at most one Spark container at a time (shared UI port and memory).
executor = multiprocess_executor.configured({
    "max_concurrent": 4,
    "tag_concurrency_limits": [{"key": "resource", "value": "spark", "limit": 1}],
})
full_pipeline_job = _job("full_pipeline", AssetSelection.all(),
                         "Every stage for one run. Steps already done are verified and skipped, never repeated.")

defs = Definitions(
    assets=all_assets,
    jobs=[source.generate_dataset_job, ingest_source_job, streaming_job, batch_job, lakehouse_job, marts_job,
          full_pipeline_job, live_demo],
    sensors=[generator_run_sensor, archive_complete_sensor],
    executor=executor,
    resources={
        "compose": ComposeResource(),
        "minio": MinioResource(),
        "kafka": KafkaResource(),
        "flink": FlinkResource(),
        "trino": TrinoResource(),
    },
)
