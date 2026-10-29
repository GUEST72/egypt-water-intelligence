# 8. What we changed, file by file

Starting point: the generator with its publisher and archiver (tracked in Git), plus the Flink teammate's jobs and
scripts, the batch teammate's Spark jobs, and leftovers from two earlier orchestration attempts (Airflow, and an
Iceberg loader), all handed over as loose files.

## Teammates' code we kept unchanged

| Area | Files |
|---|---|
| Generator | everything under `Generator/` except one new `.dockerignore` (the simulation, publisher, archiver and validator are untouched) |
| Flink jobs | `flink/jobs/*.py`, `flink/Dockerfile` |
| Flink scripts | `common.sh`, `start_all.sh`, `stop_all.sh` |
| Batch transformations | the cleaning, water-balance and star-schema logic inside `batch/jobs/build_batch.py` and `validate_batch.py` |

## Repository reorganisation

| Before | After | Why |
|---|---|---|
| `analysis/` at the root | `flink/analysis/` | these are the Flink teammate's study and evaluation scripts |
| `guide_for_run_kafka_and_flink.md` | `flink/README.md` | the Flink guide lives with the Flink code |
| `Flink-Kakfa-Ahmed-guide.md`, `Batch-processing-minio-Amr-guide.md` | `docs/guides/generator-guide-for-flink.md`, `generator-guide-for-batch.md` | all guides in one place |
| `water_data_injected_patterns_report.md` | `docs/reports/injected-patterns.md` | |
| `compose.yml`, `load.py`, `setup_iceberg.sh`, `run_all.sh`, `marts.sql`, `queries.sql` | `docs/reference/previous-iceberg-attempt/` | earlier Iceberg attempt, kept for reference; its root `compose.yml` also silently replaced `docker-compose.yml` when running `docker compose` |
| Airflow-era documents and `requirements-dev.txt` | `docs/reference/previous-airflow-attempt/` | describe code that is not in this repository |
| `docker-compose.override.yml` | merged into `docker-compose.yml` | one Compose file for the whole stack |

## Edited files

| File | Change |
|---|---|
| `docker-compose.yml` | Flink services merged in; Kafka host listener `localhost:29092`; archiver `restart: unless-stopped`; new services `catalog-db` (Postgres Iceberg catalog), `trino`, `spark`, `spark-history`, `live-bridge`, `grafana`; one shared generator image; `configs/` mounted into the generator; `AWS_REGION` for Spark |
| `batch/jobs/*`, `batch/Dockerfile` | see [5. Batch path](05-batch.md#every-edit-we-made-to-the-batch-code) |
| `flink/scripts/evaluate.sh`, `flink/analysis/*.py` | paths updated after moving `analysis/` |
| `flink/README.md`, `Generator/GENERATOR_BEHAVIOR.md`, `docs/guides/*` | run ID and counts updated to the current dataset |
| `batch/queries/gold_analysis.sql` | run ID updated |
| `.gitignore`, `.env.example` | local secrets, caches, Dagster state and dashboard material excluded; settings documented |

## New files

| Path | What it is |
|---|---|
| `orchestration/` | the Dagster project (assets, resources, sensors, demo job, tests) |
| `configs/pipeline-large.yml` | the dataset we run: 6 districts, 1,500 meters, 13 incidents, more faults |
| `configs/demo-live.yml` | small dataset for the live demo |
| `infrastructure/catalog-db/init.sql` | the Iceberg catalog tables in Postgres |
| `infrastructure/trino/catalog/water.properties` | Trino's view of the same catalog and warehouse |
| `infrastructure/trino/marts/*.sql` | the four marts |
| `infrastructure/grafana/provisioning/` | Grafana's datasource and dashboard folder settings |
| `live/` | the live bridge service (Kafka → Postgres for Grafana) |
| `batch/jobs/load_streaming.py`, `export_dashboard.py` | Flink results into Iceberg; Parquet exports for Power BI |
| `Generator/.dockerignore` | keeps the large `output/` folder out of image builds |
| `run.ps1` | one script for the common operations (see [Running the system](RUNNING.md)) |
| `docs/` | this documentation |

## Not in the repository (generated locally)

Dashboard material is deliberately not versioned: the Power BI project and its exported data (`dashboards/`), and
the Grafana dashboard definition and its generator (`infrastructure/grafana/dashboards/`,
`infrastructure/grafana/build_dashboard.py`). Generated datasets (`Generator/output/`) and Dagster's run history
(`orchestration/.dagster_home/`) are local too.

## Teammates' branches

`feature/incremental-batch` contains the earlier Airflow attempt and the original batch code (before the edits above);
`flink-pipeline` is the Flink upload. Their content is already included here.
