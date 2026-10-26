# Handoff from Amr Yousri to Amr AbdEl-Monem

For complete operating instructions, see the [usage guide](USER_GUIDE.md). For the implementation history and rationale, see [what changed and why](IMPLEMENTATION_AND_DECISIONS.md).

Amr Yousri owns Airflow and the shared Docker Compose environment. Amr AbdEl-Monem owns PySpark transformations and analytical Iceberg tables. Ahmed owns the Flink rules and streaming/Trino integration. These ownership boundaries follow the attached technical document; runtime schemas and paths below reflect the code in this checkout.

The infrastructure now provides a Spark runtime, persistent Iceberg JDBC catalog, warehouse in MinIO, and Trino connected to the same catalog. The default Spark job is an **infrastructure smoke test**, not the analytical batch implementation. It counts real delivery inputs, checks the manifest, writes one Iceberg evidence row, reads it back in Spark, and lets Airflow verify it through Trino.

## First run

Install Docker Desktop with Linux containers and Compose v2. Enable Docker Desktop's WSL integration if using WSL. Allow roughly 12–16 GB RAM for the complete parallel stack. The first build downloads Python packages and Spark/Iceberg jars.

From the repository root in PowerShell:

```powershell
Copy-Item .env.example .env
.\scripts\start-pipeline.ps1
```

On Linux/WSL:

```bash
cp .env.example .env
./scripts/start-pipeline.sh
```

The scripts preserve an existing `.env`. On Linux they detect the Docker socket group. If starting Compose yourself, set `DOCKER_GID` in `.env` to `stat -c %g /var/run/docker.sock`. Use simple URL-safe characters in the local Airflow database password because it is embedded in a connection URL.

Open http://localhost:8083. The example account is `amr` / `waterlocal`; `.env` controls the credentials. Trigger `egypt_water_pipeline`, choosing `config=smoke` and `batch_job=handoff_smoke`. Unpause `egypt_water_health` for monitoring between runs. The smoke configuration produces 360 district events and 48 meter readings: six districts, twelve meters, sixty minutes, no injected faults. The four Flink jobs run at parallelism one and remain running after the DAG completes.

Use a fresh `COMPOSE_PROJECT_NAME` for a different generated configuration. The existing Flink jobs do not retain `run_id` in their downstream records, so one Kafka namespace must not contain multiple simulated datasets. Stop the previous stack before changing the project name if reusing the same host ports. Existing manual Kafka data is intentionally rejected by the DAG rather than adopted without verification.

The pilot configuration is the existing 25-day, six-district, 600-meter scenario with delivery faults. The manifest determines the run ID and counts; no historical run directory is assumed to exist in a clone.

## Connections and exact input contract

| Component | Inside Docker | Host interface |
| --- | --- | --- |
| Kafka | `kafka:9092` | Kafka UI http://localhost:8080 |
| MinIO API | `minio:9000` / `http://minio:9000` for SDKs needing a URL | http://localhost:9000 |
| MinIO Console | `minio:9001` | http://localhost:9001 |
| Flink | `flink-jobmanager:8081` | http://localhost:8081 |
| Iceberg JDBC | `jdbc:postgresql://catalog-db:5432/iceberg` | Internal only |
| Trino | `trino:8080` | http://localhost:8082 |
| Airflow | `airflow-webserver:8080` | http://localhost:8083 |

For a generated `run-<16 hex characters>`, `archive-minio` writes:

```text
water-raw/
  <run_id>/_RUN_COMPLETE
  <run_id>/artifacts/districts.ndjson
  <run_id>/artifacts/meters.ndjson
  <run_id>/artifacts/network.inp
  <run_id>/artifacts/run-manifest.json
  <run_id>/artifacts/validation-report.json
  <run_id>/delivery/district-sensors.ndjson
  <run_id>/delivery/meter-readings.ndjson
water-restricted/
  <run_id>/restricted/config.yml
  <run_id>/restricted/scenario-truth.ndjson
  <run_id>/restricted/fault-truth.ndjson
water-events/
  events/<topic>/<run_id>/part-<partition>-<first_offset>-<last_offset>.ndjson.gz
  orchestration/stream.json
water-warehouse/
  # Iceberg metadata and Parquet, managed through the catalog
```

Bucket names come from `.env`. The warehouse path must also match the Trino properties file. The raw completion marker is uploaded last, after all required artifacts. Read it before starting a batch load. `artifacts/` holds public reference data and generation evidence. `delivery/` holds faulted measurement copies, which are also published to Kafka. The clean `public/` measurement files remain local generator evidence; they are **not** uploaded by `archive-minio`. `config.yml` belongs to the restricted bucket because it reveals planted incidents.

All event files are NDJSON. The Kafka archive contains gzip-compressed NDJSON, grouped by topic, run and partition. Preserve run ID and event ID during processing. The raw archive includes corrupt/null records and duplicates by design. Never clean it in place or read `water-restricted` as a transformation input.

Airflow-generated files live in the `<project>_pipeline-output` Docker volume mounted at `/app/output` in generator task containers. Manual generator commands still use `Generator/output/` on the host. Batch processing reads MinIO, so neither local path is a prerequisite for Amr AbdEl-Monem's work.

## Spark/Iceberg contract

The image is `water-spark:local`, built from `batch/Dockerfile`:

- Spark 3.5.3 / Scala 2.12, Iceberg 1.7.1 runtime and AWS bundle, PostgreSQL driver 42.7.5.
- The Spark base image uses Python 3.8.10 and MinIO SDK 7.2.10. Airflow and local tests use newer Python runtimes and retain their separate SDK pin; [MinIO SDK 7.2.15 requires Python 3.9+](https://pypi.org/project/minio/7.2.15/).
- Spark runs `local[2]` in a disposable container with a 1 GB driver. Adjust the Dockerfile for larger jobs.
- Catalog name `water`, implementation `org.apache.iceberg.jdbc.JdbcCatalog`, JDBC schema version `V1`.
- JDBC user `iceberg`, password from `CATALOG_DB_PASSWORD`.
- S3FileIO talks to MinIO with path-style access and region `us-east-1`; default warehouse `s3://water-warehouse/`.
- Spark uses UTC. `spark_session()` in `batch/jobs/handoff_smoke.py` is a working configuration example.

The catalog metadata persists in `catalog-data`; Iceberg files persist in MinIO. Spark and Trino use the same catalog name (`water`) and warehouse. Trino catalog files use environment placeholders for credentials. The JDBC metadata schema is initialized by PostgreSQL from `infrastructure/catalog-db/init.sql` on its first startup.

To test the existing raw run independently of Airflow:

```powershell
docker compose build spark
docker compose run --rm --no-deps spark /opt/spark/work-dir/jobs/handoff_smoke.py --run-id run-0123456789abcdef
```

Replace the example ID with the manifest's actual run ID, already uploaded to MinIO. The Spark image entrypoint is `spark-submit`; pass the script path and its arguments, not another `spark-submit` command.

Query the evidence through Trino:

```powershell
docker compose exec trino trino --execute "SELECT * FROM water.handoff.pipeline_runs"
```

## Add the analytical job

1. Put your job and reusable modules under `batch/jobs/`. Accept `--run-id` and obtain credentials/endpoints from the existing environment. The smoke job demonstrates both conventions. The image copies this folder, so rebuild `spark` after changes.
2. Read the exact `artifacts/` and `delivery/` paths above. Do not require the source run folder or restricted truth. For large-scale distributed raw reads, add a tested S3A connector matching Spark's Hadoop version, or a staging strategy. Iceberg's S3FileIO config alone does not configure `spark.read.json('s3a://…')`. The smoke job streams the raw objects with the MinIO SDK and is not a distributed raw-ingestion design.
3. Define your target namespace, grains, keys, partitioning, UTC interval alignment, rejected-row evidence and rerun behavior. Keep your analytical tables separate from `water.handoff.pipeline_runs`.
4. Add an approved job name/path to `runtime.spark_batch()` and the `batch_job` DAG parameter enum. Avoid arbitrary shell commands or arbitrary script paths in DAG parameters.
5. Extend `runtime.verify_trino()` to check your actual table counts, keys and water-balance invariants. The existing check verifies smoke evidence only.
6. Rebuild `spark`, trigger the DAG, and supply the output table contract and quality report for review.

The analytical acceptance criteria are in [the batch guide](guides/batch-processing.md). At minimum: explicit input schemas, accepted/rejected/duplicate counts, deterministic deduplication within each run, reference-key validation, aligned inlet/meter intervals, stable output grains, run-scoped replacement or merge, and successful read-back through Spark and Trino. The daily water-balance fact described in the PDF should derive from aligned intervals; do not compare a minute of inlet volume with fifteen minutes of consumption. Do not invent source fields that the current generator does not provide.

## Remaining team work

Amr AbdEl-Monem's facts, dimensions, cleaning and water-balance transformations are still to be implemented. Ahmed's current Flink jobs write Kafka topics; direct streaming writes to Iceberg and streaming alert tables remain his integration work. The smoke test does not certify alert accuracy, completeness of the final event-time window, or the proposed star schema. The existing offline evaluator remains available through `flink/scripts/evaluate.sh` for manual runs with local restricted truth.

Use [the orchestration runbook](ORCHESTRATION.md) for retries, monitoring and shutdown. Keep generated output, secrets and runtime state out of Git.
