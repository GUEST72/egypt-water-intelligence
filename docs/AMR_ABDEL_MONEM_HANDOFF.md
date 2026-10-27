# Amr AbdEl-Monem: batch development without the full pipeline

Your work is to turn generated measurements into cleaned data, dimensions, facts and water-balance tables using PySpark and Iceberg. You need **MinIO + catalog-db + Trino**, with Spark running only when you execute a job. Airflow, Kafka and Flink are not required for this workflow.

## 1. Where to work

| Location | Your work |
| --- | --- |
| `batch/jobs/build_batch.py` | Working starter: download raw inputs, clean/deduplicate events, write references and a fifteen-minute water balance. Extend this job for your analytical requirements. |
| `batch/jobs/validate_batch.py` | Check saved tables, input accounting, reference keys, interval coverage, balance calculations and Trino read-back. |
| `batch/jobs/test_batch.py` | Small fixture tests for cleaning, duplicates, missing coverage and validation failures. |
| `batch/jobs/handoff_smoke.py` | Use `spark_session()` as the working Spark/Iceberg connection example and its reader as a MinIO example. Keep this infrastructure check available. |
| `batch/Dockerfile` | Add batch dependencies if needed. It copies `batch/jobs/` into the image. |
| `Generator/schemas/` | Read `meter-reading.v1.json` and `district-sensor.v1.json` for the actual event fields. |
| [Batch requirements](guides/batch-processing.md) | Follow the detailed cleaning, table-grain and water-balance requirements. |

Amr Yousri handles Airflow and shared infrastructure; Ahmed handles Flink. Integrate with Airflow after your standalone jobs pass.

## 2. Start only what you need

Run all commands from the repository root with Docker running. If `.env` is missing, copy `.env.example` to `.env` once. Preserve an existing `.env`.

```bash
docker compose --profile pipeline up -d --wait minio catalog-db trino
docker compose build spark
```

Explicitly naming these three services does not start the entire pipeline. Spark runs `local[2]` in a temporary container; no Spark master is needed.

**To reuse this workstation's pilot data, keep `COMPOSE_PROJECT_NAME=ewi-pilot`.** A different project name or computer has different storage and needs its own uploaded data; see section 6.

## 3. Choose the input and check the runtime once

Open [MinIO](http://localhost:9001) using the credentials in `.env`. In the raw bucket (default `water-raw`), select a `run-…` folder containing `_RUN_COMPLETE`.

The previously verified pilot here is `run-4fae7f16e89b947e`: **216,000 delivered district records and 1,440,057 delivered meter records**. Confirm it exists in your MinIO. Replace that example ID in every command if using another run. The manifest is the source of truth for counts.

Read these objects under `water-raw/<run_id>/`:

```text
_RUN_COMPLETE                       # require before loading
artifacts/run-manifest.json          # run ID and expected delivery counts
artifacts/validation-report.json     # generation validation must have passed
artifacts/districts.ndjson           # reference data
artifacts/meters.ndjson              # reference data
delivery/district-sensors.ndjson     # one-minute district measurements
delivery/meter-readings.ndjson       # fifteen-minute meter measurements
```

Delivery files include intentional duplicates and nulls. Preserve `run_id` and `event_id`; keep raw objects unchanged. Do not read evaluation answers from `water-restricted` as transformation inputs. Bucket names and credentials come from the environment.

Run the existing infrastructure check directly:

```bash
docker compose run --rm --no-deps spark /opt/spark/work-dir/jobs/handoff_smoke.py --run-id run-4fae7f16e89b947e

docker compose exec -T trino trino --execute "SELECT * FROM water.handoff.pipeline_runs WHERE run_id = 'run-4fae7f16e89b947e'"
```

Expected: Spark exits successfully and Trino returns one row with `216000` and `1440057` for this pilot. The job checks actual MinIO input counts, writes Iceberg evidence and reads it back. Its name is `handoff_smoke`, but it checks whichever generated run you select. It does not validate your analytical transformations.

## 4. Run and extend the starter

`build_batch.py` accepts `--run-id` and writes to `water.analytics` by default. Both scripts accept `--namespace amr_dev` to use separate development tables. They use explicit schemas, UTC timestamps and the existing Spark connection settings. Each write replaces only the selected run's rows.

| Table | One row represents |
| --- | --- |
| `districts` / `meters` | One reference entity per run |
| `district_clean` / `meter_clean` | One accepted event per run and event ID |
| `rejected_events` | One rejected delivery, including raw JSON and reasons |
| `quality_report` | Input, accepted, rejected and duplicate counts for one stream/run |
| `water_balance_15m` | One district and UTC fifteen-minute interval, with coverage counts |

The starter rejects malformed records, unknown references, invalid times, non-finite/negative measures, null required measures and quality codes other than `valid`/`noisy`. A null pressure rejects the district event, so that minute is missing from balance coverage. Identical accepted deliveries collapse; conflicting accepted copies of an event fail the build. Rejected deliveries are counted before deduplication. Incomplete intervals have a null gap; complete intervals with zero inlet have a null gap percentage. Extend these explicit policies as needed.

For the pilot ID above, this policy produces 215,957 clean district events and 1,439,943 clean meter events, with 43 rejected district deliveries and 114 rejected meter deliveries. The duplicated null meter readings are rejected too, so the valid-duplicate counter is zero for this dataset.

This is a starting point: daily facts, business-specific indicators and alert consolidation still need your implementation. Writes to different tables are sequential, so rerun the build after a partial failure and require a successful validator before using the outputs.

For raw ingestion, a first implementation can download selected objects with the MinIO SDK into a temporary directory and read those files with Spark. This works with the current single-container runtime. Iceberg's S3FileIO settings alone do **not** enable `spark.read.json('s3a://…')`; distributed raw reads need a tested S3A connector or another staging strategy.

Run the starter, then repeat this loop after edits:

```bash
# Rebuild after edits: job files are copied into the image.
docker compose build spark
docker compose run --rm --no-deps spark /opt/spark/work-dir/jobs/build_batch.py --run-id run-4fae7f16e89b947e
```

The entrypoint already runs `spark-submit`; pass only the script and arguments. Your input comes from MinIO, so there is no need to regenerate data, publish Kafka messages or trigger Airflow after each edit.

## 5. Test and validate your results

Run the included `unittest` fixtures after edits. They cover valid data, duplicates, conflicting copies, null/non-finite measurements, invalid references/timestamps, malformed JSON, a foreign run, missing coverage and a hand-calculated balance. They also deliberately corrupt a balance to check that validation fails. No running storage services are needed for these tests:

```bash
docker compose build spark
docker compose run --rm --no-deps spark /opt/spark/work-dir/jobs/test_batch.py
```

Then run `validate_batch.py` against the saved tables. It exits with an error if a check fails. Keep these requirements covered as you extend the job:

| Check | Passing result |
| --- | --- |
| Input accounting | Raw counts match the manifest's `delivery_district_records` and `delivery_meter_records`. Accepted, rejected and duplicate counts reconcile under your documented processing order. |
| Cleaning | Rejected rows have reasons; required keys, timestamps, measures and reference relationships satisfy your rules. Null handling is explicit. |
| Unique keys | Clean events are unique by `(run_id, event_id)`; analytical rows are unique at each declared table grain. |
| Interval alignment | Compare the sum of 15 one-minute inlet volumes with meter volumes for the same fifteen-minute interval. Join on interval boundaries: district event time is interval start; meter event time is interval end. |
| Water balance | `gap_m3 = inlet_volume_m3 - metered_consumption_m3` within a documented tolerance. Report missing coverage and define zero-inlet percentage handling. |
| Saved output | Spark and Trino can read expected counts, keys and sample calculations for this run. |
| Safe rerun | A second build gives the same keys, counts and values, without changing other runs. |

Validate and inspect the saved tables:

```bash
docker compose run --rm --no-deps spark /opt/spark/work-dir/jobs/validate_batch.py --run-id run-4fae7f16e89b947e

docker compose exec -T trino trino --execute "SHOW TABLES FROM water.analytics"
```

The validator reads every table through Spark, compares its row count through Trino, and prints a final JSON report with counts and content checksums. Run the build and validator again and compare those summaries to check rerun stability. Also query a few known intervals through Trino. A successful build exit alone is not enough. Extend the validator alongside any new tables or business rules.

Hand over the code, table schemas/grains, exact test and SQL commands, run ID, and a short quality report with counts and sample calculations. Then coordinate with Amr Yousri to register your job in `runtime.spark_batch()`, the DAG's `batch_job` enum and `runtime.verify_trino()`.

## 6. Only if no generated data is uploaded yet

Skip this section when section 3 already passes. With the three services from section 2 running, create the warehouse bucket in MinIO if missing (`MINIO_WAREHOUSE_BUCKET`, default `water-warehouse`). Then:

```bash
docker compose build generator
docker compose run --rm --no-deps generator generate --config /app/configs/pilot.yml
```

Copy the printed dataset ID and replace `run-REPLACE_ME` below:

```bash
docker compose run --rm --no-deps generator validate --run-dir /app/output/run-REPLACE_ME
docker compose run --rm --no-deps generator archive-minio --run-dir /app/output/run-REPLACE_ME
```

The uploader creates the raw/restricted buckets if missing. `--no-deps` keeps Kafka/init from starting. No publication or full pipeline run is needed. Use `/app/configs/smoke.yml` for a quick small dataset; it has no planted faults, so also test cleaning on pilot and targeted fixtures.

If a run is missing, check the project name and MinIO folder. Use a dataset ID (`run-…`), not the Airflow execution name `pilot-first-run`. If code is missing or stale, rebuild `spark`. Preserve MinIO/catalog volumes between sessions; `down -v` deletes them. Keep credentials and generated/runtime files out of Git.

For detailed requirements, see the [batch guide](guides/batch-processing.md); for full-stack operation, see the [usage guide](USER_GUIDE.md).
