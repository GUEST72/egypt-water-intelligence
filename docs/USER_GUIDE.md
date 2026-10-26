# Complete setup and usage guide

This guide explains how to operate the current Egypt Water Intelligence checkout. For the reasons behind its design and a record of changes, read [What changed and why](IMPLEMENTATION_AND_DECISIONS.md). For Amr AbdEl-Monem's development contract, read [his handoff](AMR_ABDEL_MONEM_HANDOFF.md).

The integrated batch entrypoint is currently `handoff_smoke`: it verifies raw input counts and writes integration evidence. Analytical facts, dimensions, water balance and direct Flink-to-Iceberg sinks remain team work. Running services successfully is separate from completing a full data-pipeline run.

## Contents

- [1. Choose a workflow and dataset](#1-choose-a-workflow-and-dataset)
- [2. Install and check prerequisites](#2-install-and-check-prerequisites)
- [3. Configure the environment](#3-configure-the-environment)
- [4. Start the stack](#4-start-the-stack)
- [5. Run and inspect Airflow](#5-run-and-inspect-airflow)
- [6. Understand every pipeline task](#6-understand-every-pipeline-task)
- [7. Use Kafka and Kafka UI](#7-use-kafka-and-kafka-ui)
- [8. Use MinIO and find your files](#8-use-minio-and-find-your-files)
- [9. Use and inspect Flink](#9-use-and-inspect-flink)
- [10. Use Spark and Trino](#10-use-spark-and-trino)
- [11. Run the generator manually](#11-run-the-generator-manually)
- [12. Evaluate alerts and use analysis tools](#12-evaluate-alerts-and-use-analysis-tools)
- [13. Stop, resume and recover](#13-stop-resume-and-recover)
- [14. Start a separate experiment](#14-start-a-separate-experiment)
- [15. Develop and test changes](#15-develop-and-test-changes)
- [16. Troubleshoot failures](#16-troubleshoot-failures)
- [17. Preserve and back up state](#17-preserve-and-back-up-state)
- [18. Acceptance checklist](#18-acceptance-checklist)

## 1. Choose a workflow and dataset

Use **Airflow** for the integrated pipeline. It initializes resources, generates and validates the dataset, archives it, starts the stream processors, publishes, checks progress, runs Spark and verifies its output with Trino.

Use **manual component commands** when developing a generator, Flink job or batch job independently. Keep manual streaming experiments in a different Compose project from an Airflow-managed stream. The current guard refuses pre-existing unmanaged Kafka data and competing Flink jobs.

Choose the dataset before the first streaming publication:

| Configuration | Districts | Meters | Simulated duration | Clean district records | Clean meter records | Intended use |
| --- | --- | --- | --- | --- | --- | --- |
| `smoke` | 6 | 12 | 60 minutes | 360 | 48 | Small integration experiment, no incidents/faults |
| `pilot` | 6 | 600 | 25 days | 216,000 | 1,440,000 | Existing scenario dataset with configured delivery faults |

Delivery counts can differ from clean counts because of injected duplicates. The selected run's manifest is the source of truth. The smoke dataset is too short and has no planted incidents, so it is not an alert-accuracy demonstration.

**One Kafka namespace currently holds one generated dataset.** The existing Flink downstream schemas omit `run_id`. Changing the configuration changes the generator ID and is rejected once a different dataset has claimed that namespace. To run smoke and then pilot, use separate projects as explained in [section 14](#14-start-a-separate-experiment).

## 2. Install and check prerequisites

### Windows with Docker Desktop and WSL

1. Start Docker Desktop and use Linux containers.
2. In **Settings → General**, enable the WSL 2 engine if that option is shown.
3. In **Settings → Resources → WSL Integration**, enable the Ubuntu distribution you use.
4. Apply changes and reopen the Ubuntu terminal if needed.

From Windows PowerShell, check your distribution:

```powershell
wsl.exe -l -v
```

Ubuntu should show version `2`. The [official Docker WSL instructions](https://docs.docker.com/desktop/features/wsl/) describe the integration settings and requirements.

From the terminal where you will run the project:

```bash
docker version
docker compose version
docker info
```

`docker version` must show both a client and a server. Having the command available without a reachable server is not enough. Use Compose v2 or newer; the repaired startup was checked with Compose v5.6.1. Use `docker compose`, with a space.

### Linux or WSL resource planning

Allow roughly 12–16 GB of memory for the complete parallel development stack; this is planning guidance, not a measured minimum. Four Flink jobs share a TaskManager configured for a 4 GB process, the JobManager is configured for 2 GB, and the other services and task containers need additional memory. Initial image downloads and the Flink Python package are large.

Docker and Git are enough for containerized startup. Local tests additionally require Python 3.11 or 3.12. Manual Bash Flink helpers use Bash, curl, Python and common Linux utilities; run those helpers in Linux/WSL.

Work from the repository root. In this workstation's WSL checkout:

```bash
cd ~/Projects/egypt-water-intelligence
```

Use your own clone location on another machine. All services, including Flink, are defined in `docker-compose.yml`. The current integration uses no override or separate Airflow Compose file. Profiles still select the optional `pipeline` services and the `tools` Spark runtime.

## 3. Configure the environment

### Create `.env` once

The startup scripts create `.env` from `.env.example` only if it is missing. To create it yourself without overwriting an existing file:

Linux/WSL:

```bash
if [ ! -f .env ]; then cp .env.example .env; fi
```

PowerShell:

```powershell
if (-not (Test-Path .env)) { Copy-Item .env.example .env }
```

Keep `.env` private. The values in `.env.example` are documented local-development examples, not production credentials. Do not paste a rendered Compose environment or your complete `.env` into logs or screenshots when diagnosing a problem.

### Configuration reference

| Setting | Example/default | Used for / when to change |
| --- | --- | --- |
| `COMPOSE_PROJECT_NAME` | `egypt-water-intelligence` | Names the Compose project, network and persistent volumes; change for a separate experiment |
| `MINIO_ACCESS_KEY` | `wateradmin` | MinIO root user and local client access key |
| `MINIO_SECRET_KEY` | `wateradminsecret` | MinIO root password and local client secret |
| `MINIO_RAW_BUCKET` | `water-raw` | Raw references, delivery measurements and completion evidence |
| `MINIO_RESTRICTED_BUCKET` | `water-restricted` | Configuration and evaluation truth |
| `MINIO_EVENT_BUCKET` | `water-events` | Kafka archive batches and stream ownership registry |
| `MINIO_WAREHOUSE_BUCKET` | `water-warehouse` | Iceberg files; also update Trino's static warehouse property when changing it |
| `CATALOG_DB_PASSWORD` | `iceberglocal` | Iceberg PostgreSQL catalog credentials for its clients |
| `AIRFLOW_DB_PASSWORD` | `airflowlocal` | Airflow metadata database password; use URL-safe characters or correctly encode its SQLAlchemy URL |
| `AIRFLOW_ADMIN_USERNAME` | `amr` | Account created by Airflow initialization when missing |
| `AIRFLOW_ADMIN_PASSWORD` | `waterlocal` | Initial password for that account |
| `AIRFLOW_UID` | `50000` | User running Airflow containers; does not need to match the host for the named log volume |
| `DOCKER_GID` | Example `0`; this workstation required `1001` | Supplementary scheduler group with permission to access Docker's socket |

Host ports are defined in Compose, not in these environment variables. Internal service names, raw topic names, the catalog name `water` and the approved batch entrypoint list are code/configuration contracts rather than arbitrary `.env` settings.

An exported shell variable can override the same variable in `.env`. If changing `.env` appears to do nothing, check that variable in your terminal. Do not assume that the `docker compose -p` flag alone updates every explicit environment-derived network/volume name; use the documented `COMPOSE_PROJECT_NAME` setting consistently.

### Set Docker socket access

On Linux/WSL, inspect the socket:

```bash
stat -c 'owner=%U group=%G gid=%g mode=%a' /var/run/docker.sock
```

Set `.env`'s `DOCKER_GID` to the printed numeric group. On the workstation repaired during this work it was `1001`. That value is not universal. The Bash startup script detects the host socket group for its process unless `DOCKER_GID` is already exported. Persisting the correct value in `.env` also makes later direct Compose commands consistent.

The scheduler is the service that mounts the socket. It needs Docker access to start task containers and manage the existing archiver/Flink services. Recreate it after changing this setting:

```bash
docker compose --profile pipeline up -d --no-deps airflow-scheduler
```

Validate access after it starts:

```bash
docker compose exec -T airflow-scheduler python -c 'import docker; c=docker.from_env(); print(c.ping()); c.close()'
```

Expected result: `True`. This Python command is shown with Bash quoting. The UI is the recommended way to operate Airflow when using a Windows terminal with different quoting behavior.

### Understand credential changes

Changing an initial PostgreSQL password variable does not change the password already stored in an existing database volume. Change the database account and client configuration together. Likewise, editing `AIRFLOW_ADMIN_PASSWORD` does not reset an account that already exists; initialization creates missing accounts only.

For an existing Airflow user, use the web UI account controls or inspect the supported CLI before resetting it:

```bash
docker compose exec airflow-scheduler airflow users reset-password --help
```

## 4. Start the stack

### Normal startup

Linux/WSL:

```bash
./scripts/start-pipeline.sh
```

Windows PowerShell:

```powershell
.\scripts\start-pipeline.ps1
```

The scripts validate Docker/Compose and configuration, build generator/Flink/Spark/Airflow runtimes, start infrastructure, prepare the archiver without starting it, initialize Airflow and start its webserver/scheduler. A TaskManager image may be built automatically from the same cached Flink Dockerfile during service startup.

The scripts preserve an existing `.env` and data volumes. They do not generate or publish the source dataset. Airflow performs that work when you trigger the pipeline.

### Equivalent individual commands

Run these from the repository root if you need to inspect each startup phase. Set the correct socket group in `.env` first.

```bash
docker compose --profile pipeline config --quiet
docker compose build generator flink-jobmanager spark airflow-scheduler
docker compose --profile pipeline up -d kafka minio kafka-ui catalog-db trino flink-jobmanager flink-taskmanager
docker compose up --no-deps --no-start archiver
docker compose --profile pipeline up -d airflow-webserver airflow-scheduler
docker compose --profile pipeline ps -a
```

`up --no-deps --no-start archiver` prepares that container for Airflow. `create --no-deps` is not a supported substitute.

### Service directory

| Service | Host interface | Connection inside Docker | Normal role/state |
| --- | --- | --- | --- |
| `kafka` | No published broker port | `kafka:9092` | Persistent broker; healthy |
| `kafka-ui` | http://localhost:8080 | `kafka-ui:8080` | Browser topic inspection |
| `flink-jobmanager` | http://localhost:8081 | `flink-jobmanager:8081` | Flink REST/UI and job coordinator |
| `flink-taskmanager` | No separate host UI | Managed by JobManager | Four task slots; running |
| `trino` | http://localhost:8082 | `trino:8080` | Query endpoint/UI; healthy |
| `airflow-webserver` | http://localhost:8083 | `airflow-webserver:8080` | Airflow UI; healthy |
| `airflow-scheduler` | No separate host UI | Internal health endpoint | LocalExecutor and task scheduling; healthy |
| `minio` | API http://localhost:9000; console http://localhost:9001 | `minio:9000` | Object storage; healthy |
| `catalog-db` | Internal only | `catalog-db:5432`, database/user `iceberg` | Iceberg JDBC metadata; healthy |
| `airflow-db` | Internal only | `airflow-db:5432`, database/user `airflow` | Airflow metadata; healthy |
| `airflow-init` | None | One-shot command | Exited with code 0 after successful initialization |
| `archiver` | None | Kafka/MinIO clients | Created before the first pipeline; running after its Airflow startup task |
| `init` | None | One-shot generator image command | Used for manual initialization; need not run continuously |
| `generator` | None | Build/run service | Manual CLI target; not a continuously running generator |
| `spark` | None | Build/run service under `tools` profile | CLI target; Airflow creates disposable Spark containers |

A container showing `Created` can be intentional for the archiver before the pipeline runs. A completed initialization container showing `Exited (0)` is normal. Hostname `localhost` inside a container refers to that container; use the internal names above to reach other services.

Check current status:

```bash
docker compose --profile pipeline ps -a
docker compose exec airflow-scheduler airflow dags list-import-errors
docker compose exec -T airflow-webserver curl --fail --silent http://localhost:8080/health
```

Expected: no DAG import errors; metadata database and scheduler healthy. Health output for optional components such as a triggerer can be null because this stack does not run that service.

## 5. Run and inspect Airflow

### First run through the UI

1. Open http://localhost:8083 and use the account configured in `.env`. The example account is `amr` / `waterlocal`.
2. Find `egypt_water_pipeline`. DAGs start paused; unpause this DAG.
3. Choose **Trigger DAG** and select `config=smoke` or `config=pilot`, according to the namespace you chose in section 1. The current batch option is `handoff_smoke`.
4. Follow the run in Grid or Graph view. Open the first failed task's logs when diagnosing a failure.
5. Expect the readiness/progress sensors to wait and reschedule. That is normal while checkpoints and archive commits complete.
6. After a successful run, check the Spark evidence through Trino as shown in section 10.
7. Unpause `egypt_water_health` to check the persistent services every five minutes. It is also possible to trigger a health check manually.

Unpausing the main DAG does not schedule daily work: its schedule is manual. Pausing either DAG prevents future scheduling; it does not stop existing Flink jobs, the archiver or a task that is already executing.

### CLI alternative in Bash/WSL

These commands change Airflow run state. Run the trigger once per intended pipeline execution and use a unique Airflow run ID:

```bash
docker compose exec airflow-scheduler airflow dags unpause egypt_water_pipeline
docker compose exec airflow-scheduler airflow dags trigger --run-id smoke-first-run --conf '{"config":"smoke","batch_job":"handoff_smoke"}' egypt_water_pipeline
docker compose exec airflow-scheduler airflow dags unpause egypt_water_health
```

For a second pipeline execution on the **same dataset**, change the Airflow run ID, for example to `smoke-second-run`. Do not switch the config to pilot in a namespace already claimed by smoke.

Inspect runs and their task states:

```bash
docker compose exec airflow-scheduler airflow dags list-runs --dag-id egypt_water_pipeline
docker compose exec airflow-scheduler airflow tasks states-for-dag-run egypt_water_pipeline smoke-first-run
docker compose exec airflow-scheduler airflow dags list-import-errors --output json
```

If using PowerShell, prefer the UI for a JSON configuration payload. Other single-line Docker status/inspection commands in this guide generally do not require shell-specific JSON quoting.

### Airflow run IDs versus generator IDs

An **Airflow run ID** identifies one workflow execution, such as `smoke-first-run` or `manual__...`. A **generator run ID** identifies the dataset, shaped like `run-<16 hex characters>`. The latter determines paths in MinIO and the generator output volume.

Find the generator ID in the `generate` task's logs or XCom receipt, or the run manifest. Repeating the same generator configuration reuses that dataset even though the Airflow execution has a different ID. The offline smoke generation during implementation produced `run-118193801c188eb1`; always use the actual ID reported by your checkout instead of assuming a historical example.

### Retry a failed task

Read the error, fix its cause, then use the Airflow UI to clear only the task instances that need to rerun. Include downstream tasks if their inputs were not produced. Do not mark a task successful merely to bypass its validation.

Most tasks have one retry after thirty seconds. Kafka publication has no automatic retry. If its registry says `publishing`, inspect the stream before clearing it: part of the dataset may already have reached Kafka. See sections 13 and 16 for state-specific recovery.

## 6. Understand every pipeline task

| Task ID | Work performed | How to recognize progress or success |
| --- | --- | --- |
| `initialize_topics_and_buckets` | Creates missing topics and four configured buckets | Resources exist; existing data is not cleared |
| `generate` | Runs the selected config once or reuses its completed directory | A small JSON receipt includes dataset ID and manifest |
| `validate` | Validates the completed local source run | Generator exits successfully |
| `archive_raw_to_minio` | Uploads artifacts, delivery files, restricted files and then the raw completion marker | `<run_id>/_RUN_COMPLETE` exists in raw storage |
| `claim_stream_namespace` | Checks existing Kafka data and stream registry | Namespace is empty/unmanaged or already belongs to this same dataset in an allowed state |
| `ensure_archiver` | Starts the existing Compose archiver when needed | Archiver container running |
| `ensure_flink_jobs` | Submits or reattaches the four managed jobs | Task receipt contains job IDs; no competing unmanaged jobs |
| `wait_for_flink` | Polls all four job states | Each reports `RUNNING`; ten-minute maximum wait |
| `publish_delivery_to_kafka` | Publishes delivery input or skips a saved completed publication | Registry moves to `published`; raw end offsets recorded |
| `wait_for_stream_and_archive` | Checks job health, completed checkpoints and group commits against current input end offsets | All checks pass; thirty-minute maximum wait |
| `spark_batch` | Runs the approved Spark entrypoint | Container exits with code 0 and evidence is written |
| `verify_iceberg_through_trino` | Reads evidence through Trino and checks delivery counts | Query returns the selected run and exact manifest delivery counts |

The source is archived before Kafka publication, so the batch input does not depend on somebody's host output folder. The raw Kafka archiver and Flink are started before publication. The batch task is placed after stream/archive progress checks to make the integrated run's success evidence explicit.

A successful run does not imply that every final streaming window closed, that alerts must exist, or that analytical transformation work has been completed.

## 7. Use Kafka and Kafka UI

Open http://localhost:8080 and select the `water` cluster. Inspect topic messages, partitions, offsets and consumer groups there.

| Topic | Producer | Consumer / use |
| --- | --- | --- |
| `water.district-sensors.v1` | Generator delivery publisher | Validation job and raw archiver |
| `water.meter-readings.v1` | Generator delivery publisher | Raw archiver; batch work reads archived files |
| `water.district-sensors.clean.v1` | Flink validation job | Feature job |
| `water.dead-letter.district.v1` | Flink validation job | Inspect rejected district payloads |
| `water.district-features.15m.v1` | Flink feature job | Alert rules |
| `water.district-relative.15m.v1` | Flink alert job | Level-shift job |
| `water.district-alerts.v1` | Flink alert and level-shift jobs | Inspection and offline evaluation |

Initialization creates one partition and replication factor one per topic, matching the local single-broker setup.

Useful read-only CLI commands:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
docker compose exec kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic water.district-sensors.v1 --time -1
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --all-groups --describe
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic water.district-alerts.v1 --from-beginning --max-messages 5 --timeout-ms 10000
```

A console consumer can time out when fewer than five messages exist; that alone is not a pipeline failure. End offsets describe positions and are not always equivalent to retained record counts after retention or deletion. Before initialization, a topic can be absent; an offsets command may say it cannot match any topic-partitions.

The managed consumer groups are `water-event-archiver`, `ewi-validation`, `ewi-features`, `ewi-alerts` and `ewi-level_shift`. Archiver commits follow successful object uploads. Flink's committed offsets depend on checkpoint progress, so they can trail processing between checkpoints.

Do not use the legacy topic-reset/demo scripts against an Airflow-owned namespace. They can delete data and invalidate the stream registry's assumptions.

## 8. Use MinIO and find your files

Open http://localhost:9001 and use `MINIO_ACCESS_KEY` / `MINIO_SECRET_KEY` from your `.env`. The API endpoint is http://localhost:9000, which is not the console UI.

The example bucket layout is:

```text
water-raw/
  <generator_run_id>/_RUN_COMPLETE
  <generator_run_id>/artifacts/districts.ndjson
  <generator_run_id>/artifacts/meters.ndjson
  <generator_run_id>/artifacts/network.inp
  <generator_run_id>/artifacts/run-manifest.json
  <generator_run_id>/artifacts/validation-report.json
  <generator_run_id>/delivery/district-sensors.ndjson
  <generator_run_id>/delivery/meter-readings.ndjson
water-restricted/
  <generator_run_id>/restricted/config.yml
  <generator_run_id>/restricted/scenario-truth.ndjson
  <generator_run_id>/restricted/fault-truth.ndjson
water-events/
  events/<topic>/<generator_run_id>/part-<partition>-<first>-<last>.ndjson.gz
  orchestration/stream.json
water-warehouse/
  # Iceberg-managed data and metadata files
```

Bucket names can be configured. The raw completion marker is uploaded last for a newly archived run. Use the manifest's `delivery_district_records` and `delivery_meter_records` when comparing these inputs, rather than substituting clean expected counts.

`artifacts/` contains reference entities and generation evidence. `delivery/` contains the measurements that are sent to Kafka, including configured faults. The clean `public/` measurements are retained locally in the generator run but are not uploaded by `archive-minio`.

Restricted truth is for evaluation after detection. Do not join its planted labels, locations or timing into a detector or claim that the shared local credentials enforce bucket-level access isolation.

The Kafka archive preserves delivery records, including duplicates, in gzip NDJSON batches. It is an at-least-once archive with commit-after-upload behavior, not an event-level exactly-once deduplicator. Allow for its normal flush interval while waiting for a small batch to appear.

### Locate generated files

| Workflow | Storage | Visible location |
| --- | --- | --- |
| Airflow generation | `<project>_pipeline-output` named volume | `/app/output` in generator tasks; `/opt/airflow/source-output` read-only in scheduler |
| Manual `generator generate` | Host bind mount | `Generator/output/` on host, `/app/output` in the manual container |
| Batch input | MinIO raw bucket | Run-specific `artifacts/` and `delivery/` objects |

List Airflow's generated directories:

```bash
docker compose exec airflow-scheduler ls /opt/airflow/source-output
```

For inspection, copy a selected Airflow run to a local folder. Replace the example ID below with the actual generator ID:

```bash
mkdir -p inspection
docker compose cp airflow-scheduler:/opt/airflow/source-output/run-0123456789abcdef ./inspection/
```

`inspection/` is a local example folder, not a configured pipeline output. Do not commit copied datasets or restricted truth. Editing a host `Generator/output/` file does not edit the separate Airflow named volume.

## 9. Use and inspect Flink

Open http://localhost:8081. Check the TaskManager registration and four available task slots before a new stream starts. After Airflow submits the jobs, those slots can be occupied.

| Managed name | Python script | Consumer group | Main purpose |
| --- | --- | --- | --- |
| `ewi-validation` | `job2_district_validation.py` | `ewi-validation` | Validate/deduplicate district deliveries and emit clean/dead-letter records |
| `ewi-features` | `job3_district_features.py` | `ewi-features` | Aggregate district features into fifteen-minute windows |
| `ewi-alerts` | `job3b_district_alerts.py` | `ewi-alerts` | Existing rules and relative district measurements |
| `ewi-level_shift` | `job3c_level_shift.py` | `ewi-level_shift` | Existing level-shift rule |

These run at parallelism one under Airflow. A running Flink service is not the same as a running Flink job; an empty Jobs page is expected before the pipeline's submission task.

List running jobs and inspect the REST overview:

```bash
docker compose exec flink-jobmanager /opt/flink/bin/flink list -r
curl --fail http://localhost:8081/jobs/overview
```

For a selected job, the UI exposes exceptions and checkpoints. The REST equivalents are `/jobs/JOB_ID/exceptions` and `/jobs/JOB_ID/checkpoints`, replacing `JOB_ID` with the actual ID. Checkpointing is configured every thirty seconds; file paths use the shared `flink-state` volume.

### Existing manual helpers

These are component-development tools, not substitutes to run alongside the Airflow submission tasks:

| Script | Behavior |
| --- | --- |
| `flink/scripts/start_all.sh` | Processes existing raw data one stage at a time by default and cancels each finished stage; `--live` leaves jobs running |
| `flink/scripts/start_all.sh --reset` | Deletes/recreates the five downstream topics first; use only in an intentionally disposable manual experiment |
| `flink/scripts/live_demo.sh` | Deletes/recreates both raw topics and all downstream topics before its live demonstration |
| `flink/scripts/stop_all.sh` | Attempts a savepoint stop for **every** running Flink job and logs paths |
| `flink/scripts/evaluate.sh` | Exports alert-topic records and invokes the offline evaluator |

The manual helpers use older hardcoded run defaults in some places. Select the actual run path and inspect each helper before using it. Airflow does not invoke those reset/demo scripts.

## 10. Use Spark and Trino

### Run the batch smoke entrypoint independently

A raw run must already have been archived successfully. From the repository root, replace the example ID with the actual generator ID:

```bash
docker compose build spark
docker compose run --rm --no-deps spark /opt/spark/work-dir/jobs/handoff_smoke.py --run-id run-0123456789abcdef
```

The service's entrypoint is already `spark-submit --master local[2] --driver-memory 1g`. Pass the script path and its arguments; do not add a second `spark-submit`. There is no persistent Spark master/worker cluster in this integration.

The smoke job checks the raw completion marker and physical validation report, counts actual delivery objects, reconciles them with the manifest, writes one Iceberg evidence row and reads it back. It streams raw NDJSON with the MinIO SDK; its raw read is not distributed Spark ingestion.

### Query with Trino

The UI at http://localhost:8082 shows query/server information. Use the CLI for SQL:

```bash
docker compose exec trino trino --execute "SHOW SCHEMAS FROM water"
docker compose exec trino trino --execute "SHOW TABLES FROM water.handoff"
docker compose exec trino trino --execute "SELECT run_id, district_records, meter_records FROM water.handoff.pipeline_runs"
```

After a smoke run, expect one row for its dataset with `360` district records and `48` meter records. Running the batch smoke again for that same dataset should retain one row. Before the first successful batch job, the `handoff` namespace/table may not exist; that is different from a server-connectivity failure.

The default table is integration evidence. Do not expect the project's proposed fact/dimension tables to appear until their transformations are implemented.

### Where Iceberg stores things

Spark and Trino share JDBC catalog `water`, schema version V1, database `catalog-db:5432/iceberg` and the MinIO warehouse. PostgreSQL contains the catalog pointers; MinIO contains Iceberg files. Both are required to retain queryable tables.

To inspect the catalog metadata without editing it:

```bash
docker compose exec catalog-db psql -U iceberg -d iceberg -c "SELECT catalog_name, table_namespace, table_name FROM iceberg_tables;"
```

Do not rename/move warehouse objects directly in MinIO. Change tables through an engine that understands Iceberg.

## 11. Run the generator manually

Use this workflow in a separate manual experiment if the same Kafka namespace is managed by Airflow. These commands use the host-mounted `Generator/output/` location, not Airflow's output volume.

### Initialize services

```bash
docker compose up -d kafka minio kafka-ui
docker compose run --rm init
```

`init` creates topics and buckets. It does not generate, upload, publish or transform measurements. Repeating it creates only missing resources.

### Generate and validate

For the small configuration:

```bash
docker compose run --rm --no-deps generator generate --config /app/configs/smoke.yml
```

For the pilot, use `/app/configs/pilot.yml` instead. Generation prints its run directory; copy that ID for following commands. These examples use an illustrative ID that you must replace:

```bash
docker compose run --rm --no-deps generator validate --run-dir /app/output/run-0123456789abcdef
docker compose run --rm --no-deps generator archive-minio --run-dir /app/output/run-0123456789abcdef
```

The ordinary `generate` command is the existing generator CLI. Airflow's separate `generate_pipeline_run.py` wrapper adds locking and completed-run reuse. Do not assume the ordinary CLI has identical retry behavior.

### Replay locally or publish to Kafka

Print stored events in replay order:

```bash
docker compose run --rm --no-deps generator replay --run-dir /app/output/run-0123456789abcdef --input-set delivery
```

Publish stored delivery events:

```bash
docker compose run --rm --no-deps generator publish-kafka --run-dir /app/output/run-0123456789abcdef --input-set delivery --speed 0
```

`--input-set public` selects clean measurement copies. `delivery` selects faulted delivery copies. `--speed 0` disables replay delay; a positive speed specifies simulated seconds per wall-clock second. Replay prints records; publication sends them to Kafka. Neither replaces `archive-minio`.

### Run the raw event archiver manually

After initialization in a manual project:

```bash
docker compose up -d archiver
docker compose logs --tail 100 archiver
```

It consumes both raw measurement topics and writes compressed batches to the configured event bucket. It is continuous, so an ordinary successful run does not exit after the current dataset. Stop it with `docker compose stop archiver` when finished; Airflow starts/reuses this service itself in the integrated workflow.

The generator also offers `archive-kafka --max-messages N` for a bounded experiment. It waits until it receives N messages; setting a count larger than the available input does not create an idle-time exit.

### Inspect the source contracts

The versioned measurement schemas are [district-sensor.v1.json](../Generator/schemas/district-sensor.v1.json) and [meter-reading.v1.json](../Generator/schemas/meter-reading.v1.json). The [generator behavior document](../Generator/GENERATOR_BEHAVIOR.md) explains hydraulic simulation, fault injection and timestamp semantics.

District readings measure one-minute intervals with event time at interval start. Meter readings measure fifteen-minute consumption with event time at interval end. Keep `run_id` and `event_id`, and align interval boundaries before computing water balance. `inlet_volume_m3` and `consumed_volume_m3` are already volumes in cubic metres.

## 12. Evaluate alerts and use analysis tools

Evaluation happens after detection and can access restricted truth. The evaluator is not part of the current Airflow DAG.

For a manual run whose source folder is already on the host:

```bash
./flink/scripts/evaluate.sh Generator/output/run-0123456789abcdef
```

Replace the ID. The helper consumes alerts to `/tmp/alerts.ndjson` and compares them with scenario/fault truth. An empty smoke scenario file is not a useful incident-accuracy benchmark.

If the run was generated in Airflow's volume, first copy that selected run out as described in section 8. Then invoke the evaluator directly with your copied truth paths and exported alerts:

```bash
python analysis/evaluate_alerts.py --alerts /tmp/alerts.ndjson --truth inspection/run-0123456789abcdef/restricted/scenario-truth.ndjson --faults inspection/run-0123456789abcdef/restricted/fault-truth.ndjson
```

This direct command assumes `/tmp/alerts.ndjson` was actually exported for the intended experiment. When using a non-default source start time, pass the matching `--run-start` as well. Read `python analysis/evaluate_alerts.py --help` for the interface. Keep evaluation outputs identifiable by the dataset being evaluated.

The other scripts in [analysis/](../analysis/README.md) are existing investigation tools for district validation, relative ordering, thresholds, anomaly windows and water balance. Several expect hardcoded historical run paths, previously exported `/tmp/*.ndjson` files or CSVs produced by other scripts. Read each file's input constants before running it. They are not registered production batch jobs and are not automatically exercised by the root pytest suite. Hand-checkable alert cases are in `flink/tests/`, bounded UDF/UDTF checks in `flink/tests/runtime/`, and the level-shift diagnostic in `flink/tools/`; see the [Flink component guide](../flink/README.md).

## 13. Stop, resume and recover

### Pause new workflow scheduling

```bash
docker compose exec airflow-scheduler airflow dags pause egypt_water_pipeline
docker compose exec airflow-scheduler airflow dags pause egypt_water_health
```

Wait for any intended in-flight tasks to finish before shutting down their services. Pausing a DAG alone does not stop already running work or the persistent stream processors.

### Restart only Airflow

For a transient scheduler/webserver restart while leaving Kafka/Flink/MinIO running:

```bash
docker compose restart airflow-scheduler airflow-webserver
```

`restart` does not apply changed images or environment variables. After changing those, use `docker compose --profile pipeline up -d airflow-webserver airflow-scheduler` to apply configuration. Do not restart the Flink cluster merely to refresh an Airflow DAG file.

### Preserve Flink state before stopping its cluster

Inspect running jobs first. The existing helper stops all of them with savepoints:

```bash
./flink/scripts/stop_all.sh
```

Confirm a successful savepoint path for each job in `flink/scripts/savepoints.log`. The helper can continue after a failed stop, so a script exit alone is not proof that all savepoints exist. For a targeted job instead of all jobs, replace the ID with the actual one:

```bash
docker compose exec flink-jobmanager /opt/flink/bin/flink stop --savepointPath /opt/flink/state/savepoints JOB_ID
```

Then stop the stack while retaining named volumes:

```bash
docker compose --profile pipeline down
```

`docker compose stop` retains containers as well as volumes, but it still stops Flink processes. Do not assume an in-memory job automatically resumes after either form of cluster shutdown.

### Restore jobs

Start the services again. Restore each intended job using its matching savepoint, Python script, original managed name, consumer group and parallelism one. For validation:

```bash
docker compose exec flink-jobmanager /opt/flink/bin/flink run -d -p 1 -Dpipeline.name=ewi-validation -s "file:///opt/flink/state/savepoints/REPLACE_WITH_SAVED_DIRECTORY" -py /opt/flink/jobs/job2_district_validation.py --group-id ewi-validation
```

Use the other script/name/group combinations from section 9 and each job's own saved path. The path must exist inside the shared state volume. Keep the job names stable so Airflow can reattach. Once recovery succeeds, clear the failed Airflow task instances that need to verify/reuse the jobs.

After publication, a cluster with no matching running managed jobs is refused by automatic submission. Starting jobs again from empty state against populated downstream topics is not equivalent to restoring them.

### Recover an interrupted publication

Download/inspect `water-events/orchestration/stream.json` and examine raw topic offsets, archived objects and task logs. `prepared` means publication has not been recorded as started; `published` means completion and raw offset targets were recorded. `publishing` means the outcome is ambiguous.

Do not delete that registry or change its status just to bypass a failure. A manual replay can duplicate output. Resolve it with the stream owner after inspecting the evidence, or use a separate project for a new experiment while retaining the old one for investigation.

### Reset only when that is the intended outcome

`docker compose --profile pipeline down -v` deletes the project's Compose-managed data volumes. It removes Kafka data, MinIO objects, metadata databases, logs, Airflow-generated source runs and Flink state. It is not a normal retry command. Manual host files under `Generator/output/` are separate and remain unless you delete them yourself.

A separate project is usually easier to investigate than destroying a failed experiment. The startup scripts do not automatically run topic resets, volume deletion or orphan removal.

## 14. Start a separate experiment

Use a new `COMPOSE_PROJECT_NAME` for pilot after smoke, for a manual streaming experiment or for a different generator configuration. The default host ports overlap, so stop the current stack before using the same ports for another project.

1. Pause the current health DAG and complete/save any Flink state you want to retain.
2. Run `docker compose --profile pipeline down` with the **old** project setting still active.
3. Edit `.env`, for example `COMPOSE_PROJECT_NAME=ewi-pilot`. Keep the old name recorded so you can reopen its volumes later.
4. Run the appropriate startup script.
5. Trigger the main DAG with `config=pilot` if that is the intended experiment.

The new project gets its own network and volumes. Reusing the same local image tags does not mean it reuses the old project's data. The manual `Generator/output/` bind mount is a shared host path across projects, so its local source directories are not isolated by project naming; MinIO/Kafka/named-volume state is.

To return to the old experiment, stop the new stack, restore the old project name in `.env`, start its services and restore any required Flink savepoints. Existing orphan containers from a previous architecture are not automatically started or deleted by this procedure.

## 15. Develop and test changes

### Decide what needs a rebuild

| Changed files | Action |
| --- | --- |
| `orchestration/dags/*.py` and its helper package | Files are mounted read-only into Airflow; allow scheduler parsing time, then check imports |
| `orchestration/Dockerfile` / Airflow Python dependencies | Rebuild `airflow-scheduler` image and recreate scheduler/webserver; review database compatibility for version changes |
| `Generator/src`, `Generator/configs`, `Generator/scripts` or generator dependencies | Rebuild generator/archiver images; refresh any long-running archiver deliberately |
| `batch/jobs` or batch dependencies | Rebuild `spark`; new disposable task containers use that image |
| `flink/jobs` | Job scripts are bind mounted in this development stack; an already submitted job still needs a deliberate state-aware update/redeployment |
| `flink/tools` or `flink/tests` | Development files are mounted read-only into JobManager and copied into new images; use an isolated stack for diagnostic submissions |
| `flink/Dockerfile` or connectors | Rebuild both Flink services; savepoints and recovery are needed when replacing the running cluster |
| Trino catalog properties | Recreate/restart Trino after reviewing the new configuration |
| Catalog `init.sql` | A populated PostgreSQL volume does not rerun initialization SQL; plan an explicit schema migration |
| `.env` / Compose service configuration | Apply affected services with `compose up -d`; `restart` alone does not replace container configuration |

Example build commands:

```bash
docker compose build generator archiver
docker compose build spark
docker compose build airflow-scheduler
docker compose build flink-jobmanager flink-taskmanager
```

### Add Amr AbdEl-Monem's batch job

1. Add the job under `batch/jobs/` and accept `--run-id`.
2. Use the existing environment for endpoints/credentials and the exact raw paths in section 8.
3. Define explicit schemas, rejected rows, deduplication keys, interval alignment, output table grains and run-scoped write behavior.
4. Add the approved name/path to `runtime.spark_batch()` and to the DAG's `batch_job` parameter enum.
5. Extend or dispatch `runtime.verify_trino()` to validate the real output tables. Its current query/result receipt is specific to `handoff_smoke`.
6. Rebuild Spark, check DAG imports and run the relevant experiment.

Use [AMR_ABDEL_MONEM_HANDOFF.md](AMR_ABDEL_MONEM_HANDOFF.md) and the [batch requirements guide](guides/batch-processing.md) for the full contract. Arbitrary shell commands or arbitrary user-selected paths are not DAG parameters. If you add a generator config, also update the approved config choices in both the DAG and generation/runtime helpers; adding only a YAML file does not expose it in Airflow.

### Run local tests

Use Python 3.11 or 3.12. These examples create a local environment without installing the application into the system Python.

Linux/WSL:

```bash
python3.12 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements-dev.txt
python -m pytest tests
cd Generator
python -m pytest tests
cd ..
```

Use `python3.11` if that is the installed supported interpreter. In PowerShell, create the environment with `py -3.11 -m venv .venv`, then use `.\.venv\Scripts\python.exe` in place of `python` for installation and pytest. Run the generator suite from its directory in either shell.

The root suite includes a real Airflow DAG parse check when the Airflow runtime is installed. A run that skips that module is not evidence of a successful real DAG parse. The latest recorded full suite result is 28 repository cases plus four existing generator cases.

Configuration and shell checks:

```bash
docker compose --profile pipeline config --quiet
bash -n scripts/start-pipeline.sh
python -m compileall -q orchestration batch/jobs Generator/scripts
git diff --check
```

The Bash script was syntax-checked and its startup operations were exercised. The PowerShell script was kept equivalent; a native PowerShell execution was not recorded in the implementation checks.

## 16. Troubleshoot failures

Start with the first failing task or service. Inspect narrow logs instead of resetting the environment:

```bash
docker compose --profile pipeline ps -a
docker compose logs --tail 100 airflow-scheduler
docker compose logs --tail 100 kafka archiver flink-jobmanager flink-taskmanager trino
```

| Symptom | Check | Corrective action |
| --- | --- | --- |
| Docker command missing in Ubuntu | Docker Desktop's WSL integration and `wsl.exe -l -v` | Enable integration for that WSL 2 distribution and reopen the terminal |
| Docker CLI exists but no server responds | Docker Desktop running; `docker version` server section | Start/fix the engine before running Compose |
| Scheduler gets Docker socket permission denied | `id` and `stat -c '%g %a' /var/run/docker.sock` inside scheduler | Set the matching `DOCKER_GID`, recreate scheduler, verify `ping()` |
| Spark cannot install MinIO 7.2.15 | Python version inside the Spark base image | Use the supplied Spark-specific 7.2.10 pin; newer Airflow's pin is separate |
| Flink build says `CANCELED` while Spark failed | The first actual `ERROR` in the build output | Fix that build first, then rerun; cancellation alone is not a Flink dependency error |
| `ewi-net` missing | Existing container's network mode versus current Compose network | Use the updated files with network labels and rerun startup; volumes need not be deleted |
| `unknown flag: --no-deps` for `create` | Startup script command | Use the corrected `up --no-deps --no-start archiver` step |
| Orphan-container warning | Whether old service containers are stopped and still needed | Current scripts preserve them; inspect before any deliberate removal |
| Host port already allocated | Another project/service using 8080–8083 or 9000/9001 | Stop the intended old stack or deliberately change host mappings |
| `airflow-init` exits nonzero | `docker compose logs airflow-init` and metadata database health | Resolve migration/credential/user creation error; do not overwrite an older incompatible database volume |
| Login uses an old password after editing `.env` | Existing Airflow user account | Reset the existing password through UI/CLI; initialization does not replace it |
| PostgreSQL authentication fails after editing `.env` | Existing database password versus client environment | Update server account and clients consistently; initial variables do not rewrite existing accounts |
| Missing DAG | `airflow dags list-import-errors` and mounted files | Fix import/dependency/path issue and wait for parsing; ensure main DAG is unpaused |
| DAG run remains queued | Paused DAG or scheduler health | Unpause the DAG and inspect scheduler health/logs |
| Topics/buckets absent before first trigger | Whether initialization task/manual `init` ran | Starting service processes alone does not initialize application resources |
| Raw bucket empty | `archive_raw_to_minio` or manual `archive-minio` | Generation and initialization alone do not upload the dataset |
| Host output folder empty after an Airflow run | Named output volume versus manual bind mount | Inspect `/opt/airflow/source-output` in scheduler or MinIO |
| `Unmanaged Kafka data exists` | Existing offsets and missing stream registry | Use a fresh project for the orchestrated run; do not silently adopt/reset the old experiment |
| Namespace belongs to another run | Registry's dataset ID and selected config | Use the same config or create a separate project |
| Publication interrupted / registry `publishing` | Publisher logs, offsets and uploaded evidence | Investigate before replay; no automatic safe reset is implemented |
| Flink job lost running state / needs savepoint | Published stream and current/history job list | Restore the matching jobs with names/groups/savepoints before clearing tasks |
| Unmanaged Flink jobs are running | Jobs submitted through manual tools | Use a separate stack or deliberately resolve the competing jobs |
| Readiness sensor keeps rescheduling | Flink states and task slots | Confirm four jobs can reach RUNNING; inspect memory, exceptions and manager registration |
| Stream/archive sensor times out | Group commits, checkpoint counts, archiver logs and MinIO objects | Fix the blocked consumer/upload/job; quiet output alone does not prove completion |
| Alert topic empty | Dataset scenarios, rule requirements and event-time windows | A clean short smoke run may legitimately have no alerts |
| Trino table/namespace missing | Whether Spark completed its write | Start with the Spark task logs; the proposed analytical schema is not yet implemented |
| Spark raw `s3a://` read fails in a new job | Missing/mismatched Hadoop S3A dependencies | Configure/test distributed raw ingestion; Iceberg S3FileIO alone does not configure it |
| Trino count verification fails | Selected run, actual delivery counts and written evidence | Inspect the mismatch; do not mark verification successful manually |
| Trino errors loading warehouse files | Matching MinIO credentials, bucket/path, catalog and retained objects | Correct configuration and restore coherent metadata/files as needed |
| Hard-killed task left a container | Containers labeled `ewi.managed=airflow-task` | Identify ownership and whether work is still running before retrying/removing it |

Useful focused diagnostics:

```bash
docker compose exec airflow-scheduler id
docker compose exec airflow-scheduler stat -c '%g %a' /var/run/docker.sock
docker compose exec airflow-scheduler airflow dags list-import-errors --output json
docker compose exec flink-jobmanager /opt/flink/bin/flink list -r
docker ps -a --filter label=ewi.managed=airflow-task
docker network ls
docker volume ls
```

For a particular stale container, inspect only the fields needed rather than dumping its environment. Replace its name with the actual one:

```bash
docker inspect egypt-water-intelligence-kafka-1 --format '{{.HostConfig.NetworkMode}}'
```

## 17. Preserve and back up state

### Know what persists

These logical volume keys normally resolve to `<project>_<key>`. `pipeline-output` also has that name set explicitly.

| Volume key / storage | Contains |
| --- | --- |
| `kafka-data` | Broker log data at the configured mounted path |
| `minio-data` | Raw/restricted/event objects and Iceberg files |
| `catalog-data` | Iceberg JDBC catalog metadata |
| `airflow-data` | Airflow metadata database |
| `airflow-logs` | Scheduler and task logs |
| `pipeline-output` | Airflow-generated source run folders |
| `flink-state` | File checkpoints and savepoints |
| `Generator/output/` host directory | Manually generated source runs |

The Kafka image can also attach anonymous volumes. Inspect actual mounts before migration or backup instead of assuming the table describes every Docker-created mount. The network repair preserved the observed Kafka named and anonymous volume identities.

### Make a consistent backup

The repository does not provide an automated backup service or a tested full restore script. For a coherent backup, pause new pipeline work, wait for writes to finish, record the project/configuration and save Flink state. Back up the related databases and MinIO objects from a consistent experiment state. A live `tar` of a writing database or object store is not a substitute for a consistent backup procedure.

Example logical database dumps, using files inside the containers and `compose cp` to preserve binary format across shells:

```bash
mkdir -p backups
docker compose exec -T catalog-db pg_dump -U iceberg -d iceberg -Fc -f /tmp/iceberg-backup.dump
docker compose cp catalog-db:/tmp/iceberg-backup.dump ./backups/iceberg-backup.dump
docker compose exec -T airflow-db pg_dump -U airflow -d airflow -Fc -f /tmp/airflow-backup.dump
docker compose cp airflow-db:/tmp/airflow-backup.dump ./backups/airflow-backup.dump
```

Back up MinIO objects/warehouse files, generator output and Flink savepoints as well. For a stopped named-volume file backup, an example in Bash is:

```bash
docker volume inspect egypt-water-intelligence_pipeline-output
docker run --rm --volume egypt-water-intelligence_pipeline-output:/source:ro --mount "type=bind,source=$PWD/backups,target=/backup" alpine:3.20 tar -czf /backup/pipeline-output.tar.gz -C /source .
```

Replace the volume with the actual existing project volume; inspect it first so a typo does not create a new empty volume. Apply stopped-volume archiving only to the intended volumes after their writers have stopped. Keep private credentials and restricted truth out of Git; `backups/` and `inspection/` are ignored local folders, not pipeline inputs.

### Restore expectations

Restore into an intentionally selected project while services that would write to that state are stopped. Restore the databases, MinIO data, source output and Flink state together; then verify catalogs/objects and restore Flink jobs before resuming Airflow. Use PostgreSQL restore tools for the logical dumps and your object/volume backup method for files.

Restoring only `water-warehouse` objects without the catalog, or only Airflow's database without the source/stream state it refers to, is incomplete. Validate the restore with the acceptance checks before treating it as a usable experiment. These backup commands and restore expectations are operating examples, not recorded successful restore-test evidence.

## 18. Acceptance checklist

Use a dedicated project and select the dataset deliberately. This is the remaining full integration verification; do not interpret previous service startup or isolated component tests as its completion.

1. Start the stack and confirm the expected services, metadata/scheduler health, no DAG import errors and scheduler Docker `ping()`.
2. Trigger `egypt_water_pipeline` with `config=smoke` and `batch_job=handoff_smoke` in that dedicated project.
3. Confirm all twelve tasks succeed. Record both the Airflow run ID and generator run ID.
4. Verify raw artifacts, delivery files and completion marker in MinIO. Verify restricted files are separate and event batches exist under the two raw topic paths.
5. Confirm exactly four managed Flink jobs, RUNNING states, completed checkpoints and consumer progress. Do not require an alert for the clean smoke dataset.
6. Query `water.handoff.pipeline_runs` through Trino. Expect one row for the dataset with 360 district records and 48 meter records.
7. Trigger the same dataset using a new Airflow run ID. Confirm publication is reused, jobs are reattached and the evidence row is not duplicated.
8. Unpause/trigger `egypt_water_health` and verify its result. In a controlled test, stop the archiver, check that monitoring detects it and restart it using the intended pipeline recovery task.
9. Confirm that choosing another generator dataset in that claimed project fails the namespace guard. Use a separate project for pilot.
10. Before handoff, retain commands, results and any failure logs with the generator ID. Analytical table correctness and detector precision/recall need separate tests after their team implementations exist.

The changes and existing verification evidence are recorded in [IMPLEMENTATION_AND_DECISIONS.md](IMPLEMENTATION_AND_DECISIONS.md). Consult the [short orchestration runbook](ORCHESTRATION.md) for a compact state/recovery reference and [Amr AbdEl-Monem's handoff](AMR_ABDEL_MONEM_HANDOFF.md) for his implementation checklist.

## Official reference documentation

- [Docker Desktop WSL setup](https://docs.docker.com/desktop/features/wsl/) for enabling Ubuntu integration.
- [Docker Compose `up` reference](https://docs.docker.com/reference/cli/docker/compose/up/) for creation, recreation, `--no-deps` and `--no-start` behavior.
- [Airflow 2.10.5 CLI usage](https://airflow.apache.org/docs/apache-airflow/2.10.5/howto/usage-cli.html) for Airflow command conventions; the examples above were checked against this runtime's CLI help.
