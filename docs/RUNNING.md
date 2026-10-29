# Running the system, start to finish

Everything below runs from **Windows PowerShell** in the repository folder. Every step uses `run.ps1`, which calls
the same `docker compose` and `dagster` commands described in [3. Orchestration](03-orchestration.md).

## 0. What you need (once)

| Requirement | Why | Check |
|---|---|---|
| Docker Desktop with at least **10 GB** of memory for WSL 2 | the whole stack uses about 5–9 GB | `docker info` shows MemTotal ≈ 10 GB |
| `uv` (`winget install --id=astral-sh.uv -e`) | installs Dagster's Python 3.12 environment | `uv --version` |
| About 25 GB free disk | images, Kafka, MinIO and the generated dataset | |
| Free ports 3000, 3001, 8080–8082, 9000, 9001, 18080, 29092 | the web UIs and Kafka | |

To give WSL 2 more memory, create `%USERPROFILE%\.wslconfig` containing `[wsl2]` and `memory=10GB` on two lines, then
run `wsl --shutdown` and restart Docker Desktop.

## 1. Build (once, about 15 minutes)

```powershell
.\run.ps1 setup
```

Builds the generator, Flink, Spark and live-bridge images and installs the Dagster environment in
`orchestration/.venv`.

## 2. Start the services

```powershell
.\run.ps1 up
```

Starts Kafka, MinIO, the archiver, Flink, Postgres (catalog), Trino, Spark History, the live bridge and Grafana, and
waits until they are healthy. It does **not** generate or publish anything.

## 3. Start Dagster (keep this window open)

```powershell
.\run.ps1 dagster
```

Open http://localhost:3000. Use a second PowerShell window for the next steps.

## 4. Run the pipeline

```powershell
.\run.ps1 pipeline
```

What happens, in order:

| Step | Time (this laptop) | What to look at |
|---|---|---|
| generate the dataset (`configs/pipeline-large.yml`), skipped if it already exists | ~18 min the first time | `Generator/output/run-72b96af88a342375/` |
| validate it and upload reference files | ~8 min the first time | MinIO http://localhost:9001 → `water-raw` |
| start the 4 Flink jobs, publish 3.8 M events into Kafka | ~1 hour the first time | Flink http://localhost:8081, Kafka UI http://localhost:8080, Grafana http://localhost:3001 |
| prove the archive is complete, prove Flink finished | ~1 min | MinIO → `water-events` |
| Spark batch build + validation | ~11 min | Spark UI http://localhost:4040 during the job, Spark History http://localhost:18080 after |
| streaming facts into Iceberg, evaluation, marts, exports | ~2 min | Trino http://localhost:8082 |

The run's progress, logs and results are in Dagster → *Runs*. Lines starting with `[SCREENSHOT]` say which page to open.

**Running it again** takes about 4 minutes: every step that is already done is verified and skipped.

To run a different dataset, put a configuration in `configs/` and run `.\run.ps1 pipeline -Config my-config.yml`.

## 5. Look at the results

| Where | What |
|---|---|
| http://localhost:3001 | Grafana live board. Choose *Stream = main* and time range 2026-01-01 to 2026-01-26 to see the full run |
| http://localhost:3000 → *Assets* | every stage with its recorded numbers (e.g. `alert_evaluation` shows the scenario table) |
| http://localhost:8082 | Trino; or query from the command line: `docker compose exec trino trino --execute "SELECT * FROM water.marts.mart_incident_balance"` |

## 6. Live demo (for the presentation)

```powershell
.\run.ps1 demo
```

Open http://localhost:3001 first and set *Stream = demo*. The job stops the main Flink jobs with savepoints (their
results are already saved), recreates only the `demo.*` topics, starts the Flink jobs on them and replays a 2-day
dataset in about 6 minutes; incidents appear on the board roughly every 40 seconds. It can be repeated; the main
run's data is never touched. **Run it once before the presentation**: it has not been tested end to end yet.

## 7. Stop

```powershell
.\run.ps1 down
```

Stops everything and keeps all data (Kafka, MinIO, the catalog, Flink savepoints). `.\run.ps1 up` brings it back.

## Rules that protect the data

- **Never** run `docker compose down -v`: it deletes Kafka, MinIO and the catalog, i.e. every result.
- **Never** run `flink/scripts/live_demo.sh` or `start_all.sh --reset` against this stack: they delete Kafka topics
  that the archive and the publication record depend on. Use `.\run.ps1 demo` instead.
- If a publication is interrupted, do not publish again. Re-run the `kafka_delivery_streams` asset in Dagster with the
  config `resume_interrupted: true`: it proves what already arrived and sends only the rest.

## When something goes wrong

| Symptom | Cause and fix |
|---|---|
| A Spark step fails with "port is already allocated" | another Spark container is still running: `docker ps`, wait for it or stop it |
| `kafka_delivery_streams` says the archiver is not running | `docker compose up -d archiver` |
| A Flink job is reported as not running after publication | the Flink cluster was restarted before its outputs were verified; restore the job from its savepoint (`flink/scripts/stop_all.sh` logs savepoint paths) |
| Flink jobs restart repeatedly | not enough memory for Docker; see step 0 |
| `generator_run` complains the manifest is from an older generator | regenerate the run with the current `Generator/` code |
| Spark says `SourceNotReady` | the run's upload or archive proof is missing; materialize `raw_run_upload` / `raw_event_archive` first |
