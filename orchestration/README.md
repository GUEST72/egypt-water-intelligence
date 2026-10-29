# Orchestration (Dagster)

Dagster runs on the host (Windows PowerShell or WSL) and drives the Compose services with the same
commands a person would type: `docker compose run/exec`, the MinIO API on `localhost:9000`, Kafka on
`localhost:29092`, the Flink REST API on `localhost:8081` and Trino on `localhost:8082`.

How every asset works and why: [docs/03-orchestration.md](../docs/03-orchestration.md).
Running it: [docs/RUNNING.md](../docs/RUNNING.md) (`.\run.ps1 dagster`, `.\run.ps1 pipeline`).

## Setup (once)

```powershell
winget install --id=astral-sh.uv -e      # user-level tool; Python 3.12 is fetched by uv
cd orchestration
uv sync                                  # creates orchestration/.venv from uv.lock
```

## Start the UI

```powershell
powershell -ExecutionPolicy Bypass -File orchestration\dev.ps1   # WSL: orchestration/dev.sh
```

Open http://localhost:3000. Run history is kept in `orchestration/.dagster_home` (git-ignored).
The Compose stack must already be running (`docker compose up -d` from the repository root).

## Definitions

| Name | Kind | What it does |
| --- | --- | --- |
| `generator_run`, `raw_run_upload` | assets (group `source`) | validate a generated run; upload reference files and truth, `_RUN_COMPLETE` last |
| `kafka_delivery_streams`, `raw_event_archive`, `flink_district_outputs` | assets (group `streaming`) | publish once (resumable), prove the archive is complete, prove Flink finished |
| `batch_silver_gold`, `batch_validation` | assets (group `batch`) | Spark build and validation from the proven archive |
| `streaming_iceberg_facts` | asset (group `lakehouse`) | Flink's topics into `water.streaming` up to frozen offsets |
| `alert_evaluation` | asset (group `evaluation`) | alerts vs the restricted answer key, cross-checked with `evaluate_alerts.py` |
| `dashboard_marts`, `dashboard_exports` | assets (groups `marts`, `dashboard`) | Trino marts per run; Parquet exports for Power BI |
| `generate_dataset_job` | job | generates a run for a file in `configs/` (reused if it exists) and registers its partition |
| `ingest_source`, `stream_run`, `batch_run`, `streaming_lakehouse`, `build_marts`, `full_pipeline` | jobs | the asset groups above, alone or all together |
| `live_demo` | job | replays `configs/demo-live.yml` into the `demo.` namespace for the Grafana board |
| `generator_run_sensor`, `archive_complete_sensor` | sensors (off by default) | register new runs; start the batch job once per run whose archive is proven |

Every asset that produces something visible in a UI logs a `[SCREENSHOT]` line with the URL to open.

## Tests

```powershell
cd orchestration
uv run pytest
uv run dagster definitions validate
```
