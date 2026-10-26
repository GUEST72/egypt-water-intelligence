# Egypt Water Intelligence

Synthetic water telemetry flows through Kafka into Flink and a MinIO raw archive. Airflow orchestrates generation, validation, upload, streaming job startup, publication, progress checks, Spark/Iceberg processing and Trino verification.

All services are defined in [docker-compose.yml](docker-compose.yml). Profiles select the integrated `pipeline` services and the `tools` Spark runtime.

Amr Yousri owns orchestration and shared infrastructure. Amr AbdEl-Monem owns the analytical PySpark/Iceberg transformations; a working Spark smoke entrypoint is included to verify his runtime and storage connections. Ahmed's existing Flink jobs publish features and alerts to Kafka.

From the repository root, start the complete development environment:

```bash
./scripts/start-pipeline.sh
```

Or in Windows PowerShell:

```powershell
.\scripts\start-pipeline.ps1
```

The scripts create `.env` from `.env.example` when missing. Open [Airflow](http://localhost:8083), log in with the configured account (example: `amr` / `waterlocal`), and trigger `egypt_water_pipeline` with `config=smoke`. Unpause `egypt_water_health` for ongoing monitoring.

Read the [complete usage guide](docs/USER_GUIDE.md) for setup, configuration, daily operation, component commands, troubleshooting and recovery. Read [what changed and why](docs/IMPLEMENTATION_AND_DECISIONS.md) for the file-by-file implementation, design choices, fixes and verification evidence.

The [documentation index](docs/README.md) also links [Amr AbdEl-Monem's handoff](docs/AMR_ABDEL_MONEM_HANDOFF.md), the [orchestration runbook](docs/ORCHESTRATION.md) and the existing component guides.

Repository layout:

```text
Generator/       Synthetic source generation, schemas and component tests
orchestration/   Airflow image, DAGs and task helpers
flink/           Pipeline jobs, scripts, diagnostic tools and stream test cases
batch/           Spark image and batch entrypoints
infrastructure/  Iceberg catalog database and Trino configuration
analysis/        Offline evaluation and exploratory analysis
tests/           Shared orchestration and integration-contract tests
scripts/         Linux/WSL and PowerShell startup scripts
docs/            Operating guides and implementation decisions
  guides/        Component development and analytical requirements
  reports/       Data/scenario analysis reports
```

The [generator](Generator/README.md) and [manual Kafka/Flink guide](docs/guides/manual-kafka-flink.md) remain available for component development. Airflow uses its own named output volume; manual generation still writes to `Generator/output/`. Use separate Compose projects for manual experiments and orchestrated runs.

The included batch smoke job verifies infrastructure. Analytical facts/dimensions, water-balance transformations and direct Flink-to-Iceberg sinks remain team deliverables.
