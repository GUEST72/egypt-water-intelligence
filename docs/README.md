# Documentation

Start with the document that matches what you need to do.

| Document | Purpose |
| --- | --- |
| [Complete setup and usage guide](USER_GUIDE.md) | Installation, WSL/Docker, configuration, startup, Airflow runs, component commands, inspection, development, troubleshooting, recovery and backups |
| [What changed and why](IMPLEMENTATION_AND_DECISIONS.md) | Scope, architecture, file-by-file changes, design decisions, fixes, verification evidence and remaining work |
| [Amr AbdEl-Monem's handoff](AMR_ABDEL_MONEM_HANDOFF.md) | Simple batch development guide: where to code, reuse generated data, run Spark directly and validate without Airflow/Kafka/Flink |
| [Orchestration runbook](ORCHESTRATION.md) | Compact DAG, retry, stream ownership, monitoring and savepoint reference |
| [Batch processing requirements](guides/batch-processing.md) | Analytical grains, quality checks, interval alignment, water balance and acceptance evidence |
| [Generator behavior](../Generator/GENERATOR_BEHAVIOR.md) | Existing simulation, event contracts, timestamps, faults and raw data behavior |
| [Manual Kafka/Flink guide](guides/manual-kafka-flink.md) | Existing manual component workflow; use a separate project from an Airflow-owned stream |
| [Injected-pattern report](reports/injected-patterns.md) | Existing data/scenario analysis context |
| [Flink component layout](../flink/README.md) | Runtime jobs, diagnostic tools and manual test cases |
| [Offline analysis tools](../analysis/README.md) | Evaluator and exploratory scripts, with their expected inputs |

The integrated DAG uses a Spark infrastructure smoke job. The standalone batch starter provides cleaned events, reference tables, quality evidence and a fifteen-minute water balance; see Amr AbdEl-Monem's handoff to run and validate it independently. Further analytical tables, DAG integration and direct Flink-to-Iceberg sinks remain distinct deliverables. The implementation guide records the original infrastructure checks.

Commands generally assume the repository root. Linux/WSL Bash and Windows PowerShell startup examples are both provided in the usage guide. Live credentials belong in the private `.env`; committed documentation uses example defaults only.
