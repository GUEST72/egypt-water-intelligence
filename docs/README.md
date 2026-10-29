# Documentation

Read these in order the first time. Each one explains *what* happens and *why*, with screenshots from a real run.

Start with the **[interactive pipeline diagram](pipeline-diagram.html)** (open it in a browser): click any component
for what it does and how Dagster proves it, or use ◀ ▶ for an 11-step tour. **Present** (or `F`) switches to a
full-screen layout for presentations. `pipeline-diagram.html#present&step=5` opens it directly at a step.

| # | Document | You will understand |
|---|---|---|
| 1 | [Overview](01-overview.md) | what the project does, the story of one run, the tools and the dataset |
| 2 | [Architecture](02-architecture.md) | how the pieces connect and where every piece of data lives |
| 3 | [Orchestration with Dagster](03-orchestration.md) | every pipeline step, how it proves success, why re-running is safe |
| 4 | [Streaming path](04-streaming.md) | Kafka, the archiver, Flink, Flink → Iceberg, the live board and demo |
| 5 | [Batch path](05-batch.md) | the Spark job, incremental processing, every edit we made to it |
| 6 | [Lakehouse, marts and results](06-lakehouse-and-results.md) | Iceberg, Trino, the marts, the evaluation and the dashboards |
| 7 | [Design decisions](07-design-decisions.md) | the problems that shaped the design, and what makes it different |
| 8 | [What we changed](08-what-we-changed.md) | file by file: kept, moved, edited, added |
| – | [Running the system](RUNNING.md) | the commands, start to finish |

## Component and teammate guides

| Document | Purpose |
| --- | --- |
| [Generator behavior](../Generator/GENERATOR_BEHAVIOR.md) | Simulation, event contracts, timestamps and injected faults |
| [Generator guide for the Flink work](guides/generator-guide-for-flink.md) | The two event streams, timestamps and consumer groups, as handed to the Flink teammate |
| [Generator guide for the batch work](guides/generator-guide-for-batch.md) | Raw inputs, schemas and water-balance guidance, as handed to the batch teammate |
| [Batch processing requirements](guides/batch-processing.md) | Run-scoped incremental contract, table grains and acceptance evidence for `batch/` |
| [Flink: running Kafka and Flink](../flink/README.md) | The Flink teammate's guide to the four streaming jobs and their scripts |
| [Orchestration project](../orchestration/README.md) | The Dagster project's setup and tests |
| [Injected-pattern report](reports/injected-patterns.md) | What the simulated scenarios and faults look like in the data |
| [Reference material](reference/README.md) | Earlier, inactive orchestration attempts and the batch PDF |
