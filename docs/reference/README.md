# Reference material (not active)

Nothing in this folder is used by the running pipeline. These files are kept so earlier work can still be read and compared.

| Path | What it is |
| --- | --- |
| `Batch_Pipeline_Current_Code_and_Flow.pdf` | The batch teammate's write-up of `batch/jobs/`. Note: it says the Kafka archiver writes `water-raw/<run>/delivery`; that path is actually written by `water-generator archive-minio`. The archiver writes `water-events/events/...`. |
| `previous-airflow-attempt/` | Documentation from an earlier Airflow-based orchestration. Its code (`orchestration/dags`, `infrastructure/`, `scripts/`, `.env.example`) was never added to this repository, so its commands do not work here. |
| `previous-iceberg-attempt/` | An earlier additive Iceberg REST catalog + Trino + `load.py` loader for Flink outputs. Its files expected an `iceberg/` folder that does not exist. |

The active pipeline is described in [../README.md](../README.md).
