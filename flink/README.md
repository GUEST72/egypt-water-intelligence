# Flink component

The Airflow-managed stream uses exactly the four Python entrypoints in `jobs/`:

| Job | Purpose |
| --- | --- |
| `job2_district_validation.py` | District validation, deduplication and dead-letter handling |
| `job3_district_features.py` | Fifteen-minute district features |
| `job3b_district_alerts.py` | Existing alert rules and relative district measurements |
| `job3c_level_shift.py` | Existing level-shift rule |

`orchestration/dags/water_pipeline/runtime.py` owns the managed names, groups and submission order. The [usage guide](../docs/USER_GUIDE.md) explains the integrated workflow and recovery; the [manual component guide](../docs/guides/manual-kafka-flink.md) covers separate experiments.

## Layout

```text
jobs/            The four pipeline entrypoints
scripts/         Existing manual start, stop, live-demo and evaluation helpers
tests/           Input fixtures and hand-checkable alert cases
  runtime/       Small bounded Python UDF and UDTF checks
tools/           Optional level-shift diagnostic job
Dockerfile       PyFlink, Kafka connector and bundled component files
```

JobManager mounts `jobs/`, `tools/` and `tests/` from this checkout; the latter two mounts are read-only. New images also contain all three directories. Keep `jobs/` for actual pipeline entrypoints rather than scratch alternatives.

## Hand-checkable alert cases

Run from the repository root. The `make` subcommands only generate local JSON; they do not publish to Kafka or run a detector:

```bash
python flink/tests/job3b_cases.py make > /tmp/features_cases.ndjson
python flink/tests/job3c_cases.py make > /tmp/relative_cases.ndjson
```

In an isolated manual test, publish the appropriate input to the detector's configured topic, run the matching job and export its alert output. Check the exported records with:

```bash
python flink/tests/job3b_cases.py check /tmp/alerts_out.ndjson
python flink/tests/job3c_cases.py check /tmp/alerts_out.ndjson
```

Use the correct output file for each experiment. These programs are manual case generators/checkers, not root-suite pytest tests. `tests/district_cases.ndjson` is the retained district validation fixture.

## Runtime diagnostics

On an isolated development stack with available slots, the bounded checks can be submitted with:

```bash
docker compose exec flink-jobmanager /opt/flink/bin/flink run -p 1 -py /opt/flink/tests/runtime/udf_check.py
docker compose exec flink-jobmanager /opt/flink/bin/flink run -p 1 -py /opt/flink/tests/runtime/udtf_check.py
```

Their results appear in TaskManager logs. The UDF check also supports `UDF_TEST=udaf`; inspect its module docstring for expected values. Recreate JobManager with the updated Compose mounts or rebuild the image before using the relocated paths on an already existing container.

`tools/job3c_debug.py` emits daily or step intermediate views for investigating the level-shift rule. It requires `--view`, `--out-topic` and `--group-id`; prepare the intended output topic explicitly. It is not an Airflow entrypoint. Run diagnostics separately from the managed stream because the orchestration refuses competing unmanaged jobs.

The old print-only `job1_district_window.py` prototype and the unreferenced no-dedup copy of the feature job were removed during cleanup. The four pipeline jobs and their detection behavior were retained.
