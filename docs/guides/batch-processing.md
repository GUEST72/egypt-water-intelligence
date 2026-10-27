# Batch processing guide: raw data to analytical tables

Amr AbdEl-Monem owns PySpark batch transformation and analytical Iceberg storage. Amr Yousri owns Airflow and shared Compose infrastructure. Start with [Amr AbdEl-Monem's handoff](../AMR_ABDEL_MONEM_HANDOFF.md) for a simple development and validation workflow using only MinIO, the catalog database, Trino and disposable Spark containers. Integrate with Airflow after the standalone jobs pass. The [orchestration runbook](../ORCHESTRATION.md) covers scheduling, monitoring and retries.

The integrated stack now includes Spark, a persistent JDBC Iceberg catalog and Trino. `batch/jobs/handoff_smoke.py` proves that real raw objects can be read and a Spark-written Iceberg record can be queried through Trino. It does not implement the analytical tables described here.

The standalone starter `batch/jobs/build_batch.py` implements reference tables, cleaned events, rejected-delivery evidence, quality counts and a coverage-aware fifteen-minute balance. `validate_batch.py` checks those saved outputs through Spark and Trino; `test_batch.py` provides small fixtures. These jobs can run without Airflow, Kafka or Flink and are not yet registered in the DAG. Daily facts and further analytical requirements below remain work to extend the starter.

## Source contract

Read a selected, completed generated run from MinIO. Do not depend on a teammate's local `Generator/output/` folder or a historical hardcoded run ID. Require `<run_id>/_RUN_COMPLETE` and use `<run_id>/artifacts/run-manifest.json` to establish the run and expected counts.

The actual raw layout is:

```text
water-raw/<run_id>/artifacts/districts.ndjson
water-raw/<run_id>/artifacts/meters.ndjson
water-raw/<run_id>/artifacts/network.inp
water-raw/<run_id>/artifacts/run-manifest.json
water-raw/<run_id>/artifacts/validation-report.json
water-raw/<run_id>/delivery/district-sensors.ndjson
water-raw/<run_id>/delivery/meter-readings.ndjson
```

The uploader archives clean reference entities and **delivery** measurements, including configured faults. It does not archive clean public measurements. Configuration, scenario truth and fault truth go to `water-restricted`, outside transformation inputs. Kafka events are separately archived as compressed NDJSON in `water-events/events/<topic>/<run_id>/part-<partition>-<first>-<last>.ndjson.gz`.

All measurements use cubic metres, cubic metres per second and metres of water head, as named in the versioned schemas. The generator has meter and district entities; it has no separate customer table, installation date, prepaid flag or district pipe age. Preserve the existing attributes and do not invent missing source data to fill a proposed star schema.

## Event identities and timestamps

Use `Generator/schemas/meter-reading.v1.json` and `Generator/schemas/district-sensor.v1.json` as the event contracts. Delivery copies can violate canonical measurement constraints intentionally: null measurements, corrupt quality codes or duplicate deliveries must have explicit handling.

Both streams carry `schema_version`, `event_id`, `run_id`, `district_id`, interval boundaries, event/availability times and `quality_code`. Keep `run_id` as an isolation boundary and deduplicate by `(run_id, event_id)` with a documented policy for conflicting duplicates.

District events represent one minute; `event_time_utc` equals interval start. Their measures include `inlet_flow_m3_s`, `inlet_volume_m3` and `pressure_m_h2o`.

Meter events represent fifteen minutes; `event_time_utc` equals interval end. `consumed_volume_m3` is the interval's delivered volume, not a cumulative register. `meter_id` identifies the meter.

All timestamps are UTC ISO 8601. Demand calendars use Africa/Cairo, but timestamp parsing and interval joins must retain explicit timezone semantics. Do not equate event time with interval start across both streams.

## Transformation stages

Keep the raw objects immutable. Make the stages and their evidence inspectable:

1. Read NDJSON with explicit schemas and record source counts. If using the Kafka archive, support gzip and retain object/topic/offset lineage.
2. Validate identifiers, versions, timestamps, interval lengths, non-negative measures and the agreed quality codes. Record rejected rows and reasons; do not silently discard malformed data.
3. Deduplicate within each run and record duplicate counts. Define how conflicting copies are resolved.
4. Join reference entities only where useful. Validate meter/district membership and expected join cardinality.
5. Aggregate to aligned intervals, then derive historical indicators.
6. Write Iceberg tables with documented grains, keys, partitions and safe rerun behavior. Read the outputs back using Spark and Trino.

## Water balance and table grains

Before creating a table, state what one row represents. The project document proposes meter-period facts, district-minute facts, daily district water balance and alert incidents. Implement only outputs with real inputs and clear ownership. Flink currently owns real-time detection; batch alert consolidation requires an explicit Kafka/Iceberg contract from Ahmed.

For an aligned fifteen-minute district interval:

```text
district_inlet_volume_m3 = sum(the 15 one-minute inlet_volume_m3 values)
district_metered_consumption_m3 = sum(meter consumed_volume_m3 for that interval)
balance_gap_m3 = district_inlet_volume_m3 - district_metered_consumption_m3
```

A daily balance should derive from consistent intervals and a documented UTC or Cairo calendar. Record missing coverage rather than implying complete totals. The gap is signed evidence, not confirmed leakage; physical losses, delivery faults and operational disturbances can affect it. Define zero-inlet handling before calculating gap percentage.

Reference dimensions can include the actual district and meter attributes. Date/time dimensions are useful only when their calendar semantics and attributes are defined. A junction dimension is optional: the current generator has one service junction per district and no cross-district transfers or storage tanks.

## Acceptance evidence

Supply the job entrypoint, environment requirements, input/output schemas, table grains and repeatable commands. Register the job and its Trino verification in Airflow as described in the handoff. Demonstrate:

- source counts reconciled with the delivery counts in the selected run manifest;
- accepted, rejected and duplicate counts, including intentional test faults;
- no duplicate canonical event identities after cleaning;
- valid reference relationships and interval boundaries;
- aligned volumes and explicit missing-data coverage;
- stable counts after retrying or reprocessing the same run;
- successful queries of the saved Iceberg tables through Spark and Trino.

Keep scenario/fault truth out of the detection and transformation inputs. Evaluation may read restricted truth after the output is produced. Hand-crafted tests must be labeled as such; they are not evidence that every fault occurred in a particular generated run.
