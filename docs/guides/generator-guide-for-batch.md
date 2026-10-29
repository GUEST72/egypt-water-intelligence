# Batch-processing teammate guide: raw data to analytical tables

This guide is for the batch-processing part of `egypt-water-intelligence`. The idea is to start with the data that already exists, understand where it lives, and build a repeatable PySpark pipeline that produces clean, queryable analytical tables. You should not need to change the generator or reverse-engineer the Kafka setup before you can begin.

The agreed direction for this task is **PySpark for batch processing and Apache Iceberg for analytical tables**. The shared infrastructure includes Kafka, MinIO, Kafka UI, the generator, and a Kafka-to-MinIO archiver. The Spark/Iceberg processing job is your part of the work; coordinate the shared service configuration with amr yousri for the main Compose file.

## 1. Get the repository and check the environment

You need Git and Docker Desktop with Linux containers enabled. You can develop the job inside a container so that the required Python and Spark versions are consistent. You do not need to install Kafka or MinIO directly on your Windows machine.

Open PowerShell and clone the repository using the URL shared by the team:

```powershell
git clone <YOUR_REPOSITORY_URL>
cd egypt-water-intelligence
```

Check that Docker and Compose are available:

```powershell
docker --version
docker compose version
docker info
```

If Docker is not running, start Docker Desktop and wait until the engine is ready.

The shared Compose file is `docker-compose.yml`. The generator and its configuration live under `Generator/`. Your batch job should be kept separate from the generator’s responsibilities: the generator creates the simulated source data, while your job reads that data and turns it into analytical outputs.

## 2. Start Kafka, MinIO, and Kafka UI

Start the shared infrastructure:

```powershell
docker compose up -d kafka minio kafka-ui
docker compose ps
```

Wait for Kafka and MinIO to become healthy, then initialize the topics and buckets:

```powershell
docker compose run --rm init
```

The initialization step creates the configured Kafka topics and MinIO buckets. It does **not** generate a dataset, upload the dataset, publish events, or run a Spark job.

The browser interfaces are:

- Kafka UI: `http://localhost:8080`
- MinIO Console: `http://localhost:9001`
- MinIO API: `http://localhost:9000`

The default MinIO credentials in the current Compose configuration are:

```text
Username: wateradmin
Password: wateradminsecret
```

Use your local configured credentials if they have been overridden.

For containers on the shared Compose network, use `minio:9000` as the MinIO endpoint and `kafka:9092` as the Kafka bootstrap address. Do not use `localhost` from inside a container to reach another container. Also, the current Compose file does not publish Kafka on a host port, so a Spark job running directly on Windows cannot assume that `localhost:9092` will work. A containerized Spark job attached to the Compose network avoids that issue.

## 3. Make sure the reference run exists

The current reference run is:

```text
run-72b96af88a342375
```

Its local output folder should be:

```text
Generator/output/run-72b96af88a342375
```

Check whether the run exists:

```powershell
Test-Path .\Generator\output\run-72b96af88a342375\_RUN_COMPLETE
```

If this returns `True`, the run folder is present and marked complete. If it returns `False`, your clone may not include the generated output. You can generate a run using the repository’s Docker image:

```powershell
docker compose build generator
docker compose run --rm generator generate --config /app/pipeline-configs/pipeline-large.yml
```

The generated output is mounted back to your host under `Generator/output/`. If the run ID differs, use the directory that was actually created.

The current reference run contains:

- 6 districts
- 1,500 meters (250 per district)
- 216,000 district sensor events (216,071 in delivery: 71 duplicates)
- 3,600,000 meter-reading events (3,600,454 in delivery: 454 duplicates)
- 3,816,000 public events in total

These figures describe the clean public streams for the reference run. The delivery streams may include injected faults depending on the selected configuration. Verify the actual counts using the run manifest and the files available for your run rather than assuming every run has identical contents.

The district event count is six districts multiplied by 36,000 one-minute intervals. The meter count is 1,500 meters multiplied by 2,400 fifteen-minute intervals over the 25-day run.

The run manifest is the right place to verify counts if the generator configuration changes.

## 4. Put the raw artifacts in MinIO

The batch job should start from the archived raw data, not depend on somebody’s local generator output path. Upload the current run artifacts to MinIO with:

```powershell
docker compose run --rm generator archive-minio --run-dir /app/output/run-72b96af88a342375
```

Replace the run ID if necessary.

Open the MinIO Console at `http://localhost:9001` and inspect the `water-raw` bucket. The uploaded objects are prefixed by the run directory, so do not assume the data files sit directly at the bucket root. Browse the object paths until you find the current run’s `public` data and run metadata.

The public raw artifacts include:

```text
public/districts.ndjson
public/meters.ndjson
public/network.inp
public/district-sensors.ndjson
public/meter-readings.ndjson
```

The exact MinIO object names include the run prefix. The archive also stores run metadata, such as the configuration and manifest, in the raw bucket.

The restricted artifacts are uploaded separately to `water-restricted`. They include scenario truth and fault truth. Treat that bucket as evaluation/reference data, not a normal input to your detection logic.

There is a second, separate archive path worth knowing about. The continuous Kafka archiver writes the events it consumes into compressed batches in the `water-events` bucket. Start it with:

```powershell
docker compose up -d archiver
```

Then publish the event stream if you also need to populate Kafka and exercise the archiver. To publish the clean public streams:

```powershell
docker compose run --rm --no-deps generator publish-kafka --run-dir /app/output/run-72b96af88a342375 --input-set public --bootstrap-servers kafka:9092 --speed 0
```

To publish the delivery streams, which may include injected faults:

```powershell
docker compose run --rm --no-deps generator publish-kafka --run-dir /app/output/run-72b96af88a342375 --input-set delivery --bootstrap-servers kafka:9092 --speed 0
```

These commands publish the selected stored event files to Kafka without replay delays. The archiver consumes the two Kafka topics independently and writes compressed event batches into `water-events`.

For your batch job, `water-raw` is the simplest place to begin because it contains the canonical public files and reference data. `water-events` is useful when you specifically want to process the event batches produced from Kafka. Do not assume the two buckets have identical file layouts or compression: inspect the object names and formats before coding against them.

## 5. Know what each input file means

The project has three broad categories of data.

**Reference data** describes the entities in the simulated utility:

- `public/districts.ndjson`
- `public/meters.ndjson`
- `public/network.inp`

**Event data** describes changing measurements over time:

- `public/district-sensors.ndjson`
- `public/meter-readings.ndjson`

**Restricted truth and run evidence** are used for evaluation and reproducibility:

- `restricted/scenario-truth.ndjson`
- `restricted/fault-truth.ndjson`
- `config.yml`
- `run-manifest.json`
- `validation-report.json`

The last three metadata files are stored with the raw run artifacts. They help you establish which configuration generated a run, how many records were expected, and whether the generator’s validation passed.

The event files are **NDJSON**: one JSON object per line, not one large JSON array. A batch reader should parse each line as a separate record. When reading compressed event objects from `water-events`, make sure the reader handles gzip-compressed NDJSON.

### District reference data

`districts.ndjson` has one row per district. It includes `district_id`, `inlet_link_id`, `pressure_monitor_node_id`, `service_junction_ids`, and flags for cross-boundary transfers and internal storage.

For example, `D001` maps to inlet link `D001_INLET` and pressure-monitoring/service junction `D001_SERVICE`.

### Meter reference data

`meters.ndjson` has one row per meter. It includes `meter_id`, `district_id`, `customer_class`, `baseline_requested_m3_s`, and `service_junction_id`.

A meter record is not a time-series reading. The baseline is the meter’s synthetic reference demand rate; it is not the same as the measured `consumed_volume_m3` in an event.

There is no separate customer entity or `customers.ndjson` in the current generator. For the current model, the meter is the demand-bearing customer entity.

### Junctions and the network model

A junction is a node in the EPANET hydraulic model. It represents a point where pipes connect or where demand and pressure are represented. The current pilot uses one service junction per district, and meter reference rows map meters to their service junction.

You can ignore junction-level mapping if your analytical model only needs to group readings by meter and district. You do not have to add a junction dimension just because the field exists. Keep the information available, though, if you need to relate meter readings to the pressure-monitoring node or inspect the hydraulic topology.

`network.inp` is the full EPANET network description. It can be useful for a later network-oriented analysis, but you do not need to parse it for basic meter/district aggregations. The current pilot uses a simplified network: one source, one service junction, and one inlet per district, with optional leak branches. There are no cross-district transfer links or internal storage in this version.

## 6. Understand the event schemas before transforming anything

The two public event contracts are:

```text
Generator/schemas/district-sensor.v1.json
Generator/schemas/meter-reading.v1.json
```

Both event types have `schema_version`, `event_id`, `run_id`, interval timestamps, event/availability timestamps, and `quality_code`.

A district sensor event contains:

```text
district_id
inlet_link_id
pressure_monitor_node_id
inlet_flow_m3_s
inlet_volume_m3
pressure_m_h2o
```

A meter-reading event contains:

```text
meter_id
district_id
consumed_volume_m3
```

The district stream is one-minute data. The meter stream is fifteen-minute data. Do not join them directly on raw timestamps and expect one row from each side to match. If you want to calculate a district water-balance measure, align them to the same time interval first.

The district event’s `inlet_volume_m3` is the volume for its one-minute interval. The meter event’s `consumed_volume_m3` is the volume consumed by that meter over its fifteen-minute interval. A reasonable comparison is to aggregate district inlet volume across the same fifteen minutes and sum the meter volumes for the same district and interval.

Use `event_id` as the event-level identity when you need to detect duplicate events. It is unique to the logical event in the generator’s canonical data. Preserve `run_id` as well: it tells you which generated run a row belongs to and prevents accidental mixing of records from separate runs.

The clean reference stream uses `quality_code = "valid"` for its canonical records. Delivery records can contain injected data-quality faults, depending on the selected fault configuration. Faulted records may use `quality_code = "corrupted"`. Your pipeline should implement the agreed handling for unexpected data, but do not claim you tested every fault type unless you actually ran a test containing it.

## 7. Timestamps and interval alignment

All event timestamps are UTC ISO 8601 strings ending in `Z`.

There are four timestamp fields to keep straight:

- `interval_start_utc`: start of the measured interval
- `interval_end_utc`: end of the measured interval
- `event_time_utc`: the timestamp assigned to the event by the generator
- `availability_time_utc`: when the record is available in the simulated delivery timeline

For district sensor events, `event_time_utc` is the interval start and `availability_time_utc` is the interval end. For meter readings, both `event_time_utc` and `availability_time_utc` are the interval end.

For historical batch analytics, choose the interval key that matches the metric you are calculating and document it. If you are aggregating a meter volume over fifteen minutes, using the interval end as the period label can be convenient, but keep the interval start/end fields where they help establish the actual interval. For a water-balance calculation, align both data sources to the same fifteen-minute boundary rather than comparing one-minute inlet values with a fifteen-minute meter total.

Do not silently treat `event_time_utc` as the same thing as `interval_start_utc` across both event types. Their semantics differ.

## 8. How to organize the batch pipeline

Keep the stages explicit and make each stage’s output inspectable. The raw input should remain unchanged so that you can rerun the transformations after changing cleaning logic or correcting a calculation.

A practical initial flow is:

```text
MinIO raw artifacts
    -> read and parse
    -> validate and normalize
    -> deduplicate and handle quality issues
    -> enrich with reference data
    -> calculate aligned historical measures
    -> write analytical Iceberg tables
    -> query and verify the outputs
```

You can organize the implementation as separate scripts or as clearly separated functions in one job. What matters is that every stage has a defined input, output, and validation step.

For example:

**Read and parse.** Read only the relevant public event files from the selected run. Support NDJSON and, if you choose to read the Kafka archive, gzip-compressed NDJSON. Parse timestamps into consistent Spark timestamp types and apply an explicit schema rather than relying on inference for the full dataset.

**Validate and normalize.** Check required identifiers, timestamps, non-negative volumes/flows, schema version, and quality code. Keep a count of accepted and rejected records. Decide how malformed rows are recorded so they can be inspected later; do not make invalid data disappear without evidence.

**Deduplicate.** Use a documented business key. For canonical generator events, `event_id` is a natural event identity. If the same event is present twice, retain one canonical record and count the duplicates. If you read several runs together, preserve `run_id` as part of the identity boundary.

**Enrich.** Join events to `meters.ndjson` or `districts.ndjson` only when the analysis needs attributes from those reference tables. For example, meter reference data can add customer class or service junction; district reference data can add the inlet link and pressure monitor. Keep the join keys and expected cardinality clear. You do not need to add the full network topology to every output table.

**Calculate historical measures.** Aggregate meter and district data over aligned intervals. Make units explicit, and separate the calculated metric from its input measurements so the result can be checked.

**Write and query.** Persist the outputs to Iceberg tables with stable names and documented grains. Read the written tables back and run validation queries to prove that the result is queryable.

These are processing responsibilities, not a requirement to build six separate services. Start with the smallest implementation that makes each stage testable.

## 9. Decide the output grain before creating the Iceberg tables

Before you write a table, state what one row represents. This avoids mixing raw events, meter-period summaries, and district-period calculations in the same table.

A useful starting design is to keep the event-level facts separate:

- A meter-reading fact with one row per meter per fifteen-minute interval.
- A district-sensor fact with one row per district per one-minute interval.
- A district water-balance fact with one row per district per agreed comparison interval, such as fifteen minutes, once the inlet and meter volumes have been aligned.
- An alert or detection-result fact only if the batch job is responsible for producing or consolidating those results.

The fourth table is optional; do not create it simply to reach a particular number of facts. It should exist only if there is a real result grain and a defined owner for the detection output. Flink may be the source of real-time alerts, while batch processing may evaluate or summarize them later.

Reference dimensions can be small tables such as `dim_district`, `dim_meter`, `dim_date`, and `dim_time` if the agreed analytical model benefits from them. Do not invent dimensions that have no useful attributes or relationships. A junction dimension is optional for the reasons explained earlier.

For each table, document its grain, primary/business key, foreign keys, units, partitioning choice, and how it is populated. Avoid storing one-minute district values alongside fifteen-minute meter volumes in one fact table unless the model explicitly defines how the different grains are reconciled.

The final table names and catalog configuration should follow the team’s agreed schema. Treat the names above as a design starting point until the table contract has been agreed and recorded.

## 10. Use Iceberg for the analytical outputs, not as a replacement for raw evidence

MinIO keeps the raw source artifacts. Iceberg is the table format for the transformed analytical data. They serve different purposes.

The raw layer lets you go back to the original generated records. The analytical tables should be repeatable outputs of your transformation job. Do not overwrite or “clean” the raw files in place.

The exact Iceberg catalog, warehouse location, and query endpoint need to match the integrated project configuration. They are not currently provided as working services by the base Compose file described in this guide, so do not assume that a catalog or Trino endpoint already exists. Coordinate those requirements with the shared Compose owner before hardcoding hostnames, ports, or credentials into the job.

When integrating, provide:

- the Spark/PySpark version and runtime requirements;
- the Iceberg Spark runtime dependency matching that version;
- the catalog type and configuration;
- the MinIO endpoint, credentials, bucket or warehouse path;
- the table namespace and names;
- the exact startup command;
- the input and output schemas;
- the required environment variables.

Kee

## 11. Data-quality checks and water-balance calculations

A useful batch pipeline should demonstrate that its output is correct, not just that Spark finished successfully.

At minimum, track the number of source rows read, rows accepted, rows rejected, duplicates found, and rows written to each output table. Compare source counts with the run manifest. The manifest is the reference for expected event counts for that run.

For the district water-balance calculation, make the units and interval explicit. The input district sensor record provides `inlet_volume_m3` for one minute. Sum those inlet volumes over the chosen interval. Sum `consumed_volume_m3` for all meters in the same district and interval. The difference is a balance gap for that interval, not automatically proof of a leak.

For example, if you use a fifteen-minute interval, calculate:

```text
district_inlet_volume_m3
    = sum(one-minute inlet_volume_m3 records for the district and interval)

district_metered_consumption_m3
    = sum(meter consumed_volume_m3 records for the same district and interval)

balance_gap_m3
    = district_inlet_volume_m3 - district_metered_consumption_m3
```

Document whether the gap is signed or whether you also report an absolute gap or a ratio. Do not call the gap “confirmed leakage”: the project includes leak incidents, demand surges, and supply disturbances, and data-quality problems can also affect the balance. It is an analytical indicator that needs interpretation.

Other checks can include:

- no duplicate canonical `event_id` values after deduplication;
- all event `district_id` values map to the district reference data;
- all meter IDs map to the meter reference data;
- interval end is later than interval start;
- no negative volumes or flows where the schema forbids them;
- output counts match the expected grain;
- every derived balance row uses aligned district and meter intervals;
- the saved Iceberg tables can be read back successfully.

Record the results so that a teammate can reproduce the checks.

## 12. Restricted truth and evaluation

The generator stores scenario truth separately in the restricted output. It describes the synthetic scenarios and their expected effects. This is valuable for evaluation, but it should not be mixed into the public measurement data used to derive a detection.

For example, you can use restricted scenario truth after running the detector to evaluate whether an alert overlaps the known scenario interval and district. Do not join `scenario-truth.ndjson` into the detection input in a way that exposes scenario labels or start/end times to the logic being evaluated.

Delivery-fault truth records the injected faults when a fault configuration is used. If you test deduplication or malformed-record handling using hand-crafted test inputs, label those as test cases rather than implying the reference run contains them.

## 13. Verify the archive and output locations

The three buckets have different purposes:

```text
water-raw
    Canonical public artifacts and run metadata

water-restricted
    Scenario/fault truth and other restricted artifacts

water-events
    Compressed event batches archived from Kafka
```

Browse the MinIO Console to confirm that the objects exist before you debug Spark. If the `water-raw` bucket is empty, the likely missing step is the explicit `archive-minio` command; starting MinIO or running `init` only creates the service/bucket, not the uploaded dataset.

If you are reading from `water-events`, inspect the object path and compression format first. The Kafka archiver writes gzip-compressed NDJSON objects under paths shaped like:

```text
events/{topic}/{run_id}/part-{partition}-{first_offset}-{last_offset}.ndjson.gz
```

These are batches, not one object per event. Your Spark reader should handle the compressed NDJSON content and preserve enough metadata to identify the source topic, partition, offsets, and run. The object naming pattern records the topic, run, partition, and offset range; it is useful for traceability and investigating reprocessing.

The Kafka archiver is a continuous service, so its logs may not show a message for every successful upload. Check its container status and inspect `water-events` in MinIO rather than using log volume as the only health check.

## 14. Useful commands while developing

Check the shared services:

```powershell
docker compose ps -a
```

Inspect the archiver:

```powershell
docker compose logs --tail 100 archiver
```

Rebuild the generator and archiver images if their source changes:

```powershell
docker compose build generator archiver
```

Stop services while keeping the persistent data:

```powershell
docker compose down
```

Kafka and MinIO data are held in named Docker volumes, so they survive an ordinary stop/start. That is helpful for development but means old records and objects may still be present. Before rerunning a full load, decide whether you want to append a new run, reuse the existing run, or start from a deliberately clean state.

Do not run `docker compose down -v` casually: it deletes the Compose-managed volumes, including Kafka state and MinIO objects. The generated local files under `Generator/output/` are separate and are not removed by deleting those Docker volumes.
