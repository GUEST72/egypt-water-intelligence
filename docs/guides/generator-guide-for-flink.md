# Flink teammate guide: getting started with the live water-data stream

This guide gets you from a fresh clone of `egypt-water-intelligence` to a working Flink development environment. It covers the existing infrastructure, how to connect to Kafka, what the events contain, and how to test your job.

The repository includes the generator, Kafka, MinIO, Kafka UI, and a Kafka-to-MinIO archiver. Flink is not currently included as a service in the shared Compose configuration, so you will need to provide the Flink job and its runtime configuration. Coordinate any changes to the main `docker-compose.yml` with amr yousri for the shared infrastructure.

## 1. Get the repository and check Docker

You need Git, Docker Desktop with Linux containers enabled, and Docker Compose. You do not need to install Kafka or MinIO directly on Windows; they run in containers.

Open PowerShell and clone the repository using the URL shared by the team:

```powershell
git clone <YOUR_REPOSITORY_URL>
cd egypt-water-intelligence
```

Check that Docker is running:

```powershell
docker --version
docker compose version
docker info
```

If `docker info` fails, start Docker Desktop and wait until its engine is ready before continuing.

The main file you will work around is `docker-compose.yml`. The generator code and configuration are under `Generator/`.

## 2. Start the shared infrastructure

From the repository root, start Kafka, MinIO, and Kafka UI:

```powershell
docker compose up -d kafka minio kafka-ui
docker compose ps
```

Wait until Kafka and MinIO are healthy. Then run the initialization service:

```powershell
docker compose run --rm init
```

This creates the configured Kafka topics and MinIO buckets. It does not generate the dataset, upload reference files, publish events, or create Flink output.

You can open these pages in your browser:

- Kafka UI: `http://localhost:8080`
- MinIO Console: `http://localhost:9001`
- MinIO API from your Windows browser or another host application: `http://localhost:9000`

The default MinIO credentials in the current Compose configuration are:

```text
Username: wateradmin
Password: wateradminsecret
```

Use the credentials configured in your local environment if they have been overridden.

One networking detail is worth remembering: Kafka is not published on a host port in the current Compose file. A Flink container attached to the same Compose network should connect to Kafka using `kafka:9092`, not `localhost:9092`. Inside a Flink container, `localhost` means that container itself. Similarly, a container should use `minio:9000` for MinIO.

## 3. Make sure there is a dataset to work with

Starting the services does not automatically populate them. A fresh clone and fresh Docker volumes start with empty Kafka topics and empty MinIO buckets until you explicitly load data.

The team's current reference run is:

```text
run-72b96af88a342375
```

Its local output directory should be:

```text
Generator/output/run-72b96af88a342375
```

Check whether it exists:

```powershell
Test-Path .\Generator\output\run-72b96af88a342375\_RUN_COMPLETE
```

If this returns `True`, the local run folder is present and marked complete. If it returns `False`, the dataset may not have been included in your clone. Generate it with the repository's generator image:

```powershell
docker compose build generator
docker compose run --rm generator generate --config /app/pipeline-configs/pipeline-large.yml
```

The generator writes to the mounted output directory, so the generated run should appear under `Generator/output/` on your machine. The run ID is derived from the configuration, so use the directory name actually created if it differs from the reference run above.

The run contains separate `public/` and `delivery/` datasets. The `public/` directory holds the clean reference event streams, while `delivery/` holds the streams intended for delivery and can include injected data-quality faults. Restricted evaluation information is stored separately under `restricted/`.

If your Flink logic needs the reference files, upload the run artifacts to MinIO with:

```powershell
docker compose run --rm generator archive-minio --run-dir /app/output/run-72b96af88a342375
```

Replace the run ID if your generated directory has a different name. This command uploads the run artifacts directly from the generator to MinIO. It is separate from publishing events to Kafka and from the Kafka-to-MinIO archiver. (you won't need to do so the data will be there don't worry)

The Flink job can initially work entirely from Kafka. You only need the reference files if your chosen logic requires them.

## 4. Publish the event streams

The generator produces two logical event streams:

```text
water.district-sensors.v1
water.meter-readings.v1
```

The first contains district-level sensor measurements. The second contains individual meter readings.

To publish the clean reference streams:

```powershell
docker compose run --rm --no-deps generator publish-kafka --run-dir /app/output/run-72b96af88a342375 --input-set public --bootstrap-servers kafka:9092 --speed 0
```



To publish the delivery streams, which may include injected faults: (YOU WOULD USE THIS YA AHMED)

```powershell
docker compose run --rm --no-deps generator publish-kafka --run-dir /app/output/run-72b96af88a342375 --input-set delivery --bootstrap-servers kafka:9092 --speed 0
```

Use the appropriate run ID if yours differs.

The `--input-set` option selects which dataset is published: `public` or `delivery`.

FOCUS HERE:
The `--speed 0` option publishes the stored events without replay delays. This is useful for testing because it avoids waiting for simulated time to pass. It is not a realistic live-speed demonstration, and the full dataset can be published quickly.

For a clean streaming test, start Kafka and MinIO, initialize the services, start the Kafka-to-MinIO archiver, start your Flink job, and then publish the events. This lets you observe the job consuming events while the producer is running. You can also start a new consumer group after events have been published and read retained Kafka records from the beginning.

Start the archiver before publishing if you want the Kafka events stored in MinIO as well:

```powershell
docker compose up -d archiver
```

The archiver is a separate consumer. It reads both topics and stores compressed event batches in the `water-events` bucket. Its default consumer group is `water-event-archiver`. Do not use that consumer-group ID for Flink. Give your Flink source its own group ID so that Flink and the archiver can read the same events independently.

## 5. Check the topics and the messages

Open Kafka UI at `http://localhost:8080`, select the cluster, and look for these topics:

```text
water.district-sensors.v1
water.meter-readings.v1
```

Use the topic view to inspect messages and confirm that records are being published. You should see JSON event values containing fields such as `event_id`, `run_id`, `district_id`, and timestamps.

You can also inspect topic offsets from PowerShell:

```powershell
docker compose exec kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic water.district-sensors.v1 --time -1
docker compose exec kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic water.meter-readings.v1 --time -1
```

For the current reference run, the expected counts are:

| Stream                 | Clean reference (`public`) | Delivery (`delivery`) |
| ---------------------- | -------------------------: | --------------------: |
| District sensor events |                    216,000 |               216,071 |
| Meter-reading events   |                  3,600,000 |             3,600,454 |
| Total                  |                  3,816,000 |             3,816,525 |

The delivery meter count is higher because duplicate records can be injected. Null-value faults change measurement values but do not increase the number of records. The actual counts depend on the selected run configuration.

Kafka offsets are zero-based, so the final offset is not itself the number of messages. For a single partition with no previous records, 216,000 messages would normally have a final offset of 215,999. If the topic has multiple partitions, inspect all returned partition offsets and account for the partition layout.

If you do not see messages, check that the generator command completed, that the correct run directory and `--input-set` were supplied, and that you are inspecting the correct topics. Starting the services alone is not enough to publish anything.

## 6. Understand the two streams before writing your job

You can choose to consume one stream or both. There is no requirement to force both into the first version of the job. Choose the input that makes sense for the detection or cleaning logic you want to demonstrate, and explain why you chose it.

District sensor events come from the selected dataset's `district-sensors.ndjson` file and are published to `water.district-sensors.v1`. Each record describes one district over a one-minute interval. It includes the district's inlet flow and estimated inlet volume, pressure at the designated monitoring node, interval timestamps, and a quality code.

Fields you will likely use include:

```text
event_id
run_id
district_id
interval_start_utc
interval_end_utc
event_time_utc
availability_time_utc
inlet_flow_m3_s
inlet_volume_m3
pressure_m_h2o
quality_code
```

Meter-reading events come from the selected dataset's `meter-readings.ndjson` file and are published to `water.meter-readings.v1`. Each record describes one meter's consumption over a fifteen-minute interval. It includes the meter ID, district ID, interval timestamps, consumed volume, and a quality code.

Fields you will likely use include:

```text
event_id
run_id
meter_id
district_id
interval_start_utc
interval_end_utc
event_time_utc
availability_time_utc
consumed_volume_m3
quality_code
```

Both streams include `schema_version`, currently `1.0.0`. The formal contracts are in:

```text
Generator/schemas/district-sensor.v1.json
Generator/schemas/meter-reading.v1.json
```

Use those schema files as the contract instead of relying only on the examples in this guide. The schemas define the required fields and reject additional properties.

The clean reference stream is intended to contain valid measurements. The delivery stream can contain injected faults, such as null measurement values and duplicate meter events. Faulted records can use `quality_code: "corrupted"`. Your job should handle unexpected or invalid data defensively, rather than assuming every incoming measurement is usable.

## 7. Understand the timestamps; this matters for Flink

The event records contain several timestamp fields because they describe different things.

`interval_start_utc` and `interval_end_utc` define the interval being measured. `event_time_utc` is the timestamp assigned to the event for event-time processing. `availability_time_utc` describes when the record becomes available in the simulated delivery timeline.

In the current implementation, district sensor events represent one-minute intervals. Their `event_time_utc` is the interval start, while their `availability_time_utc` is the interval end. Meter readings represent fifteen-minute intervals, and both their `event_time_utc` and `availability_time_utc` are the interval end.

For example, a district event can describe the interval from `00:00` to `00:01` UTC, carry `event_time_utc` of `00:00`, and become available at `00:01`. A meter event for the interval from `00:00` to `00:15` uses `00:15` as its event and availability time.

Use `event_time_utc` as the event-time field if your Flink logic needs event-time windows. Do not blindly use the interval start for every record or treat the two streams as if they have the same granularity. Choose and document a watermark strategy based on the job's intended behavior. Delivery faults do not necessarily imply event-time delays, so make your handling of late or out-of-order records explicit.

The timestamps are UTC strings ending in `Z`. Convert them into a timestamp type that your chosen Flink connector and runtime handle consistently.

## 8. Reference data: what it is, and when you can ignore it

The generator writes these public reference artifacts:

```text
public/districts.ndjson
public/meters.ndjson
public/network.inp
```

They can be uploaded to the raw MinIO bucket by `archive-minio`. The exact object prefix is based on the run directory, so browse the `water-raw` bucket in the MinIO Console rather than assuming every object is stored at the bucket root.

`districts.ndjson` contains one record per district. It maps a district ID such as `D001` to its inlet link and pressure-monitoring node. It also lists the service junction IDs and indicates whether the district has cross-boundary transfers or internal storage.

`meters.ndjson` contains one record per meter. It maps a meter ID to its district, customer class, baseline requested flow, and service junction. In the current generator, the meter is the demand-bearing customer entity; there is no separate `customers.ndjson` file or customer ID.

`network.inp` is the EPANET network model. It describes the hydraulic network and its nodes and links. It is useful if your logic needs to understand network topology, but you do not need to parse it for a basic Kafka consumer or a district-level time-window query.

A junction is a node in the hydraulic model where pipes connect or demand/pressure is represented. In this pilot, each district has a designated service junction, such as `D001_SERVICE`, which is also the pressure-monitoring node. The meter reference records map meters to their service junctions.

You can ignore junction-level mapping if your first query only groups records by `district_id` and uses the measurements already present in the events. Keep it available if you later need to explain which network node a measurement refers to or connect a district or meter to the EPANET model. It is reference information, not another live Kafka stream.

The current network has isolated district branches: one source, one service junction, and one inlet link per district, with optional leak branches. There are no cross-district transfer links or internal storage in this pilot. Do not assume that this simplified topology represents every real water network.

## 9. Scenario truth and delivery-fault truth are for evaluation

The generator stores restricted information separately from the public event streams:

```text
restricted/scenario-truth.ndjson
restricted/fault-truth.ndjson
```

Scenario truth describes the synthetic cases inserted into the simulation, including the scenario type, district, time interval, physical parameters, and expected effects. The configured scenario families include leak incidents, a supply disturbance, and a demand surge. Fault truth records the injected delivery faults, such as duplicated records or null measurement values.

This information is useful for offline evaluation and checking whether a detection was correct. Do not feed truth labels into the live detection logic if you intend to evaluate the job fairly. That would reveal the answer to the detector. Use public event measurements as the detection input, then compare emitted alerts with restricted truth in a separate evaluation step.

The presence of fault truth depends on the selected run and fault configuration. Do not assume every run contains delivery faults; inspect its configuration and audit data when you need to know which cases were injected.

## 10. Choosing your first Flink implementation

Start with the smallest end-to-end job you can verify. You do not have to settle every detection rule before proving that Flink can read and process the data.

A sensible first milestone is:

1. Connect to one chosen Kafka topic.
2. Deserialize the JSON value and validate the fields needed by the job.
3. Parse the event-time timestamp.
4. Assign event timestamps and a documented watermark strategy if using event-time windows.
5. Apply a simple transformation or aggregate.
6. Write the result to a clearly defined sink, or emit it to a console/log sink for the first test.
7. Demonstrate that the job can start, consume events, and recover or replay them as expected.

After that works, add the cleaning rules, windows, and detection logic required by the project. The project task calls for stream-level cleaning and validation, relevant real-time detection rules, a documented alert schema, and tests for representative inputs. Examples include suspicious flow changes and data-quality warnings, but the exact thresholds and output schema should be justified and documented rather than chosen arbitrarily.

If you decide to compare district inlet volume with meter consumption, account for the different intervals first. District sensor events arrive once per minute, while each meter contributes a fifteen-minute volume. A meaningful comparison would aggregate the district's one-minute inlet volumes over the same fifteen-minute interval and aggregate meter volumes for the same district and interval. The district event already includes `inlet_volume_m3`; you do not need to derive it from flow unless you specifically want to validate the relationship. Be explicit about boundary alignment, missing readings, duplicates, and the effect of invalid quality codes.

## 11. Consumer groups, replay, and testing

Give your Flink source a dedicated group ID, for example:

```text
flink-water-detection-dev
```

Do not reuse `water-event-archiver`. Kafka consumer groups keep their offsets independently, which lets the archiver and Flink process the same topic without competing for the same group's assignments.

Be careful when rerunning tests. A consumer group that has already committed offsets will normally continue from its saved position. Setting `auto.offset.reset=earliest` only applies when the group has no valid committed offset or its offset is no longer available; it does not automatically rewind a group every time the job restarts. For repeatable tests, use a fresh development group ID or deliberately reset the group offsets when appropriate.

For the first integration test, try a small input or a controlled replay rather than assuming the whole 3.8-million-event run is necessary to debug every change. Once the job behaves correctly on representative records, test the full stream and record the input counts, output counts, and any expected exclusions.

The job should document what happens to malformed JSON, missing required fields, unknown quality codes, duplicate event IDs, and late records. Do not silently discard such cases without making the behavior observable. If you introduce a dead-letter or rejected-record output, document its schema and destination.

## 12. Useful commands while developing

Check service state:

```powershell
docker compose ps -a
```

Check the archiver's container logs:

```powershell
docker compose logs --tail 100 archiver
```

The archiver may not print a line for every archived batch, so quiet logs alone do not prove it is broken. Verify its container state and inspect the `water-events` bucket in MinIO.

Rebuild the generator and archiver images after changing their source code:

```powershell
docker compose build generator archiver
```

Stop the services without deleting stored Kafka or MinIO data:

```powershell
docker compose down
```

The named volumes persist across ordinary restarts. This is useful, but it also means that a later test may encounter old Kafka messages, consumer offsets, or MinIO objects. Before testing, check whether the existing state is intentional. Do not delete volumes just to make a job start; removing the volumes deletes stored Kafka state and MinIO objects.
