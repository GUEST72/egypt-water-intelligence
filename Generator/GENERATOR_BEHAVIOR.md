# Egypt Water Intelligence — Data Generation and Ingestion Engineering

## 1. Before the data reaches the pipeline

Imagine trying to understand what is happening inside a water network using only the numbers arriving every few seconds. One district reports a drop in pressure. A few meters report unusual consumption. Somewhere in the network, water may be leaking, demand may be changing, or the system may simply be behaving differently from yesterday.

The difficult part is not producing numbers that look realistic. It is producing data that describes a coherent physical situation, preserves the timing of that situation, and remains useful when it travels through the rest of the system.

That is the responsibility of this part of Egypt Water Intelligence.

The generator creates a simulated water network, runs hydraulic simulations, turns the results into structured events, and prepares those events for the data pipeline. The ingestion side then moves the events through Kafka and into MinIO, giving the downstream components something consistent to consume and investigate.

Before looking at how this works, it helps to meet the two event types that carry most of the story.

## 2. Meet the two event streams

Our system observes the network at two different levels.

The first is the **district sensor stream**. It tells us what is happening at a district inlet: how much water is flowing in, how much volume passes through during an interval, and what pressure is measured.

The second is the **meter reading stream**. It tells us how much water an individual meter records over its own measurement interval.

They describe related parts of the same network, but they are not interchangeable. District sensors observe water entering a district, while meters observe consumption at individual measurement points.

### District sensor event

The district stream produces one event per district per minute.

```json
{
  "district_id": "district_001",
  "inlet_asset_id": "inlet_001",
  "pressure_asset_id": "pressure_001",
  "event_time_utc": "2026-01-01T00:00:00Z",
  "availability_time_utc": "2026-01-01T00:01:00Z",
  "inlet_flow_m3_s": 0.125,
  "inlet_volume_m3": 7.5,
  "pressure_m_h2o": 32.4,
  "quality_code": "valid",
  "schema_version": "v1"
}
```

This example illustrates the shape and meaning of a district sensor event. The identifiers tell us which district and assets the event describes. The timestamps tell us which interval the observation belongs to and when the interval has finished. The measurements describe flow, volume, and pressure.

The numerical values above are illustrative, not a claim that these exact readings were produced by the reference run.

### Meter reading event

The meter stream produces one event per meter every fifteen minutes.

```json
{
  "meter_id": "meter_000001",
  "district_id": "district_001",
  "event_time_utc": "2026-01-01T00:15:00Z",
  "availability_time_utc": "2026-01-01T00:15:00Z",
  "consumed_volume_m3": 0.84,
  "quality_code": "valid",
  "schema_version": "v1"
}
```

Again, the measurement is illustrative. The important distinction is that a meter event represents the volume consumed over a fifteen-minute interval, rather than an instantaneous flow measurement.

### What each field contributes

| Field | District sensor stream | Meter reading stream |
|---|---|---|
| Entity identifiers | District and relevant sensor assets | Meter and its district |
| `event_time_utc` | Start of the one-minute interval | End of the fifteen-minute interval |
| `availability_time_utc` | End of the one-minute interval | End of the fifteen-minute interval |
| Measurements | Flow, inlet volume, pressure | Consumed volume |
| `quality_code` | Describes the record's data quality | Describes the record's data quality |
| `schema_version` | Identifies the event contract version | Identifies the event contract version |

All timestamps use UTC in ISO 8601 format, ending in `Z`.

There is a subtle but important difference between the two timestamps. `event_time_utc` tells us how to place a measurement on the timeline; `availability_time_utc` tells us when the observation becomes available to the pipeline. These concepts matter when events arrive late, are replayed, or are processed in time-based windows.

The fields shown here describe the intended event contracts in readable form. The versioned JSON Schema files remain the authoritative machine-readable contracts for validation.

## 3. The first decision: generate a network, not a pile of numbers

A generic generator can create thousands or millions of rows by choosing random values within reasonable-looking ranges. That may be enough to test whether a database accepts data, but it does not tell us whether a water network is behaving plausibly.

If pressure drops, should the inlet flow remain completely unrelated? If a leak is introduced, should the resulting measurements continue behaving as though nothing happened? If a meter records consumption, should that number be generated independently of the hydraulic conditions?

Those questions are why the generator begins with a model of the network itself.

The system uses EPANET 2.2 through WNTR, with pressure-dependent demand (PDD), to simulate hydraulic behavior. The resulting measurements are derived from the simulation rather than being generated as unrelated random values.

This gives the data a meaningful relationship with the simulated physical system. Flow, pressure, and delivered demand belong to a common hydraulic story.

It does not mean that every real-world complexity is represented. The network is deliberately simplified, and the simulation is only as realistic as its assumptions. The value is that the data is internally connected to a physical model instead of merely resembling operational data.

## 4. What the simulated network actually represents

The current network is intentionally manageable. It has one source, a service junction, district inlets, and optional branches that can represent leaks.

There are six districts in the reference configuration (`configs/pipeline-large.yml`), with 1,500 meters in total, 250 per district. The original pilot (`Generator/configs/pilot.yml`) used 600 meters. Each district receives water through its modeled inlet, while meters represent demand-bearing measurement points associated with districts.

The current model does not represent cross-district transfers or internal district storage. Those are important boundaries because they affect how we interpret water balance. We should not claim to model water movement that the network itself does not contain.

The aim is to create a coherent foundation for downstream engineering and analytics, not to reproduce every detail of a real city's water infrastructure.

## 5. Controlled variation: where the data becomes useful

A pipeline tested against only one unchanging scenario can appear reliable while remaining unprepared for anything different.

Water systems change. Demand varies, operating conditions shift, and faults may affect what sensors report. A useful dataset should give downstream components a chance to encounter variation rather than repeatedly processing the same situation.

The generator therefore separates the network model and run configuration from the data produced by the simulation. This makes the scenario configurable and the output reproducible, rather than tying every run to one fixed set of values.

The configuration describes the intended run, while the simulation produces measurements consistent with that configuration. Where supported, scenario and fault information is also retained separately so it can be used for evaluation without automatically becoming part of the public event stream.

This distinction matters: a scenario should influence the measurements through the simulation or the configured data-quality process, not simply be mentioned in a label while the measurements remain unchanged.

## 6. Reproducibility: every run needs an identity

Imagine comparing two pipeline implementations. One produces a different anomaly count from the other. Is the pipeline behavior actually different, or did the generator create a different dataset each time?

Without a clear run identity and configuration record, that question becomes unnecessarily difficult to answer.

The generator uses a YAML configuration, seeded random behavior where applicable, and a run identifier derived from the canonical configuration. Each run also carries metadata describing what was generated and a validation report describing the outcome of the checks performed.

For the reference run, the identifier is:

`run-72b96af88a342375`

The artifacts for that run are stored together so the dataset can be traced back to its configuration and validation results.

Reproducibility does not mean that every external system will behave identically on every replay. It means the generation process has a stable basis for identifying and explaining the data it produced.

## 7. Time is part of the data contract

The reference simulation covers 25 days beginning at `2026-01-01T00:00:00Z`.

At first glance, 25 days sounds straightforward. The interesting part is that the two event streams use different intervals.

District sensors report once per minute. Across 36,000 one-minute intervals and six districts, the expected public district stream contains:

**36,000 × 6 = 216,000 events.**

Meters report once every fifteen minutes. Over 25 days, that gives 2,400 intervals per meter. With 1,500 meters, the expected public meter stream contains:

**2,400 × 1,500 = 3,600,000 events.**

Together, the two clean public streams contain **3,816,000 events**. (The 600-meter pilot produces 1,440,000 meter events and 1,656,000 in total.)

The hydraulic simulation itself has 36,001 snapshots because the initial and terminal simulation states are part of the simulated timeline. That does not mean the event stream should contain 36,001 complete one-minute intervals. The initial state and the 36,000 intervals are different things.

Keeping this distinction clear prevents a common class of off-by-one errors when measurements are transformed into events.

## 8. The interval problem: comparing the right things

Suppose a district inlet reports water entering the district every minute, while the meters report consumption every fifteen minutes. Comparing one minute of inlet volume against fifteen minutes of consumption would be misleading.

The intervals need to be aligned first.

For a fifteen-minute period, the district inlet volume should be aggregated across the fifteen corresponding one-minute intervals. That total can then be compared with the meter volumes covering the same district and period.

The flow measurement `inlet_flow_m3_s` represents a rate. The `inlet_volume_m3` field represents a volume over the interval. They are related, but they are not the same quantity and should not be substituted for each other.

This is one of the reasons the event contracts carry explicit units and timing semantics. Correct field names are helpful, but correct interpretation is what makes the data useful.

A district's inlet volume and the sum of its meter consumption also should not automatically be expected to match perfectly in every real-world setting. Real systems may include losses, unmetered consumption, timing differences, and other effects. Our simplified simulation provides a controlled foundation, while any water-balance analysis must respect the assumptions of that model.

## 9. Clean data and delivery data serve different purposes

The generator keeps two important versions of the event data separate.

The **public dataset** contains the clean canonical event streams. These are the measurements intended to represent the simulated system before delivery faults are applied.

The **delivery dataset** represents what downstream systems may receive when configured data-quality faults are introduced. It can include corrupted values or duplicate records.

This is more useful than overwriting the clean output. The clean version provides a stable reference, while the delivery version allows downstream components to be tested against imperfect input.

For example, a consumer can compare its results against the clean dataset while also checking whether it detects or handles corrupted delivery records. The same run configuration and underlying scenario provide a shared point of reference for those comparisons.

## 10. Faults are deliberate, not accidental

The reference configuration includes four data-quality fault rules:

- Meter events can receive null values in `consumed_volume_m3`.
- Meter events can be duplicated.
- District sensor events can receive null values in `pressure_m_h2o`.
- District sensor events can be duplicated.

The cadences are different prime numbers, so a record is never both nulled and duplicated: every duplicate is a valid copy, which exercises deduplication downstream. A nulled record also gets `quality_code: corrupted`.

The configured rules are:

```yaml
faults:
  - stream: meter
    type: null_value
    every_n: 4999
    field: consumed_volume_m3
  - stream: meter
    type: duplicate
    every_n: 7919
  - stream: district
    type: null_value
    every_n: 1999
    field: pressure_m_h2o
  - stream: district
    type: duplicate
    every_n: 3001
```

The `every_n` setting describes the configured fault frequency in the relevant stream. These rules should be interpreted as deliberate transformations of the delivery data, not as changes to the underlying hydraulic truth.

That separation lets us ask two different questions: what happened in the simulated network, and what did the downstream pipeline actually receive?

For the configured reference delivery, the expected totals are:

| Stream | Clean public events | Delivery events |
|---|---:|---:|
| District sensors | 216,000 | 216,071 |
| Meter readings | 3,600,000 | 3,600,454 |
| **Total** | **3,816,000** | **3,816,525** |

The delivery output has 525 additional duplicate records (71 district, 454 meter) and 828 null-value fault records (720 meter `consumed_volume_m3`, 108 district `pressure_m_h2o`): 1,353 fault audit records in total.

A null value does not add a record, while a duplicate does. That is why the delivery count increases by 525 rather than by the total number of fault audit records.

## 11. Keep the truth separate from what the pipeline sees

A dataset becomes much harder to use for honest evaluation if the system being evaluated can directly read the answer key.

The generator therefore separates the public event data from restricted scenario and fault truth. The public events describe the measurements available to a downstream consumer. Restricted artifacts describe the scenario or faults that explain how those measurements were produced.

This separation is especially valuable when testing data-quality detection, anomaly analysis, or downstream processing. A component should be evaluated on the information it is supposed to receive, not on hidden labels that disclose the intended outcome.

The distinction is not just about access permissions. It is about preserving a fair boundary between input data and evaluation truth.

## 12. Schema contracts: making events predictable

A schema is the agreement between the component that produces an event and the component that consumes it.

Without that agreement, a field may be renamed, a numeric measurement may become a string, or an unexpected field may silently appear. The producer might consider the change harmless, while a downstream job fails or, worse, produces incorrect results.

The generator uses versioned JSON Schema contracts for the district sensor events, meter reading events, and scenario truth. The public event schemas reject additional properties, helping keep the event shapes explicit rather than allowing undocumented fields to accumulate.

The `schema_version` field identifies the contract version carried by an event. It is not a substitute for validation, but it helps make the event's intended structure visible to consumers.

The two event examples near the beginning of this document show the principal fields and their meaning. For strict machine validation, the versioned schema files are the authoritative definition of the allowed types, required fields, and property constraints.

## 13. MinIO: keeping the run's artifacts together

Kafka is useful for moving events, but a generated run also contains files that are not themselves live event messages. Configuration, reference data, manifests, validation reports, and restricted truth need somewhere durable to live.

MinIO provides S3-compatible object storage for these artifacts and for archived event batches.

The setup separates storage into three buckets:

- `water-raw` stores public source artifacts, reference data, and run metadata.
- `water-restricted` stores restricted scenario and fault truth.
- `water-events` stores event batches archived from Kafka.

The direct artifact upload step places the public artifacts in `water-raw` and restricted truth in `water-restricted`. The event archiver has a different responsibility: it consumes the Kafka topics and writes batches of events into `water-events`.

These are related storage paths, but they are not the same operation. Uploading the generated source files does not prove that the Kafka event stream has been archived successfully.

The local MinIO API is available at `http://localhost:9000`, and the Console is available at `http://localhost:9001`.

## 14. Kafka: moving the events

Once the event data is ready for publishing, Kafka acts as the message transport layer.

The system uses two versioned topics:

- `water.district-sensors.v1`
- `water.meter-readings.v1`

The event is serialized as the message value, while `event_id` is used as the Kafka key. The key gives each message a stable event identity at the publishing boundary, which can be useful for inspecting and tracing records.

The Kafka setup uses Apache Kafka 3.7.1 in KRaft mode, so it does not require a separate ZooKeeper service. Within the Docker Compose network, the broker is reachable at `kafka:9092`. Kafka UI is available locally at `http://localhost:8080` for inspecting topics and observing message activity.

The generator can publish either the clean public stream or the delivery stream. That makes it possible to test the transport path with clean events and then repeat the exercise with configured data-quality faults.

Publishing events is only one stage of the journey. Seeing the message count increase in Kafka confirms that messages are arriving at the broker; it does not, by itself, confirm that every message has been archived, validated by a downstream consumer, or processed correctly.

## 15. The archiver: turning a stream into stored batches

A stream is useful for consumers that need to react to incoming events. Stored batches are useful for replay, inspection, and later processing.

The event archiver bridges those needs. It consumes both event topics using a separate consumer group named `water-event-archiver` and writes compressed newline-delimited JSON batches into the `water-events` bucket.

The default batch thresholds are:

- 10,000 messages
- 32 MiB of raw buffered data
- 30 seconds

These thresholds provide several ways to flush a batch instead of waiting for one condition alone. The resulting objects use a topic-, run-, partition-, and offset-based naming pattern so archived data can be associated with its source stream.

The archiver commits Kafka offsets after a successful object upload. This ordering reduces the risk of acknowledging messages before their batch has been stored. It does not provide end-to-end exactly-once processing: retries and failures can still produce repeated data, so downstream consumers should not assume that every event appears exactly once across the whole system.

That is an important engineering distinction. A reliable pipeline needs to define how it behaves when a step fails, not just how it behaves when every step succeeds.

## 16. Validation: what has actually been checked?

A successful run should be more than a directory full of files. It should leave evidence that the expected output was produced and that the checks performed on it passed.

The reference run's validation report for the clean public streams recorded:

```json
{
  "district_records": 216000,
  "errors": [],
  "meter_records": 3600000,
  "status": "passed"
}
```

This confirms that the clean public district and meter streams passed the generator's reported validation checks with the expected record counts.

It does not establish that every possible hydraulic behavior is realistic, that the faulted delivery stream passes the same checks, or that every Kafka message has been archived into MinIO. Those are separate questions requiring their own evidence.

Keeping validation claims specific makes the documentation more trustworthy. A passed check should mean exactly what it checked, rather than becoming a blanket claim that the entire pipeline is flawless.

## 17. How to reproduce the reference flow

The project is designed to run through Docker Compose, with the generator, Kafka, MinIO, Kafka UI, initialization service, and event archiver connected through the Compose environment.

The reference run directory inside the generator container is:

`/app/output/run-72b96af88a342375`

To publish the clean public streams to Kafka:

```powershell
docker compose run --rm --no-deps generator publish-kafka --run-dir /app/output/run-72b96af88a342375 --input-set public --bootstrap-servers kafka:9092 --speed 0
```

To publish the delivery streams with the configured faults:

```powershell
docker compose run --rm --no-deps generator publish-kafka --run-dir /app/output/run-72b96af88a342375 --input-set delivery --bootstrap-servers kafka:9092 --speed 0
```

To upload the generated source artifacts and restricted truth to their corresponding MinIO buckets:

```powershell
docker compose run --rm generator archive-minio --run-dir /app/output/run-72b96af88a342375
```

The last command is the direct artifact upload step. It should not be confused with the separate Kafka event archiver.

The host-side reference run is stored under:

`Generator\output\run-72b96af88a342375`

Docker Compose uses named volumes for Kafka and MinIO persistence. A normal `docker compose down` stops the services without deleting those volumes. Using `docker compose down -v` removes the named volumes and therefore deletes the persisted Kafka and MinIO data associated with them.

## 18. What makes this more than a data generator?

The strength of this design is not simply that it can create a large dataset. It connects several decisions that are easy to overlook when building a test pipeline.

First, the measurements come from a hydraulic simulation, giving them a shared physical basis. Second, the two streams have explicit measurement intervals and units, so their values can be interpreted and aligned correctly. Third, clean public data is kept separate from faulted delivery data and restricted truth, allowing the pipeline to be tested without exposing the answer key. Fourth, each run carries configuration and validation artifacts, making results easier to trace and reproduce. Finally, the Kafka and MinIO paths separate event transport from durable storage, with the archiver's retry and offset behavior made explicit.

Each decision solves a different problem. Together, they create a more meaningful environment for developing and evaluating downstream data engineering.

There are still boundaries. The network is simplified, the reference configuration represents a particular set of assumptions, and successful generator validation does not automatically prove that every later pipeline stage is correct. These limits should remain visible as the project grows.

That is part of the engineering story too: a useful system is not one that claims to model everything. It is one that makes its assumptions clear, produces data that can be traced back to those assumptions, and gives the next component a well-defined contract to work with.

#

## 20. The point of the whole exercise

A downstream pipeline should not have to guess what a field means, which time interval a reading represents, whether a null value is intentional, or where the data came from.

This part of Egypt Water Intelligence is designed to answer those questions before the events reach the next stage. The simulation gives the measurements a physical basis. The event contracts define how those measurements are represented. The clean and delivery datasets separate the simulated truth from the imperfections introduced for testing. The run artifacts preserve the context, while Kafka and MinIO provide distinct paths for moving and storing the data.

The result is a controlled starting point for the rest of the system: not a claim that every aspect of a real water network has been captured, but a dataset and ingestion path whose behavior, assumptions, and boundaries can be examined rather than guessed.

That is what makes the data useful. It is not only available; it has a story behind it, and the pipeline can preserve that story as the data moves forward.