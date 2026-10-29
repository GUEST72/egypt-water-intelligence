# 7. Design decisions: the problems that shaped this pipeline

This page lists the design-level problems we met, and the decision each one forced. It is the "why" behind the
architecture, and the material for answering questions in a presentation.

## Problems and decisions

### "The command finished" is not "the data is complete"
A publisher can return while the archiver still holds messages in memory; a Flink job can be "running" without having
processed the last record; silence on a topic can mean a slow publisher, not the end.
**Decision:** every stage proves completion from durable facts: committed Kafka positions (which the archiver and
Flink only advance after their output is safe), archive files that cover every published offset, and record counts
that equal the generator's manifest. Timeouts only ever cause a failure, never a success.

### Flink's results do not say which run they belong to
Flink's output rows carry no `run_id`, so if two runs went through the same topics their results would be mixed.
**Decision:** one run owns the streaming namespace; when Flink is verified for that run, the end positions of its
output topics are frozen, and loading into Iceberg reads only up to them. Later runs are still archived and
batch-processed. The live demo uses its own `demo.` topics for the same reason.

### Events must reach the lakehouse through the streaming infrastructure
The generator's upload command also copied the event files straight into MinIO, which let the batch path bypass
Kafka entirely.
**Decision:** events reach MinIO **only** through Kafka and the archiver. The orchestration uploads reference files
and the answer key itself, and the batch job reads only the proven-complete Kafka archive.

### A completed run must never change
The batch job recognises unchanged input by a fingerprint that includes file timestamps. Re-uploading the same
files would change those timestamps and force a needless rebuild.
**Decision:** completed runs are immutable. The raw upload, archive manifest and publication record are verified on
re-runs, never rewritten. Completion markers are always written last.

### An interrupted publication cannot simply be repeated
A Kafka hiccup stopped the first publication at 3.3 M of 3.8 M records. Repeating it would duplicate 3.3 M records;
skipping it would leave a gap.
**Decision:** a verified resume. The publisher's order is deterministic, so the orchestration proves Kafka holds an
exact prefix of that order and sends only the rest. An interrupted publication is never resumed silently: it must be
requested.

### A bigger dataset made the incidents smaller
Scaling from 600 to 1,500 meters multiplies normal flow by 2.5, but a leak's size does not depend on the number of
meters, so every incident shrank relative to the flow and some fell below detection thresholds. And in the original
configuration, duplicates and nulls hit the same records, so duplicates were always rejected and deduplication was
never exercised.
**Decision:** leak coefficients scaled by 2.5 (checked with a short simulation before the full run); 13 incidents spread
across all districts without overlapping in time; nulls and duplicates on different prime-number cadences, so every
duplicate is a valid copy. Six districts were kept because Flink's peer-comparison job is built for exactly six.

### A finite replay never closes its last window
The last 15-minute window and the last day never close when the data simply stops.
**Decision:** expected counts are derived from the manifest with this rule built in (14,394 windows, not 14,400),
instead of hard-coded numbers or false failures.

### Sensors alone cannot tell a burst from a demand surge
**Decision:** combine the paths. The incident-balance mart explains each streaming incident with the batch meter
balance, and separates leaks, demand and supply problems without using the answer key.

### The answer key must never influence detection
**Decision:** truth files live in a separate bucket; only the evaluation asset reads them, and only after detection.
The evaluation must agree with the Flink teammate's own script.

### A dashboard must be live to show what streaming is for
**Decision:** two dashboards with different roles: Grafana for live operations (fed in seconds by a small bridge from
Flink's topics), Power BI for verified analysis.

## What makes this pipeline different

- **Proof instead of hope.** Every stage proves its output is complete and correct before the next one starts:
  offset coverage of the archive, line-by-line comparison with the source, record counts against the manifest,
  committed consumer positions, independent validation of the batch tables.
- **Safe to run again.** A full second run takes about 4 minutes and changes nothing: everything already done is
  verified and skipped.
- **Recovery without duplicates.** An interrupted publication resumes from a proven point.
- **Truly incremental batch.** Each run is processed once, only its own files are read, only its own partitions are
  written, and new runs can be picked up automatically.
- **Both paths in one lakehouse.** Streaming and batch results are Iceberg tables in the same catalog, queryable
  together, each namespace with a single writer.
- **Explained detections.** Every streaming incident is classified as leak, demand or supply from meter data,
  answering a question the stream alone cannot.
- **Honest evaluation.** Detection never sees the answer key; the evaluation is cross-checked with the original
  script, and its limits (the false alarm, the undetectable background loss) are explained, not hidden.
- **Live and analytical views** of the same data.
