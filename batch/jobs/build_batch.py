"""Incremental batch job: one completed, fully archived run -> cleaned Iceberg tables.

Reference artifacts come from water-raw/<run_id>/artifacts; delivery events come from the Kafka archive
(water-events/events/<topic>/<run_id>/*.ndjson.gz), limited to the objects listed in
water-events/_manifests/<run_id>/archive-manifest.json, which orchestration writes only after proving the
archive covers every published offset.

Reject invalid rows before deduplication. Identical valid deliveries collapse;
conflicting valid copies of an event stop the build. Each run is an immutable
batch boundary; reruns replace only that run and record completion last.
"""
import argparse
import hashlib
import json
import os
import re
from pathlib import Path
from tempfile import TemporaryDirectory

from pyspark import StorageLevel
from pyspark.sql import functions as F

from handoff_smoke import read_json, spark_session

PIPELINE_VERSION = "3"  # 3: delivery events read from the Kafka archive (water-events)
COMMON = """schema_version string, event_id string, run_id string, district_id string,
    interval_start_utc string, interval_end_utc string, event_time_utc string,
    availability_time_utc string, quality_code string"""
EVENT_SCHEMAS = {
    "district": COMMON + """, inlet_link_id string, pressure_monitor_node_id string,
        inlet_flow_m3_s double, inlet_volume_m3 double, pressure_m_h2o double""",
    "meter": COMMON + ", meter_id string, consumed_volume_m3 double",
}
REFERENCE_SCHEMAS = {
    "district": """schema_version string, district_id string, inlet_link_id string,
        pressure_monitor_node_id string, service_junction_ids array<string>,
        has_cross_boundary_transfer boolean, has_internal_storage boolean""",
    "meter": """meter_id string, district_id string, customer_class string,
        service_junction_id string, baseline_requested_m3_s double""",
}
TABLE_KEYS = {
    "districts": ["district_id"],
    "meters": ["meter_id"],
    "district_clean": ["event_id"],
    "meter_clean": ["event_id"],
    "water_balance_15m": ["district_id", "interval_start_utc"],
    "dim_district": ["district_id"],
    "dim_meter": ["meter_id"],
    "dim_interval": ["interval_start_utc"],
    "fact_water_balance_15m": ["district_id", "interval_start_utc"],
    "fact_meter_consumption_15m": ["meter_id", "interval_start_utc"],
    "gold_district_daily_water_balance": ["district_id", "date_utc"],
    "gold_district_daily_consumption_by_class": ["district_id", "date_utc", "customer_class"],
    "gold_data_quality_daily": ["district_id", "date_utc", "stream", "outcome"],
    "quality_report": ["stream"],
    "batch_manifest": [],
}
GOLD_TABLES = (
    "dim_district", "dim_meter", "dim_interval",
    "fact_water_balance_15m", "fact_meter_consumption_15m",
    "gold_district_daily_water_balance",
    "gold_district_daily_consumption_by_class", "gold_data_quality_daily",
)
# The utility's local time zone (public: utility.timezone in the run configuration).
LOCAL_TZ = "Africa/Cairo"
TABLES = tuple(name for name in TABLE_KEYS if name not in GOLD_TABLES) + ("rejected_events",) + GOLD_TABLES
TIMES = ("interval_start_utc", "interval_end_utc", "event_time_utc", "availability_time_utc")


def arguments(description, include_reprocess=False):
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--namespace", default="analytics", help="Silver and quality namespace in catalog water")
    parser.add_argument("--gold-namespace", default="gold", help="Gold star-schema namespace in catalog water")
    if include_reprocess:
        parser.add_argument(
            "--reprocess",
            action="store_true",
            help="rebuild this complete run even if its source fingerprint is unchanged",
        )
    args = parser.parse_args()
    if not re.fullmatch(r"run-[0-9a-f]{16}", args.run_id):
        parser.error("--run-id must be run- followed by 16 lowercase hex digits")
    for name, value in (("--namespace", args.namespace), ("--gold-namespace", args.gold_namespace)):
        if not re.fullmatch(r"[a-z][a-z0-9_]*", value):
            parser.error(name + " must contain lowercase letters, digits or underscores")
    return args


class SourceNotReady(ValueError):
    """A required source object is missing: the run is not (yet) eligible for processing."""


def archive_manifest_key(run_id):
    return "_manifests/" + run_id + "/archive-manifest.json"


def stat(client, bucket, key):
    from minio.error import S3Error
    try:
        return client.stat_object(bucket, key)
    except S3Error as exc:
        if exc.code in {"NoSuchKey", "NoSuchObject", "NotFound"}:
            raise SourceNotReady(bucket + "/" + key + " is missing; the run is not ready for batch processing") from exc
        raise


def archive_objects(client, run_id):
    """Delivery events come from the Kafka archive, limited to the objects the archive manifest proved complete."""
    events_bucket = os.environ["MINIO_EVENT_BUCKET"]
    stat(client, events_bucket, archive_manifest_key(run_id))
    archive = read_json(client, events_bucket, archive_manifest_key(run_id))
    if archive.get("run_id") != run_id or archive.get("status") != "complete":
        raise SourceNotReady("Archive manifest for " + run_id + " is not complete")
    return events_bucket, {stream: archive["topics"][stream]["objects"] for stream in ("district", "meter")}


def source_fingerprint(client, bucket, run_id):
    source_keys = (
        run_id + "/_RUN_COMPLETE",
        run_id + "/artifacts/run-manifest.json",
        run_id + "/artifacts/validation-report.json",
        run_id + "/artifacts/districts.ndjson",
        run_id + "/artifacts/meters.ndjson",
    )
    events_bucket, archived = archive_objects(client, run_id)
    sources = [(bucket, key) for key in source_keys] + [(events_bucket, archive_manifest_key(run_id))]
    sources += [(events_bucket, key) for stream in ("district", "meter") for key in archived[stream]]
    objects = []
    for source_bucket, key in sources:
        info = stat(client, source_bucket, key)
        objects.append({
            "key": source_bucket + "/" + key,
            "etag": info.etag,
            "size": info.size,
            "last_modified": info.last_modified.isoformat(),
            "version_id": info.version_id,
        })

    fingerprint_data = json.dumps(
        {"pipeline_version": PIPELINE_VERSION, "objects": objects},
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(fingerprint_data).hexdigest()


def source_manifest(run_id):
    from minio import Minio
    client = Minio(os.environ["MINIO_ENDPOINT"], access_key=os.environ["MINIO_ACCESS_KEY"],
                   secret_key=os.environ["MINIO_SECRET_KEY"], secure=False)
    bucket = os.environ["MINIO_RAW_BUCKET"]
    fingerprint = source_fingerprint(client, bucket, run_id)
    manifest = read_json(client, bucket, run_id + "/artifacts/run-manifest.json")
    report = read_json(client, bucket, run_id + "/artifacts/validation-report.json")
    if manifest.get("run_id") != run_id or manifest.get("status") != "complete" or report.get("status") != "passed":
        raise ValueError("Selected raw run is incomplete or failed generation validation")
    return client, bucket, manifest, fingerprint


def require(condition, message):
    if not condition:
        raise ValueError(message)


def unique(frame, keys, label):
    require(not frame.groupBy(*keys).count().filter("count > 1").take(1), label + ": duplicate keys")
    require(not frame.filter(F.expr(" OR ".join("`%s` IS NULL" % key for key in keys))).take(1),
            label + ": null keys")


def read_events(spark, path, stream):
    lines = spark.read.text(str(path)).filter(F.length(F.trim("value")) > 0)
    schema = EVENT_SCHEMAS[stream] + ", _corrupt_record string"
    return lines.select(F.col("value").alias("raw_json"),
                        F.from_json("value", schema).alias("event")).select("raw_json", "event.*")


def clean_events(raw, references, stream, run_id):
    """Return clean rows, rejected deliveries and mutually exclusive accounting counts."""
    require(not raw.filter(F.col("run_id").isNotNull() & (F.col("run_id") != run_id)).take(1),
            "Foreign run found in " + stream + " input")
    reference_keys = ["district_id"] if stream == "district" else ["meter_id", "district_id"]
    extra_refs = [] if stream == "meter" else [
        F.col("inlet_link_id").alias("_ref_inlet"),
        F.col("pressure_monitor_node_id").alias("_ref_pressure"),
    ]
    refs = references.select(*reference_keys, *extra_refs).withColumn("_known_reference", F.lit(True))
    data = raw.join(F.broadcast(refs), reference_keys, "left")
    checks = {
        "malformed_json": F.col("_corrupt_record").isNull(),
        "schema_version": F.col("schema_version") == "1.0.0",
        "run_id": F.col("run_id") == run_id,
        "event_id": F.length(F.trim("event_id")) > 0,
        "reference": F.col("_known_reference"),
        "quality_code": F.col("quality_code").isin("valid", "noisy"),
    }
    for name in TIMES:
        # Explicit UTC suffix; do not interpret timezone-free strings as local time.
        data = data.withColumn("_valid_" + name,
                               F.col(name).rlike(r"(Z|\+00:00)$") & F.to_timestamp(name).isNotNull())
        checks[name] = F.col("_valid_" + name)
        data = data.withColumn(name, F.to_timestamp(name))
    seconds = 60 if stream == "district" else 900
    start = F.col("interval_start_utc").cast("double")
    checks["interval"] = ((F.col("interval_end_utc").cast("double") - start) == seconds) & (start % seconds == 0)
    checks["event_time"] = F.col("event_time_utc") == F.col("interval_start_utc" if stream == "district" else "interval_end_utc")
    measures = ["consumed_volume_m3"] if stream == "meter" else ["inlet_flow_m3_s", "inlet_volume_m3", "pressure_m_h2o"]
    for name in measures:
        checks[name] = (F.col(name) >= 0) & (F.col(name) < float("inf")) & ~F.isnan(name)
    if stream == "district":
        checks["sensor_reference"] = (
            F.col("inlet_link_id") == F.col("_ref_inlet")
        ) & (F.col("pressure_monitor_node_id") == F.col("_ref_pressure"))
    reasons = [F.when(~F.coalesce(valid, F.lit(False)), F.lit(reason)) for reason, valid in checks.items()]
    data = data.withColumn("reason", F.concat_ws(";", *reasons)).persist(StorageLevel.DISK_ONLY)
    rejected = data.filter("reason != ''").select(
        F.lit(run_id).alias("run_id"), F.col("run_id").alias("source_run_id"),
        F.lit(stream).alias("stream"), "event_id", "reason", "raw_json")
    valid = data.filter("reason = ''").drop("raw_json", "_corrupt_record", "_known_reference", "reason",
                                           "_ref_inlet", "_ref_pressure", *["_valid_" + name for name in TIMES])
    versions = valid.groupBy("run_id", "event_id").agg(
        F.countDistinct(F.to_json(F.struct(*[F.col(c) for c in sorted(valid.columns)]))).alias("versions"))
    require(not versions.filter("versions > 1").take(1), "Conflicting valid copies of an event in " + stream)
    clean = valid.dropDuplicates(["run_id", "event_id"]).persist(StorageLevel.DISK_ONLY)
    entity = "district_id" if stream == "district" else "meter_id"
    unique(clean, ["run_id", entity, "interval_start_utc"], stream + " intervals")
    raw_count, rejected_count, accepted_count = raw.count(), rejected.count(), clean.count()
    counts = (run_id, stream, raw_count, accepted_count, rejected_count,
              raw_count - rejected_count - accepted_count)
    return clean, rejected, counts, data


def water_balance(district, meter, districts, meters, start_seconds, end_seconds):
    """Include every expected interval; publish a gap only with complete coverage."""
    spark = district.sparkSession
    periods = spark.range(start_seconds, end_seconds, 900).select(
        F.col("id").cast("timestamp").alias("interval_start_utc"),
        (F.col("id") + 900).cast("timestamp").alias("interval_end_utc"))
    expected = meters.groupBy("run_id", "district_id").agg(F.count("meter_id").alias("expected_meters"))
    grid = districts.select("run_id", "district_id").join(expected, ["run_id", "district_id"], "left").fillna(0, ["expected_meters"]).crossJoin(periods)
    inlet = district.withColumn("interval_start_utc", F.window("interval_start_utc", "15 minutes")["start"]).groupBy(
        "run_id", "district_id", "interval_start_utc").agg(
        F.sum("inlet_volume_m3").alias("inlet_volume_m3"), F.count("event_id").alias("inlet_minutes"))
    consumption = meter.groupBy("run_id", "district_id", "interval_start_utc").agg(
        F.sum("consumed_volume_m3").alias("metered_consumption_m3"), F.count("meter_id").alias("meter_count"))
    keys = ["run_id", "district_id", "interval_start_utc"]
    balance = grid.join(inlet, keys, "left").join(consumption, keys, "left").fillna(
        0, ["inlet_minutes", "meter_count", "inlet_volume_m3", "metered_consumption_m3"])
    balance = balance.withColumn("complete", (F.col("inlet_minutes") == 15) & (F.col("meter_count") == F.col("expected_meters")))
    balance = balance.withColumn("gap_m3", F.when(F.col("complete"), F.col("inlet_volume_m3") - F.col("metered_consumption_m3")))
    return balance.withColumn("gap_pct", F.when(F.col("complete") & (F.col("inlet_volume_m3") > 0),
                                                100 * F.col("gap_m3") / F.col("inlet_volume_m3")))


def dimensional_outputs(districts, meters, meter_clean, balance, district_clean, rejected):
    """Build Gold dimensions, facts and a joined daily aggregate."""
    dim_district = districts
    dim_meter = meters
    local = F.from_utc_timestamp("interval_start_utc", LOCAL_TZ)  # wall-clock time in the utility's zone
    dim_interval = (
        balance.select("run_id", "interval_start_utc", "interval_end_utc").distinct()
        .withColumn("interval_key", F.col("interval_start_utc").cast("long"))
        .withColumn("date_utc", F.to_date("interval_start_utc"))
        .withColumn("hour_utc", F.hour("interval_start_utc"))
        .withColumn("minute_utc", F.minute("interval_start_utc"))
        .withColumn(
            "quarter_of_day",
            F.hour("interval_start_utc") * 4 + F.floor(F.minute("interval_start_utc") / 15).cast("int") + 1,
        )
        # Demand follows local habits (morning/evening peaks, Friday prayer, Fri/Sat weekend).
        .withColumn("local_date_cairo", F.to_date(local))
        .withColumn("local_hour_cairo", F.hour(local))
        .withColumn("day_of_week_cairo", F.date_format(local, "EEEE"))
        .withColumn("day_of_week_number_cairo", ((F.dayofweek(local) + 5) % 7 + 1).cast("int"))  # Monday = 1
        .withColumn("is_weekend_cairo", F.dayofweek(local).isin(6, 7))  # Friday, Saturday
    )

    fact_balance = balance.withColumn(
        "interval_key", F.col("interval_start_utc").cast("long")
    )
    fact_meter = meter_clean.select(
        "run_id", "meter_id", "district_id", "event_id", "interval_start_utc", "consumed_volume_m3"
    ).withColumn("interval_key", F.col("interval_start_utc").cast("long"))

    # Join the facts to descriptive dimensions, then aggregate only complete
    # intervals so missing readings are not treated as zero water consumption.
    district_attributes = dim_district.select(
        "run_id", "district_id", "inlet_link_id", "pressure_monitor_node_id",
        "has_cross_boundary_transfer", "has_internal_storage",
    )
    interval_attributes = dim_interval.select(
        "run_id", "interval_start_utc", "date_utc"
    )
    daily_source = (
        fact_balance.join(interval_attributes, ["run_id", "interval_start_utc"], "inner")
        .join(district_attributes, ["run_id", "district_id"], "inner")
    )
    group_columns = [
        "run_id", "district_id", "date_utc", "inlet_link_id",
        "pressure_monitor_node_id", "has_cross_boundary_transfer", "has_internal_storage",
    ]
    daily = daily_source.groupBy(*group_columns).agg(
        F.count("*").cast("long").alias("total_periods"),
        F.sum(F.when(F.col("complete"), 1).otherwise(0)).cast("long").alias("complete_periods"),
        F.sum(F.when(~F.col("complete"), 1).otherwise(0)).cast("long").alias("incomplete_periods"),
        F.sum(F.when(F.col("complete"), F.col("inlet_volume_m3")).otherwise(0.0)).alias("inlet_volume_m3"),
        F.sum(F.when(F.col("complete"), F.col("metered_consumption_m3")).otherwise(0.0)).alias("metered_consumption_m3"),
    )
    daily = (
        daily.withColumn(
            "coverage_pct", 100.0 * F.col("complete_periods") / F.col("total_periods")
        )
        .withColumn("gap_m3", F.col("inlet_volume_m3") - F.col("metered_consumption_m3"))
        .withColumn(
            "gap_pct",
            F.when(F.col("inlet_volume_m3") > 0, 100.0 * F.col("gap_m3") / F.col("inlet_volume_m3")),
        )
    )
    return {
        "dim_district": dim_district,
        "dim_meter": dim_meter,
        "dim_interval": dim_interval,
        "fact_water_balance_15m": fact_balance,
        "fact_meter_consumption_15m": fact_meter,
        "gold_district_daily_water_balance": daily,
        "gold_district_daily_consumption_by_class": consumption_by_class(fact_meter, dim_meter, dim_interval),
        "gold_data_quality_daily": data_quality_daily(district_clean, meter_clean, rejected),
    }


def consumption_by_class(fact_meter, dim_meter, dim_interval):
    """One row per district, UTC date and customer class: metered volume and reading coverage."""
    periods = dim_interval.groupBy("run_id", "date_utc").agg(F.count("*").cast("long").alias("periods"))
    meters_in_class = dim_meter.groupBy("run_id", "district_id", "customer_class").agg(
        F.count("*").cast("long").alias("meters"))
    readings = (
        fact_meter.withColumn("date_utc", F.to_date("interval_start_utc"))
        .join(F.broadcast(dim_meter.select("run_id", "meter_id", "customer_class")), ["run_id", "meter_id"])
        .groupBy("run_id", "district_id", "date_utc", "customer_class")
        .agg(F.sum("consumed_volume_m3").alias("consumed_volume_m3"),
             F.count("*").cast("long").alias("readings"))
    )
    return (
        readings.join(F.broadcast(meters_in_class), ["run_id", "district_id", "customer_class"])
        .join(F.broadcast(periods), ["run_id", "date_utc"])
        .withColumn("expected_readings", F.col("meters") * F.col("periods"))
        .withColumn("coverage_pct", 100.0 * F.col("readings") / F.col("expected_readings"))
        .withColumn("avg_volume_per_meter_m3", F.col("consumed_volume_m3") / F.col("meters"))
        .drop("periods")
    )


def data_quality_daily(district_clean, meter_clean, rejected):
    """Long format: one row per district, UTC date, stream and outcome ('accepted' or 'rejected:<reasons>').

    Rejected rows are located through their raw JSON; a delivery too malformed to name its district and
    interval cannot be placed on a day and stays only in rejected_events.
    """
    accepted = [
        clean.groupBy("run_id", "district_id", F.to_date("interval_start_utc").alias("date_utc"))
        .agg(F.count("*").cast("long").alias("records"))
        .withColumn("stream", F.lit(stream)).withColumn("outcome", F.lit("accepted"))
        for stream, clean in (("district", district_clean), ("meter", meter_clean))
    ]
    located = rejected.select(
        "run_id", "stream", F.concat(F.lit("rejected:"), F.col("reason")).alias("outcome"),
        F.get_json_object("raw_json", "$.district_id").alias("district_id"),
        F.to_date(F.to_timestamp(F.get_json_object("raw_json", "$.interval_start_utc"))).alias("date_utc"),
    ).filter(F.col("district_id").isNotNull() & F.col("date_utc").isNotNull())
    rejected_daily = located.groupBy("run_id", "district_id", "date_utc", "stream", "outcome").agg(
        F.count("*").cast("long").alias("records"))
    columns = ["run_id", "district_id", "date_utc", "stream", "outcome", "records"]
    return accepted[0].select(*columns).unionByName(accepted[1].select(*columns)).unionByName(rejected_daily.select(*columns))


def write_run(frame, table, run_id):
    """Replace exactly this run's partition (atomic per table); other runs' files are untouched.

    write.distribution-mode=none: every write holds a single run_id, so Iceberg's default hash distribution
    would shuffle all rows into one writer task for no benefit.
    """
    if not frame.sparkSession.catalog.tableExists(table):
        frame.writeTo(table).using("iceberg").partitionedBy("run_id").tableProperty("format-version", "2") \
            .tableProperty("write.distribution-mode", "none").create()
    else:
        frame.writeTo(table).overwrite(F.col("run_id") == run_id)


def write_batch_manifest(spark, table, run_id, fingerprint, namespace, gold_namespace, status):
    row = spark.createDataFrame(
        [(run_id, fingerprint, namespace, gold_namespace, status, PIPELINE_VERSION)],
        "run_id string, source_fingerprint string, namespace string, gold_namespace string, status string, pipeline_version string",
    ).withColumn("updated_at_utc", F.current_timestamp())
    write_run(row, table, run_id)


def build(spark, run_id, namespace, gold_namespace="gold", reprocess=False):
    client, bucket, manifest, fingerprint = source_manifest(run_id)
    spark.sql("CREATE NAMESPACE IF NOT EXISTS water." + namespace)
    spark.sql("CREATE NAMESPACE IF NOT EXISTS water." + gold_namespace)
    batch_manifest_table = "water." + namespace + ".batch_manifest"
    if not spark.catalog.tableExists(batch_manifest_table):
        empty_manifest = spark.createDataFrame(
            [],
            "run_id string, source_fingerprint string, namespace string, gold_namespace string, status string, pipeline_version string, updated_at_utc timestamp",
        )
        empty_manifest.writeTo(batch_manifest_table).using("iceberg").partitionedBy("run_id").tableProperty(
            "format-version", "2"
        ).create()
    previous = (
        spark.table(batch_manifest_table)
        .filter(F.col("run_id") == run_id)
        .select("source_fingerprint", "namespace", "gold_namespace", "status")
        .first()
    )
    if previous and previous.status == "completed":
        if (
            previous.source_fingerprint == fingerprint
            and previous.namespace == namespace
            and previous.gold_namespace == gold_namespace
            and not reprocess
        ):
            return {
                "run_id": run_id,
                "namespace": namespace,
                "gold_namespace": gold_namespace,
                "status": "skipped",
                "reason": "source fingerprint already completed",
            }
        if not reprocess:
            raise ValueError(
                "Source objects or output namespace changed after this run was processed; "
                "rerun with --reprocess to replace its outputs"
            )

    write_batch_manifest(
        spark, batch_manifest_table, run_id, fingerprint, namespace, gold_namespace, "processing"
    )
    with TemporaryDirectory(prefix="water-batch-") as folder:
        cached = []
        try:
            refs, raw = {}, {}
            for stream, filename in (("district", "districts"), ("meter", "meters")):
                path = Path(folder) / (filename + ".ndjson")
                client.fget_object(bucket, run_id + "/artifacts/" + path.name, str(path))
                refs[stream] = spark.read.schema(REFERENCE_SCHEMAS[stream]).option("mode", "FAILFAST").json(str(path)).withColumn("run_id", F.lit(run_id))
                unique(refs[stream], ["run_id", stream + "_id"], filename)
            require(not refs["meter"].join(refs["district"], ["run_id", "district_id"], "left_anti").take(1), "Unknown district in meter reference")
            events_bucket, archived = archive_objects(client, run_id)
            for stream in ("district", "meter"):
                stream_folder = Path(folder) / stream
                for key in archived[stream]:
                    client.fget_object(events_bucket, key, str(stream_folder / key.rsplit("/", 1)[-1]))
                raw[stream] = read_events(spark, stream_folder, stream)  # Spark reads the .ndjson.gz parts
            bounds = raw["district"].select(F.min(F.to_timestamp("interval_start_utc").cast("long")),
                                                F.max(F.to_timestamp("interval_end_utc").cast("long"))).first()
            start, end = bounds
            require(start is not None and end is not None and start % 900 == end % 900 == 0,
                    "District input must span complete UTC fifteen-minute intervals")
            require(end - start == manifest["logical_interval_count"] * 60, "Input time range differs from manifest")
            cleaned, rejects, counts = {}, [], []
            for stream in ("district", "meter"):
                clean, rejected, count, cache = clean_events(raw[stream], refs[stream], stream, run_id)
                require(count[2] == manifest["delivery_" + stream + "_records"], "Raw count differs from manifest: " + stream)
                cleaned[stream] = clean
                rejects.append(rejected)
                counts.append(count)
                cached.extend([clean, cache])
            balance = water_balance(
                cleaned["district"], cleaned["meter"], refs["district"], refs["meter"], start, end
            )
            rejected_events = rejects[0].unionByName(rejects[1])
            gold = dimensional_outputs(refs["district"], refs["meter"], cleaned["meter"], balance,
                                       cleaned["district"], rejected_events)
            outputs = {
                "districts": refs["district"], "meters": refs["meter"],
                "district_clean": cleaned["district"], "meter_clean": cleaned["meter"],
                "rejected_events": rejected_events,
                "water_balance_15m": balance,
                **gold,
                "quality_report": spark.createDataFrame(counts, "run_id string, stream string, raw_records long, accepted_records long, rejected_records long, duplicate_records long"),
            }
            for name, frame in outputs.items():
                if name == "quality_report":
                    continue
                target_namespace = gold_namespace if name in GOLD_TABLES else namespace
                write_run(frame, "water." + target_namespace + "." + name, run_id)
            write_run(outputs["quality_report"], "water." + namespace + ".quality_report", run_id)
        finally:
            for frame in cached:
                frame.unpersist()
    require(
        source_fingerprint(client, bucket, run_id) == fingerprint,
        "Source objects changed during processing; batch remains processing and must be retried",
    )
    write_batch_manifest(
        spark, batch_manifest_table, run_id, fingerprint, namespace, gold_namespace, "completed"
    )
    return {
        "run_id": run_id,
        "namespace": namespace,
        "gold_namespace": gold_namespace,
        "status": "completed",
        "source_fingerprint": fingerprint,
        "counts": [dict(zip(
            ["run_id", "stream", "raw_records", "accepted_records", "rejected_records", "duplicate_records"], row
        )) for row in counts],
    }


def main():
    args = arguments(__doc__, include_reprocess=True)
    spark = spark_session()
    spark.sparkContext.setLogLevel("WARN")
    try:
        print(json.dumps(build(
            spark, args.run_id, args.namespace, args.gold_namespace, args.reprocess
        ), sort_keys=True))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
