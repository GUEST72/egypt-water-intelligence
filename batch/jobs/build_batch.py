"""Small standalone batch job: MinIO NDJSON -> cleaned Iceberg tables.

Reject invalid rows before deduplication. Identical valid deliveries collapse;
conflicting valid copies of an event stop the build. Writes replace one run only.
"""
import argparse
import json
import os
import re
from pathlib import Path
from tempfile import TemporaryDirectory

from pyspark import StorageLevel
from pyspark.sql import functions as F

from handoff_smoke import read_json, spark_session

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
    "quality_report": ["stream"],
}
GOLD_TABLES = (
    "dim_district", "dim_meter", "dim_interval",
    "fact_water_balance_15m", "fact_meter_consumption_15m",
    "gold_district_daily_water_balance",
)
TABLES = tuple(name for name in TABLE_KEYS if name not in GOLD_TABLES) + ("rejected_events",) + GOLD_TABLES
TIMES = ("interval_start_utc", "interval_end_utc", "event_time_utc", "availability_time_utc")


def arguments(description):
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--namespace", default="analytics", help="Silver and quality namespace in catalog water")
    parser.add_argument("--gold-namespace", default="gold", help="Gold star-schema namespace in catalog water")
    args = parser.parse_args()
    if not re.fullmatch(r"run-[0-9a-f]{16}", args.run_id):
        parser.error("--run-id must be run- followed by 16 lowercase hex digits")
    for name, value in (("--namespace", args.namespace), ("--gold-namespace", args.gold_namespace)):
        if not re.fullmatch(r"[a-z][a-z0-9_]*", value):
            parser.error(name + " must contain lowercase letters, digits or underscores")
    return args


def source_manifest(run_id):
    from minio import Minio
    client = Minio(os.environ["MINIO_ENDPOINT"], access_key=os.environ["MINIO_ACCESS_KEY"],
                   secret_key=os.environ["MINIO_SECRET_KEY"], secure=False)
    bucket = os.environ["MINIO_RAW_BUCKET"]
    client.stat_object(bucket, run_id + "/_RUN_COMPLETE")
    manifest = read_json(client, bucket, run_id + "/artifacts/run-manifest.json")
    report = read_json(client, bucket, run_id + "/artifacts/validation-report.json")
    if manifest.get("run_id") != run_id or manifest.get("status") != "complete" or report.get("status") != "passed":
        raise ValueError("Selected raw run is incomplete or failed generation validation")
    return client, bucket, manifest


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


def dimensional_outputs(districts, meters, meter_clean, balance):
    """Build Gold dimensions, facts and a joined daily aggregate."""
    dim_district = districts
    dim_meter = meters
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
    }


def write_run(frame, table, run_id):
    if not frame.sparkSession.catalog.tableExists(table):
        frame.writeTo(table).using("iceberg").partitionedBy("run_id").tableProperty("format-version", "2").create()
    else:
        frame.writeTo(table).overwrite(F.col("run_id") == run_id)


def build(spark, run_id, namespace, gold_namespace="gold"):
    client, bucket, manifest = source_manifest(run_id)
    with TemporaryDirectory(prefix="water-batch-") as folder:
        refs, raw = {}, {}
        for stream, filename in (("district", "districts"), ("meter", "meters")):
            path = Path(folder) / (filename + ".ndjson")
            client.fget_object(bucket, run_id + "/artifacts/" + path.name, str(path))
            refs[stream] = spark.read.schema(REFERENCE_SCHEMAS[stream]).option("mode", "FAILFAST").json(str(path)).withColumn("run_id", F.lit(run_id))
            unique(refs[stream], ["run_id", stream + "_id"], filename)
        require(not refs["meter"].join(refs["district"], ["run_id", "district_id"], "left_anti").take(1), "Unknown district in meter reference")
        for stream, filename in (("district", "district-sensors"), ("meter", "meter-readings")):
            path = Path(folder) / (filename + ".ndjson")
            client.fget_object(bucket, run_id + "/delivery/" + path.name, str(path))
            raw[stream] = read_events(spark, path, stream)
        bounds = raw["district"].select(F.min(F.to_timestamp("interval_start_utc").cast("long")),
                                            F.max(F.to_timestamp("interval_end_utc").cast("long"))).first()
        start, end = bounds
        require(start is not None and end is not None and start % 900 == end % 900 == 0,
                "District input must span complete UTC fifteen-minute intervals")
        require(end - start == manifest["logical_interval_count"] * 60, "Input time range differs from manifest")
        cleaned, rejects, counts, cached = {}, [], [], []
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
        gold = dimensional_outputs(refs["district"], refs["meter"], cleaned["meter"], balance)
        outputs = {
            "districts": refs["district"], "meters": refs["meter"],
            "district_clean": cleaned["district"], "meter_clean": cleaned["meter"],
            "rejected_events": rejects[0].unionByName(rejects[1]),
            "water_balance_15m": balance,
            **gold,
            # Written last: evidence that all preceding table writes finished.
            "quality_report": spark.createDataFrame(counts, "run_id string, stream string, raw_records long, accepted_records long, rejected_records long, duplicate_records long"),
        }
        spark.sql("CREATE NAMESPACE IF NOT EXISTS water." + namespace)
        spark.sql("CREATE NAMESPACE IF NOT EXISTS water." + gold_namespace)
        for name, frame in outputs.items():
            if name == "quality_report":
                continue
            target_namespace = gold_namespace if name in GOLD_TABLES else namespace
            write_run(frame, "water." + target_namespace + "." + name, run_id)
        # The quality report is written last as the completion marker for the whole run.
        write_run(outputs["quality_report"], "water." + namespace + ".quality_report", run_id)
        for cache in cached:
            cache.unpersist()
    return {"run_id": run_id, "namespace": namespace, "gold_namespace": gold_namespace, "counts": [dict(zip(
        ["run_id", "stream", "raw_records", "accepted_records", "rejected_records", "duplicate_records"], row)) for row in counts]}


def main():
    args = arguments(__doc__)
    spark = spark_session()
    spark.sparkContext.setLogLevel("WARN")
    try:
        print(json.dumps(build(spark, args.run_id, args.namespace, args.gold_namespace), sort_keys=True))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
