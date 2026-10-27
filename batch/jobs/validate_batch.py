"""Validate saved batch tables against manifest, references, aggregates and Trino.

Print counts and order-independent content checksums; compare these after a rerun.
Any failed check raises an error and makes spark-submit exit unsuccessfully.
"""
import json
from urllib.request import Request, urlopen

from pyspark import StorageLevel
from pyspark.sql import functions as F

from build_batch import TABLES, TABLE_KEYS, arguments, require, source_manifest, unique
from handoff_smoke import spark_session


def no_bad_rows(frame, valid, label):
    require(not frame.filter(~F.coalesce(valid, F.lit(False))).take(1), label)


def trino_query(sql):
    request = Request("http://trino:8080/v1/statement", data=sql.encode(), method="POST",
                      headers={"X-Trino-User": "batch-validator"})
    rows = []
    while True:
        with urlopen(request, timeout=60) as response:
            page = json.load(response)
        if "error" in page:
            raise ValueError("Trino: " + page["error"].get("message", str(page["error"])))
        rows.extend(page.get("data", []))
        if not page.get("nextUri"):
            return rows
        request = Request(page["nextUri"], headers={"X-Trino-User": "batch-validator"})


def check_clean(frame, references, stream):
    entity = "meter_id" if stream == "meter" else "district_id"
    unique(frame, ["run_id", entity, "interval_start_utc"], stream + " interval keys")
    keys = ["run_id", "district_id"] + (["meter_id"] if stream == "meter" else [])
    require(not frame.join(references.select(*keys), keys, "left_anti").take(1), stream + ": unknown reference")
    seconds = 900 if stream == "meter" else 60
    start = F.col("interval_start_utc").cast("double")
    no_bad_rows(frame, (F.col("schema_version") == "1.0.0") & (F.length(F.trim("event_id")) > 0) &
                F.col("quality_code").isin("valid", "noisy") & F.col("availability_time_utc").isNotNull() &
                (start % seconds == 0) & (F.col("interval_end_utc").cast("double") - start == seconds) &
                (F.col("event_time_utc") == F.col("interval_end_utc" if stream == "meter" else "interval_start_utc")),
                stream + ": invalid schema, identity, timestamps or quality")
    measures = ["consumed_volume_m3"] if stream == "meter" else ["inlet_flow_m3_s", "inlet_volume_m3", "pressure_m_h2o"]
    for name in measures:
        no_bad_rows(frame, (F.col(name) >= 0) & (F.col(name) < float("inf")) & ~F.isnan(name), stream + ": invalid " + name)
    if stream == "district":
        refs = references.select("run_id", "district_id", F.col("inlet_link_id").alias("ref_inlet"),
                                 F.col("pressure_monitor_node_id").alias("ref_pressure"))
        no_bad_rows(frame.join(refs, ["run_id", "district_id"]),
                    (F.col("inlet_link_id") == F.col("ref_inlet")) &
                    (F.col("pressure_monitor_node_id") == F.col("ref_pressure")), "District sensor does not match reference")


def close(left, right):
    return F.abs(left - right) <= 1e-8 * F.greatest(F.lit(1.0), F.abs(right))


def check_balance(tables, manifest):
    balance = tables["water_balance_15m"]
    expected_periods = manifest["logical_interval_count"] // 15
    require(manifest["logical_interval_count"] % 15 == 0, "Manifest duration must contain whole fifteen-minute intervals")
    coverage = balance.groupBy("run_id", "district_id").agg(
        F.count("*").alias("periods"), F.min("interval_start_utc").alias("first"), F.max("interval_end_utc").alias("last"))
    require(coverage.select("run_id", "district_id").exceptAll(tables["districts"].select("run_id", "district_id")).count() == 0
            and tables["districts"].select("run_id", "district_id").exceptAll(coverage.select("run_id", "district_id")).count() == 0,
            "Balance is missing districts or contains unknown districts")
    no_bad_rows(coverage, (F.col("periods") == expected_periods) &
                (F.col("last").cast("long") - F.col("first").cast("long") == expected_periods * 900), "Balance interval grid is incomplete")
    start = F.col("interval_start_utc").cast("double")
    no_bad_rows(balance, (start % 900 == 0) & (F.col("interval_end_utc").cast("double") - start == 900), "Balance interval is invalid")
    keys = ["run_id", "district_id", "interval_start_utc"]
    # Derive the aggregates from saved events independently of the builder's window expression.
    inlet = tables["district_clean"].withColumn("interval_start_utc", (
        F.floor(F.col("interval_start_utc").cast("long") / 900) * 900).cast("timestamp")).groupBy(*keys).agg(
        F.count("*").alias("actual_minutes"), F.sum("inlet_volume_m3").alias("actual_inlet"))
    meter = tables["meter_clean"].groupBy(*keys).agg(
        F.count("*").alias("actual_meters"), F.sum("consumed_volume_m3").alias("actual_consumption"))
    for aggregate in (inlet, meter):
        require(not aggregate.join(balance.select(*keys), keys, "left_anti").take(1), "Clean events fall outside balance grid")
    expected = tables["meters"].groupBy("run_id", "district_id").agg(F.count("*").alias("actual_expected_meters"))
    joined = balance.join(inlet, keys, "left").join(meter, keys, "left").join(expected, ["run_id", "district_id"], "left").fillna(
        0, ["actual_minutes", "actual_inlet", "actual_meters", "actual_consumption", "actual_expected_meters"])
    no_bad_rows(joined, (F.col("inlet_minutes") == F.col("actual_minutes")) &
                (F.col("meter_count") == F.col("actual_meters")) & (F.col("expected_meters") == F.col("actual_expected_meters")) &
                close(F.col("inlet_volume_m3"), F.col("actual_inlet")) &
                close(F.col("metered_consumption_m3"), F.col("actual_consumption")), "Balance differs from saved clean events")
    complete = (F.col("actual_minutes") == 15) & (F.col("actual_meters") == F.col("actual_expected_meters"))
    no_bad_rows(joined, F.col("complete") == complete, "Incorrect balance completeness flag")
    no_bad_rows(joined, F.when(complete, close(F.col("gap_m3"), F.col("actual_inlet") - F.col("actual_consumption")))
                .otherwise(F.col("gap_m3").isNull()), "Incorrect or misleading balance gap")
    no_bad_rows(joined, F.when(complete & (F.col("actual_inlet") > 0),
                close(F.col("gap_pct"), 100 * (F.col("actual_inlet") - F.col("actual_consumption")) / F.col("actual_inlet")))
                .otherwise(F.col("gap_pct").isNull()), "Incorrect balance percentage")


def validate(spark, run_id, namespace):
    _, _, manifest = source_manifest(run_id)
    tables, summaries = {}, {}
    try:
        for name in TABLES:
            table = "water." + namespace + "." + name
            tables[name] = spark.table(table).filter(F.col("run_id") == run_id).persist(StorageLevel.DISK_ONLY)
            if name in TABLE_KEYS:
                unique(tables[name], ["run_id"] + TABLE_KEYS[name], name)
            summary = tables[name].agg(F.count("*").alias("records"),
                F.sum(F.xxhash64(*sorted(tables[name].columns)).cast("decimal(38,0)")).alias("checksum")).first()
            summaries[name] = {"records": summary.records, "checksum": str(summary.checksum)}
            require(trino_query("SELECT count(*) FROM " + table + " WHERE run_id = '" + run_id + "'") == [[summary.records]],
                    "Trino/Spark count mismatch: " + name)
        require(summaries["districts"]["records"] > 0 and summaries["meters"]["records"] > 0, "Reference tables are empty")
        require(not tables["meters"].join(tables["districts"], ["run_id", "district_id"], "left_anti").take(1), "Unknown meter district")
        for stream in ("district", "meter"):
            check_clean(tables[stream + "_clean"], tables["districts" if stream == "district" else "meters"], stream)
        no_bad_rows(tables["rejected_events"], (F.length("reason") > 0) & F.col("raw_json").isNotNull() &
                    F.col("stream").isin("district", "meter"), "Rejected deliveries lack evidence")
        reports = tables["quality_report"].collect()
        require(sorted(row.stream for row in reports) == ["district", "meter"], "Quality report must contain both streams")
        for row in reports:
            require(row.raw_records == manifest["delivery_" + row.stream + "_records"], "Input count differs from manifest")
            require(row.accepted_records == summaries[row.stream + "_clean"]["records"], "Accepted count differs from saved events")
            require(row.rejected_records == tables["rejected_events"].filter(F.col("stream") == row.stream).count(), "Rejected count differs from saved rejects")
            require(all(x is not None and x >= 0 for x in (row.raw_records, row.accepted_records, row.rejected_records, row.duplicate_records)), "Invalid accounting count")
            require(row.raw_records == row.accepted_records + row.rejected_records + row.duplicate_records, "Input accounting does not reconcile")
        check_balance(tables, manifest)
        return {"status": "passed", "run_id": run_id, "namespace": namespace, "tables": summaries}
    finally:
        for frame in tables.values():
            frame.unpersist()


def main():
    args = arguments(__doc__)
    spark = spark_session()
    spark.sparkContext.setLogLevel("WARN")
    try:
        print(json.dumps(validate(spark, args.run_id, args.namespace), sort_keys=True))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
