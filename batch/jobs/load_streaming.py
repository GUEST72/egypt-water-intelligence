"""Load Flink's Kafka outputs for one run into Iceberg (namespace water.streaming).

Bounded read: each topic is read from its earliest offset up to the end offsets passed by orchestration,
which were measured after every Flink job had consumed its input to the end. Flink rows carry no run_id;
orchestration only calls this for the run that owns the streaming namespace, and the run_id is added here.

Flink's Kafka sinks are at-least-once, so identical rows can repeat after a job restart:
  fact_district_alert       one row per alert_id
  fact_district_window_15m  one row per district and 15-minute window (features joined with relative values)
  fact_dead_letter          one row per rejected source offset
Each table is partitioned by run_id and only this run's partition is replaced.
"""
import argparse
import json
import re

from pyspark.sql import functions as F

from build_batch import write_run
from handoff_smoke import spark_session

TOPICS = {
    "alerts": "water.district-alerts.v1",
    "features": "water.district-features.15m.v1",
    "relative": "water.district-relative.15m.v1",
    "dead_letter": "water.dead-letter.district.v1",
}
SCHEMAS = {
    "alerts": """schema_version string, alert_id string, alert_type string, rule_id string, severity string,
        status string, district_id string, window_start string, window_end string, first_flagged_window_start string,
        rel_flow double, rel_pressure double, avg_flow_m3_s double, min_pressure_m double, n_events long,
        n_null_pressure long, n_flagged long, reason string, source string, baseline_rel_flow double,
        level_shift_pct double""",
    "features": """window_start string, window_end string, district_id string, avg_flow_m3_s double,
        inlet_volume_m3 double, min_pressure_m double, avg_pressure_m double, n_events long,
        n_null_pressure long, n_flagged long""",
    "relative": """window_start string, window_end string, district_id string, avg_flow_m3_s double,
        min_pressure_m double, rel_flow double, rel_pressure double, cond string""",
    "dead_letter": """job string, source_topic string, source_partition int, source_offset long,
        reject_reason string, raw_payload string""",
}
# Flink writes TIMESTAMP(3) as ISO-8601 without a zone; every job sets table.local-time-zone=UTC.
FLINK_TIME = "yyyy-MM-dd'T'HH:mm:ss[.SSS]"


def read_topic(spark, bootstrap, name, end_offsets):
    topic = TOPICS[name]
    ending = {topic: {str(p): int(o) for p, o in end_offsets[topic].items()}}
    starting = {topic: {str(p): -2 for p in end_offsets[topic]}}  # -2 = earliest
    raw = (spark.read.format("kafka")
           .option("kafka.bootstrap.servers", bootstrap)
           .option("subscribe", topic)
           .option("startingOffsets", json.dumps(starting))
           .option("endingOffsets", json.dumps(ending))
           .load())
    return raw.select(F.from_json(F.col("value").cast("string"), SCHEMAS[name]).alias("r")).select("r.*")


def timestamps(frame, *columns):
    for column in columns:
        frame = frame.withColumn(column, F.to_timestamp(column, FLINK_TIME))
    return frame


def build(spark, run_id, bootstrap, end_offsets, namespace):
    spark.sql("CREATE NAMESPACE IF NOT EXISTS water." + namespace)
    raw = {name: read_topic(spark, bootstrap, name, end_offsets) for name in TOPICS}

    alerts = timestamps(raw["alerts"], "window_start", "window_end", "first_flagged_window_start")
    alerts = alerts.dropDuplicates(["alert_id"]).withColumn("run_id", F.lit(run_id)) \
        .withColumn("alert_date", F.to_date("window_start"))

    features = timestamps(raw["features"], "window_start", "window_end").dropDuplicates(["district_id", "window_start"])
    relative = timestamps(raw["relative"], "window_start", "window_end").dropDuplicates(["district_id", "window_start"])
    windows = features.join(
        relative.select("district_id", "window_start", "rel_flow", "rel_pressure", "cond"),
        ["district_id", "window_start"], "left",
    ).withColumn("run_id", F.lit(run_id))

    dead = raw["dead_letter"].dropDuplicates(["source_topic", "source_partition", "source_offset"]) \
        .withColumn("run_id", F.lit(run_id))

    outputs = {"fact_district_alert": alerts, "fact_district_window_15m": windows, "fact_dead_letter": dead}
    report = {"run_id": run_id, "namespace": namespace, "status": "completed",
              "kafka_records": {name: raw[name].count() for name in TOPICS}, "tables": {}}
    for table, frame in outputs.items():
        write_run(frame, "water." + namespace + "." + table, run_id)
        report["tables"][table] = spark.table("water." + namespace + "." + table).filter(F.col("run_id") == run_id).count()
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--end-offsets", required=True, help='JSON {"topic": {"partition": end_offset}}')
    parser.add_argument("--bootstrap", default="kafka:9092")
    parser.add_argument("--namespace", default="streaming")
    args = parser.parse_args()
    if not re.fullmatch(r"run-[0-9a-f]{16}", args.run_id):
        parser.error("--run-id must be run- followed by 16 lowercase hex digits")
    end_offsets = json.loads(args.end_offsets)
    missing = [topic for topic in TOPICS.values() if topic not in end_offsets]
    if missing:
        parser.error("--end-offsets lacks " + ", ".join(missing))
    spark = spark_session()
    spark.sparkContext.setLogLevel("WARN")
    try:
        print(json.dumps(build(spark, args.run_id, args.bootstrap, end_offsets, args.namespace), sort_keys=True))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
