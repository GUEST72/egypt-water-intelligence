"""Lakehouse stage: streaming facts in Iceberg, and the offline evaluation of alerts against restricted truth."""
# No `from __future__ import annotations` here: Dagster resolves op/asset annotations at runtime.
import json
import re
import subprocess
import sys
import tempfile
from pathlib import Path

from dagster import AssetExecutionContext, Failure, MaterializeResult, MetadataValue, asset

from .. import checks
from ..evaluation import evaluate
from ..resources import ComposeResource, KafkaResource, MinioResource, TrinoResource
from ..screenshots import prompt
from ..settings import PATHS, setting
from .batch import SPARK, _spark, batch_validation
from .source import generator_runs
from .streaming import ALERTS_TOPIC, _registry, flink_district_outputs

MARTS_DIR = PATHS.repo / "infrastructure" / "trino" / "marts"


def _owned_entry(minio: MinioResource, run_id: str) -> dict:
    registry = _registry(minio)
    entry = registry["runs"].get(run_id, {})
    if registry["streaming_owner"] != run_id or "flink_output_end_offsets" not in entry:
        raise Failure(description=f"{run_id} has no verified Flink outputs (materialize flink_district_outputs first)")
    return entry


@asset(
    group_name="lakehouse",
    op_tags=SPARK,
    partitions_def=generator_runs,
    deps=[flink_district_outputs],
    description=(
        "water.streaming.fact_district_alert / fact_district_window_15m / fact_dead_letter: Flink's Kafka outputs, "
        "read by Spark up to the offsets frozen when Flink finished this run, deduplicated, partitioned by run_id."
    ),
)
def streaming_iceberg_facts(context: AssetExecutionContext, compose: ComposeResource, minio: MinioResource) -> MaterializeResult:
    run_id = checks.require_run_id(context.partition_key)
    offsets = _owned_entry(minio, run_id)["flink_output_end_offsets"]
    report = _spark(context, compose, "load_streaming.py",
                    ["--run-id", run_id, "--end-offsets", json.dumps(offsets, separators=(",", ":"))])
    if report.get("status") != "completed":
        raise Failure(description=f"load_streaming did not complete: {report}")
    return MaterializeResult(metadata={
        "kafka_records_read": MetadataValue.json(report["kafka_records"]),
        **{f"{table}_rows": rows for table, rows in report["tables"].items()},
        **prompt(context, "http://localhost:8082/ui/",
                 "Trino: SELECT alert_type, status, count(*) FROM water.streaming.fact_district_alert GROUP BY 1, 2"),
    })


# ---------------------------------------------------------------- evaluation (restricted truth, after detection)

def _alerts_up_to(kafka: KafkaResource, end_offsets: dict[str, int]) -> list[dict]:
    from kafka import KafkaConsumer, TopicPartition
    consumer = KafkaConsumer(bootstrap_servers=kafka.bootstrap, group_id=None, enable_auto_commit=False,
                             consumer_timeout_ms=10000)
    rows = []
    try:
        for partition, end in end_offsets.items():
            tp = TopicPartition(ALERTS_TOPIC, int(partition))
            consumer.assign([tp])
            consumer.seek_to_beginning(tp)
            for message in consumer:
                if message.offset >= end:
                    break
                rows.append(json.loads(message.value))
                if message.offset + 1 >= end:
                    break
    finally:
        consumer.close()
    return rows


def _ndjson(minio: MinioResource, bucket: str, key: str) -> list[dict]:
    response = minio.client().get_object(bucket, key)
    try:
        return [json.loads(line) for line in response.read().splitlines() if line.strip()]
    finally:
        response.close()
        response.release_conn()


def _script_summary(alerts: list[dict], truth: list[dict], faults: list[dict], run_start: str) -> dict:
    """Run the Flink teammate's original evaluate_alerts.py on the same inputs and parse its numbers."""
    with tempfile.TemporaryDirectory() as folder:
        paths = {}
        for name, rows in (("alerts", alerts), ("truth", truth), ("faults", faults)):
            paths[name] = Path(folder) / f"{name}.ndjson"
            paths[name].write_text("".join(json.dumps(r) + "\n" for r in rows), encoding="utf-8")
        output = subprocess.run(
            [sys.executable, str(PATHS.repo / "flink" / "analysis" / "evaluate_alerts.py"),
             "--alerts", str(paths["alerts"]), "--truth", str(paths["truth"]), "--faults", str(paths["faults"]),
             "--run-start", run_start],
            capture_output=True, text=True, check=True).stdout
    number = lambda pattern: [int(x) for x in re.search(pattern, output).groups()]  # noqa: E731
    alerts_n, opens = number(r"alerts: (\d+) \| incident opens: (\d+)")
    detected, detectable = number(r"recall, detectable scope \(\d+\):\s+(\d+)/(\d+)")
    (false_alarms,) = number(r"false alarms \(open incident alerts\): (\d+)")
    dq_faults, dq_alerted = number(r"data quality: faults (\d+) \| alerted (\d+)")
    return {"alerts": alerts_n, "incident_opens": opens, "detectable_detected": detected,
            "scenarios_detectable": detectable, "false_alarms": false_alarms,
            "dq_faults": dq_faults, "dq_alerted": dq_alerted, "script_output": output}


def _sql(value) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return repr(value)
    return "'" + str(value).replace("'", "''") + "'"


@asset(
    group_name="evaluation",
    partitions_def=generator_runs,
    deps=[streaming_iceberg_facts],
    description=(
        "Alerts compared with the restricted scenario and fault truth, after detection. The only asset that reads "
        "water-restricted. Must agree with flink/analysis/evaluate_alerts.py. Tables in water.evaluation."
    ),
)
def alert_evaluation(context: AssetExecutionContext, kafka: KafkaResource, minio: MinioResource,
                     trino: TrinoResource) -> MaterializeResult:
    run_id = checks.require_run_id(context.partition_key)
    entry = _owned_entry(minio, run_id)
    alerts = _alerts_up_to(kafka, entry["flink_output_end_offsets"][ALERTS_TOPIC])
    restricted = setting("MINIO_RESTRICTED_BUCKET")
    truth = _ndjson(minio, restricted, f"{run_id}/restricted/scenario-truth.ndjson")
    faults = _ndjson(minio, restricted, f"{run_id}/restricted/fault-truth.ndjson")
    manifest = checks.read_manifest(PATHS.run_dir(run_id))
    with (PATHS.run_dir(run_id) / "public" / "district-sensors.ndjson").open(encoding="utf-8") as handle:
        run_start = json.loads(handle.readline())["interval_start_utc"].replace("Z", "")  # first simulated minute
    result = evaluate(alerts, truth, faults, run_start)
    summary = result["summary"]

    script = _script_summary(alerts, truth, faults, run_start)
    disagreements = {k: (summary[k], script[k]) for k in script if k != "script_output" and summary[k] != script[k]}
    if disagreements:
        raise Failure(description=f"evaluation differs from evaluate_alerts.py: {disagreements}",
                      metadata={"script_output": script["script_output"]})

    rid = _sql(run_id)
    trino.query("CREATE SCHEMA IF NOT EXISTS water.evaluation")
    trino.query("""CREATE TABLE IF NOT EXISTS water.evaluation.eval_scenario_result (
        run_id varchar, scenario_id varchar, district_id varchar, scenario_type varchar,
        start_time_utc timestamp(6) with time zone, end_time_utc timestamp(6) with time zone,
        detectable boolean, detected boolean, expected_alert_type varchar, alert_type varchar,
        type_ok boolean, delay_min integer) WITH (partitioning = ARRAY['run_id'])""")
    trino.query("""CREATE TABLE IF NOT EXISTS water.evaluation.eval_summary (
        run_id varchar, alerts integer, incident_opens integer, scenarios_total integer, scenarios_detected integer,
        scenarios_detectable integer, detectable_detected integer, false_alarms integer, precision double,
        dq_faults integer, dq_alerted integer, dq_missed integer, dq_extra_alerts integer)
        WITH (partitioning = ARRAY['run_id'])""")
    for table in ("eval_scenario_result", "eval_summary"):
        trino.query(f"DELETE FROM water.evaluation.{table} WHERE run_id = {rid}")
    rows = ", ".join(
        "(" + ", ".join([rid, _sql(r["scenario_id"]), _sql(r["district_id"]), _sql(r["scenario_type"]),
                         f"from_iso8601_timestamp({_sql(r['start_time_utc'])})",
                         f"from_iso8601_timestamp({_sql(r['end_time_utc'])})",
                         _sql(r["detectable"]), _sql(r["detected"]), _sql(r["expected_alert_type"]),
                         _sql(r["alert_type"]), _sql(r["type_ok"]), _sql(r["delay_min"])]) + ")"
        for r in result["scenarios"])
    trino.query(f"INSERT INTO water.evaluation.eval_scenario_result VALUES {rows}")
    columns = ["alerts", "incident_opens", "scenarios_total", "scenarios_detected", "scenarios_detectable",
               "detectable_detected", "false_alarms", "precision", "dq_faults", "dq_alerted", "dq_missed", "dq_extra_alerts"]
    trino.query("INSERT INTO water.evaluation.eval_summary VALUES (" + rid + ", "
                + ", ".join(_sql(summary[c]) for c in columns) + ")")

    context.log.info("\n" + script["script_output"])
    return MaterializeResult(metadata={
        **{k: v for k, v in summary.items() if v is not None},
        "scenario_count_in_manifest": manifest.get("scenario_count"),
        "matches_evaluate_alerts_py": True,
        "scenarios": MetadataValue.md(
            "| scenario | district | type | detected | alert | delay min |\n|---|---|---|---|---|---|\n" + "\n".join(
                f"| {r['scenario_id']} | {r['district_id']} | {r['scenario_type']} | "
                f"{'yes' if r['detected'] else ('not detectable' if not r['detectable'] else 'NO')} | "
                f"{r['alert_type'] or ''} | {r['delay_min'] if r['delay_min'] is not None else ''} |"
                for r in result["scenarios"])),
        **prompt(context, "http://localhost:3000/assets/alert_evaluation",
                 "Dagster asset page of alert_evaluation with the scenario table in its metadata"),
    })


# ---------------------------------------------------------------- dashboard marts (streaming + batch together)

@asset(
    group_name="marts",
    partitions_def=generator_runs,
    deps=[batch_validation, streaming_iceberg_facts],
    description=(
        "water.marts.*: incidents, incidents explained by the meter balance, a district-day overview and an hour-of-week "
        "profile. Each mart is one SELECT in infrastructure/trino/marts/, rebuilt for this run only (DELETE + INSERT)."
    ),
)
def dashboard_marts(context: AssetExecutionContext, trino: TrinoResource) -> MaterializeResult:
    run_id = checks.require_run_id(context.partition_key)
    trino.query("CREATE SCHEMA IF NOT EXISTS water.marts")
    rows = {}
    for path in sorted(MARTS_DIR.glob("*.sql")):  # numbered in dependency order
        name = path.stem.split("_", 1)[1]
        select = path.read_text(encoding="utf-8").replace("{run_id}", run_id).strip()
        table = f"water.marts.{name}"
        trino.query(f"CREATE TABLE IF NOT EXISTS {table} WITH (partitioning = ARRAY['run_id']) AS {select}\nWITH NO DATA")
        trino.query(f"DELETE FROM {table} WHERE run_id = '{run_id}'")
        trino.query(f"INSERT INTO {table} {select}")
        rows[name] = trino.query(f"SELECT count(*) FROM {table} WHERE run_id = '{run_id}'")[0][0]
        context.log.info(f"{table}: {rows[name]} rows for {run_id}")
    explained = trino.query(
        f"SELECT meter_explanation, count(*) FROM water.marts.mart_incident_balance WHERE run_id = '{run_id}' "
        "GROUP BY 1 ORDER BY 1")
    return MaterializeResult(metadata={
        **{f"{name}_rows": count for name, count in rows.items()},
        "incident_meter_explanations": MetadataValue.json(dict(explained)),
        **prompt(context, "http://localhost:8082/ui/",
                 "Trino: SELECT * FROM water.marts.mart_incident_balance ORDER BY incident_start"),
    })


# ---------------------------------------------------------------- Power BI exports

@asset(
    group_name="dashboard",
    op_tags=SPARK,
    partitions_def=generator_runs,
    deps=[dashboard_marts, alert_evaluation],
    description=(
        "Parquet copies of this run's marts, Gold, streaming and evaluation tables in dashboards/data/, imported by "
        "the Power BI analytics dashboard (refresh Power BI after this runs)."
    ),
)
def dashboard_exports(context: AssetExecutionContext, compose: ComposeResource) -> MaterializeResult:
    run_id = checks.require_run_id(context.partition_key)
    report = _spark(context, compose, "export_dashboard.py", ["--run-id", run_id])
    if report.get("status") != "completed":
        raise Failure(description=f"export_dashboard did not complete: {report}")
    return MaterializeResult(metadata={
        "folder": str(PATHS.repo / "dashboards" / "data"),
        **{f"{name}_rows": rows for name, rows in report["tables"].items()},
        **prompt(context, "Power BI Desktop", "open dashboards/WaterIntelligence.pbip and click Refresh"),
    })
