"""Load the Flink outputs from Kafka into Iceberg (namespace `flink`).

Safe by design:
  * reads Kafka with NO consumer group (nothing is committed, no other job is affected)
  * never writes to Kafka
  * idempotent: every run rebuilds the tables from the topics (Iceberg keeps the snapshot history)
  * the restricted truth is read ONLY by the `eval` step, after detection, never by Flink

Usage: python load.py facts | eval | all
"""
import json
import os
import sys
import time
from datetime import datetime, timedelta

import pyarrow as pa

BOOT = os.environ.get("BOOTSTRAP", "kafka:9092")
T_ALERTS = os.environ.get("ALERTS_TOPIC", "water.district-alerts.v1")
T_FEAT = os.environ.get("FEATURES_TOPIC", "water.district-features.15m.v1")
T_REL = os.environ.get("RELATIVE_TOPIC", "water.district-relative.15m.v1")
T_DEAD = os.environ.get("DEAD_TOPIC", "water.dead-letter.district.v1")
RUN_ID = os.environ.get("RUN_ID", "run-bdc85c43085c4dc8")
TRUTH_DIR = os.environ.get("TRUTH_DIR", f"/truth/{RUN_ID}/restricted")
RUN_START = datetime(2026, 1, 1)
NS = "flink"

TS = pa.timestamp("us")


def ts(s):
    if s is None:
        return None
    return datetime.fromisoformat(str(s).replace("Z", "").replace("+00:00", ""))


# ---------------------------------------------------------------- schemas
S_ALERT = pa.schema([
    ("alert_id", pa.string()), ("alert_type", pa.string()), ("rule_id", pa.string()),
    ("severity", pa.string()), ("status", pa.string()), ("district_id", pa.string()),
    ("window_start", TS), ("window_end", TS), ("first_flagged_window_start", TS),
    ("rel_flow", pa.float64()), ("rel_pressure", pa.float64()),
    ("avg_flow_m3_s", pa.float64()), ("min_pressure_m", pa.float64()),
    ("n_events", pa.int64()), ("n_null_pressure", pa.int64()), ("n_flagged", pa.int64()),
    ("reason", pa.string()), ("source", pa.string()),
    ("baseline_rel_flow", pa.float64()), ("level_shift_pct", pa.float64()),
    ("alert_date", pa.date32()),
])
S_FLOW = pa.schema([
    ("district_id", pa.string()), ("window_start", TS), ("window_end", TS),
    ("avg_flow_m3_s", pa.float64()), ("inlet_volume_m3", pa.float64()),
    ("min_pressure_m", pa.float64()), ("avg_pressure_m", pa.float64()),
    ("n_events", pa.int64()), ("n_null_pressure", pa.int64()), ("n_flagged", pa.int64()),
    ("rel_flow", pa.float64()), ("rel_pressure", pa.float64()), ("cond", pa.string()),
])
S_DEAD = pa.schema([
    ("job", pa.string()), ("source_topic", pa.string()), ("source_partition", pa.int64()),
    ("source_offset", pa.int64()), ("reject_reason", pa.string()), ("raw_payload", pa.string()),
])
S_DIM = pa.schema([
    ("district_id", pa.string()), ("district_name", pa.string()),
    ("inlet_link_id", pa.string()), ("pressure_monitor_node_id", pa.string()),
])
S_EVAL = pa.schema([
    ("scenario_id", pa.string()), ("district_id", pa.string()), ("scenario_type", pa.string()),
    ("scenario_class", pa.string()), ("start_time", TS), ("end_time", TS),
    ("detectable", pa.bool_()), ("detected", pa.bool_()), ("expected_type", pa.string()),
    ("alert_type", pa.string()), ("type_ok", pa.bool_()), ("delay_min", pa.int64()),
])
S_SUM = pa.schema([
    ("scenarios_total", pa.int64()), ("scenarios_detected", pa.int64()),
    ("scenarios_detectable", pa.int64()), ("detected_detectable", pa.int64()),
    ("recall_all", pa.float64()), ("recall_detectable", pa.float64()),
    ("incident_open_alerts", pa.int64()), ("false_alarms", pa.int64()), ("precision", pa.float64()),
    ("dq_faults", pa.int64()), ("dq_alerted", pa.int64()), ("dq_missed", pa.int64()),
    ("evaluated_at", TS),
])


# ---------------------------------------------------------------- kafka / catalog
def read_topic(topic):
    from kafka import KafkaConsumer
    c = KafkaConsumer(topic, bootstrap_servers=BOOT, group_id=None,
                      auto_offset_reset="earliest", enable_auto_commit=False,
                      consumer_timeout_ms=15000)
    rows = []
    for m in c:
        line = m.value.decode("utf-8", "replace").strip()
        if line.startswith("{"):
            rows.append(json.loads(line))
    c.close()
    print(f"  read {len(rows):>7} rows from {topic}")
    return rows


def get_catalog():
    from minio import Minio
    from pyiceberg.catalog import load_catalog
    ak = os.environ.get("MINIO_ACCESS_KEY", "wateradmin")
    sk = os.environ.get("MINIO_SECRET_KEY", "wateradminsecret")
    ep = os.environ.get("MINIO_ENDPOINT", "minio:9000")
    bucket = os.environ.get("WAREHOUSE_BUCKET", "water-warehouse")
    m = Minio(ep, access_key=ak, secret_key=sk, secure=False)
    if not m.bucket_exists(bucket):
        m.make_bucket(bucket)
    props = {
        "type": "rest", "uri": os.environ.get("ICEBERG_URI", "http://iceberg-rest:8181"),
        "s3.endpoint": f"http://{ep}", "s3.access-key-id": ak, "s3.secret-access-key": sk,
        "s3.path-style-access": "true", "s3.region": "us-east-1",
    }
    for i in range(20):
        try:
            cat = load_catalog("rest", **props)
            cat.list_namespaces()
            return cat
        except Exception as e:  # catalog still starting
            print("  waiting for catalog:", type(e).__name__)
            time.sleep(3)
    raise SystemExit("catalog not reachable")


def write_table(cat, name, schema, rows):
    try:
        cat.create_namespace(NS)
    except Exception:
        pass
    tbl = cat.create_table_if_not_exists(f"{NS}.{name}", schema=schema)
    if rows:
        tbl.overwrite(pa.Table.from_pylist(rows, schema=schema))
    print(f"  wrote {len(rows):>7} rows to iceberg {NS}.{name}")


# ---------------------------------------------------------------- builders
def build_alerts(raw):
    by_id = {r["alert_id"]: r for r in raw}  # replay-safe: same alert_id = same alert
    out = []
    for r in by_id.values():
        row = {k: r.get(k) for k in S_ALERT.names if k != "alert_date"}
        for k in ("window_start", "window_end", "first_flagged_window_start"):
            row[k] = ts(row[k])
        row["alert_date"] = row["window_start"].date() if row["window_start"] else None
        out.append(row)
    return out


def build_flow(feat, rel):
    relmap = {(r["district_id"], r["window_start"][:19]): r for r in rel}
    out = {}
    for f in feat:
        key = (f["district_id"], f["window_start"][:19])
        r = relmap.get(key, {})
        out[key] = {
            "district_id": f["district_id"], "window_start": ts(f["window_start"]),
            "window_end": ts(f["window_end"]), "avg_flow_m3_s": f.get("avg_flow_m3_s"),
            "inlet_volume_m3": f.get("inlet_volume_m3"), "min_pressure_m": f.get("min_pressure_m"),
            "avg_pressure_m": f.get("avg_pressure_m"), "n_events": f.get("n_events"),
            "n_null_pressure": f.get("n_null_pressure"), "n_flagged": f.get("n_flagged"),
            "rel_flow": r.get("rel_flow"), "rel_pressure": r.get("rel_pressure"),
            "cond": r.get("cond"),
        }
    return sorted(out.values(), key=lambda x: (x["window_start"], x["district_id"]))


def build_dead(raw):
    return [{k: r.get(k) for k in S_DEAD.names} for r in raw]


def build_dim():
    return [{"district_id": f"D{i:03d}", "district_name": f"District {i}",
             "inlet_link_id": f"D{i:03d}_INLET", "pressure_monitor_node_id": f"D{i:03d}_SERVICE"}
            for i in range(1, 7)]


EXPECTED_TYPE = {
    "burst": "flow_anomaly", "gradual_leak": "flow_anomaly", "demand_surge": "flow_anomaly",
    "supply_disturbance": "supply_disturbance", "persistent_background_loss": "flow_level_shift",
}


def jsonl(path):
    with open(path, encoding="utf-8") as f:
        return [json.loads(l) for l in f if l.strip().startswith("{")]


def build_eval(alerts, truth, faults):
    """Same matching rule as evaluate_alerts.py (author's own definition)."""
    tol = timedelta(minutes=15)
    opens = [a for a in alerts if a["alert_type"] != "data_quality" and a["status"] in ("open", "instant")]
    matched, rows = set(), []
    for t in truth:
        s, e = ts(t["start_time_utc"]), ts(t["end_time_utc"])
        want = EXPECTED_TYPE.get(t["scenario_type"])
        if t["scenario_type"] == "persistent_background_loss" and s <= RUN_START:
            want = None  # constant from minute 0: nothing to detect from district data
        hits = [o for o in opens if o["district_id"] == t["district_id"]
                and o["first_flagged_window_start"] and s - tol <= o["first_flagged_window_start"] < e]
        for o in hits:
            matched.add(o["alert_id"])
        hit = min(hits, key=lambda o: o["window_end"]) if hits else None
        rows.append({
            "scenario_id": t["scenario_id"], "district_id": t["district_id"],
            "scenario_type": t["scenario_type"], "scenario_class": t["scenario_class"],
            "start_time": s, "end_time": e, "detectable": want is not None,
            "detected": hit is not None, "expected_type": want,
            "alert_type": hit["alert_type"] if hit else None,
            "type_ok": (hit["alert_type"] == want) if (hit and want) else None,
            "delay_min": round((hit["window_end"] - s).total_seconds() / 60) if hit else None,
        })
    false_alarms = [o for o in opens if o["alert_id"] not in matched]
    detected = sum(r["detected"] for r in rows)
    detectable = sum(r["detectable"] for r in rows)
    det_det = sum(1 for r in rows if r["detectable"] and r["detected"])
    dq = {(a["district_id"], a["window_start"].strftime("%Y-%m-%dT%H:%M"))
          for a in alerts if a["alert_type"] == "data_quality"}
    exp = set()
    for f in faults:
        if f["stream"] == "district" and f["fault_type"] == "null_value":
            did, minute = f["event_id"].split(":")[-2:]
            w = RUN_START + timedelta(minutes=int(minute) // 15 * 15)
            exp.add((did, w.strftime("%Y-%m-%dT%H:%M")))
    summary = [{
        "scenarios_total": len(rows), "scenarios_detected": detected,
        "scenarios_detectable": detectable, "detected_detectable": det_det,
        "recall_all": detected / len(rows) if rows else None,
        "recall_detectable": det_det / detectable if detectable else None,
        "incident_open_alerts": len(opens), "false_alarms": len(false_alarms),
        "precision": (len(opens) - len(false_alarms)) / len(opens) if opens else None,
        "dq_faults": len(exp), "dq_alerted": len(exp & dq), "dq_missed": len(exp - dq),
        "evaluated_at": datetime.utcnow(),
    }]
    return rows, summary


# ---------------------------------------------------------------- steps
def step_facts(cat):
    print("facts:")
    alerts = build_alerts(read_topic(T_ALERTS))
    write_table(cat, "fact_alert", S_ALERT, alerts)
    write_table(cat, "fact_district_flow_15m", S_FLOW, build_flow(read_topic(T_FEAT), read_topic(T_REL)))
    write_table(cat, "fact_dead_letter", S_DEAD, build_dead(read_topic(T_DEAD)))
    write_table(cat, "dim_district", S_DIM, build_dim())
    return alerts


def step_eval(cat, alerts=None):
    print("eval (reads restricted truth OFFLINE, after detection):")
    if alerts is None:
        alerts = build_alerts(read_topic(T_ALERTS))
    truth = jsonl(f"{TRUTH_DIR}/scenario-truth.ndjson")
    faults = jsonl(f"{TRUTH_DIR}/fault-truth.ndjson")
    rows, summary = build_eval(alerts, truth, faults)
    write_table(cat, "eval_scenario_result", S_EVAL, rows)
    write_table(cat, "eval_summary", S_SUM, summary)
    s = summary[0]
    print(f"  recall_all={s['recall_all']:.3f} recall_detectable={s['recall_detectable']:.3f} "
          f"false_alarms={s['false_alarms']} dq={s['dq_alerted']}/{s['dq_faults']}")


if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else "all"
    catalog = get_catalog()
    a = None
    if mode in ("facts", "all"):
        a = step_facts(catalog)
    if mode in ("eval", "all"):
        step_eval(catalog, a)
