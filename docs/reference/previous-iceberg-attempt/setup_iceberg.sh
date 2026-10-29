#!/usr/bin/env bash
# Creates the NEW files under ./iceberg (run from the project root). Existing files are not touched.
set -euo pipefail
[ -f docker-compose.yml ] || { echo "run from the project root"; exit 1; }
mkdir -p iceberg/trino-catalog
cat > iceberg/load.py << 'EOF_ICEBERG_FILE'
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
EOF_ICEBERG_FILE
cat > iceberg/compose.yml << 'EOF_ICEBERG_FILE'
# Additive file: used ONLY together with the existing compose files (see iceberg/run_all.sh).
# It adds services and one volume. It does not change kafka, minio, kafka-ui or flink.
services:
  iceberg-rest:
    image: tabulario/iceberg-rest:1.6.0
    environment:
      AWS_ACCESS_KEY_ID: ${MINIO_ACCESS_KEY:-wateradmin}
      AWS_SECRET_ACCESS_KEY: ${MINIO_SECRET_KEY:-wateradminsecret}
      AWS_REGION: us-east-1
      CATALOG_WAREHOUSE: s3://water-warehouse/
      CATALOG_IO__IMPL: org.apache.iceberg.aws.s3.S3FileIO
      CATALOG_S3_ENDPOINT: http://minio:9000
      CATALOG_S3_PATH__STYLE__ACCESS: "true"
      CATALOG_URI: jdbc:sqlite:file:/data/iceberg_catalog.db
    volumes:
      - iceberg-catalog:/data
    ports:
      - "8181:8181"
    depends_on:
      minio:
        condition: service_healthy

  trino:
    image: trinodb/trino:465
    ports:
      - "8082:8080"
    volumes:
      - ./iceberg/trino-catalog:/etc/trino/catalog:ro
      - ./iceberg:/iceberg:ro
    depends_on:
      - iceberg-rest

  iceberg-loader:
    image: python:3.11-slim
    profiles: ["tools"]
    working_dir: /app
    environment:
      BOOTSTRAP: kafka:9092
      MINIO_ENDPOINT: minio:9000
      MINIO_ACCESS_KEY: ${MINIO_ACCESS_KEY:-wateradmin}
      MINIO_SECRET_KEY: ${MINIO_SECRET_KEY:-wateradminsecret}
      ICEBERG_URI: http://iceberg-rest:8181
    volumes:
      - ./iceberg:/app:ro
      - ./Generator/output:/truth:ro
    entrypoint: ["sh", "-c", "pip install -q 'pyiceberg[pyarrow]==0.12.0' kafka-python-ng minio && exec python load.py \"$@\"", "--"]
    command: ["all"]
    depends_on:
      kafka:
        condition: service_healthy
      iceberg-rest:
        condition: service_started

volumes:
  iceberg-catalog:
EOF_ICEBERG_FILE
cat > iceberg/trino-catalog/iceberg.properties << 'EOF_ICEBERG_FILE'
connector.name=iceberg
iceberg.catalog.type=rest
iceberg.rest-catalog.uri=http://iceberg-rest:8181
fs.native-s3.enabled=true
s3.endpoint=http://minio:9000
s3.region=us-east-1
s3.path-style-access=true
s3.aws-access-key=wateradmin
s3.aws-secret-key=wateradminsecret
EOF_ICEBERG_FILE
cat > iceberg/marts.sql << 'EOF_ICEBERG_FILE'
-- Marts for the dashboard. Rebuilt from the fact tables on every run.
DROP TABLE IF EXISTS iceberg.flink.mart_incidents;
CREATE TABLE iceberg.flink.mart_incidents AS
WITH o AS (
  SELECT alert_id AS incident_id, district_id, alert_type, rule_id, severity,
         first_flagged_window_start AS incident_start, window_start AS confirm_ws, window_end AS confirm_we
  FROM iceberg.flink.fact_alert WHERE status = 'open'
), c AS (
  SELECT district_id, alert_type, window_start AS last_ws, window_end AS last_we
  FROM iceberg.flink.fact_alert WHERE status = 'closed'
), paired AS (
  SELECT o.incident_id, o.district_id, o.alert_type, o.rule_id, o.severity,
         o.incident_start, o.confirm_ws, o.confirm_we, min(c.last_we) AS incident_end
  FROM o LEFT JOIN c
    ON c.district_id = o.district_id AND c.alert_type = o.alert_type AND c.last_ws >= o.confirm_ws
  GROUP BY o.incident_id, o.district_id, o.alert_type, o.rule_id, o.severity,
           o.incident_start, o.confirm_ws, o.confirm_we
), peaks AS (
  SELECT p.incident_id,
         max_by(f.rel_flow, abs(f.rel_flow - 1)) AS peak_rel_flow,
         min(f.rel_pressure) AS min_rel_pressure,
         count(*) AS n_windows
  FROM paired p
  JOIN iceberg.flink.fact_district_flow_15m f
    ON f.district_id = p.district_id
   AND f.window_start >= p.incident_start
   AND f.window_start < coalesce(p.incident_end, p.confirm_we)
  GROUP BY p.incident_id
)
SELECT p.incident_id, p.district_id, p.alert_type, p.rule_id, p.severity,
       p.incident_start, p.incident_end,
       date_diff('minute', p.incident_start, p.incident_end) AS duration_min,
       date_diff('minute', p.incident_start, p.confirm_we) AS detection_delay_min,
       k.peak_rel_flow, k.min_rel_pressure, k.n_windows
FROM paired p LEFT JOIN peaks k ON k.incident_id = p.incident_id
UNION ALL
SELECT alert_id, district_id, alert_type, rule_id, severity,
       first_flagged_window_start, window_end,
       date_diff('minute', first_flagged_window_start, window_end),
       date_diff('minute', first_flagged_window_start, window_end),
       rel_flow, CAST(NULL AS double), CAST(NULL AS bigint)
FROM iceberg.flink.fact_alert WHERE alert_type = 'flow_level_shift';

DROP TABLE IF EXISTS iceberg.flink.mart_alert_summary;
CREATE TABLE iceberg.flink.mart_alert_summary AS
SELECT district_id, alert_type, rule_id, severity, status,
       count(*) AS n_alerts, min(window_start) AS first_seen, max(window_start) AS last_seen
FROM iceberg.flink.fact_alert
GROUP BY district_id, alert_type, rule_id, severity, status;

DROP TABLE IF EXISTS iceberg.flink.mart_data_quality_daily;
CREATE TABLE iceberg.flink.mart_data_quality_daily AS
SELECT date(window_start) AS day, district_id,
       count(*) AS windows, sum(n_events) AS events,
       sum(n_null_pressure) AS null_pressure_readings,
       count_if(n_events <> 15) AS incomplete_windows,
       count_if(n_flagged > 0) AS flagged_windows
FROM iceberg.flink.fact_district_flow_15m
GROUP BY date(window_start), district_id;

DROP TABLE IF EXISTS iceberg.flink.mart_dead_letter_summary;
CREATE TABLE iceberg.flink.mart_dead_letter_summary AS
SELECT reject_reason, count(*) AS n_rejected
FROM iceberg.flink.fact_dead_letter
GROUP BY reject_reason;

DROP TABLE IF EXISTS iceberg.flink.mart_district_trend;
CREATE TABLE iceberg.flink.mart_district_trend AS
SELECT f.district_id, f.window_start, f.window_end, f.avg_flow_m3_s, f.inlet_volume_m3,
       f.min_pressure_m, f.rel_flow, f.rel_pressure, f.n_null_pressure,
       date(f.window_start) AS day, hour(f.window_start) AS hour_of_day,
       i.alert_type AS incident_type, (i.alert_type IS NOT NULL) AS in_incident
FROM iceberg.flink.fact_district_flow_15m f
LEFT JOIN iceberg.flink.mart_incidents i
  ON i.district_id = f.district_id
 AND i.alert_type IN ('flow_anomaly', 'supply_disturbance')
 AND f.window_start >= i.incident_start AND f.window_start < i.incident_end;

DROP TABLE IF EXISTS iceberg.flink.mart_kpi;
CREATE TABLE iceberg.flink.mart_kpi AS
SELECT s.scenarios_total, s.scenarios_detected, s.scenarios_detectable, s.detected_detectable,
       s.recall_all, s.recall_detectable, s.false_alarms, s.precision,
       s.dq_faults, s.dq_alerted, s.dq_missed,
       (SELECT count(*) FROM iceberg.flink.mart_incidents WHERE alert_type <> 'flow_level_shift') AS n_incidents,
       (SELECT avg(detection_delay_min) FROM iceberg.flink.mart_incidents WHERE alert_type <> 'flow_level_shift') AS avg_delay_min
FROM iceberg.flink.eval_summary s;
EOF_ICEBERG_FILE
cat > iceberg/queries.sql << 'EOF_ICEBERG_FILE'
-- Showcase queries (one screenshot each)
-- Q1: what is in the lakehouse
SELECT 'fact_alert' AS tbl, count(*) AS n FROM iceberg.flink.fact_alert
UNION ALL SELECT 'fact_district_flow_15m', count(*) FROM iceberg.flink.fact_district_flow_15m
UNION ALL SELECT 'fact_dead_letter', count(*) FROM iceberg.flink.fact_dead_letter
UNION ALL SELECT 'eval_scenario_result', count(*) FROM iceberg.flink.eval_scenario_result;

-- Q2: incidents found by the stream, with delay and peak
SELECT district_id, alert_type, incident_start, incident_end, duration_min, detection_delay_min, round(peak_rel_flow, 2) AS peak_rel_flow
FROM iceberg.flink.mart_incidents ORDER BY incident_start;

-- Q3: evaluation per planted scenario
SELECT scenario_id, district_id, scenario_type, detectable, detected, alert_type, type_ok, delay_min
FROM iceberg.flink.eval_scenario_result ORDER BY start_time;

-- Q4: recall / precision / data quality
SELECT scenarios_detected, scenarios_total, round(recall_all, 3) AS recall_all,
       round(recall_detectable, 3) AS recall_detectable, false_alarms, round(precision, 2) AS precision,
       dq_alerted, dq_faults, n_incidents, avg_delay_min
FROM iceberg.flink.mart_kpi;

-- Q5: alerts per district and type
SELECT district_id, alert_type, severity, status, n_alerts FROM iceberg.flink.mart_alert_summary ORDER BY district_id, alert_type, status;

-- Q6: data quality per day
SELECT day, sum(null_pressure_readings) AS null_pressure, sum(incomplete_windows) AS incomplete_windows, sum(windows) AS windows
FROM iceberg.flink.mart_data_quality_daily GROUP BY day ORDER BY day LIMIT 10;

-- Q7: Iceberg time travel / history (shows the table format at work)
SELECT snapshot_id, committed_at, operation FROM iceberg.flink."fact_alert$snapshots" ORDER BY committed_at;
EOF_ICEBERG_FILE
cat > iceberg/run_all.sh << 'EOF_ICEBERG_FILE'
#!/usr/bin/env bash
# Load Flink outputs into Iceberg, build the marts, export CSVs for Power BI.
# Touches nothing of the existing pipeline: it only starts NEW services (iceberg-rest, trino).
set -euo pipefail
cd "$(dirname "$0")/.."
OVR=""; [ -f docker-compose.override.yml ] && OVR="-f docker-compose.override.yml"
F="-f docker-compose.yml $OVR -f iceberg/compose.yml"
dc() { docker compose $F "$@"; }

echo "== 1/5 services (kafka, minio stay as they are)"
dc up -d kafka minio iceberg-rest trino
echo "== 2/5 waiting for Trino"
for i in $(seq 1 40); do dc exec -T trino trino --execute "SELECT 1" >/dev/null 2>&1 && break; sleep 5; done

echo "== 3/5 loading Kafka topics into Iceberg"
dc run --rm iceberg-loader all

echo "== 4/5 building marts in Trino"
dc exec -T trino trino --file /iceberg/marts.sql >/dev/null
dc exec -T trino trino --execute "SHOW TABLES FROM iceberg.flink"

echo "== 5/5 exporting CSVs for Power BI -> iceberg/export/"
mkdir -p iceberg/export
for t in mart_incidents mart_district_trend mart_alert_summary mart_data_quality_daily \
         mart_dead_letter_summary mart_kpi eval_scenario_result dim_district; do
  dc exec -T trino trino --output-format CSV_HEADER --execute "SELECT * FROM iceberg.flink.$t" > iceberg/export/$t.csv
  echo "   $t.csv  ($(($(wc -l < iceberg/export/$t.csv) - 1)) rows)"
done
echo "done."
EOF_ICEBERG_FILE
chmod +x iceberg/run_all.sh
echo "created: iceberg/{load.py,compose.yml,marts.sql,queries.sql,run_all.sh,trino-catalog/iceberg.properties}"
