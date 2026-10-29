"""Live bridge: Flink's Kafka outputs -> Postgres schema `live` -> Grafana (auto-refreshing operations board).

Kafka is the system of record; this is a serving copy for a low-latency dashboard. Rows are upserted by their
natural key (alert_id; district + window), so re-reading a topic can never duplicate a row. Topics are matched
by pattern, so the main namespace (water.*) and the resettable live-demo namespace (demo.water.*) are both
served, and recreated demo topics are picked up automatically. Every few seconds the bridge also samples
topic end offsets, which the board turns into events per second.
"""
import json
import os
import time
from datetime import datetime, timezone

import psycopg
from kafka import KafkaConsumer, TopicPartition

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
DSN = (f"host={os.environ.get('LIVE_DB_HOST', 'catalog-db')} dbname=iceberg user=iceberg "
       f"password={os.environ['CATALOG_DB_PASSWORD']}")
READER_PASSWORD = os.environ.get("GRAFANA_DB_PASSWORD", "grafana-live")
PATTERN = r"^(demo\.)?water\.district-(alerts|relative\.15m|features\.15m)\.v1$"
SAMPLED = ["water.district-sensors.v1", "water.meter-readings.v1", "water.district-sensors.clean.v1",
           "water.district-alerts.v1"]
SAMPLE_SECONDS = 2

SCHEMA = f"""
CREATE SCHEMA IF NOT EXISTS live;
CREATE TABLE IF NOT EXISTS live.relative (
    ns text, district_id text, window_start timestamptz, window_end timestamptz,
    avg_flow_m3_s double precision, min_pressure_m double precision,
    rel_flow double precision, rel_pressure double precision, cond text, ingested_at timestamptz,
    PRIMARY KEY (ns, district_id, window_start));
CREATE TABLE IF NOT EXISTS live.features (
    ns text, district_id text, window_start timestamptz, inlet_volume_m3 double precision,
    n_events bigint, n_null_pressure bigint, n_flagged bigint, ingested_at timestamptz,
    PRIMARY KEY (ns, district_id, window_start));
CREATE TABLE IF NOT EXISTS live.alerts (
    ns text, alert_id text, alert_type text, rule_id text, severity text, status text, district_id text,
    window_start timestamptz, window_end timestamptz, first_flagged_window_start timestamptz,
    rel_flow double precision, rel_pressure double precision, reason text, ingested_at timestamptz,
    PRIMARY KEY (ns, alert_id));
CREATE TABLE IF NOT EXISTS live.throughput (
    ns text, topic text, measured_at timestamptz, end_offset bigint,
    PRIMARY KEY (ns, topic, measured_at));
DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'grafana_live') THEN
        CREATE ROLE grafana_live LOGIN PASSWORD '{READER_PASSWORD}';
    END IF;
END $$;
GRANT USAGE ON SCHEMA live TO grafana_live;
GRANT SELECT ON ALL TABLES IN SCHEMA live TO grafana_live;
ALTER DEFAULT PRIVILEGES IN SCHEMA live GRANT SELECT ON TABLES TO grafana_live;
"""

UPSERT = {
    "relative": """INSERT INTO live.relative VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (ns, district_id, window_start) DO UPDATE SET rel_flow = EXCLUDED.rel_flow,
        rel_pressure = EXCLUDED.rel_pressure, cond = EXCLUDED.cond, ingested_at = EXCLUDED.ingested_at""",
    "features": """INSERT INTO live.features VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (ns, district_id, window_start) DO UPDATE SET n_events = EXCLUDED.n_events,
        n_null_pressure = EXCLUDED.n_null_pressure, n_flagged = EXCLUDED.n_flagged""",
    "alerts": """INSERT INTO live.alerts VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (ns, alert_id) DO NOTHING""",
}


def utc(value):
    """Flink writes TIMESTAMP(3) as ISO-8601 without a zone; every job runs with local-time-zone UTC."""
    return None if value is None else datetime.fromisoformat(value).replace(tzinfo=timezone.utc)


def row(kind, ns, r, now):
    if kind == "relative":
        return (ns, r["district_id"], utc(r["window_start"]), utc(r["window_end"]), r["avg_flow_m3_s"],
                r["min_pressure_m"], r["rel_flow"], r["rel_pressure"], r["cond"], now)
    if kind == "features":
        return (ns, r["district_id"], utc(r["window_start"]), r["inlet_volume_m3"], r["n_events"],
                r["n_null_pressure"], r["n_flagged"], now)
    return (ns, r["alert_id"], r["alert_type"], r["rule_id"], r["severity"], r["status"], r["district_id"],
            utc(r["window_start"]), utc(r["window_end"]), utc(r["first_flagged_window_start"]),
            r["rel_flow"], r["rel_pressure"], r["reason"], now)


def kind_of(topic):
    return "alerts" if "alerts" in topic else "relative" if "relative" in topic else "features"


def connect():
    for attempt in range(30):
        try:
            return psycopg.connect(DSN, autocommit=True)
        except psycopg.OperationalError:
            time.sleep(2)
    raise SystemExit("Postgres not reachable")


def main():
    db = connect()
    db.execute(SCHEMA)
    consumer = KafkaConsumer(bootstrap_servers=BOOTSTRAP, group_id="ewi-live-bridge", auto_offset_reset="earliest",
                             enable_auto_commit=True, metadata_max_age_ms=5000)
    consumer.subscribe(pattern=PATTERN)
    sampler = KafkaConsumer(bootstrap_servers=BOOTSTRAP, group_id=None, enable_auto_commit=False)
    next_sample = 0.0
    print("live bridge running", flush=True)
    while True:
        batch = consumer.poll(timeout_ms=1000, max_records=2000)
        now = datetime.now(timezone.utc)
        rows: dict[str, list] = {"relative": [], "features": [], "alerts": []}
        for tp, messages in batch.items():
            ns = "demo" if tp.topic.startswith("demo.") else "main"
            kind = kind_of(tp.topic)
            for message in messages:
                rows[kind].append(row(kind, ns, json.loads(message.value), now))
        with db.cursor() as cursor:
            for kind, values in rows.items():
                if values:
                    cursor.executemany(UPSERT[kind], values)
        if time.monotonic() >= next_sample:
            next_sample = time.monotonic() + SAMPLE_SECONDS
            topics = sampler.topics()
            samples = []
            for ns, prefix in (("main", ""), ("demo", "demo.")):
                for base in SAMPLED:
                    topic = prefix + base
                    if topic in topics:
                        ends = sampler.end_offsets([TopicPartition(topic, p) for p in sampler.partitions_for_topic(topic) or []])
                        samples.append((ns, base, now, sum(ends.values())))
            if samples:
                with db.cursor() as cursor:
                    cursor.executemany("INSERT INTO live.throughput VALUES (%s,%s,%s,%s) ON CONFLICT DO NOTHING", samples)


if __name__ == "__main__":
    main()
