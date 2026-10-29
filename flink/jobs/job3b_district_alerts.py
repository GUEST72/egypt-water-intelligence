"""Job 3b: district alerts from the 15-minute feature windows.

Reads  : water.district-features.15m.v1   (written by job3_district_features.py)
Writes : water.district-alerts.v1         (one documented alert schema)
         water.district-relative.15m.v1   (rel_flow / rel_pressure per district and window,
                                           read by job3c_level_shift.py)

Rules (district stream only, no meter data, no restricted truth files):

  R1 flow_anomaly       |rel_flow - 1| > FLOW_THRESHOLD for 2 consecutive windows
  R2 supply_disturbance rel_flow < SUPPLY_FLOW and rel_pressure < SUPPLY_PRESSURE
                        for 2 consecutive windows. R2 wins over R1 for the same
                        window, so a supply problem is never reported as a leak.
  R3 data_quality       n_events != 15, or n_null_pressure > 0, or n_flagged > 0
                        (one alert per window, no confirmation needed)

rel_flow     = district avg flow / median of the OTHER five districts, same window
rel_pressure = district min pressure / median of the OTHER five districts' min pressure
A NULL pressure gives a NULL rel_pressure, which can never satisfy R2.

Alert lifecycle for R1 and R2 (status):
  open     emitted when the 2nd consecutive flagged window arrives
  closed   emitted when the first unflagged window arrives after a confirmed run
  (no per-window "ongoing" rows: an incident is exactly one open and one closed row)
R3 alerts use status = 'instant'.

Known limits (also write them in the docs):
  * Districts are hard-coded D001..D006 (the pivot needs a fixed list).
  * Persistent small losses (D005, D006) do not show in a peer comparison.
  * A burst and a demand surge look the same from district data alone.
  * The first two windows of the stream and the last open window cannot alert.

The relative topic is written in time order (all 6 districts of a window, then the next window);
job3c depends on that order (its watermark is the window start).

Settings: environment variables or the matching --options.
"""

import argparse
import os

from pyflink.table import DataTypes, EnvironmentSettings, TableEnvironment
from pyflink.table.udf import udtf

DISTRICTS = [f"D{i:03d}" for i in range(1, 7)]


def _median(vals):
    # median of the non-null inputs (same logic as before)
    vals = sorted(v for v in vals if v is not None)
    if not vals:
        return None
    mid = len(vals) // 2
    return vals[mid] if len(vals) % 2 else (vals[mid - 1] + vals[mid]) / 2


@udtf(result_types=[
    DataTypes.STRING(),   # district_id
    DataTypes.DOUBLE(),   # avg_flow_m3_s
    DataTypes.DOUBLE(),   # min_pressure_m
    DataTypes.BIGINT(),   # n_events
    DataTypes.BIGINT(),   # n_null_pressure
    DataTypes.BIGINT(),   # n_flagged
    DataTypes.DOUBLE(),   # rel_flow
    DataTypes.DOUBLE(),   # rel_pressure
])
def unpivot_districts(f, p, e, n, q):
    """One wide row (6 districts side by side) -> 6 rows, emitted back to back by ONE operator.
    This replaces the old UNION ALL of 6 branches, which wrote the relative topic in blocks
    per district (not ordered by time) and made job3c drop most rows as late.
    f/p/e/n/q are arrays of 6 values: flow, min pressure, n_events, n_null, n_flagged.
    rel_* = value / median of the OTHER five districts (NULL values ignored)."""
    for i in range(len(f)):
        if f[i] is None:
            continue
        med_f = _median([f[j] for j in range(len(f)) if j != i])
        med_p = _median([p[j] for j in range(len(p)) if j != i])
        rel_f = f[i] / med_f if med_f else None
        rel_p = p[i] / med_p if (p[i] is not None and med_p) else None
        yield (DISTRICTS[i], f[i], p[i], e[i], n[i], q[i], rel_f, rel_p)


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--bootstrap", default=os.environ.get("BOOTSTRAP", "kafka:9092"))
    p.add_argument("--features-topic", default=os.environ.get("FEATURES_TOPIC", "water.district-features.15m.v1"))
    p.add_argument("--alerts-topic", default=os.environ.get("ALERTS_TOPIC", "water.district-alerts.v1"))
    p.add_argument("--relative-topic", default=os.environ.get("RELATIVE_TOPIC", "water.district-relative.15m.v1"))
    p.add_argument("--group-id", default=os.environ.get("GROUP_ID", "flink-water-alerts-dev"))
    p.add_argument("--startup-mode", default=os.environ.get("STARTUP_MODE", "earliest-offset"))
    p.add_argument("--flow-threshold", type=float, default=float(os.environ.get("FLOW_THRESHOLD", "0.10")))
    p.add_argument("--supply-flow", type=float, default=float(os.environ.get("SUPPLY_FLOW", "0.90")))
    p.add_argument("--supply-pressure", type=float, default=float(os.environ.get("SUPPLY_PRESSURE", "0.80")))
    p.add_argument("--expected-events", type=int, default=int(os.environ.get("EXPECTED_EVENTS", "15")))
    # Join state only needs the previous two windows (30 min of event time).
    p.add_argument("--state-ttl", default=os.environ.get("STATE_TTL", "2 h"))
    args, _ = p.parse_known_args()
    return args


def pivot_columns():
    """One MAX(CASE ...) per district and per measure: f=flow, p=min pressure,
    e=n_events, n=n_null_pressure, q=n_flagged."""
    cols = []
    for i, d in enumerate(DISTRICTS, 1):
        for alias, src in (
            ("f", "avg_flow_m3_s"),
            ("p", "min_pressure_m"),
            ("e", "n_events"),
            ("n", "n_null_pressure"),
            ("q", "n_flagged"),
        ):
            cols.append(f"MAX(CASE WHEN district_id = '{d}' THEN {src} END) AS {alias}{i}")
    return ",\n    ".join(cols)


def unpivot_sql():
    """Back to one row per district and window, with the two relative values.
    One lateral table function call per wide row keeps the 6 rows together and in time order."""
    def arr(prefix):
        return "ARRAY[" + ", ".join(f"pv.{prefix}{i}" for i in range(1, 7)) + "]"
    return f"""
    SELECT pv.window_start, pv.window_end, t.district_id, t.avg_flow_m3_s, t.min_pressure_m,
           t.n_events, t.n_null_pressure, t.n_flagged, t.rel_flow, t.rel_pressure
    FROM pivoted pv,
         LATERAL TABLE(unpivot_districts({arr("f")}, {arr("p")}, {arr("e")}, {arr("n")}, {arr("q")}))
         AS t(district_id, avg_flow_m3_s, min_pressure_m, n_events, n_null_pressure, n_flagged,
              rel_flow, rel_pressure)"""


# Common SELECT tail of the two incident alerts: r = current, p = previous, q = two back.
JOIN3 = """
FROM rel r
JOIN rel p ON r.district_id = p.district_id
          AND p.window_start = r.window_start - INTERVAL '15' MINUTE
JOIN rel q ON r.district_id = q.district_id
          AND q.window_start = r.window_start - INTERVAL '30' MINUTE
"""


def incident_select(e, status):
    """e = alias of the evidence row. 'open' describes the confirming window (r),
    'closed' describes the last flagged window (p)."""
    first = "p.window_start" if status == "open" else "CAST(NULL AS TIMESTAMP(3))"
    return f"""
SELECT
    '1.0.0',
    CONCAT_WS('|', {e}.district_id, {e}.cond, '{status}', DATE_FORMAT({e}.window_start, 'yyyyMMddHHmm')),
    {e}.cond,
    CASE {e}.cond WHEN 'supply_disturbance' THEN 'R2' ELSE 'R1' END,
    CASE {e}.cond WHEN 'supply_disturbance' THEN 'medium' ELSE 'high' END,
    '{status}',
    {e}.district_id,
    {e}.window_start,
    {e}.window_end,
    {first},
    {e}.rel_flow, {e}.rel_pressure, {e}.avg_flow_m3_s, {e}.min_pressure_m,
    {e}.n_events, {e}.n_null_pressure, {e}.n_flagged,
    CASE WHEN {e}.cond = 'supply_disturbance' THEN 'low_flow_and_low_pressure'
         WHEN {e}.rel_flow > 1 THEN 'flow_above_peers'
         ELSE 'flow_below_peers' END,
    'flink_stream',
    CAST(NULL AS DOUBLE), CAST(NULL AS DOUBLE)
"""


def main():
    a = parse_args()
    t_env = TableEnvironment.create(EnvironmentSettings.in_streaming_mode())
    cfg = t_env.get_config()
    cfg.set("table.local-time-zone", "UTC")
    cfg.set("table.exec.source.idle-timeout", "10 s")
    cfg.set("table.exec.state.ttl", a.state_ttl)
    t_env.create_temporary_function("unpivot_districts", unpivot_districts)

    # Features source. The watermark is the window start itself: all six districts
    # of window W are emitted together by job 3a, before any window W+15.
    t_env.execute_sql(f"""
    CREATE TABLE district_features (
        window_start TIMESTAMP(3),
        window_end TIMESTAMP(3),
        district_id STRING,
        avg_flow_m3_s DOUBLE,
        inlet_volume_m3 DOUBLE,
        min_pressure_m DOUBLE,
        avg_pressure_m DOUBLE,
        n_events BIGINT,
        n_null_pressure BIGINT,
        n_flagged BIGINT,
        WATERMARK FOR window_start AS window_start
    ) WITH (
        'connector' = 'kafka',
        'topic' = '{a.features_topic}',
        'properties.bootstrap.servers' = '{a.bootstrap}',
        'properties.group.id' = '{a.group_id}',
        'scan.startup.mode' = '{a.startup_mode}',
        'format' = 'json',
        'json.timestamp-format.standard' = 'ISO-8601'
    )
    """)

    t_env.execute_sql(f"""
    CREATE TABLE district_alerts (
        schema_version STRING,
        alert_id STRING,
        alert_type STRING,
        rule_id STRING,
        severity STRING,
        status STRING,
        district_id STRING,
        window_start TIMESTAMP(3),
        window_end TIMESTAMP(3),
        first_flagged_window_start TIMESTAMP(3),
        rel_flow DOUBLE,
        rel_pressure DOUBLE,
        avg_flow_m3_s DOUBLE,
        min_pressure_m DOUBLE,
        n_events BIGINT,
        n_null_pressure BIGINT,
        n_flagged BIGINT,
        reason STRING,
        source STRING,
        baseline_rel_flow DOUBLE,
        level_shift_pct DOUBLE
    ) WITH (
        'connector' = 'kafka',
        'topic' = '{a.alerts_topic}',
        'properties.bootstrap.servers' = '{a.bootstrap}',
        'format' = 'json',
        'json.timestamp-format.standard' = 'ISO-8601',
        'sink.delivery-guarantee' = 'at-least-once'
    )
    """)

    # Per district and window relative values: input of job3c (level shift) and a useful
    # artefact for dashboards. cond is the single-window rule result (before confirmation).
    t_env.execute_sql(f"""
    CREATE TABLE district_relative (
        window_start TIMESTAMP(3),
        window_end TIMESTAMP(3),
        district_id STRING,
        avg_flow_m3_s DOUBLE,
        min_pressure_m DOUBLE,
        rel_flow DOUBLE,
        rel_pressure DOUBLE,
        cond STRING
    ) WITH (
        'connector' = 'kafka',
        'topic' = '{a.relative_topic}',
        'properties.bootstrap.servers' = '{a.bootstrap}',
        'format' = 'json',
        'json.timestamp-format.standard' = 'ISO-8601',
        'sink.delivery-guarantee' = 'at-least-once'
    )
    """)

    # The TVF adds its own window_start/window_end, so the source columns are renamed first.
    t_env.execute_sql("""
    CREATE TEMPORARY VIEW features_v AS
    SELECT window_start AS fw_start, district_id, avg_flow_m3_s, min_pressure_m,
           n_events, n_null_pressure, n_flagged
    FROM district_features
    """)

    # One wide row per 15-minute window, with all six districts side by side.
    t_env.execute_sql(f"""
    CREATE TEMPORARY VIEW pivoted AS
    SELECT window_start, window_end,
    {pivot_columns()}
    FROM TABLE(TUMBLE(TABLE features_v, DESCRIPTOR(fw_start), INTERVAL '15' MINUTES))
    GROUP BY window_start, window_end
    """)

    t_env.execute_sql(f"CREATE TEMPORARY VIEW unpivoted AS {unpivot_sql()}")

    # Per district and window: which rule (if any) is true. R2 is checked first.
    t_env.execute_sql(f"""
    CREATE TEMPORARY VIEW rel AS
    SELECT *,
        CASE
            WHEN rel_flow < {a.supply_flow} AND rel_pressure < {a.supply_pressure}
                THEN 'supply_disturbance'
            WHEN ABS(rel_flow - 1) > {a.flow_threshold}
                THEN 'flow_anomaly'
            ELSE CAST(NULL AS STRING)
        END AS cond
    FROM unpivoted
    """)

    stmt = t_env.create_statement_set()

    stmt.add_insert_sql("""
    INSERT INTO district_relative
    SELECT window_start, window_end, district_id, avg_flow_m3_s, min_pressure_m,
           rel_flow, rel_pressure, cond
    FROM rel
    """)

    # open: 2nd consecutive window with the same rule; the window before was not.
    stmt.add_insert_sql(
        "INSERT INTO district_alerts"
        + incident_select("r", "open")
        + JOIN3
        + "WHERE r.cond IS NOT NULL AND p.cond = r.cond AND q.cond IS DISTINCT FROM r.cond"
    )

    # closed: a confirmed run (p and q flagged) ended with an unflagged or different window r.
    stmt.add_insert_sql(
        "INSERT INTO district_alerts"
        + incident_select("p", "closed")
        + JOIN3
        + "WHERE p.cond IS NOT NULL AND q.cond = p.cond AND r.cond IS DISTINCT FROM p.cond"
    )

    # R3: straight from the features, no confirmation needed.
    stmt.add_insert_sql(f"""
    INSERT INTO district_alerts
    SELECT
        '1.0.0',
        CONCAT_WS('|', district_id, 'data_quality', 'instant', DATE_FORMAT(window_start, 'yyyyMMddHHmm')),
        'data_quality', 'R3', 'low', 'instant',
        district_id, window_start, window_end,
        CAST(NULL AS TIMESTAMP(3)), CAST(NULL AS DOUBLE), CAST(NULL AS DOUBLE),
        avg_flow_m3_s, min_pressure_m, n_events, n_null_pressure, n_flagged,
        CASE WHEN n_events <> {a.expected_events} THEN 'incomplete_window'
             WHEN n_null_pressure > 0 THEN 'null_pressure'
             ELSE 'quality_code_flagged' END,
        'flink_stream',
        CAST(NULL AS DOUBLE), CAST(NULL AS DOUBLE)
    FROM district_features
    WHERE n_events <> {a.expected_events} OR n_null_pressure > 0 OR n_flagged > 0
    """)

    stmt.execute()


if __name__ == "__main__":
    main()
