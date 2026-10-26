"""Job 3c: flow_level_shift (rule R4) - a sustained change in a district's own flow level.

Reads  : water.district-relative.15m.v1  (written by job3b_district_alerts.py)
Writes : water.district-alerts.v1        (same alert schema as job3b)

Why this rule exists
  R1/R2 compare a district with its peers over 15-minute windows and need a jump of
  more than 10%. A small persistent loss (+4 to +5% of flow) never reaches that.
  R4 compares a district with ITSELF at day level, after removing the windows that
  R1/R2 already explain.

Computation (per district)
  day_mean = average rel_flow over the UTC day, using ONLY windows where cond IS NULL
             (windows already flagged by R1/R2 are not counted twice)
  n_ok     = number of such windows (a full day has 96)
  base     = median of the previous 6 daily means (the existing median_of UDF)
  step     = day_mean / base - 1
  alert    when step > LEVEL_THRESHOLD and n_ok >= MIN_OK_WINDOWS
           and the previous day was NOT already above the threshold
           (one alert per shift, not one per day)

Alert: alert_type = flow_level_shift, rule_id = R4, severity = medium, status = instant.
  window_start / window_end = the UTC day that confirmed the shift,
  first_flagged_window_start = that day's start,
  rel_flow = day_mean, baseline_rel_flow = base, level_shift_pct = step * 100.

Known limits (also write them in the docs):
  * Detection delay is about one day (the day must finish before its mean exists).
  * Only INCREASES alert. A decrease is not reported.
  * The baseline adapts: after about 3 days the shifted level becomes the new normal,
    so this is an onset alert, not a state.
  * A loss that is constant from the very first day (D006) cannot be detected: there is
    no change to measure. Only the water balance (meters) sees it.
  * The first 6 days of a stream cannot alert (no baseline yet).
  * The 3% threshold was chosen from ONE event (D005, +4.2%) on noise-free data.
  * Other districts show mirrored steps of about -1.7% / +1.9% while one district shifts,
    because the shifted district moves the peer median. 3% sits between 1.9% and 4.2%.
  * Join state must survive 7 days of event time. STATE_TTL is processing time: with
    accelerated replay the default is fine; for a live-speed demo keep it above 7 days.
"""

import argparse
import os

from pyflink.table import DataTypes, EnvironmentSettings, TableEnvironment
from pyflink.table.udf import udf


@udf(result_type=DataTypes.DOUBLE())
def median_of(a, b, c, d, e, f):
    # Same median as job3b and tests/runtime/udf_check.py: ignore null inputs.
    vals = sorted(v for v in (a, b, c, d, e, f) if v is not None)
    if not vals:
        return None
    mid = len(vals) // 2
    return vals[mid] if len(vals) % 2 else (vals[mid - 1] + vals[mid]) / 2


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--bootstrap", default=os.environ.get("BOOTSTRAP", "kafka:9092"))
    p.add_argument("--relative-topic", default=os.environ.get("RELATIVE_TOPIC", "water.district-relative.15m.v1"))
    p.add_argument("--alerts-topic", default=os.environ.get("ALERTS_TOPIC", "water.district-alerts.v1"))
    p.add_argument("--group-id", default=os.environ.get("GROUP_ID", "flink-water-levelshift-dev"))
    p.add_argument("--startup-mode", default=os.environ.get("STARTUP_MODE", "earliest-offset"))
    p.add_argument("--level-threshold", type=float, default=float(os.environ.get("LEVEL_THRESHOLD", "0.03")))
    p.add_argument("--min-ok-windows", type=int, default=int(os.environ.get("MIN_OK_WINDOWS", "72")))
    p.add_argument("--state-ttl", default=os.environ.get("STATE_TTL", "8 d"))
    args, _ = p.parse_known_args()
    return args


def main():
    a = parse_args()
    t_env = TableEnvironment.create(EnvironmentSettings.in_streaming_mode())
    cfg = t_env.get_config()
    cfg.set("table.local-time-zone", "UTC")
    cfg.set("table.exec.source.idle-timeout", "10 s")
    cfg.set("table.exec.state.ttl", a.state_ttl)
    t_env.create_temporary_function("median_of", median_of)

    t_env.execute_sql(f"""
    CREATE TABLE district_relative (
        window_start TIMESTAMP(3),
        window_end TIMESTAMP(3),
        district_id STRING,
        avg_flow_m3_s DOUBLE,
        min_pressure_m DOUBLE,
        rel_flow DOUBLE,
        rel_pressure DOUBLE,
        cond STRING,
        WATERMARK FOR window_start AS window_start
    ) WITH (
        'connector' = 'kafka',
        'topic' = '{a.relative_topic}',
        'properties.bootstrap.servers' = '{a.bootstrap}',
        'properties.group.id' = '{a.group_id}',
        'scan.startup.mode' = '{a.startup_mode}',
        'format' = 'json',
        'json.timestamp-format.standard' = 'ISO-8601'
    )
    """)

    # Same column list and order as job3b's alert table.
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

    # The TVF adds its own window_start/window_end, so rename the source time column.
    t_env.execute_sql("""
    CREATE TEMPORARY VIEW rel_v AS
    SELECT window_start AS ws, district_id, rel_flow, cond
    FROM district_relative
    """)

    # One row per district and UTC day. AVG ignores NULL, so flagged windows drop out.
    t_env.execute_sql("""
    CREATE TEMPORARY VIEW daily AS
    SELECT window_start AS day_start, window_end AS day_end, district_id,
           AVG(CASE WHEN cond IS NULL THEN rel_flow END) AS day_mean,
           CAST(SUM(CASE WHEN cond IS NULL AND rel_flow IS NOT NULL THEN 1 ELSE 0 END) AS BIGINT) AS n_ok
    FROM TABLE(TUMBLE(TABLE rel_v, DESCRIPTOR(ws), INTERVAL '1' DAY))
    GROUP BY window_start, window_end, district_id
    """)

    # Baseline = median of the previous 6 daily means (inner joins keep the stream append-only).
    joins = "\n".join(
        f"JOIN daily b{k} ON c.district_id = b{k}.district_id "
        f"AND b{k}.day_start = c.day_start - INTERVAL '{k}' DAY"
        for k in range(1, 7)
    )
    base = "median_of(" + ", ".join(f"b{k}.day_mean" for k in range(1, 7)) + ")"
    t_env.execute_sql(f"""
    CREATE TEMPORARY VIEW steps AS
    SELECT c.day_start, c.day_end, c.district_id, c.day_mean, c.n_ok,
           {base} AS base,
           c.day_mean / NULLIF({base}, 0) - 1 AS step
    FROM daily c
    {joins}
    """)

    # Alert only on the first day above the threshold: the previous day must exist
    # (inner join) and must not have been above it.
    t_env.execute_sql(f"""
    INSERT INTO district_alerts
    SELECT
        '1.0.0',
        CONCAT_WS('|', c.district_id, 'flow_level_shift', 'instant', DATE_FORMAT(c.day_start, 'yyyyMMddHHmm')),
        'flow_level_shift', 'R4', 'medium', 'instant',
        c.district_id, c.day_start, c.day_end, c.day_start,
        c.day_mean, CAST(NULL AS DOUBLE), CAST(NULL AS DOUBLE), CAST(NULL AS DOUBLE),
        CAST(NULL AS BIGINT), CAST(NULL AS BIGINT), CAST(NULL AS BIGINT),
        'sustained_increase_vs_own_history',
        'flink_stream',
        c.base, c.step * 100
    FROM steps c
    JOIN steps p ON c.district_id = p.district_id
                AND p.day_start = c.day_start - INTERVAL '1' DAY
    WHERE c.step > {a.level_threshold}
      AND c.n_ok >= {a.min_ok_windows}
      AND COALESCE(p.step, 0) <= {a.level_threshold}
    """)


if __name__ == "__main__":
    main()
