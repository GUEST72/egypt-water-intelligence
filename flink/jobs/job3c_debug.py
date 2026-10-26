"""Debug helper for job3c_level_shift.py. NOT part of the pipeline.

Runs the SAME views as job3c (rel_v, daily, steps) but writes one intermediate view
to a test topic instead of producing alerts, so it can be compared with the pandas
calculation done by hand on the same relative topic.

  --view daily : one row per district and UTC day  (day_mean, n_ok)
  --view steps : same, plus base (median of previous 6 daily means) and step

No restricted truth files are used.
"""

import argparse
import os

from pyflink.table import DataTypes, EnvironmentSettings, TableEnvironment
from pyflink.table.udf import udf


@udf(result_type=DataTypes.DOUBLE())
def median_of(a, b, c, d, e, f):
    vals = sorted(v for v in (a, b, c, d, e, f) if v is not None)
    if not vals:
        return None
    mid = len(vals) // 2
    return vals[mid] if len(vals) % 2 else (vals[mid - 1] + vals[mid]) / 2


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--bootstrap", default=os.environ.get("BOOTSTRAP", "kafka:9092"))
    p.add_argument("--relative-topic", default="water.district-relative.15m.v1")
    p.add_argument("--out-topic", required=True)
    p.add_argument("--view", choices=["daily", "steps"], required=True)
    p.add_argument("--group-id", required=True)
    p.add_argument("--state-ttl", default="8 d")
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
        'scan.startup.mode' = 'earliest-offset',
        'format' = 'json',
        'json.timestamp-format.standard' = 'ISO-8601'
    )
    """)

    t_env.execute_sql("""
    CREATE TEMPORARY VIEW rel_v AS
    SELECT window_start AS ws, district_id, rel_flow, cond
    FROM district_relative
    """)

    t_env.execute_sql("""
    CREATE TEMPORARY VIEW daily AS
    SELECT window_start AS day_start, window_end AS day_end, district_id,
           AVG(CASE WHEN cond IS NULL THEN rel_flow END) AS day_mean,
           CAST(SUM(CASE WHEN cond IS NULL AND rel_flow IS NOT NULL THEN 1 ELSE 0 END) AS BIGINT) AS n_ok
    FROM TABLE(TUMBLE(TABLE rel_v, DESCRIPTOR(ws), INTERVAL '1' DAY))
    GROUP BY window_start, window_end, district_id
    """)

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

    sink_opts = f"""
        'connector' = 'kafka',
        'topic' = '{a.out_topic}',
        'properties.bootstrap.servers' = '{a.bootstrap}',
        'format' = 'json',
        'json.timestamp-format.standard' = 'ISO-8601',
        'sink.delivery-guarantee' = 'at-least-once'
    """
    if a.view == "daily":
        t_env.execute_sql(f"""
        CREATE TABLE debug_out (
            day_start TIMESTAMP(3), day_end TIMESTAMP(3), district_id STRING,
            day_mean DOUBLE, n_ok BIGINT
        ) WITH ({sink_opts})
        """)
        t_env.execute_sql(
            "INSERT INTO debug_out SELECT day_start, day_end, district_id, day_mean, n_ok FROM daily"
        )
    else:
        t_env.execute_sql(f"""
        CREATE TABLE debug_out (
            day_start TIMESTAMP(3), district_id STRING,
            day_mean DOUBLE, n_ok BIGINT, base DOUBLE, step DOUBLE
        ) WITH ({sink_opts})
        """)
        t_env.execute_sql(
            "INSERT INTO debug_out SELECT day_start, district_id, day_mean, n_ok, base, step FROM steps"
        )


if __name__ == "__main__":
    main()
