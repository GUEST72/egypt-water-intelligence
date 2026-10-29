"""Job 3a: 15-minute window features per district.

Reads the CLEAN district topic written by job2_district_validation.py and
writes one row per (district, 15-minute window) to a features topic.
The detection rules (next job) read this topic, so thresholds are never
mixed with window arithmetic.

Features per window:
  avg_flow_m3_s, inlet_volume_m3, min_pressure_m, avg_pressure_m,
  n_events (15 expected), n_null_pressure, n_flagged (quality_code != valid)

NULL pressure values are ignored by MIN/AVG; they are never treated as 0.
No restricted truth files are used anywhere in this job.

Settings come from environment variables (or the matching --options):
  BOOTSTRAP, INPUT_TOPIC, FEATURES_TOPIC, GROUP_ID, STARTUP_MODE
"""

import argparse
import os

from pyflink.table import EnvironmentSettings, TableEnvironment


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--bootstrap", default=os.environ.get("BOOTSTRAP", "kafka:9092"))
    p.add_argument("--input-topic", default=os.environ.get("INPUT_TOPIC", "water.district-sensors.clean.v1"))
    p.add_argument("--features-topic", default=os.environ.get("FEATURES_TOPIC", "water.district-features.15m.v1"))
    p.add_argument("--group-id", default=os.environ.get("GROUP_ID", "flink-water-detection-dev"))
    p.add_argument("--startup-mode", default=os.environ.get("STARTUP_MODE", "earliest-offset"))
    args, _ = p.parse_known_args()
    return args


def main():
    a = parse_args()
    t_env = TableEnvironment.create(EnvironmentSettings.in_streaming_mode())
    cfg = t_env.get_config()
    cfg.set("table.local-time-zone", "UTC")
    # The topic has one partition but the job runs with parallelism 2, so one
    # source subtask stays empty. Without an idle timeout its watermark would
    # never advance and no window would close.
    cfg.set("table.exec.source.idle-timeout", "10 s")

    t_env.execute_sql(f"""
    CREATE TABLE district_clean (
        event_id STRING,
        district_id STRING,
        event_time_utc TIMESTAMP_LTZ(3),
        inlet_flow_m3_s DOUBLE,
        inlet_volume_m3 DOUBLE,
        pressure_m_h2o DOUBLE,
        is_null_pressure BOOLEAN,
        quality_code STRING,
        WATERMARK FOR event_time_utc AS event_time_utc - INTERVAL '1' MINUTE
    ) WITH (
        'connector' = 'kafka',
        'topic' = '{a.input_topic}',
        'properties.bootstrap.servers' = '{a.bootstrap}',
        'properties.group.id' = '{a.group_id}',
        'scan.startup.mode' = '{a.startup_mode}',
        'format' = 'json',
        'json.timestamp-format.standard' = 'ISO-8601'
    )
    """)

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
        n_flagged BIGINT
    ) WITH (
        'connector' = 'kafka',
        'topic' = '{a.features_topic}',
        'properties.bootstrap.servers' = '{a.bootstrap}',
        'format' = 'json',
        'json.timestamp-format.standard' = 'ISO-8601',
        'sink.delivery-guarantee' = 'at-least-once'
    )
    """)

    t_env.execute_sql("""
    INSERT INTO district_features
    SELECT
        window_start,
        window_end,
        district_id,
        AVG(inlet_flow_m3_s),
        SUM(inlet_volume_m3),
        MIN(pressure_m_h2o),
        AVG(pressure_m_h2o),
        COUNT(*),
        COUNT(*) - COUNT(pressure_m_h2o),
        CAST(SUM(CASE WHEN quality_code <> 'valid' THEN 1 ELSE 0 END) AS BIGINT)
    FROM TABLE(
        TUMBLE(TABLE district_clean, DESCRIPTOR(event_time_utc), INTERVAL '15' MINUTES))
    GROUP BY window_start, window_end, district_id
    """)


if __name__ == "__main__":
    main()
