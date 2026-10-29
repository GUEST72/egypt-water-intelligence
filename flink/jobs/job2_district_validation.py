"""Job 2 (districts): validate, deduplicate and route district sensor readings.

The raw topic is read as plain text so that malformed JSON can be captured
instead of silently dropped. The stream is then split in two:

  * rejected records -> dead-letter topic, with a reject_reason
  * valid records    -> deduplicated by event_id -> clean topic

A null pressure_m_h2o is NOT a rejection. It stays NULL (never 0) and is
flagged with is_null_pressure, so a missing reading can never look like a
pressure drop in the detection rules.

No restricted truth files are used anywhere in this job.

Settings come from environment variables (or the matching --options), so the
same file can run on the test topics and on the real topics:
  BOOTSTRAP, INPUT_TOPIC, CLEAN_TOPIC, DEAD_LETTER_TOPIC,
  GROUP_ID, STARTUP_MODE, STATE_TTL
"""

import argparse
import os

from pyflink.table import EnvironmentSettings, TableEnvironment


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--bootstrap", default=os.environ.get("BOOTSTRAP", "kafka:9092"))
    p.add_argument("--input-topic", default=os.environ.get("INPUT_TOPIC", "water.district-sensors.v1"))
    p.add_argument("--clean-topic", default=os.environ.get("CLEAN_TOPIC", "water.district-sensors.clean.v1"))
    p.add_argument("--dead-letter-topic", default=os.environ.get("DEAD_LETTER_TOPIC", "water.dead-letter.district.v1"))
    p.add_argument("--group-id", default=os.environ.get("GROUP_ID", "flink-water-detection-dev"))
    p.add_argument("--startup-mode", default=os.environ.get("STARTUP_MODE", "earliest-offset"))
    # Only bounds state size. Identical duplicates sit next to each other in the
    # delivery files, so a short TTL is enough (inference, verified by the counts).
    p.add_argument("--state-ttl", default=os.environ.get("STATE_TTL", "10 min"))
    # Ignore any extra arguments added by the Flink CLI.
    args, _ = p.parse_known_args()
    return args


def main():
    a = parse_args()
    t_env = TableEnvironment.create(EnvironmentSettings.in_streaming_mode())
    cfg = t_env.get_config()
    cfg.set("table.local-time-zone", "UTC")
    cfg.set("table.exec.state.ttl", a.state_ttl)

    # Raw source: one Kafka message = one text payload (no JSON parsing yet).
    t_env.execute_sql(f"""
    CREATE TABLE district_raw (
        payload STRING,
        kafka_partition INT METADATA FROM 'partition' VIRTUAL,
        kafka_offset BIGINT METADATA FROM 'offset' VIRTUAL
    ) WITH (
        'connector' = 'kafka',
        'topic' = '{a.input_topic}',
        'properties.bootstrap.servers' = '{a.bootstrap}',
        'properties.group.id' = '{a.group_id}',
        'scan.startup.mode' = '{a.startup_mode}',
        'format' = 'raw'
    )
    """)

    # Sink 1: clean, deduplicated readings. NULL pressure stays NULL.
    t_env.execute_sql(f"""
    CREATE TABLE district_clean (
        schema_version STRING,
        event_id STRING,
        run_id STRING,
        district_id STRING,
        inlet_link_id STRING,
        pressure_monitor_node_id STRING,
        interval_start_utc STRING,
        interval_end_utc STRING,
        event_time_utc STRING,
        availability_time_utc STRING,
        inlet_flow_m3_s DOUBLE,
        inlet_volume_m3 DOUBLE,
        pressure_m_h2o DOUBLE,
        is_null_pressure BOOLEAN,
        quality_code STRING
    ) WITH (
        'connector' = 'kafka',
        'topic' = '{a.clean_topic}',
        'properties.bootstrap.servers' = '{a.bootstrap}',
        'format' = 'json',
        'sink.delivery-guarantee' = 'at-least-once'
    )
    """)

    # Sink 2: dead-letter topic with the reason and the untouched raw payload.
    t_env.execute_sql(f"""
    CREATE TABLE district_dead_letter (
        job STRING,
        source_topic STRING,
        source_partition INT,
        source_offset BIGINT,
        reject_reason STRING,
        raw_payload STRING
    ) WITH (
        'connector' = 'kafka',
        'topic' = '{a.dead_letter_topic}',
        'properties.bootstrap.servers' = '{a.bootstrap}',
        'format' = 'json',
        'sink.delivery-guarantee' = 'at-least-once'
    )
    """)

    # Step 1: extract every field as text. JSON_VALUE returns NULL when a field
    # is missing or the payload is not valid JSON.
    # has_pressure_key tells "key present with null value" (allowed) apart from
    # "key missing" (rejected). The LIKE is a fallback in case JSON_EXISTS
    # reports false for an explicit null.
    t_env.execute_sql("""
    CREATE TEMPORARY VIEW parsed AS
    SELECT
        payload,
        kafka_partition,
        kafka_offset,
        COALESCE(payload IS JSON OBJECT, FALSE) AS is_json,
        JSON_VALUE(payload, '$.schema_version') AS schema_version,
        JSON_VALUE(payload, '$.event_id') AS event_id,
        JSON_VALUE(payload, '$.run_id') AS run_id,
        JSON_VALUE(payload, '$.district_id') AS district_id,
        JSON_VALUE(payload, '$.inlet_link_id') AS inlet_link_id,
        JSON_VALUE(payload, '$.pressure_monitor_node_id') AS pressure_monitor_node_id,
        JSON_VALUE(payload, '$.interval_start_utc') AS interval_start_utc,
        JSON_VALUE(payload, '$.interval_end_utc') AS interval_end_utc,
        JSON_VALUE(payload, '$.event_time_utc') AS event_time_utc,
        JSON_VALUE(payload, '$.availability_time_utc') AS availability_time_utc,
        JSON_VALUE(payload, '$.quality_code') AS quality_code,
        JSON_VALUE(payload, '$.inlet_flow_m3_s') AS flow_text,
        JSON_VALUE(payload, '$.inlet_volume_m3') AS volume_text,
        JSON_VALUE(payload, '$.pressure_m_h2o') AS pressure_text,
        (COALESCE(JSON_EXISTS(payload, '$.pressure_m_h2o'), FALSE)
            OR COALESCE(payload LIKE '%"pressure_m_h2o"%', FALSE)) AS has_pressure_key,
        TO_TIMESTAMP(REPLACE(REPLACE(JSON_VALUE(payload, '$.interval_start_utc'), 'T', ' '), 'Z', '')) AS start_ts,
        TO_TIMESTAMP(REPLACE(REPLACE(JSON_VALUE(payload, '$.interval_end_utc'), 'T', ' '), 'Z', '')) AS end_ts,
        TO_TIMESTAMP(REPLACE(REPLACE(JSON_VALUE(payload, '$.event_time_utc'), 'T', ' '), 'Z', '')) AS event_ts,
        TO_TIMESTAMP(REPLACE(REPLACE(JSON_VALUE(payload, '$.availability_time_utc'), 'T', ' '), 'Z', '')) AS avail_ts
    FROM district_raw
    """)

    # Step 2: pick the FIRST failing rule as the reject reason.
    # reject_reason IS NULL means the record is valid.
    t_env.execute_sql("""
    CREATE TEMPORARY VIEW classified AS
    SELECT
        *,
        CASE
            WHEN NOT is_json THEN 'malformed_json'
            WHEN schema_version IS NULL OR schema_version <> '1.0.0'
                THEN 'unsupported_schema_version'
            WHEN event_id IS NULL OR event_id = '' OR run_id IS NULL
                 OR district_id IS NULL OR inlet_link_id IS NULL
                 OR pressure_monitor_node_id IS NULL OR quality_code IS NULL
                 OR interval_start_utc IS NULL OR interval_end_utc IS NULL
                 OR event_time_utc IS NULL OR availability_time_utc IS NULL
                 OR flow_text IS NULL OR volume_text IS NULL
                 OR NOT has_pressure_key
                THEN 'missing_required_field'
            WHEN NOT COALESCE(REGEXP(district_id, '^D[0-9][0-9][0-9]$'), FALSE)
                THEN 'invalid_district_id'
            WHEN quality_code NOT IN ('valid', 'noisy', 'stuck', 'corrupted')
                THEN 'unknown_quality_code'
            WHEN start_ts IS NULL OR end_ts IS NULL OR event_ts IS NULL OR avail_ts IS NULL
                THEN 'invalid_timestamp'
            -- District events must cover exactly one minute and use the interval start as event time.
            WHEN TIMESTAMPDIFF(SECOND, start_ts, end_ts) <> 60 OR event_ts <> start_ts
                THEN 'invalid_interval'
            WHEN TRY_CAST(flow_text AS DOUBLE) IS NULL
                 OR TRY_CAST(volume_text AS DOUBLE) IS NULL
                 OR (pressure_text IS NOT NULL AND TRY_CAST(pressure_text AS DOUBLE) IS NULL)
                THEN 'non_numeric_value'
            WHEN TRY_CAST(flow_text AS DOUBLE) < 0
                 OR TRY_CAST(volume_text AS DOUBLE) < 0
                 OR TRY_CAST(pressure_text AS DOUBLE) < 0
                THEN 'negative_value'
            ELSE CAST(NULL AS STRING)
        END AS reject_reason
    FROM parsed
    """)

    # Step 3: valid records only, with a processing-time column for dedup ordering.
    t_env.execute_sql("""
    CREATE TEMPORARY VIEW valid_events AS
    SELECT
        schema_version, event_id, run_id, district_id, inlet_link_id,
        pressure_monitor_node_id, interval_start_utc, interval_end_utc,
        event_time_utc, availability_time_utc,
        TRY_CAST(flow_text AS DOUBLE) AS inlet_flow_m3_s,
        TRY_CAST(volume_text AS DOUBLE) AS inlet_volume_m3,
        TRY_CAST(pressure_text AS DOUBLE) AS pressure_m_h2o,
        (pressure_text IS NULL) AS is_null_pressure,
        quality_code,
        PROCTIME() AS proc_time
    FROM classified
    WHERE reject_reason IS NULL
    """)

    # Step 4: keep the first occurrence of every event_id.
    t_env.execute_sql("""
    CREATE TEMPORARY VIEW deduplicated AS
    SELECT
        schema_version, event_id, run_id, district_id, inlet_link_id,
        pressure_monitor_node_id, interval_start_utc, interval_end_utc,
        event_time_utc, availability_time_utc,
        inlet_flow_m3_s, inlet_volume_m3, pressure_m_h2o,
        is_null_pressure, quality_code
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY proc_time ASC) AS rn
        FROM valid_events
    )
    WHERE rn = 1
    """)

    # Both inserts run as ONE job so the source is read once.
    stmt = t_env.create_statement_set()
    stmt.add_insert_sql("INSERT INTO district_clean SELECT * FROM deduplicated")
    stmt.add_insert_sql(f"""
        INSERT INTO district_dead_letter
        SELECT
            'job2_district_validation',
            '{a.input_topic}',
            kafka_partition,
            kafka_offset,
            reject_reason,
            payload
        FROM classified
        WHERE reject_reason IS NOT NULL
    """)
    stmt.execute()


if __name__ == "__main__":
    main()
