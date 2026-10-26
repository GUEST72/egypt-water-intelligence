from pyflink.table import EnvironmentSettings, TableEnvironment

env = EnvironmentSettings.in_streaming_mode()
t_env = TableEnvironment.create(env)
t_env.get_config().set("table.exec.source.idle-timeout", "10 s")

# Source: Kafka district sensors
t_env.execute_sql("""
CREATE TABLE district_sensors (
    event_id STRING,
    run_id STRING,
    district_id STRING,
    event_time_utc TIMESTAMP_LTZ(3),
    inlet_flow_m3_s DOUBLE,
    inlet_volume_m3 DOUBLE,
    pressure_m_h2o DOUBLE,
    quality_code STRING,
    WATERMARK FOR event_time_utc AS event_time_utc - INTERVAL '1' MINUTE
) WITH (
    'connector' = 'kafka',
    'topic' = 'water.district-sensors.v1',
    'properties.bootstrap.servers' = 'kafka:9092',
    'properties.group.id' = 'flink-water-detection-dev',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json',
    'json.timestamp-format.standard' = 'ISO-8601'
)
""")

# Sink: print to TaskManager log
t_env.execute_sql("""
CREATE TABLE district_window_print (
    window_start TIMESTAMP(3),
    window_end TIMESTAMP(3),
    district_id STRING,
    inlet_volume_m3 DOUBLE,
    n_events BIGINT,
    n_null_pressure BIGINT
) WITH ('connector' = 'print')
""")

t_env.execute_sql("""
INSERT INTO district_window_print
SELECT
    window_start,
    window_end,
    district_id,
    SUM(inlet_volume_m3),
    COUNT(*),
    COUNT(*) - COUNT(pressure_m_h2o)
FROM TABLE(
    TUMBLE(TABLE district_sensors, DESCRIPTOR(event_time_utc), INTERVAL '15' MINUTES))
GROUP BY window_start, window_end, district_id
""")
