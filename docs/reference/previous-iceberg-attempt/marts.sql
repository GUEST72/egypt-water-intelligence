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
