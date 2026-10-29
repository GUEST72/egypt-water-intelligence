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
