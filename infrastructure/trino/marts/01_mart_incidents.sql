-- One row per streaming incident (Flink alerts), with its duration, detection delay and peak deviation.
-- R1/R2 incidents pair each 'open' alert with the first 'closed' alert of the same district and type;
-- R4 level shifts are single 'instant' alerts that span one UTC day.
WITH alerts AS (
    SELECT * FROM water.streaming.fact_district_alert WHERE run_id = '{run_id}'
),
opened AS (
    SELECT alert_id, district_id, alert_type, rule_id, severity,
           first_flagged_window_start AS incident_start, window_end AS confirmed_at
    FROM alerts
    WHERE status = 'open'
),
closed AS (
    SELECT district_id, alert_type, window_start AS last_window_start, window_end AS last_window_end
    FROM alerts
    WHERE status = 'closed'
),
paired AS (
    SELECT o.alert_id, o.district_id, o.alert_type, o.rule_id, o.severity, o.incident_start, o.confirmed_at,
           min(c.last_window_end) AS incident_end
    FROM opened o
    LEFT JOIN closed c
      ON c.district_id = o.district_id AND c.alert_type = o.alert_type AND c.last_window_start >= o.incident_start
    GROUP BY o.alert_id, o.district_id, o.alert_type, o.rule_id, o.severity, o.incident_start, o.confirmed_at
),
level_shifts AS (
    SELECT alert_id, district_id, alert_type, rule_id, severity,
           first_flagged_window_start AS incident_start, window_end AS confirmed_at, window_end AS incident_end
    FROM alerts
    WHERE alert_type = 'flow_level_shift'
),
incidents AS (
    SELECT * FROM paired
    UNION ALL
    SELECT * FROM level_shifts
)
SELECT '{run_id}' AS run_id,
       i.alert_id AS incident_id,
       i.district_id,
       i.alert_type,
       i.rule_id,
       i.severity,
       i.incident_start,
       i.confirmed_at,
       i.incident_end,
       i.incident_end IS NULL AS still_open_at_end_of_data,
       date_diff('minute', i.incident_start, i.confirmed_at) AS detection_delay_min,
       date_diff('minute', i.incident_start, coalesce(i.incident_end, i.confirmed_at)) AS duration_min,
       max_by(w.rel_flow, abs(w.rel_flow - 1)) AS peak_rel_flow,
       min(w.rel_pressure) AS min_rel_pressure,
       count(w.window_start) AS windows
FROM incidents i
LEFT JOIN water.streaming.fact_district_window_15m w
  ON w.run_id = '{run_id}'
 AND w.district_id = i.district_id
 AND w.window_start >= i.incident_start
 AND w.window_start < coalesce(i.incident_end, i.confirmed_at)
GROUP BY i.alert_id, i.district_id, i.alert_type, i.rule_id, i.severity,
         i.incident_start, i.confirmed_at, i.incident_end
