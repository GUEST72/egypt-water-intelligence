-- The main dashboard grain: one row per district and UTC day, joining the batch water balance,
-- the streaming alerts and the batch data-quality outcome counts.
WITH daily AS (
    SELECT * FROM water.gold.gold_district_daily_water_balance WHERE run_id = '{run_id}'
),
alerts AS (
    SELECT district_id, alert_date AS date_utc,
           count_if(alert_type IN ('flow_anomaly', 'supply_disturbance') AND status = 'open') AS incidents_opened,
           count_if(alert_type = 'flow_level_shift') AS level_shift_alerts,
           count_if(alert_type = 'data_quality') AS data_quality_alerts
    FROM water.streaming.fact_district_alert
    WHERE run_id = '{run_id}'
    GROUP BY district_id, alert_date
),
quality AS (
    SELECT district_id, date_utc,
           sum(records) FILTER (WHERE outcome = 'accepted') AS accepted_records,
           sum(records) FILTER (WHERE outcome LIKE 'rejected:%') AS rejected_records
    FROM water.gold.gold_data_quality_daily
    WHERE run_id = '{run_id}'
    GROUP BY district_id, date_utc
)
SELECT d.run_id, d.district_id, d.date_utc,
       d.inlet_volume_m3, d.metered_consumption_m3, d.gap_m3, d.gap_pct,
       d.total_periods, d.complete_periods, d.coverage_pct,
       coalesce(a.incidents_opened, 0) AS incidents_opened,
       coalesce(a.level_shift_alerts, 0) AS level_shift_alerts,
       coalesce(a.data_quality_alerts, 0) AS data_quality_alerts,
       coalesce(q.accepted_records, 0) AS accepted_records,
       coalesce(q.rejected_records, 0) AS rejected_records,
       100 * CAST(coalesce(q.rejected_records, 0) AS double)
           / nullif(coalesce(q.accepted_records, 0) + coalesce(q.rejected_records, 0), 0) AS rejected_pct
FROM daily d
LEFT JOIN alerts a ON a.district_id = d.district_id AND a.date_utc = d.date_utc
LEFT JOIN quality q ON q.district_id = d.district_id AND q.date_utc = d.date_utc
