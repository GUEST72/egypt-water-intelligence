-- Streaming incidents explained with the batch water balance (meters), without any truth data.
--
-- Flink sees only district inlet flow, so a burst (water lost) and a demand surge (customers use more)
-- look the same. The meters tell them apart. For each R1/R2 incident we compare the complete 15-minute
-- balance intervals inside it with the same district's normal level: the median at the same Cairo
-- weekday and hour, taken from intervals outside every incident.
--   supply_like : inlet fell more than 10% below normal
--   loss_like   : extra unmetered water (gap above normal) exceeds 10% of normal inlet
--   demand_like : metered consumption rose more than 10% while the gap stayed normal
--   unclear     : none of the above; no_balance: no complete interval inside the incident
WITH incidents AS (
    SELECT * FROM water.marts.mart_incidents
    WHERE run_id = '{run_id}' AND alert_type IN ('flow_anomaly', 'supply_disturbance')
),
balance AS (
    SELECT b.district_id, b.interval_start_utc, b.inlet_volume_m3, b.metered_consumption_m3, b.gap_m3,
           d.day_of_week_number_cairo AS dow, d.local_hour_cairo AS hr
    FROM water.gold.fact_water_balance_15m b
    JOIN water.gold.dim_interval d
      ON d.run_id = b.run_id AND d.interval_start_utc = b.interval_start_utc
    WHERE b.run_id = '{run_id}' AND b.complete
),
during AS (
    SELECT i.incident_id, b.*
    FROM incidents i
    JOIN balance b
      ON b.district_id = i.district_id
     AND b.interval_start_utc >= i.incident_start
     AND b.interval_start_utc < coalesce(i.incident_end, i.confirmed_at)
),
any_incident AS (
    SELECT DISTINCT i.district_id, b.interval_start_utc
    FROM water.marts.mart_incidents i
    JOIN balance b
      ON b.district_id = i.district_id
     AND b.interval_start_utc >= i.incident_start
     AND b.interval_start_utc < coalesce(i.incident_end, i.confirmed_at)
    WHERE i.run_id = '{run_id}'
),
normal AS (
    SELECT b.district_id, b.dow, b.hr,
           approx_percentile(b.inlet_volume_m3, 0.5) AS inlet,
           approx_percentile(b.metered_consumption_m3, 0.5) AS metered,
           approx_percentile(b.gap_m3, 0.5) AS gap
    FROM balance b
    LEFT JOIN any_incident x ON x.district_id = b.district_id AND x.interval_start_utc = b.interval_start_utc
    WHERE x.district_id IS NULL
    GROUP BY b.district_id, b.dow, b.hr
),
compared AS (
    SELECT d.incident_id,
           count(*) AS complete_intervals,
           sum(d.inlet_volume_m3) AS inlet_m3,
           sum(d.metered_consumption_m3) AS metered_m3,
           sum(d.gap_m3) AS gap_m3,
           sum(n.inlet) AS normal_inlet_m3,
           sum(n.metered) AS normal_metered_m3,
           sum(n.gap) AS normal_gap_m3
    FROM during d
    JOIN normal n ON n.district_id = d.district_id AND n.dow = d.dow AND n.hr = d.hr
    GROUP BY d.incident_id
)
SELECT '{run_id}' AS run_id,
       i.incident_id, i.district_id, i.alert_type, i.incident_start, i.incident_end, i.peak_rel_flow,
       coalesce(c.complete_intervals, 0) AS complete_intervals,
       c.inlet_m3, c.normal_inlet_m3, c.metered_m3, c.normal_metered_m3, c.gap_m3, c.normal_gap_m3,
       c.inlet_m3 / nullif(c.normal_inlet_m3, 0) - 1 AS inlet_change_ratio,
       c.metered_m3 / nullif(c.normal_metered_m3, 0) - 1 AS metered_change_ratio,
       (c.gap_m3 - c.normal_gap_m3) / nullif(c.normal_inlet_m3, 0) AS excess_loss_share,
       CASE
           WHEN c.complete_intervals IS NULL THEN 'no_balance'
           WHEN c.inlet_m3 / nullif(c.normal_inlet_m3, 0) - 1 < -0.10 THEN 'supply_like'
           WHEN (c.gap_m3 - c.normal_gap_m3) / nullif(c.normal_inlet_m3, 0) > 0.10 THEN 'loss_like'
           WHEN c.metered_m3 / nullif(c.normal_metered_m3, 0) - 1 > 0.10 THEN 'demand_like'
           ELSE 'unclear'
       END AS meter_explanation
FROM incidents i
LEFT JOIN compared c ON c.incident_id = i.incident_id
