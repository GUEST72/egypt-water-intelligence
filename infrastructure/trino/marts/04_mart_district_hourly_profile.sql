-- Typical week per district in Cairo local time (weekday x hour), from complete balance intervals only.
-- Volumes are per hour (four 15-minute intervals). Feeds the hour-of-week heatmap.
SELECT b.run_id, b.district_id,
       d.day_of_week_number_cairo, d.day_of_week_cairo, d.is_weekend_cairo, d.local_hour_cairo,
       4 * avg(b.inlet_volume_m3) AS avg_inlet_m3_per_hour,
       4 * avg(b.metered_consumption_m3) AS avg_metered_m3_per_hour,
       avg(b.gap_pct) AS avg_gap_pct,
       count(*) AS complete_intervals
FROM water.gold.fact_water_balance_15m b
JOIN water.gold.dim_interval d
  ON d.run_id = b.run_id AND d.interval_start_utc = b.interval_start_utc
WHERE b.run_id = '{run_id}' AND b.complete
GROUP BY b.run_id, b.district_id, d.day_of_week_number_cairo, d.day_of_week_cairo, d.is_weekend_cairo,
         d.local_hour_cairo
