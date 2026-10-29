-- Read-only Gold analysis queries for run-72b96af88a342375.
-- Run each SELECT in the Trino CLI with catalog=water and schema=gold.
-- A water-balance gap is an indicator for investigation, not proof of a leak.

-- Q1. Rank all districts by total water-balance gap and data coverage.
SELECT district_id,
       SUM(total_periods) AS total_periods,
       SUM(complete_periods) AS complete_periods,
       SUM(incomplete_periods) AS incomplete_periods,
       ROUND(100.0 * SUM(complete_periods) / NULLIF(SUM(total_periods), 0), 2) AS coverage_pct,
       ROUND(SUM(inlet_volume_m3), 2) AS inlet_m3,
       ROUND(SUM(metered_consumption_m3), 2) AS consumption_m3,
       ROUND(SUM(gap_m3), 2) AS gap_m3,
       ROUND(100.0 * SUM(gap_m3) / NULLIF(SUM(inlet_volume_m3), 0), 2) AS gap_pct
FROM water.gold.gold_district_daily_water_balance
WHERE run_id = 'run-72b96af88a342375'
GROUP BY district_id
ORDER BY gap_pct DESC;

-- Q2. Show the day-by-day gap trend for D005 and D006.
SELECT district_id, date_utc, complete_periods, incomplete_periods,
       ROUND(inlet_volume_m3, 2) AS inlet_m3,
       ROUND(metered_consumption_m3, 2) AS consumption_m3,
       ROUND(gap_m3, 6) AS gap_m3,
       ROUND(gap_pct, 4) AS gap_pct
FROM water.gold.gold_district_daily_water_balance
WHERE run_id = 'run-72b96af88a342375'
  AND district_id IN ('D005', 'D006')
ORDER BY district_id, date_utc;

-- Q3. Find the five complete 15-minute periods with the highest gap percentage in D005 and D006.
WITH ranked AS (
    SELECT district_id, interval_start_utc, inlet_volume_m3,
           metered_consumption_m3, gap_m3, gap_pct,
           row_number() OVER (PARTITION BY district_id ORDER BY gap_pct DESC) AS rn
    FROM water.gold.fact_water_balance_15m
    WHERE run_id = 'run-72b96af88a342375'
      AND district_id IN ('D005', 'D006')
      AND complete = true
)
SELECT district_id, interval_start_utc,
       ROUND(inlet_volume_m3, 5) AS inlet_m3,
       ROUND(metered_consumption_m3, 5) AS consumption_m3,
       ROUND(gap_m3, 6) AS gap_m3,
       ROUND(gap_pct, 4) AS gap_pct
FROM ranked
WHERE rn <= 5
ORDER BY district_id, gap_pct DESC;

-- Q4. Compare D005 sensor pressure and inlet flow before, during, and after its elevated-gap period.
SELECT CASE
           WHEN CAST(event_time_utc AS DATE) <= DATE '2026-01-10' THEN '2026-01-01 to 2026-01-10 baseline'
           WHEN CAST(event_time_utc AS DATE) <= DATE '2026-01-20' THEN '2026-01-11 to 2026-01-20 elevated-gap period'
           ELSE '2026-01-21 to 2026-01-25 after'
       END AS period,
       COUNT(*) AS sensor_readings,
       COUNT_IF(pressure_m_h2o IS NULL) AS missing_pressure_readings,
       ROUND(AVG(inlet_flow_m3_s), 6) AS avg_inlet_flow_m3_s,
       ROUND(AVG(pressure_m_h2o), 3) AS avg_pressure_m_h2o
FROM water.analytics.district_clean
WHERE run_id = 'run-72b96af88a342375'
  AND district_id = 'D005'
GROUP BY 1
ORDER BY 1;

-- Q5. Check known district topology attributes when interpreting the gaps.
SELECT district_id, inlet_link_id, pressure_monitor_node_id,
       has_cross_boundary_transfer, has_internal_storage
FROM water.gold.dim_district
WHERE run_id = 'run-72b96af88a342375'
  AND district_id IN ('D005', 'D006')
ORDER BY district_id;

-- Q6. Find the single day with the largest total gap in each district.
WITH ranked AS (
    SELECT district_id, date_utc, complete_periods, incomplete_periods,
           inlet_volume_m3, metered_consumption_m3, gap_m3, gap_pct,
           row_number() OVER (PARTITION BY district_id ORDER BY gap_m3 DESC) AS rn
    FROM water.gold.gold_district_daily_water_balance
    WHERE run_id = 'run-72b96af88a342375'
)
SELECT district_id, date_utc, complete_periods, incomplete_periods,
       ROUND(inlet_volume_m3, 2) AS inlet_m3,
       ROUND(metered_consumption_m3, 2) AS consumption_m3,
       ROUND(gap_m3, 3) AS gap_m3,
       ROUND(gap_pct, 3) AS gap_pct
FROM ranked
WHERE rn = 1
ORDER BY gap_m3 DESC;

-- Q7. Drill into D001 on 2026-01-02 to locate the daily gap in 15-minute intervals.
SELECT interval_start_utc,
       ROUND(inlet_volume_m3, 6) AS inlet_m3,
       ROUND(metered_consumption_m3, 6) AS consumption_m3,
       ROUND(gap_m3, 6) AS gap_m3,
       ROUND(gap_pct, 4) AS gap_pct,
       complete
FROM water.gold.fact_water_balance_15m
WHERE run_id = 'run-72b96af88a342375'
  AND district_id = 'D001'
  AND CAST(interval_start_utc AS DATE) = DATE '2026-01-02'
  AND complete = true
ORDER BY gap_m3 DESC
LIMIT 10;

-- Q8. Summarize source-record quality results for the batch run.
SELECT stream, raw_records, accepted_records, rejected_records, duplicate_records
FROM water.analytics.quality_report
WHERE run_id = 'run-72b96af88a342375'
ORDER BY stream;
