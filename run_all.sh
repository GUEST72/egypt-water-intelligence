#!/usr/bin/env bash
# Load Flink outputs into Iceberg, build the marts, export CSVs for Power BI.
# Touches nothing of the existing pipeline: it only starts NEW services (iceberg-rest, trino).
set -euo pipefail
cd "$(dirname "$0")/.."
OVR=""; [ -f docker-compose.override.yml ] && OVR="-f docker-compose.override.yml"
F="-f docker-compose.yml $OVR -f iceberg/compose.yml"
dc() { docker compose $F "$@"; }

echo "== 1/5 services (kafka, minio stay as they are)"
dc up -d kafka minio iceberg-rest trino
echo "== 2/5 waiting for Trino"
for i in $(seq 1 40); do dc exec -T trino trino --execute "SELECT 1" >/dev/null 2>&1 && break; sleep 5; done

echo "== 3/5 loading Kafka topics into Iceberg"
dc run --rm iceberg-loader all

echo "== 4/5 building marts in Trino"
dc exec -T trino trino --file /iceberg/marts.sql >/dev/null
dc exec -T trino trino --execute "SHOW TABLES FROM iceberg.flink"

echo "== 5/5 exporting CSVs for Power BI -> iceberg/export/"
mkdir -p iceberg/export
for t in mart_incidents mart_district_trend mart_alert_summary mart_data_quality_daily \
         mart_dead_letter_summary mart_kpi eval_scenario_result dim_district; do
  dc exec -T trino trino --output-format CSV_HEADER --execute "SELECT * FROM iceberg.flink.$t" > iceberg/export/$t.csv
  echo "   $t.csv  ($(($(wc -l < iceberg/export/$t.csv) - 1)) rows)"
done
echo "done."
