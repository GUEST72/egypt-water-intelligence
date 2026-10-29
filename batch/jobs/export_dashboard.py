"""Export one run's dashboard tables from Iceberg to Parquet for Power BI (dashboards/data/<table>/).

Power BI Desktop cannot query the local Trino reliably, so the analytical dashboard imports these files.
They are produced only from committed Iceberg tables (marts, Gold, streaming facts, evaluation) and are
replaced on every export; refreshing Power BI after an export shows the current data.
"""
import argparse
import json
import re

from pyspark.sql import functions as F

from handoff_smoke import spark_session

TABLES = {
    "overview_daily": "water.marts.mart_district_daily_overview",
    "incidents": "water.marts.mart_incidents",
    "incident_balance": "water.marts.mart_incident_balance",
    "hourly_profile": "water.marts.mart_district_hourly_profile",
    "consumption_by_class": "water.gold.gold_district_daily_consumption_by_class",
    "data_quality_daily": "water.gold.gold_data_quality_daily",
    "districts": "water.gold.dim_district",
    "district_windows": "water.streaming.fact_district_window_15m",
    "evaluation_scenarios": "water.evaluation.eval_scenario_result",
    "evaluation_summary": "water.evaluation.eval_summary",
}
OUT = "/opt/spark/dashboard-data"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", required=True)
    args = parser.parse_args()
    if not re.fullmatch(r"run-[0-9a-f]{16}", args.run_id):
        parser.error("--run-id must be run- followed by 16 lowercase hex digits")
    spark = spark_session()
    spark.sparkContext.setLogLevel("WARN")
    # Standard Parquet timestamps (not legacy INT96) so Power BI reads them as date/time.
    spark.conf.set("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
    report = {"run_id": args.run_id, "status": "completed", "tables": {}}
    try:
        for name, table in TABLES.items():
            frame = spark.table(table).filter(F.col("run_id") == args.run_id)
            # service_junction_ids is an array (not needed in the dashboard and awkward in Power Query).
            frame = frame.drop(*[c for c, t in frame.dtypes if t.startswith("array")])
            frame.coalesce(1).write.mode("overwrite").parquet(f"{OUT}/{name}")
            report["tables"][name] = frame.count()
    finally:
        spark.stop()
    print(json.dumps(report, sort_keys=True))


if __name__ == "__main__":
    main()
