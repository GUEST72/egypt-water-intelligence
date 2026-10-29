"""Small Spark fixture tests; no MinIO, catalog, Trino, Kafka or Airflow required."""
import json
import tempfile
import unittest
from pathlib import Path

from pyspark.sql import SparkSession

from build_batch import clean_events, read_events, water_balance
from validate_batch import check_balance

RID = "run-0000000000000001"


class BatchTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.master("local[2]").appName("batch-fixture-tests").config(
            "spark.sql.session.timeZone", "UTC").config("spark.sql.shuffle.partitions", "2").getOrCreate()
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def setUp(self):
        self.folder = tempfile.TemporaryDirectory()
        self.addCleanup(self.folder.cleanup)
        self.districts = self.spark.createDataFrame([(RID, "D001", "D001_INLET", "D001_SERVICE")],
            "run_id string, district_id string, inlet_link_id string, pressure_monitor_node_id string")
        self.meters = self.spark.createDataFrame([(RID, "M000001", "D001")],
            "run_id string, meter_id string, district_id string")

    def event(self, stream="meter", minute=0, **changes):
        start = "2026-01-01T00:%02d:00Z" % minute
        end = "2026-01-01T00:%02d:00Z" % (minute + (15 if stream == "meter" else 1))
        row = dict(schema_version="1.0.0", event_id=stream + str(minute), run_id=RID, district_id="D001",
                   interval_start_utc=start, interval_end_utc=end,
                   event_time_utc=end if stream == "meter" else start,
                   availability_time_utc=end, quality_code="valid")
        if stream == "meter":
            row.update(meter_id="M000001", consumed_volume_m3=10.0)
        else:
            row.update(inlet_link_id="D001_INLET", pressure_monitor_node_id="D001_SERVICE",
                       inlet_flow_m3_s=2.0 / 60, inlet_volume_m3=2.0, pressure_m_h2o=20.0)
        row.update(changes)
        return row

    def clean(self, rows, stream="meter", malformed=False):
        path = Path(self.folder.name) / (stream + ".ndjson")
        path.write_text("\n".join(json.dumps(row) for row in rows) + ("\n{broken" if malformed else "") + "\n")
        raw = read_events(self.spark, path, stream)
        result = clean_events(raw, self.meters if stream == "meter" else self.districts, stream, RID)
        self.addCleanup(result[0].unpersist)
        self.addCleanup(result[3].unpersist)
        return result

    def test_rejections_and_duplicates_reconcile(self):
        row = self.event()
        clean, rejected, counts, _ = self.clean([
            row, row, self.event(event_id="null", consumed_volume_m3=None),
            self.event(event_id="unknown", meter_id="M999999"),
            self.event(event_id="infinity", consumed_volume_m3=float("inf")),
            self.event(event_id="time", interval_end_utc="broken"),
        ], malformed=True)
        self.assertEqual(counts[2:], (7, 1, 5, 1))
        self.assertEqual(clean.first().consumed_volume_m3, 10.0)
        reasons = [r.reason for r in rejected.collect()]
        self.assertTrue(any("malformed_json" in r for r in reasons))
        self.assertTrue(any("reference" in r for r in reasons))

    def test_conflicting_valid_copies_fail(self):
        with self.assertRaisesRegex(ValueError, "Conflicting valid copies"):
            self.clean([self.event(), self.event(consumed_volume_m3=11.0)])
        self.spark.catalog.clearCache()

    def test_foreign_run_fails(self):
        with self.assertRaisesRegex(ValueError, "Foreign run"):
            self.clean([self.event(run_id="run-0000000000000002")])

    def test_balance_uses_aligned_volumes_and_flags_missing_coverage(self):
        district, _, _, _ = self.clean([self.event("district", minute=i) for i in range(15)], "district")
        meter, _, _, _ = self.clean([self.event()])
        start = 1767225600  # 2026-01-01T00:00:00Z
        balance = water_balance(district, meter, self.districts, self.meters, start, start + 1800)
        rows = balance.orderBy("interval_start_utc").collect()
        self.assertEqual((rows[0].inlet_minutes, rows[0].meter_count, rows[0].gap_m3), (15, 1, 20.0))
        self.assertTrue(rows[0].complete)
        self.assertFalse(rows[1].complete)
        self.assertIsNone(rows[1].gap_m3)
        tables = dict(districts=self.districts, meters=self.meters, district_clean=district,
                      meter_clean=meter, water_balance_15m=balance)
        check_balance(tables, {"logical_interval_count": 30})
        # Prove validation catches a plausible but incorrect output calculation.
        from pyspark.sql import functions as F
        tables["water_balance_15m"] = balance.withColumn("gap_m3", F.lit(0.0))
        with self.assertRaisesRegex(ValueError, "balance gap"):
            check_balance(tables, {"logical_interval_count": 30})

    def test_reads_gzip_archive_parts_like_the_kafka_archiver_writes(self):
        import gzip
        folder = Path(self.folder.name) / "archive"
        folder.mkdir()
        rows = [json.dumps(self.event(minute=m * 15, event_id="e%d" % m), sort_keys=True) for m in range(3)]
        # Two parts, the second holding an intentional duplicate delivery of e2.
        (folder / "part-0-0-1.ndjson.gz").write_bytes(gzip.compress(("\n".join(rows[:2]) + "\n").encode()))
        (folder / "part-0-2-3.ndjson.gz").write_bytes(gzip.compress((rows[2] + "\n" + rows[2] + "\n").encode()))
        raw = read_events(self.spark, folder, "meter")
        clean, _, counts, cache = clean_events(raw, self.meters, "meter", RID)
        self.addCleanup(clean.unpersist)
        self.addCleanup(cache.unpersist)
        self.assertEqual(counts[2:], (4, 3, 0, 1))

    def test_dashboard_gold_tables(self):
        from build_batch import consumption_by_class, data_quality_daily
        from pyspark.sql import functions as F
        # 2026-01-01 is a Thursday; 22:00 UTC is Friday 00:00 in Cairo (UTC+2 in January).
        balance = self.spark.createDataFrame(
            [(RID, "D001", "2026-01-01 22:00:00", "2026-01-01 22:15:00")],
            "run_id string, district_id string, interval_start_utc string, interval_end_utc string",
        ).select("run_id", "district_id", F.to_timestamp("interval_start_utc").alias("interval_start_utc"),
                 F.to_timestamp("interval_end_utc").alias("interval_end_utc"))
        local = F.from_utc_timestamp("interval_start_utc", "Africa/Cairo")
        row = balance.select(F.to_date(local).alias("d"), F.dayofweek(local).isin(6, 7).alias("weekend")).first()
        self.assertEqual((str(row.d), row.weekend), ("2026-01-02", True))

        meters = self.meters.withColumn("customer_class", F.lit("residential"))
        meter, rejected, _, _ = self.clean([self.event(), self.event(event_id="bad", consumed_volume_m3=None)])
        fact = meter.select("run_id", "meter_id", "district_id", "event_id", "interval_start_utc", "consumed_volume_m3")
        dim_interval = fact.select("run_id", F.to_date("interval_start_utc").alias("date_utc")).distinct()
        by_class = consumption_by_class(fact, meters, dim_interval).first()
        self.assertEqual((by_class.readings, by_class.consumed_volume_m3, by_class.expected_readings), (1, 10.0, 1))

        district, _, _, _ = self.clean([self.event("district", minute=0)], "district")
        dq = {(r.stream, r.outcome): r.records for r in data_quality_daily(district, meter, rejected).collect()}
        self.assertEqual(dq[("meter", "accepted")], 1)
        self.assertEqual(dq[("district", "accepted")], 1)
        self.assertEqual(sum(v for (s, o), v in dq.items() if o.startswith("rejected:")), 1)

    def test_null_pressure_rejects_minute_and_leaves_gap_null(self):
        rows = [self.event("district", minute=i) for i in range(15)]
        rows[0]["pressure_m_h2o"] = None
        district, _, counts, _ = self.clean(rows, "district")
        meter, _, _, _ = self.clean([self.event()])
        start = 1767225600
        row = water_balance(district, meter, self.districts, self.meters, start, start + 900).first()
        self.assertEqual(counts[2:], (15, 14, 1, 0))
        self.assertFalse(row.complete)
        self.assertIsNone(row.gap_m3)


if __name__ == "__main__":
    unittest.main()
