"""Exercise actual raw inputs → Spark → Iceberg → Trino before Amr AbdEl-Monem's batch work.

This writes integration evidence only. It deliberately makes no claim to implement
cleaning, facts, dimensions, water balance, or anomaly detection.
"""
from __future__ import annotations

import argparse
import json
import os
import re

def spark_session():
    from pyspark.sql import SparkSession
    return (
        SparkSession.builder.appName("water-handoff-smoke")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.water", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.water.catalog-impl", "org.apache.iceberg.jdbc.JdbcCatalog")
        .config("spark.sql.catalog.water.uri", "jdbc:postgresql://catalog-db:5432/iceberg")
        .config("spark.sql.catalog.water.jdbc.user", "iceberg")
        .config("spark.sql.catalog.water.jdbc.password", os.environ["CATALOG_DB_PASSWORD"])
        .config("spark.sql.catalog.water.jdbc.schema-version", "V1")
        .config("spark.sql.catalog.water.warehouse", f"s3://{os.environ['MINIO_WAREHOUSE_BUCKET']}/")
        .config("spark.sql.catalog.water.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.water.s3.endpoint", f"http://{os.environ['MINIO_ENDPOINT']}")
        .config("spark.sql.catalog.water.s3.path-style-access", "true")
        .config("spark.sql.catalog.water.s3.access-key-id", os.environ["MINIO_ACCESS_KEY"])
        .config("spark.sql.catalog.water.s3.secret-access-key", os.environ["MINIO_SECRET_KEY"])
        .getOrCreate()
    )


def read_json(client, bucket, key):
    response = client.get_object(bucket, key)
    try:
        return json.load(response)
    finally:
        response.close()
        response.release_conn()


def count_records(client, bucket, key, run_id):
    """Bound memory; inspect each actual delivered record instead of trusting the manifest."""
    response = client.get_object(bucket, key)
    pending = b""
    count = 0
    try:
        for chunk in response.stream(1024 * 1024):
            lines = (pending + chunk).split(b"\n")
            pending = lines.pop()
            for line in lines:
                if line.strip():
                    if json.loads(line)["run_id"] != run_id:
                        raise ValueError(f"Foreign run in {key}")
                    count += 1
        if pending.strip():
            if json.loads(pending)["run_id"] != run_id:
                raise ValueError(f"Foreign run in {key}")
            count += 1
        return count
    finally:
        response.close()
        response.release_conn()


def main():
    from minio import Minio
    from pyspark.sql import functions as F
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", required=True)
    args = parser.parse_args()
    rid = args.run_id
    if not re.fullmatch(r"run-[0-9a-f]{16}", rid):
        raise ValueError("Invalid run ID")
    client = Minio(os.environ["MINIO_ENDPOINT"], access_key=os.environ["MINIO_ACCESS_KEY"],
                   secret_key=os.environ["MINIO_SECRET_KEY"], secure=False)
    bucket = os.environ["MINIO_RAW_BUCKET"]
    client.stat_object(bucket, f"{rid}/_RUN_COMPLETE")
    manifest = read_json(client, bucket, f"{rid}/artifacts/run-manifest.json")
    report = read_json(client, bucket, f"{rid}/artifacts/validation-report.json")
    if manifest["run_id"] != rid or manifest["status"] != "complete" or report["status"] != "passed":
        raise ValueError("Raw run did not pass generation validation")
    # Delivery events are read from the Kafka archive (water-events), never from a file copy in water-raw.
    import gzip
    from build_batch import archive_objects
    events_bucket, archived = archive_objects(client, rid)
    counts = []
    for stream in ("district", "meter"):
        count = 0
        for key in archived[stream]:
            response = client.get_object(events_bucket, key)
            try:
                for line in gzip.decompress(response.read()).splitlines():
                    if line.strip():
                        if json.loads(line)["run_id"] != rid:
                            raise ValueError(f"Foreign run in {key}")
                        count += 1
            finally:
                response.close()
                response.release_conn()
        field = f"delivery_{stream}_records"
        if count != manifest[field]:
            raise ValueError(f"{stream}: counted {count} archived events, manifest says {manifest[field]}")
        counts.append(count)

    spark = spark_session()
    try:
        spark.sql("CREATE NAMESPACE IF NOT EXISTS water.handoff")
        spark.sql("""CREATE TABLE IF NOT EXISTS water.handoff.pipeline_runs (
            run_id STRING, district_records BIGINT, meter_records BIGINT
        ) USING iceberg PARTITIONED BY (run_id) TBLPROPERTIES ('format-version'='2')""")
        data = spark.createDataFrame([(rid, *counts)], "run_id string, district_records long, meter_records long")
        # Replace only this run. A task retry does not append duplicate evidence.
        data.writeTo("water.handoff.pipeline_runs").overwrite(F.col("run_id") == F.lit(rid))
        actual = spark.table("water.handoff.pipeline_runs").filter(F.col("run_id") == rid).collect()
        if len(actual) != 1 or (actual[0].district_records, actual[0].meter_records) != tuple(counts):
            raise RuntimeError("Iceberg read-back did not match the written record")
        print(json.dumps({"run_id": rid, "district_records": counts[0], "meter_records": counts[1]}))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
