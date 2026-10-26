"""Create the local Compose topics and MinIO buckets."""

from __future__ import annotations

import os
import time

from kafka.admin import KafkaAdminClient, NewTopic
from minio import Minio


def main() -> None:
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    for attempt in range(12):
        try:
            admin = KafkaAdminClient(bootstrap_servers=bootstrap, client_id="water-init")
            existing = set(admin.list_topics())
            topics = [
                NewTopic(name, num_partitions=1, replication_factor=1)
                for name in (
                    "water.district-sensors.v1",
                    "water.meter-readings.v1",
                    "water.district-sensors.clean.v1",
                    "water.dead-letter.district.v1",
                    "water.district-features.15m.v1",
                    "water.district-relative.15m.v1",
                    "water.district-alerts.v1",
                )
            ]
            missing = [topic for topic in topics if topic.name not in existing]
            if missing:
                admin.create_topics(missing)
            admin.close()
            break
        except Exception:
            if attempt == 11:
                raise
            time.sleep(5)
    client = Minio(
        os.environ.get("MINIO_ENDPOINT", "minio:9000"),
        access_key=os.environ["MINIO_ACCESS_KEY"],
        secret_key=os.environ["MINIO_SECRET_KEY"],
        secure=False,
    )
    for bucket in (
        os.environ.get("MINIO_RAW_BUCKET", "water-raw"),
        os.environ.get("MINIO_RESTRICTED_BUCKET", "water-restricted"),
        os.environ.get("MINIO_EVENT_BUCKET", "water-events"),
        os.environ.get("MINIO_WAREHOUSE_BUCKET", "water-warehouse"),
    ):
        if not client.bucket_exists(bucket):
            client.make_bucket(bucket)


if __name__ == "__main__":
    main()
