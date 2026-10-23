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
                NewTopic("water.district-sensors.v1", num_partitions=1, replication_factor=1),
                NewTopic("water.meter-readings.v1", num_partitions=1, replication_factor=1),
            ]
            admin.create_topics([topic for topic in topics if topic.name not in existing])
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
    ):
        if not client.bucket_exists(bucket):
            client.make_bucket(bucket)


if __name__ == "__main__":
    main()
