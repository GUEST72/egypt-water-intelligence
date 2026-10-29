
import json
import sys
import types
import unittest
import gzip
from collections import namedtuple
from unittest.mock import Mock, patch

from egypt_water_intelligence.generator.archiver import archive_kafka # type: ignore


class FakeS3Error(Exception):
    def __init__(self, code):
        super().__init__(code)
        self.code = code


class FakeMessage:
    def __init__(self, record, offset):
        self.topic = "water.district-sensors.v1"
        self.partition = 0
        self.offset = offset
        self.value = json.dumps(record).encode("utf-8")
        
class FakeMinio:
    def __init__(self, *args, **kwargs):
        self.objects = {}
        self.put_calls = 0

    def bucket_exists(self, bucket):
        return True

    def stat_object(self, bucket, object_name):
        if object_name not in self.objects:
            raise FakeS3Error("NoSuchKey")
        return self.objects[object_name]

    def put_object(
        self,
        bucket,
        object_name,
        data,
        length,
        content_type,
        part_size=None,
    ):
        self.put_calls += 1
        self.objects[object_name] = data.read()        


class FakeConsumer:
    def __init__(self, *args, **kwargs):
        record = {
            "run_id": "idempotency-test",
            "event_id": "event-001",
            "event_type": "district_sensor",
        }
        self.messages = [
            FakeMessage(record, 0),
            FakeMessage(record, 1),
        ]
        self.commits = []
        self.closed = False
        self.delivered = False

    def poll(self, timeout_ms=1000, max_records=None):
        if self.delivered:
            return {}

        self.delivered = True
        return {
            ("water.district-sensors.v1", 0): self.messages
        }

    def commit(self, offsets):
        self.commits.append(offsets)

    def close(self):
        self.closed = True


class TestArchiveIdempotency(unittest.TestCase):
    def test_duplicate_event_is_uploaded_only_once(self):
        consumer = FakeConsumer()
        minio = FakeMinio()

        kafka_module = types.ModuleType("kafka")
        kafka_module.KafkaConsumer = lambda *a, **kw: consumer # type: ignore

        kafka_structs = types.ModuleType("kafka.structs")
        kafka_structs.TopicPartition = namedtuple( # type: ignore
            "TopicPartition", ["topic", "partition"]
        )
        kafka_structs.OffsetAndMetadata = namedtuple( # type: ignore
            "OffsetAndMetadata", ["offset", "metadata"]
        )

        minio_module = types.ModuleType("minio")
        minio_module.Minio = lambda *a, **kw: minio # type: ignore

        minio_error = types.ModuleType("minio.error")
        minio_error.S3Error = FakeS3Error # type: ignore

        fake_modules = {
            "kafka": kafka_module,
            "kafka.structs": kafka_structs,
            "minio": minio_module,
            "minio.error": minio_error,
        }

        with patch.dict(sys.modules, fake_modules):
            archived = archive_kafka(
                bootstrap_servers="fake:9092",
                endpoint="fake:9000",
                access_key="test",
                secret_key="test",
                bucket="water-events",
                group_id="idempotency-test",
                max_messages=2,
            )

        self.assertEqual(archived, 2)
        self.assertEqual(minio.put_calls, 1)
        self.assertEqual(len(minio.objects), 1)
        self.assertEqual(len(consumer.commits), 1)
        self.assertTrue(consumer.closed)

        saved_data = next(iter(minio.objects.values()))
        decompressed = gzip.decompress(saved_data).decode("utf-8")

        saved_records = [
            json.loads(line)
            for line in decompressed.splitlines()
            if line.strip()
        ]

        self.assertEqual(len(saved_records), 2)
        self.assertEqual(saved_records[0]["event_id"], "event-001")
        self.assertEqual(saved_records[1]["event_id"], "event-001")


if __name__ == "__main__":
    unittest.main()
