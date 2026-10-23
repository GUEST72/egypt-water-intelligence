
"""At-least-once Kafka-to-MinIO batched event archiver."""

from __future__ import annotations

import gzip
import io
import json
import time
from dataclasses import dataclass
from typing import Any


@dataclass
class _Batch:
    stream: io.BytesIO
    compressor: gzip.GzipFile
    first_offset: int
    last_offset: int
    count: int = 0
    raw_bytes: int = 0


def archive_kafka(
    bootstrap_servers: str,
    endpoint: str,
    access_key: str,
    secret_key: str,
    bucket: str,
    group_id: str = "water-event-archiver",
    secure: bool = False,
    max_messages: int | None = None,
    district_topic: str = "water.district-sensors.v1",
    meter_topic: str = "water.meter-readings.v1",
    max_batch_messages: int = 10_000,
    max_batch_bytes: int = 32 * 1024 * 1024,
    flush_interval_seconds: int = 30,
) -> int:
    """Archive bounded batches and commit offsets after successful uploads."""
    try:
        from kafka import KafkaConsumer
        from kafka.structs import OffsetAndMetadata, TopicPartition
        from minio import Minio
        from minio.error import S3Error
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError(
            "install the generator with the kafka and minio extras"
        ) from exc

    if max_batch_messages < 1 or max_batch_bytes < 1:
        raise ValueError("Batch limits must be positive")
    if flush_interval_seconds < 1:
        raise ValueError("Flush interval must be positive")

    client = Minio(
        endpoint,
        access_key=access_key,
        secret_key=secret_key,
        secure=secure,
    )
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)

    consumer = KafkaConsumer(
        district_topic,
        meter_topic,
        bootstrap_servers=bootstrap_servers,
        group_id=group_id,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
    )

    # Each buffer contains one topic, run and Kafka partition.
    buffers: dict[tuple[str, str, int], _Batch] = {}
    buffered_bytes = 0
    archived = 0
    last_flush = time.monotonic()

    def flush_batches() -> None:
        nonlocal buffered_bytes, last_flush

        if not buffers:
            return

        offsets: dict[Any, Any] = {}

        # Finish and upload every object before committing any offsets.
        for (topic, run_id, partition), batch in buffers.items():
            batch.compressor.close()
            length = batch.stream.tell()
            batch.stream.seek(0)

            object_name = (
                f"events/{topic}/{run_id}/"
                f"part-{partition}-{batch.first_offset}-"
                f"{batch.last_offset}.ndjson.gz"
            )

            try:
                client.stat_object(bucket, object_name)
            except S3Error as exc:
                if exc.code not in {
                    "NoSuchKey",
                    "NoSuchObject",
                    "NotFound",
                }:
                    raise

                client.put_object(
                    bucket,
                    object_name,
                    batch.stream,
                    length=length,
                    content_type="application/gzip",
                )

            tp = TopicPartition(topic, partition)
            next_offset = batch.last_offset + 1

            if (
                tp not in offsets
                or next_offset > offsets[tp].offset
            ):
                offsets[tp] = OffsetAndMetadata(next_offset, "")

        # Commit only after every object in this flush is present in MinIO.
        consumer.commit(offsets)

        for batch in buffers.values():
            batch.stream.close()

        buffers.clear()
        buffered_bytes = 0
        last_flush = time.monotonic()

    try:
        stop = False

        while not stop:
            polled = consumer.poll(timeout_ms=1000, max_records=100)

            for tp, messages in polled.items():
                for message in messages:
                    record: dict[str, Any] = json.loads(message.value)
                    run_id = str(record["run_id"])

                    line = (
                        json.dumps(
                            record,
                            sort_keys=True,
                            separators=(",", ":"),
                        ).encode("utf-8")
                        + b"\n"
                    )

                    key = (message.topic, run_id, message.partition)
                    batch = buffers.get(key)

                    # Flush before adding a record that would exceed the
                    # global buffer limit, unless the buffer is empty.
                    if (
                        buffers
                        and buffered_bytes + len(line) > max_batch_bytes
                    ):
                        flush_batches()
                        batch = None

                    if batch is None:
                        stream = io.BytesIO()
                        batch = _Batch(
                            stream=stream,
                            compressor=gzip.GzipFile(
                                fileobj=stream,
                                mode="wb",
                                mtime=0,
                            ),
                            first_offset=message.offset,
                            last_offset=message.offset,
                        )
                        buffers[key] = batch

                    batch.compressor.write(line)
                    batch.last_offset = message.offset
                    batch.count += 1
                    batch.raw_bytes += len(line)
                    buffered_bytes += len(line)
                    archived += 1

                    if (
                        max_messages is not None
                        and archived >= max_messages
                    ):
                        stop = True
                        break

                if stop:
                    break

            should_flush = (
                buffered_bytes >= max_batch_bytes
                or any(
                    batch.count >= max_batch_messages
                    for batch in buffers.values()
                )
                or (
                    buffers
                    and time.monotonic() - last_flush
                    >= flush_interval_seconds
                )
                or stop
            )

            if should_flush:
                flush_batches()

    except KeyboardInterrupt:
        # Preserve already-consumed events when stopping normally with Ctrl+C.
        flush_batches()
    finally:
        consumer.close()

    return archived
