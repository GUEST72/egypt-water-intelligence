
"""Bounded-memory event replay and Kafka publication."""

from __future__ import annotations

import heapq
import json
import time
from datetime import datetime
from pathlib import Path
from typing import Iterator


TOPICS = {
    "district-sensors.ndjson": "water.district-sensors.v1",
    "meter-readings.ndjson": "water.meter-readings.v1",
}


def _event_time(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def iter_events(
    run_dir: str | Path,
    input_set: str = "public",
) -> Iterator[tuple[str, dict, bytes]]:
    """Merge the selected event files by availability time without loading them all."""
    if input_set not in {"public", "delivery"}:
        raise ValueError("input_set must be 'public' or 'delivery'")

    root = Path(run_dir)
    input_dir = root / input_set
    handles = []
    heap: list[tuple[str, str, int, dict, bytes]] = []

    try:
        for index, filename in enumerate(TOPICS):
            path = input_dir / filename
            if not path.is_file():
                raise FileNotFoundError(
                    f"Missing {input_set} input file: {path}"
                )

            handle = path.open("rb")
            handles.append(handle)
            line = handle.readline()

            if line:
                payload = line.rstrip(b"\r\n")
                record = json.loads(payload)
                heapq.heappush(
                    heap,
                    (
                        record["availability_time_utc"],
                        record["event_id"],
                        index,
                        record,
                        payload,
                    ),
                )

        while heap:
            _, _, index, record, payload = heapq.heappop(heap)
            filename = list(TOPICS)[index]
            yield TOPICS[filename], record, payload

            line = handles[index].readline()
            if line:
                payload = line.rstrip(b"\r\n")
                record = json.loads(payload)
                heapq.heappush(
                    heap,
                    (
                        record["availability_time_utc"],
                        record["event_id"],
                        index,
                        record,
                        payload,
                    ),
                )
    finally:
        for handle in handles:
            handle.close()


def replay_to_kafka(
    run_dir: str | Path,
    bootstrap_servers: str,
    speed: float = 0.0,
    client_id: str = "water-generator",
    topics: dict[str, str] | None = None,
    input_set: str = "public",
) -> int:
    """Publish selected events at accelerated speed or without sleeping.

    ``speed=0`` disables sleeping. A positive speed maps one simulated second
    to ``1 / speed`` wall-clock seconds. Delivery is at-least-once.
    """
    try:
        from kafka import KafkaProducer # type: ignore
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError("install the generator with the kafka extra") from exc

    if speed < 0:
        raise ValueError("replay speed must be non-negative")
    if input_set not in {"public", "delivery"}:
        raise ValueError("input_set must be 'public' or 'delivery'")

    topic_map = topics or TOPICS
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        client_id=client_id,
        acks="all",
        retries=5,
        max_in_flight_requests_per_connection=1,
        value_serializer=None,
        key_serializer=None,
    )

    count = 0
    previous_time: datetime | None = None

    try:
        for default_topic, record, payload in iter_events(
            run_dir, input_set=input_set
        ):
            current_time = _event_time(record["availability_time_utc"])

            if speed > 0 and previous_time is not None:
                delay = (current_time - previous_time).total_seconds() / speed
                if delay > 0:
                    time.sleep(delay)

            previous_time = current_time
            filename = (
                "district-sensors.ndjson"
                if default_topic == TOPICS["district-sensors.ndjson"]
                else "meter-readings.ndjson"
            )
            topic = topic_map.get(filename, default_topic)

            producer.send(
                topic,
                key=record["event_id"].encode("utf-8"),
                value=payload,
            ).get(timeout=30)
            count += 1

        producer.flush(timeout=30)
    finally:
        producer.close()

    return count
