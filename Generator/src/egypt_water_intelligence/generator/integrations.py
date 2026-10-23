
"""Optional delivery adapters; neither participates in hydraulic simulation."""

from __future__ import annotations

from pathlib import Path

from .replay import TOPICS, replay_to_kafka


def publish_kafka(
    run_dir: str | Path,
    bootstrap_servers: str,
    client_id: str = "water-generator",
    speed: float = 0.0,
    district_topic: str | None = None,
    meter_topic: str | None = None,
    input_set: str = "public",
) -> int:
    """Publish merged events from the selected input set to Kafka."""
    topics = {
        "district-sensors.ndjson": (
            district_topic or TOPICS["district-sensors.ndjson"]
        ),
        "meter-readings.ndjson": (
            meter_topic or TOPICS["meter-readings.ndjson"]
        ),
    }
    return replay_to_kafka(
        run_dir,
        bootstrap_servers,
        speed=speed,
        client_id=client_id,
        topics=topics,
        input_set=input_set,
    )


def archive_minio(
    run_dir: str | Path,
    endpoint: str,
    access_key: str,
    secret_key: str,
    raw_bucket: str,
    restricted_bucket: str,
    secure: bool = False,
) -> int:
    """Archive clean references, delivery events, and restricted truth separately."""
    try:
        from minio import Minio
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError("install the generator with the minio extra") from exc

    root = Path(run_dir)
    client = Minio(
        endpoint,
        access_key=access_key,
        secret_key=secret_key,
        secure=secure,
    )

    count = 0
    for bucket in (raw_bucket, restricted_bucket):
        if not client.bucket_exists(bucket):
            client.make_bucket(bucket)

    approved_public = (
        root / "public" / "districts.ndjson",
        root / "public" / "meters.ndjson",
        root / "public" / "network.inp",
        root / "run-manifest.json",
        root / "validation-report.json",
    )

    for file in approved_public:
        if file.is_file():
            client.fput_object(
                raw_bucket,
                f"{root.name}/artifacts/{file.name}",
                str(file),
            )
            count += 1

    delivery_files = (
        root / "delivery" / "district-sensors.ndjson",
        root / "delivery" / "meter-readings.ndjson",
    )
    for file in delivery_files:
        if file.is_file():
            client.fput_object(
                raw_bucket,
                f"{root.name}/delivery/{file.name}",
                str(file),
            )
            count += 1

    restricted_files = (
        root / "config.yml",
        root / "restricted" / "scenario-truth.ndjson",
        root / "restricted" / "fault-truth.ndjson",
    )

    for file in restricted_files:
        if file.is_file():
            client.fput_object(
                restricted_bucket,
                f"{root.name}/restricted/{file.name}",
                str(file),
            )
            count += 1

    return count
