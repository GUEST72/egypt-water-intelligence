
"""Command-line entry points for local generation, validation, and replay."""

from __future__ import annotations

import argparse
import json
import os

from .archiver import archive_kafka
from .config import load_config
from .integrations import archive_minio, publish_kafka
from .outputs import run_id, write_run
from .replay import iter_events
from .simulation import simulate
from .validation import validate_run, validate_simulation


def _required(value: str | None, name: str) -> str:
    if not value:
        raise RuntimeError(f"{name} must be supplied by argument or environment")
    return value


def _generate(args: argparse.Namespace) -> int:
    config = load_config(args.config)
    result = simulate(config)
    physical = validate_simulation(config, result)

    if physical["status"] != "passed":
        raise RuntimeError(
            "physical validation failed: " + "; ".join(physical["errors"])
        )

    root = write_run(config, result)
    physical = {"run_id": run_id(config), **physical}
    (root / "validation-report.json").write_text(
        json.dumps(physical, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    structural = validate_run(root)
    if structural["status"] != "passed":
        raise RuntimeError(
            "structural validation failed: "
            + "; ".join(structural["errors"][:3])
        )

    print(root)
    return 0


def _validate(args: argparse.Namespace) -> int:
    report = validate_run(args.run_dir)
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if report["status"] == "passed" else 1


def _replay(args: argparse.Namespace) -> int:
    for _, event, _ in iter_events(args.run_dir, input_set=args.input_set):
        print(json.dumps(event, sort_keys=True))
    return 0


def _publish_kafka(args: argparse.Namespace) -> int:
    print(
        publish_kafka(
            args.run_dir,
            _required(args.bootstrap_servers, "Kafka bootstrap servers"),
            speed=args.speed,
            district_topic=args.district_topic,
            meter_topic=args.meter_topic,
            input_set=args.input_set,
        )
    )
    return 0


def _archive_minio(args: argparse.Namespace) -> int:
    print(
        archive_minio(
            args.run_dir,
            _required(args.endpoint, "MinIO endpoint"),
            _required(args.access_key, "MinIO access key"),
            _required(args.secret_key, "MinIO secret key"),
            _required(args.raw_bucket, "raw bucket"),
            _required(args.restricted_bucket, "restricted bucket"),
            args.secure,
        )
    )
    return 0


def _archive_kafka(args: argparse.Namespace) -> int:
    print(
        archive_kafka(
            _required(args.bootstrap_servers, "Kafka bootstrap servers"),
            _required(args.endpoint, "MinIO endpoint"),
            _required(args.access_key, "MinIO access key"),
            _required(args.secret_key, "MinIO secret key"),
            args.bucket,
            args.group_id,
            args.secure,
            args.max_messages,
            args.district_topic,
            args.meter_topic,
        )
    )
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(prog="water-generator")
    commands = parser.add_subparsers(dest="command", required=True)

    generate = commands.add_parser(
        "generate",
        help="simulate, write clean outputs, and create delivery copies",
    )
    generate.add_argument("--config", required=True)
    generate.set_defaults(func=_generate)

    validate = commands.add_parser(
        "validate",
        help="validate a completed local run",
    )
    validate.add_argument("--run-dir", required=True)
    validate.set_defaults(func=_validate)

    replay = commands.add_parser(
        "replay",
        help="write selected events to standard output",
    )
    replay.add_argument("--run-dir", required=True)
    replay.add_argument(
        "--input-set",
        choices=("public", "delivery"),
        default="public",
        help="which event dataset to replay",
    )
    replay.set_defaults(func=_replay)

    kafka = commands.add_parser(
        "publish-kafka",
        help="publish selected event streams to Kafka",
    )
    kafka.add_argument("--run-dir", required=True)
    kafka.add_argument("--bootstrap-servers", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS"))
    kafka.add_argument(
        "--input-set",
        choices=("public", "delivery"),
        default="public",
        help="which event dataset to publish",
    )
    kafka.add_argument(
        "--speed",
        type=float,
        default=0.0,
        help="simulated seconds per wall-clock second; 0 disables sleeping",
    )
    kafka.add_argument(
        "--district-topic",
        default=os.getenv("KAFKA_DISTRICT_TOPIC", "water.district-sensors.v1"),
    )
    kafka.add_argument(
        "--meter-topic",
        default=os.getenv("KAFKA_METER_TOPIC", "water.meter-readings.v1"),
    )
    kafka.set_defaults(func=_publish_kafka)

    minio = commands.add_parser(
        "archive-minio",
        help="archive clean references, delivery events, and restricted files",
    )
    minio.add_argument("--run-dir", required=True)
    minio.add_argument("--endpoint", default=os.getenv("MINIO_ENDPOINT"))
    minio.add_argument("--access-key", default=os.getenv("MINIO_ACCESS_KEY"))
    minio.add_argument("--secret-key", default=os.getenv("MINIO_SECRET_KEY"))
    minio.add_argument("--raw-bucket", default=os.getenv("MINIO_RAW_BUCKET", "water-raw"))
    minio.add_argument(
        "--restricted-bucket",
        default=os.getenv("MINIO_RESTRICTED_BUCKET", "water-restricted"),
    )
    minio.add_argument("--secure", action="store_true")
    minio.set_defaults(func=_archive_minio)

    archiver = commands.add_parser(
        "archive-kafka",
        help="archive Kafka events to MinIO",
    )
    archiver.add_argument("--bootstrap-servers", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS"))
    archiver.add_argument("--endpoint", default=os.getenv("MINIO_ENDPOINT"))
    archiver.add_argument("--access-key", default=os.getenv("MINIO_ACCESS_KEY"))
    archiver.add_argument("--secret-key", default=os.getenv("MINIO_SECRET_KEY"))
    archiver.add_argument("--bucket", default=os.getenv("MINIO_EVENT_BUCKET", "water-events"))
    archiver.add_argument("--group-id", default="water-event-archiver")
    archiver.add_argument("--max-messages", type=int)
    archiver.add_argument(
        "--district-topic",
        default=os.getenv("KAFKA_DISTRICT_TOPIC", "water.district-sensors.v1"),
    )
    archiver.add_argument(
        "--meter-topic",
        default=os.getenv("KAFKA_METER_TOPIC", "water.meter-readings.v1"),
    )
    archiver.add_argument("--secure", action="store_true")
    archiver.set_defaults(func=_archive_kafka)

    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
