"""Pure checks used by the assets. No network or Docker access, so they are unit-tested."""
from __future__ import annotations

import json
import re
from pathlib import Path

RUN_ID_PATTERN = re.compile(r"run-[0-9a-f]{16}")

# Objects that make a raw run complete in MinIO. _RUN_COMPLETE is written after all of them.
# Event data is deliberately absent: delivery events reach MinIO only through Kafka and the archiver
# (water-events), never as a direct file copy, so batch processing cannot bypass the streaming path.
RAW_OBJECTS = (
    "artifacts/run-manifest.json",
    "artifacts/validation-report.json",
    "artifacts/districts.ndjson",
    "artifacts/meters.ndjson",
    "artifacts/network.inp",
)
RESTRICTED_OBJECTS = (
    "restricted/config.yml",
    "restricted/scenario-truth.ndjson",
    "restricted/fault-truth.ndjson",
)
# Local source file for each uploaded object (paths relative to the run directory).
LOCAL_SOURCES = {
    "artifacts/run-manifest.json": "run-manifest.json",
    "artifacts/validation-report.json": "validation-report.json",
    "artifacts/districts.ndjson": "public/districts.ndjson",
    "artifacts/meters.ndjson": "public/meters.ndjson",
    "artifacts/network.inp": "public/network.inp",
    "restricted/config.yml": "config.yml",
    "restricted/scenario-truth.ndjson": "restricted/scenario-truth.ndjson",
    "restricted/fault-truth.ndjson": "restricted/fault-truth.ndjson",
}


class CheckFailed(Exception):
    """A data or contract check failed; the message explains what to do."""


def require_run_id(run_id: str) -> str:
    if not RUN_ID_PATTERN.fullmatch(run_id):
        raise CheckFailed(f"{run_id!r} is not a generator run ID (run- followed by 16 hex digits)")
    return run_id


def read_manifest(run_dir: Path) -> dict:
    """Load and check the manifest written by the current generator (outputs.write_run)."""
    if not (run_dir / "_RUN_COMPLETE").is_file():
        raise CheckFailed(f"{run_dir.name}: _RUN_COMPLETE is missing, generation did not finish")
    manifest = json.loads((run_dir / "run-manifest.json").read_text(encoding="utf-8"))
    required = ("run_id", "status", "logical_interval_count",
                "delivery_district_records", "delivery_meter_records")
    missing = [key for key in required if key not in manifest]
    if missing:
        raise CheckFailed(
            f"{run_dir.name}: manifest lacks {missing}. It was produced by an older generator; "
            "regenerate the run with the current Generator/ code."
        )
    if manifest["run_id"] != run_dir.name or manifest["status"] != "complete":
        raise CheckFailed(f"{run_dir.name}: manifest run_id/status do not describe this completed run")
    return manifest


def files_signature(run_dir: Path) -> str:
    """Size and modification time of every file in the run; changes if any file is rewritten."""
    import hashlib
    parts = sorted(f"{p.relative_to(run_dir).as_posix()}:{p.stat().st_size}:{p.stat().st_mtime_ns}"
                   for p in run_dir.rglob("*") if p.is_file())
    return hashlib.sha256("\n".join(parts).encode()).hexdigest()


def count_lines(path: Path, chunk_size: int = 8 * 1024 * 1024) -> int:
    """Count newline-terminated records without loading the file."""
    count = 0
    with path.open("rb") as handle:
        while chunk := handle.read(chunk_size):
            count += chunk.count(b"\n")
    return count


def check_delivery_counts(run_dir: Path, manifest: dict) -> dict[str, int]:
    counts = {
        "district": count_lines(run_dir / "delivery" / "district-sensors.ndjson"),
        "meter": count_lines(run_dir / "delivery" / "meter-readings.ndjson"),
    }
    for stream, actual in counts.items():
        expected = manifest[f"delivery_{stream}_records"]
        if actual != expected:
            raise CheckFailed(f"delivery {stream} file has {actual} records, manifest says {expected}")
    return counts


def compare_uploaded(local_sizes: dict[str, int], remote_sizes: dict[str, int | None]) -> list[str]:
    """Return problems for objects that are missing in MinIO or differ in size from the local file."""
    problems = []
    for key, size in local_sizes.items():
        remote = remote_sizes.get(key)
        if remote is None:
            problems.append(f"{key}: missing in MinIO")
        elif remote != size:
            problems.append(f"{key}: MinIO size {remote} != local size {size}")
    return problems
