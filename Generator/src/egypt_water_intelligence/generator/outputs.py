
"""Local raw-output writers, restricted truth, and deterministic replay inputs."""

from __future__ import annotations

import hashlib
import heapq
import json
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable

import numpy as np
import wntr
import yaml

from .config import RunConfig
from .simulation import SECONDS_PER_MINUTE, SimulationResult, district_id


_SORT_CHUNK_SIZE = 50_000

_ALLOWED_NULL_FIELDS = {
    "meter": {"consumed_volume_m3"},
    "district": {"pressure_m_h2o"},
}


def _canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def run_id(config: RunConfig) -> str:
    return (
        "run-"
        + hashlib.sha256(_canonical_json(config.raw).encode("utf-8")).hexdigest()[:16]
    )


def _write_jsonl(path: Path, rows: Iterable[dict[str, Any]]) -> int:
    count = 0
    with path.open("w", encoding="utf-8", newline="\n") as handle:
        for row in rows:
            handle.write(_canonical_json(row) + "\n")
            count += 1
    return count


def _count_lines(path: Path) -> int:
    with path.open("rb") as handle:
        return sum(1 for _ in handle)


def _iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _time_range(start: datetime, minute: int, width_minutes: int) -> tuple[str, str]:
    lower = start + timedelta(minutes=minute)
    return _iso(lower), _iso(lower + timedelta(minutes=width_minutes))


def _availability(interval_end: str) -> str:
    # The base mode has no artificial delivery delay.
    return interval_end


def _district_references(config: RunConfig) -> list[dict[str, Any]]:
    return [
        {
            "schema_version": "1.0.0",
            "district_id": district_id(i),
            "inlet_link_id": f"{district_id(i)}_INLET",
            "pressure_monitor_node_id": f"{district_id(i)}_SERVICE",
            "service_junction_ids": [f"{district_id(i)}_SERVICE"],
            "has_cross_boundary_transfer": False,
            "has_internal_storage": False,
        }
        for i in range(1, config.district_count + 1)
    ]


def _meter_events(
    config: RunConfig, result: SimulationResult, rid: str
) -> Iterable[dict[str, Any]]:
    start = datetime.fromisoformat(
        config.raw["run"]["start_time_utc"].replace("Z", "+00:00")
    )
    noise = float(config.raw["sensor"].get("noise_relative_stddev", 0.0))
    rng = np.random.default_rng(config.seed + 501)

    for meter_index, meter in enumerate(result.meters):
        for minute in range(0, config.duration_minutes, 15):
            interval_start, interval_end = _time_range(start, minute, 15)
            volume = float(
                result.delivered_m3_s[meter_index, minute : minute + 15].sum()
                * SECONDS_PER_MINUTE
            )
            quality = "valid"
            if noise:
                volume = max(0.0, volume * (1.0 + rng.normal(0.0, noise)))
                quality = "noisy"

            yield {
                "schema_version": "1.0.0",
                "event_id": f"{rid}:meter:{meter['meter_id']}:{minute:06d}",
                "run_id": rid,
                "meter_id": meter["meter_id"],
                "district_id": meter["district_id"],
                "interval_start_utc": interval_start,
                "interval_end_utc": interval_end,
                "event_time_utc": interval_end,
                "consumed_volume_m3": volume,
                "availability_time_utc": _availability(interval_end),
                "quality_code": quality,
            }


def _district_events(
    config: RunConfig, result: SimulationResult, rid: str
) -> Iterable[dict[str, Any]]:
    start = datetime.fromisoformat(
        config.raw["run"]["start_time_utc"].replace("Z", "+00:00")
    )
    noise = float(config.raw["sensor"].get("noise_relative_stddev", 0.0))
    rng = np.random.default_rng(config.seed + 502)

    for number in range(1, config.district_count + 1):
        did = district_id(number)
        for minute in range(config.duration_minutes):
            interval_start, interval_end = _time_range(start, minute, 1)
            flow = float(result.inlet_flow_m3_s[did][minute])
            pressure = float(result.pressure_m_h2o[did][minute])
            quality = "valid"

            if noise:
                flow = max(0.0, flow * (1.0 + rng.normal(0.0, noise)))
                pressure = max(0.0, pressure * (1.0 + rng.normal(0.0, noise)))
                quality = "noisy"

            yield {
                "schema_version": "1.0.0",
                "event_id": f"{rid}:district:{did}:{minute:06d}",
                "run_id": rid,
                "district_id": did,
                "inlet_link_id": f"{did}_INLET",
                "pressure_monitor_node_id": f"{did}_SERVICE",
                "interval_start_utc": interval_start,
                "interval_end_utc": interval_end,
                "event_time_utc": interval_start,
                "inlet_flow_m3_s": flow,
                "inlet_volume_m3": flow * SECONDS_PER_MINUTE,
                "pressure_m_h2o": pressure,
                "availability_time_utc": _availability(interval_end),
                "quality_code": quality,
            }


def _sort_ndjson(
    source: Path,
    destination: Path,
    chunk_size: int = _SORT_CHUNK_SIZE,
) -> int:
    """Sort NDJSON by availability time with bounded memory."""
    if chunk_size < 1:
        raise ValueError("chunk_size must be positive")

    destination.parent.mkdir(parents=True, exist_ok=True)
    total = 0

    with tempfile.TemporaryDirectory(
        prefix="water-sort-", dir=destination.parent
    ) as temp_name:
        temp_dir = Path(temp_name)
        chunk_paths: list[Path] = []
        chunk: list[dict[str, Any]] = []

        def flush_chunk() -> None:
            if not chunk:
                return
            chunk.sort(
                key=lambda row: (
                    row["availability_time_utc"],
                    row["event_id"],
                )
            )
            chunk_path = temp_dir / f"chunk-{len(chunk_paths):05d}.ndjson"
            _write_jsonl(chunk_path, chunk)
            chunk_paths.append(chunk_path)
            chunk.clear()

        with source.open("r", encoding="utf-8") as handle:
            for line in handle:
                if not line.strip():
                    continue
                chunk.append(json.loads(line))
                if len(chunk) >= chunk_size:
                    flush_chunk()
            flush_chunk()

        handles = []
        heap: list[tuple[str, str, int, str]] = []

        try:
            for index, chunk_path in enumerate(chunk_paths):
                handle = chunk_path.open("r", encoding="utf-8")
                handles.append(handle)
                line = handle.readline()
                if line:
                    record = json.loads(line)
                    heapq.heappush(
                        heap,
                        (
                            record["availability_time_utc"],
                            record["event_id"],
                            index,
                            line,
                        ),
                    )

            with destination.open("w", encoding="utf-8", newline="\n") as output:
                while heap:
                    _, _, index, line = heapq.heappop(heap)
                    output.write(line.rstrip("\r\n") + "\n")
                    total += 1

                    next_line = handles[index].readline()
                    if next_line:
                        record = json.loads(next_line)
                        heapq.heappush(
                            heap,
                            (
                                record["availability_time_utc"],
                                record["event_id"],
                                index,
                                next_line,
                            ),
                        )
        finally:
            for handle in handles:
                handle.close()

    return total



def _faulted_delivery(
    config: RunConfig, source: Path, destination: Path, stream: str
) -> list[dict[str, Any]]:
    """Create a sorted delivery copy; leave the clean source untouched."""
    if stream not in _ALLOWED_NULL_FIELDS:
        raise ValueError(f"Unsupported stream: {stream}")

    rules = [
        rule
        for rule in config.raw.get("faults", [])
        if rule.get("stream") == stream
    ]
    allowed_fields = _ALLOWED_NULL_FIELDS[stream]
    audit: list[dict[str, Any]] = []

    for rule in rules:
        fault_type = rule.get("type")
        if fault_type not in {
            "null_value",
            "null_measurement",
            "duplicate",
        }:
            raise ValueError(
                f"Unsupported fault type {fault_type!r}; "
                "supported types are null_value, null_measurement, duplicate"
            )

        every_n = rule.get("every_n")
        if (
            not isinstance(every_n, int)
            or isinstance(every_n, bool)
            or every_n < 1
        ):
            raise ValueError(
                "Each fault rule requires a positive integer every_n"
            )

        if fault_type in {"null_value", "null_measurement"}:
            field = rule.get("field")
            if field not in allowed_fields:
                raise ValueError(
                    f"Field {field!r} cannot be nulled for stream {stream!r}; "
                    f"allowed fields: {sorted(allowed_fields)}"
                )

    destination.parent.mkdir(parents=True, exist_ok=True)
    unsorted_path: Path | None = None

    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            newline="\n",
            prefix="water-delivery-",
            suffix=".ndjson",
            dir=destination.parent,
            delete=False,
        ) as temp_handle:
            unsorted_path = Path(temp_handle.name)

            with source.open("r", encoding="utf-8") as input_handle:
                for number, line in enumerate(input_handle, start=1):
                    if not line.strip():
                        continue

                    original = json.loads(line)
                    row = dict(original)
                    duplicate_count = 0

                    for rule in rules:
                        if number % rule["every_n"] != 0:
                            continue

                        fault_type = rule["type"]

                        if fault_type in {"null_value", "null_measurement"}:
                            field = rule["field"]
                            row[field] = None
                            row["quality_code"] = "corrupted"

                            audit.append(
                                {
                                    "schema_version": "1.0.0",
                                    "stream": stream,
                                    "event_id": original["event_id"],
                                    "fault_type": "null_value",
                                    "affected_fields": [field],
                                    "expected_result": "null_measurement_flagged",
                                }
                            )

                        elif fault_type == "duplicate":
                            duplicate_count += 1
                            audit.append(
                                {
                                    "schema_version": "1.0.0",
                                    "stream": stream,
                                    "event_id": original["event_id"],
                                    "fault_type": "duplicate",
                                    "affected_fields": [],
                                    "expected_result": "duplicate_event_id",
                                }
                            )

                    serialized_row = _canonical_json(row)
                    temp_handle.write(serialized_row + "\n")

                    for _ in range(duplicate_count):
                        temp_handle.write(serialized_row + "\n")

        _sort_ndjson(unsorted_path, destination)

    finally:
        if unsorted_path is not None:
            unsorted_path.unlink(missing_ok=True)

    return audit



def write_run(config: RunConfig, result: SimulationResult) -> Path:
    """Write clean raw outputs and separate faulted delivery copies."""
    rid = run_id(config)
    root = Path(config.raw["run"]["output_root"]) / rid
    public = root / "public"
    restricted = root / "restricted"
    delivery = root / "delivery"

    for directory in (public, restricted, delivery):
        directory.mkdir(parents=True, exist_ok=True)

    with (root / "config.yml").open("w", encoding="utf-8", newline="\n") as handle:
        yaml.safe_dump(config.raw, handle, sort_keys=True)

    wntr.network.write_inpfile(
        result.network, str(public / "network.inp"), units="LPS", version=2.2
    )
    _write_jsonl(public / "districts.ndjson", _district_references(config))
    _write_jsonl(public / "meters.ndjson", result.meters)

    district_count = _write_jsonl(
        public / "district-sensors.ndjson", _district_events(config, result, rid)
    )
    meter_count = _write_jsonl(
        public / "meter-readings.ndjson", _meter_events(config, result, rid)
    )

    truth = [{**row, "run_id": rid} for row in result.scenario_truth]
    _write_jsonl(restricted / "scenario-truth.ndjson", truth)

    audit = _faulted_delivery(
        config,
        public / "meter-readings.ndjson",
        delivery / "meter-readings.ndjson",
        "meter",
    )
    audit += _faulted_delivery(
        config,
        public / "district-sensors.ndjson",
        delivery / "district-sensors.ndjson",
        "district",
    )
    _write_jsonl(
        restricted / "fault-truth.ndjson",
        [{**row, "run_id": rid} for row in audit],
    )

    manifest = {
        "schema_version": "1.0.0",
        "run_id": rid,
        "engine": "EPANET 2.2 via WNTR",
        "logical_interval_count": config.duration_minutes,
        "expected_internal_hydraulic_snapshots": config.duration_minutes + 1,
        "expected_district_records": config.district_count * config.duration_minutes,
        "actual_district_records": district_count,
        "expected_meter_records": config.meter_count * (config.duration_minutes // 15),
        "actual_meter_records": meter_count,
        "delivery_district_records": _count_lines(
            delivery / "district-sensors.ndjson"
        ),
        "delivery_meter_records": _count_lines(
            delivery / "meter-readings.ndjson"
        ),
        "scenario_count": len(truth),
        "fault_count": len(audit),
        "public_root": "public",
        "delivery_root": "delivery",
        "restricted_root": "restricted",
        "status": "complete",
    }
    (root / "run-manifest.json").write_text(
        _canonical_json(manifest) + "\n", encoding="utf-8"
    )
    (root / "_RUN_COMPLETE").write_text("complete\n", encoding="utf-8")
    return root
