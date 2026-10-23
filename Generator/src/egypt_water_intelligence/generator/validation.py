"""Structural, temporal, physical, and truth-isolation validation."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

import numpy as np
from jsonschema import Draft202012Validator, FormatChecker

from .config import RunConfig
from .simulation import SECONDS_PER_MINUTE, SimulationResult


def _schema(name: str) -> Draft202012Validator:
    candidates = [Path(__file__).resolve().parents[3], Path.cwd()]
    configured_root = os.getenv("WATER_GENERATOR_ROOT")
    if configured_root:
        candidates.insert(0, Path(configured_root))
    for root in candidates:
        schema_path = root / "schemas" / name
        if schema_path.is_file():
            raw = json.loads(schema_path.read_text(encoding="utf-8"))
            return Draft202012Validator(raw, format_checker=FormatChecker())
    searched = ", ".join(str(root / "schemas" / name) for root in candidates)
    raise FileNotFoundError(f"schema {name!r} was not found; searched: {searched}")


def validate_simulation(config: RunConfig, result: SimulationResult) -> dict[str, Any]:
    """Validate clean hydraulic truth before measurement faults are applied."""
    errors: list[str] = []
    warnings: list[str] = []
    scenario_by_id = {
        scenario["scenario_id"]: scenario for scenario in result.scenario_truth
    }
    max_residual_m3 = 0.0
    worst_location: dict[str, Any] | None = None
    for did, inlet in result.inlet_flow_m3_s.items():
        meter_indices = [
            index
            for index, meter in enumerate(result.meters)
            if meter["district_id"] == did
        ]
        customer = result.delivered_m3_s[meter_indices].sum(axis=0)
        leak = np.zeros(config.duration_minutes)
        for scenario_id, leak_flow in result.leak_flow_m3_s.items():
            if scenario_by_id[scenario_id]["district_id"] == did:
                leak += leak_flow
        residual = (inlet - customer - leak) * SECONDS_PER_MINUTE
        local_index = int(np.argmax(np.abs(residual)))
        local_max = float(abs(residual[local_index]))
        if local_max > max_residual_m3:
            max_residual_m3 = local_max
            worst_location = {"district_id": did, "minute_index": local_index}
    tolerance = float(config.raw["hydraulics"]["balance_tolerance_m3"])
    if max_residual_m3 > tolerance:
        errors.append(
            f"maximum minute balance residual {max_residual_m3} exceeds {tolerance}"
        )
    for scenario in config.raw["scenarios"]:
        if scenario["type"] not in {
            "burst",
            "gradual_leak",
            "persistent_background_loss",
        }:
            continue
        flow = result.leak_flow_m3_s[scenario["scenario_id"]]
        start, end = scenario["start_minute"], scenario["end_minute"]
        active = flow[start:end]
        inactive = np.concatenate((flow[:start], flow[end:]))
        if float(np.mean(active)) <= 1e-7:
            errors.append(
                f"leak branch did not produce material loss: {scenario['scenario_id']}"
            )
        if inactive.size and float(np.max(np.abs(inactive))) > max(
            1e-7, float(np.mean(active)) * 0.01
        ):
            errors.append(f"leak branch did not close: {scenario['scenario_id']}")
    if config.raw["sensor"].get("noise_relative_stddev", 0.0):
        warnings.append(
            "public measurements include configured sensor noise; exact balance applies to internal truth"
        )
    return {
        "status": "failed" if errors else "passed",
        "errors": errors,
        "warnings": warnings,
        "max_minute_balance_residual_m3": max_residual_m3,
        "worst_balance_location": worst_location,
        "balance_tolerance_m3": tolerance,
        "internal_hydraulic_snapshots": config.duration_minutes + 1,
        "public_logical_intervals": config.duration_minutes,
    }


def _validate_ndjson(
    path: Path, validator: Draft202012Validator, required: set[str]
) -> tuple[int, list[str]]:
    count, errors = 0, []
    with path.open("r", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            count += 1
            try:
                record = json.loads(line)
            except json.JSONDecodeError as exc:
                errors.append(f"{path.name}:{line_number}: invalid JSON: {exc.msg}")
                continue
            for error in validator.iter_errors(record):
                errors.append(f"{path.name}:{line_number}: {error.message}")
            missing = required - set(record)
            unexpected = set(record) - required
            if missing or unexpected:
                errors.append(f"{path.name}:{line_number}: fields differ from contract")
            if record.get("schema_version") != "1.0.0":
                errors.append(f"{path.name}:{line_number}: unsupported schema version")
    return count, errors


def validate_run(run_dir: str | Path) -> dict[str, Any]:
    root = Path(run_dir)
    if not (root / "_RUN_COMPLETE").is_file():
        return {
            "status": "failed",
            "errors": ["run completion marker is missing"],
            "district_records": 0,
            "meter_records": 0,
        }
    manifest = json.loads((root / "run-manifest.json").read_text(encoding="utf-8"))
    public = root / "public"
    district_required = {
        "schema_version",
        "event_id",
        "run_id",
        "district_id",
        "inlet_link_id",
        "pressure_monitor_node_id",
        "interval_start_utc",
        "interval_end_utc",
        "event_time_utc",
        "inlet_flow_m3_s",
        "inlet_volume_m3",
        "pressure_m_h2o",
        "availability_time_utc",
        "quality_code",
    }
    meter_required = {
        "schema_version",
        "event_id",
        "run_id",
        "meter_id",
        "district_id",
        "interval_start_utc",
        "interval_end_utc",
        "event_time_utc",
        "consumed_volume_m3",
        "availability_time_utc",
        "quality_code",
    }
    truth_required = {
        "schema_version",
        "run_id",
        "scenario_id",
        "scenario_type",
        "scenario_class",
        "district_id",
        "start_time_utc",
        "end_time_utc",
        "physical_parameters",
        "expected_effects",
    }
    district_count, district_errors = _validate_ndjson(
        public / "district-sensors.ndjson",
        _schema("district-sensor.v1.json"),
        district_required,
    )
    meter_count, meter_errors = _validate_ndjson(
        public / "meter-readings.ndjson",
        _schema("meter-reading.v1.json"),
        meter_required,
    )
    _, truth_errors = _validate_ndjson(
        root / "restricted" / "scenario-truth.ndjson",
        _schema("scenario-truth.v1.json"),
        truth_required,
    )
    errors = district_errors + meter_errors + truth_errors
    if district_count != manifest["expected_district_records"]:
        errors.append("district public count differs from manifest expectation")
    if meter_count != manifest["expected_meter_records"]:
        errors.append("meter public count differs from manifest expectation")
    district_ids: set[str] = set()
    meter_ids: set[str] = set()
    for line in (public / "districts.ndjson").read_text(encoding="utf-8").splitlines():
        record = json.loads(line)
        district = record.get("district_id")
        if district in district_ids:
            errors.append(f"duplicate district reference: {district}")
        district_ids.add(district)
    for line in (public / "meters.ndjson").read_text(encoding="utf-8").splitlines():
        record = json.loads(line)
        meter = record.get("meter_id")
        if meter in meter_ids:
            errors.append(f"duplicate meter reference: {meter}")
        meter_ids.add(meter)
        if record.get("district_id") not in district_ids:
            errors.append(f"meter references unknown district: {record.get('district_id')}")
    # Public records must never carry scenario or fault labels.
    forbidden = {"scenario_id", "scenario_class", "physical_parameters", "fault_type"}
    for name in ("district-sensors.ndjson", "meter-readings.ndjson"):
        with (public / name).open("r", encoding="utf-8") as handle:
            for line_number, line in enumerate(handle, start=1):
                if forbidden & set(json.loads(line)):
                    errors.append(
                        f"{name}:{line_number}: restricted truth field in public record"
                    )
                    break
    return {
        "status": "failed" if errors else "passed",
        "errors": errors,
        "district_records": district_count,
        "meter_records": meter_count,
    }
