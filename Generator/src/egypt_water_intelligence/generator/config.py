"""Configuration loading and invariant checks for the local generator."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml
from zoneinfo import ZoneInfo


class ConfigurationError(ValueError):
    """Raised when a run configuration violates a generator contract."""


@dataclass(frozen=True)
class RunConfig:
    raw: dict[str, Any]

    @property
    def duration_minutes(self) -> int:
        return int(self.raw["run"]["duration_minutes"])

    @property
    def district_count(self) -> int:
        return int(self.raw["utility"]["district_count"])

    @property
    def meter_count(self) -> int:
        return int(self.raw["utility"]["meter_count"])

    @property
    def seed(self) -> int:
        return int(self.raw["run"]["seed"])


def load_config(path: str | Path) -> RunConfig:
    with Path(path).open("r", encoding="utf-8") as handle:
        raw = yaml.safe_load(handle)
    validate_config(raw)
    return RunConfig(raw=raw)


def validate_config(raw: dict[str, Any]) -> None:
    required = {
        "schema_version",
        "run",
        "utility",
        "hydraulics",
        "scenarios",
        "sensor",
        "faults",
    }
    missing = required - set(raw or {})
    if missing:
        raise ConfigurationError(f"missing top-level keys: {sorted(missing)}")
    if raw["schema_version"] != "1.0.0":
        raise ConfigurationError("only configuration schema version 1.0.0 is supported")
    duration = raw["run"].get("duration_minutes")
    if (
        not isinstance(duration, int)
        or duration <= 0
        or duration % 15
        or duration > 25 * 24 * 60
    ):
        raise ConfigurationError(
            "duration_minutes must be a positive multiple of 15 and no more than 25 days"
        )
    if not isinstance(raw["utility"].get("district_count"), int) or not isinstance(
        raw["utility"].get("meter_count"), int
    ):
        raise ConfigurationError("district_count and meter_count must be integers")
    if (
        raw["utility"].get("district_count", 0) <= 0
        or raw["utility"].get("meter_count", 0) <= 0
    ):
        raise ConfigurationError("district_count and meter_count must be positive")
    try:
        ZoneInfo(raw["utility"].get("timezone", "Africa/Cairo"))
    except Exception as exc:
        raise ConfigurationError("utility.timezone must be a valid IANA timezone") from exc
    hyd = raw["hydraulics"]
    if hyd.get("engine") != "epanet_2_2" or hyd.get("demand_model") != "PDD":
        raise ConfigurationError("the local generator requires EPANET 2.2 with PDD")
    if {
        hyd.get("hydraulic_timestep_seconds"),
        hyd.get("pattern_timestep_seconds"),
        hyd.get("report_timestep_seconds"),
    } != {60}:
        raise ConfigurationError(
            "hydraulic, pattern, and reporting time steps must each be 60 seconds"
        )
    minimum = float(hyd.get("minimum_pressure_m", -1))
    required_pressure = float(hyd.get("required_pressure_m", -1))
    if required_pressure < minimum + 0.1:
        raise ConfigurationError(
            "required_pressure_m must be at least minimum_pressure_m + 0.1"
        )
    classes = raw.get("demand", {}).get("customer_classes", {})
    if classes:
        shares = [float(value.get("share", -1)) for value in classes.values()]
        if any(share < 0 for share in shares) or abs(sum(shares) - 1.0) > 1e-9:
            raise ConfigurationError(
                "demand.customer_classes shares must be non-negative and sum to 1"
            )
    spatial_keys = {
        "district_pdd_thresholds",
        "junction_pdd_thresholds",
        "spatial_pdd_thresholds",
    }
    if spatial_keys & set(hyd):
        raise ConfigurationError(
            "spatial PDD thresholds are not supported by EpanetSimulator"
        )
    district_ids = {
        f"D{i:03d}" for i in range(1, int(raw["utility"]["district_count"]) + 1)
    }
    for scenario in raw["scenarios"]:
        if scenario.get("district_id") not in district_ids:
            raise ConfigurationError(
                f"scenario references unknown district: {scenario.get('district_id')}"
            )
        start, end = scenario.get("start_minute"), scenario.get("end_minute")
        if (
            not isinstance(start, int)
            or not isinstance(end, int)
            or not 0 <= start < end <= duration
        ):
            raise ConfigurationError(
                f"invalid scenario interval: {scenario.get('scenario_id')}"
            )
        if scenario.get("type") not in {
            "burst",
            "gradual_leak",
            "persistent_background_loss",
            "supply_disturbance",
            "demand_surge",
        }:
            raise ConfigurationError(
                f"unsupported scenario type: {scenario.get('type')}"
            )
    validate_fault_rules(raw)        



def validate_fault_rules(raw: dict) -> None:
    """Validate supported delivery fault rules."""
    faults = raw.get("faults")

    if not isinstance(faults, list):
        raise ConfigurationError("'faults' must be a list")

    allowed_fields = {
        "meter": {"consumed_volume_m3"},
        "district": {"pressure_m_h2o"},
    }

    for index, rule in enumerate(faults):
        if not isinstance(rule, dict):
            raise ConfigurationError(f"faults[{index}] must be a mapping")

        stream = rule.get("stream")
        if stream not in allowed_fields:
            raise ConfigurationError(
                f"faults[{index}].stream must be 'meter' or 'district'"
            )

        fault_type = rule.get("type")
        if fault_type not in {
            "null_value",
            "null_measurement",
            "duplicate",
        }:
            raise ConfigurationError(
                f"faults[{index}].type must be null_value, "
                "null_measurement, or duplicate"
            )

        every_n = rule.get("every_n")
        if (
            not isinstance(every_n, int)
            or isinstance(every_n, bool)
            or every_n < 1
        ):
            raise ConfigurationError(
                f"faults[{index}].every_n must be a positive integer"
            )

        if fault_type in {"null_value", "null_measurement"}:
            field = rule.get("field")
            if field not in allowed_fields[stream]:
                raise ConfigurationError(
                    f"faults[{index}].field cannot be nulled for "
                    f"stream {stream!r}; "
                    f"allowed fields: {sorted(allowed_fields[stream])}"
                )
