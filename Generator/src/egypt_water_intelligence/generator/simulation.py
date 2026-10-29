"""Coherent demand creation and EPANET 2.2 hydraulic simulation."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any
from zoneinfo import ZoneInfo

import numpy as np # type: ignore
import wntr # type: ignore
from wntr.network.controls import Control, ControlAction, SimTimeCondition # type: ignore

from .config import RunConfig

SECONDS_PER_MINUTE = 60


@dataclass
class SimulationResult:
    districts: list[dict[str, Any]]
    meters: list[dict[str, Any]]
    requested_m3_s: np.ndarray
    delivered_m3_s: np.ndarray
    inlet_flow_m3_s: dict[str, np.ndarray]
    pressure_m_h2o: dict[str, np.ndarray]
    leak_flow_m3_s: dict[str, np.ndarray]
    scenario_truth: list[dict[str, Any]]
    network: Any


def district_id(index: int) -> str:
    return f"D{index:03d}"


def _scenario_class(scenario_type: str) -> str:
    if scenario_type in {"burst", "gradual_leak", "persistent_background_loss"}:
        return "leak_incident"
    if scenario_type == "supply_disturbance":
        return "operational_disturbance"
    return "demand_confounder"


def _iso_at(start: datetime, minute: int) -> str:
    return (start + timedelta(minutes=minute)).isoformat().replace("+00:00", "Z")


def build_meter_demands(
    config: RunConfig,
) -> tuple[list[dict[str, Any]], np.ndarray, dict[str, np.ndarray]]:
    """Return reference data, per-meter requested rates, and district sums."""
    raw = config.raw
    n_minutes, n_meters = config.duration_minutes, config.meter_count
    demand_config = raw.get("demand", {})
    timezone_name = raw["utility"].get("timezone", "Africa/Cairo")
    try:
        local_zone = ZoneInfo(timezone_name)
    except Exception as exc:
        raise ValueError(f"invalid IANA timezone: {timezone_name}") from exc
    seed_sequence = np.random.SeedSequence(config.seed)
    baseline_rng, assignment_rng, variation_rng = (
        np.random.default_rng(child) for child in seed_sequence.spawn(3)
    )
    start_utc = datetime.fromisoformat(raw["run"]["start_time_utc"].replace("Z", "+00:00")).astimezone(timezone.utc)
    district_ids = [district_id(i) for i in range(1, config.district_count + 1)]
    meter_districts = np.array(
        [district_ids[i % len(district_ids)] for i in range(n_meters)]
    )
    class_config = demand_config.get(
        "customer_classes",
        {
            "residential": {"share": 0.9, "baseline_mean_m3_s": 1.05e-5, "baseline_sigma": 0.35},
            "small_commercial": {"share": 0.1, "baseline_mean_m3_s": 2.10e-5, "baseline_sigma": 0.30},
        },
    )
    class_names = list(class_config)
    shares = np.array([float(class_config[name].get("share", 0.0)) for name in class_names])
    if np.any(shares < 0) or not np.isclose(shares.sum(), 1.0):
        raise ValueError("demand.customer_classes shares must be non-negative and sum to 1")
    class_indices = np.repeat(np.arange(len(class_names)), np.floor(shares * n_meters).astype(int))
    if len(class_indices) < n_meters:
        remainder = n_meters - len(class_indices)
        class_indices = np.concatenate((class_indices, assignment_rng.choice(len(class_names), remainder, p=shares)))
    assignment_rng.shuffle(class_indices)
    customer_classes = np.array([class_names[index] for index in class_indices])
    baselines = np.array(
        [
            baseline_rng.lognormal(
                mean=np.log(float(class_config[str(customer_class)].get("baseline_mean_m3_s", 1.05e-5))),
                sigma=float(class_config[str(customer_class)].get("baseline_sigma", 0.35)),
            )
            for customer_class in customer_classes
        ]
    )
    minutes = np.arange(n_minutes)
    local_times = [start_utc + timedelta(minutes=int(m)) for m in minutes]
    local_times = [value.astimezone(local_zone) for value in local_times]
    minute_of_day = np.array([value.hour * 60 + value.minute for value in local_times])
    morning = np.exp(-(((minute_of_day - 450) / 170) ** 2))
    evening = np.exp(-(((minute_of_day - 1170) / 210) ** 2))
    profile = 0.55 + 0.38 * morning + 0.42 * evening
    profile /= profile.mean()
    weekly = demand_config.get(
        "weekly_multipliers",
        {"weekday": 1.0, "thursday": 1.04, "friday": 1.0, "saturday": 0.98},
    )
    weekly_multiplier = np.array(
        [
            float(
                weekly.get(
                    "thursday" if value.weekday() == 3 else
                    "friday" if value.weekday() == 4 else
                    "saturday" if value.weekday() == 5 else
                    "weekday"
                )
            )
            for value in local_times
        ]
    )
    profile *= weekly_multiplier
    prayer = demand_config.get(
        "friday_prayer",
        {"enabled": True, "start_local": "12:00", "end_local": "13:30", "residential_multiplier": 0.75, "small_commercial_multiplier": 0.90, "rebound_multiplier": 1.05, "rebound_minutes": 60},
    )
    if prayer.get("enabled", False):
        start_hour, start_minute = (int(part) for part in str(prayer.get("start_local", "12:00")).split(":"))
        end_hour, end_minute = (int(part) for part in str(prayer.get("end_local", "13:30")).split(":"))
        prayer_start = start_hour * 60 + start_minute
        prayer_end = end_hour * 60 + end_minute
        prayer_mask = np.array([value.weekday() == 4 and prayer_start <= int(day_minute) < prayer_end for value, day_minute in zip(local_times, minute_of_day)])
        rebound_end = prayer_end + int(prayer.get("rebound_minutes", 60))
        rebound_mask = np.array([value.weekday() == 4 and prayer_end <= int(day_minute) < rebound_end for value, day_minute in zip(local_times, minute_of_day)])
    else:
        prayer_mask = np.zeros(n_minutes, dtype=bool)
        rebound_mask = np.zeros(n_minutes, dtype=bool)
    class_multipliers = np.ones((n_meters, n_minutes))
    for index, customer_class in enumerate(customer_classes):
        class_profile = class_config[str(customer_class)]
        class_multipliers[index] *= float(class_profile.get("profile_multiplier", 1.0))
        if prayer_mask.any():
            class_multipliers[index, prayer_mask] *= float(
                prayer.get(f"{customer_class}_multiplier", prayer.get("residential_multiplier", 0.75))
            )
            class_multipliers[index, rebound_mask] *= float(prayer.get("rebound_multiplier", 1.05))
    requested = baselines[:, None] * profile[None, :] * class_multipliers
    # Smooth meter-specific variation: one draw per 15 minute period, held for its interval.
    variation = variation_rng.normal(1.0, 0.035, size=(n_meters, n_minutes // 15))
    requested *= np.repeat(np.clip(variation, 0.85, 1.15), 15, axis=1)
    for scenario in raw["scenarios"]:
        if scenario["type"] == "demand_surge":
            mask = meter_districts == scenario["district_id"]
            requested[mask, scenario["start_minute"] : scenario["end_minute"]] *= float(
                scenario["demand_multiplier"]
            )
    meters = [
        {
            "meter_id": f"M{i + 1:06d}",
            "district_id": str(meter_districts[i]),
            "customer_class": str(customer_classes[i]),
            "service_junction_id": f"{meter_districts[i]}_SERVICE",
            "baseline_requested_m3_s": float(baselines[i]),
        }
        for i in range(n_meters)
    ]
    district_requested = {
        did: requested[meter_districts == did].sum(axis=0) for did in district_ids
    }
    return meters, requested, district_requested


def _add_control(wn: Any, name: str, link: Any, second: int, status: Any) -> None:
    condition = SimTimeCondition(wn, "=", second)
    action = ControlAction(link, "status", status)
    wn.add_control(name, Control(condition, action))


def build_network(
    config: RunConfig, district_requested: dict[str, np.ndarray]
) -> tuple[Any, dict[str, list[str]]]:
    """Build isolated district branches and scenario controls in a WNTR model."""
    raw, hyd = config.raw, config.raw["hydraulics"]
    n = config.duration_minutes
    wn = wntr.network.WaterNetworkModel()
    wn.options.time.duration = n * SECONDS_PER_MINUTE
    wn.options.time.hydraulic_timestep = SECONDS_PER_MINUTE
    wn.options.time.pattern_timestep = SECONDS_PER_MINUTE
    wn.options.time.report_timestep = SECONDS_PER_MINUTE
    wn.options.time.report_start = 0
    wn.options.hydraulic.demand_model = "PDD"
    wn.options.hydraulic.minimum_pressure = float(hyd["minimum_pressure_m"])
    wn.options.hydraulic.required_pressure = float(hyd["required_pressure_m"])
    wn.options.hydraulic.pressure_exponent = float(hyd["pressure_exponent"])
    wn.options.hydraulic.inpfile_units = "LPS"
    wn.options.hydraulic.unbalanced = "STOP"
    mapping: dict[str, list[str]] = {}
    for number in range(1, config.district_count + 1):
        did = district_id(number)
        demand = district_requested[did]
        nominal = float(np.mean(demand))
        pattern = (demand / nominal).tolist() if nominal > 0 else [1.0] * n
        source_pattern = np.ones(n)
        for scenario in raw["scenarios"]:
            if (
                scenario["type"] == "supply_disturbance"
                and scenario["district_id"] == did
            ):
                source_pattern[scenario["start_minute"] : scenario["end_minute"]] = (
                    float(hyd["source_head_m"]) + float(scenario["source_head_delta_m"])
                ) / float(hyd["source_head_m"])
        wn.add_pattern(f"P_DEMAND_{did}", pattern)
        wn.add_pattern(f"P_HEAD_{did}", source_pattern.tolist())
        source = f"{did}_SOURCE"
        service = f"{did}_SERVICE"
        inlet = f"{did}_INLET"
        wn.add_reservoir(
            source, base_head=float(hyd["source_head_m"]), head_pattern=f"P_HEAD_{did}"
        )
        wn.add_junction(
            service,
            base_demand=nominal,
            demand_pattern=f"P_DEMAND_{did}",
            elevation=5.0,
        )
        wn.add_pipe(
            inlet, source, service, length=900.0, diameter=0.22, roughness=125.0
        )
        mapping[did] = [source, service, inlet]
    for scenario_number, scenario in enumerate(raw["scenarios"], start=1):
        if scenario["type"] not in {
            "burst",
            "gradual_leak",
            "persistent_background_loss",
        }:
            continue
        did = scenario["district_id"]
        service = mapping[did][1]
        stages = int(scenario.get("stages", 1))
        for stage in range(stages):
            suffix = f"L{scenario_number:02d}_{stage + 1:02d}"
            leak = f"{did}_{suffix}"
            branch = f"{did}_BR_{suffix}"
            coefficient = float(scenario["emitter_coefficient"]) / stages
            wn.add_junction(leak, base_demand=0.0, elevation=5.0)
            wn.get_node(leak).emitter_coefficient = coefficient
            initially_open = scenario["start_minute"] == 0 and stage == 0
            wn.add_pipe(
                branch,
                service,
                leak,
                length=5.0,
                diameter=0.1,
                roughness=125.0,
                initial_status="OPEN" if initially_open else "CLOSED",
            )
            start = int(scenario["start_minute"] * SECONDS_PER_MINUTE)
            if scenario["type"] == "gradual_leak":
                start += int(
                    (scenario["end_minute"] - scenario["start_minute"])
                    * stage
                    / stages
                    * SECONDS_PER_MINUTE
                )
            if not initially_open:
                _add_control(
                    wn,
                    f"OPEN_{suffix}",
                    wn.get_link(branch),
                    start,
                    wntr.network.LinkStatus.Open,
                )
            _add_control(
                wn,
                f"CLOSE_{suffix}",
                wn.get_link(branch),
                int(scenario["end_minute"] * SECONDS_PER_MINUTE),
                wntr.network.LinkStatus.Closed,
            )
            mapping.setdefault(f"leak:{scenario['scenario_id']}", []).append(leak)
    return wn, mapping


def simulate(config: RunConfig) -> SimulationResult:
    meters, requested, district_requested = build_meter_demands(config)
    wn, mapping = build_network(config, district_requested)
    # Keep WNTR's transient INP/BIN/RPT artifacts out of the repository root.
    with TemporaryDirectory(prefix="egypt-water-epanet-") as temporary_directory:
        prefix = str(Path(temporary_directory) / "hydraulics")
        results = wntr.sim.EpanetSimulator(wn).run_sim(
            file_prefix=prefix, version=2.2, convergence_error=True
        )
    n = config.duration_minutes
    if len(results.node["pressure"].index) != n + 1:
        raise RuntimeError(
            f"expected {n + 1} internal hydraulic snapshots, got {len(results.node['pressure'].index)}"
        )
    district_rows: list[dict[str, Any]] = []
    inlet_flows: dict[str, np.ndarray] = {}
    pressures: dict[str, np.ndarray] = {}
    leak_flows: dict[str, np.ndarray] = {}
    for number in range(1, config.district_count + 1):
        did = district_id(number)
        source, service, inlet = mapping[did]
        flow = results.link["flowrate"].iloc[:n][inlet].to_numpy(dtype=float)
        pressure = results.node["pressure"].iloc[:n][service].to_numpy(dtype=float)
        if np.any(flow < -1e-10):
            raise RuntimeError(f"district inlet has reverse flow: {did}")
        inlet_flows[did], pressures[did] = flow, pressure
    for key, leak_nodes in mapping.items():
        if key.startswith("leak:"):
            leak_flows[key.removeprefix("leak:")] = (
                results.node["demand"]
                .iloc[:n][leak_nodes]
                .sum(axis=1)
                .to_numpy(dtype=float)
            )
    delivered = np.zeros_like(requested)
    meter_districts = np.array([m["district_id"] for m in meters])
    for did, requested_total in district_requested.items():
        service = mapping[did][1]
        actual = results.node["demand"].iloc[:n][service].to_numpy(dtype=float)
        selected = meter_districts == did
        ratios = np.divide(
            requested[selected],
            requested_total[None, :],
            out=np.zeros_like(requested[selected]),
            where=requested_total[None, :] > 0,
        )
        delivered[selected] = ratios * actual[None, :]
    start = datetime.fromisoformat(
        config.raw["run"]["start_time_utc"].replace("Z", "+00:00")
    ).astimezone(timezone.utc)
    truth = []
    for scenario in config.raw["scenarios"]:
        effect = {"expected": "inlet flow and pressure response"}
        if scenario["type"] == "demand_surge":
            effect = {"expected": "higher requested and delivered customer demand"}
        truth.append(
            {
                "schema_version": "1.0.0",
                "scenario_id": scenario["scenario_id"],
                "scenario_type": scenario["type"],
                "scenario_class": _scenario_class(scenario["type"]),
                "district_id": scenario["district_id"],
                "start_time_utc": _iso_at(start, scenario["start_minute"]),
                "end_time_utc": _iso_at(start, scenario["end_minute"]),
                "physical_parameters": {
                    k: v
                    for k, v in scenario.items()
                    if k
                    not in {
                        "scenario_id",
                        "type",
                        "district_id",
                        "start_minute",
                        "end_minute",
                    }
                },
                "expected_effects": effect,
            }
        )
    return SimulationResult(
        districts=district_rows,
        meters=meters,
        requested_m3_s=requested,
        delivered_m3_s=delivered,
        inlet_flow_m3_s=inlet_flows,
        pressure_m_h2o=pressures,
        leak_flow_m3_s=leak_flows,
        scenario_truth=truth,
        network=wn,
    )
