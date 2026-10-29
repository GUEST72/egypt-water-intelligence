"""Offline alert evaluation against restricted truth: the rules of flink/analysis/evaluate_alerts.py as a function.

Run only after detection has finished; truth is never an input to Flink or to batch transformations.
The asset that uses this also runs the original script and requires both to agree.
"""
from __future__ import annotations

from datetime import datetime, timedelta

EXPECTED_TYPE = {
    "burst": "flow_anomaly",
    "gradual_leak": "flow_anomaly",
    "demand_surge": "flow_anomaly",
    "supply_disturbance": "supply_disturbance",
    "persistent_background_loss": "flow_level_shift",
}
TOLERANCE = timedelta(minutes=15)


def ts(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", ""))


def evaluate(alerts: list[dict], truth: list[dict], faults: list[dict], run_start: str) -> dict:
    alerts = list({a["alert_id"]: a for a in alerts}.values())  # replay-safe, as in the script
    opens = [a for a in alerts if a["alert_type"] != "data_quality" and a["status"] in ("open", "instant")]
    start = ts(run_start)
    matched: set[str] = set()
    scenarios = []
    for t in truth:
        s, e = ts(t["start_time_utc"]), ts(t["end_time_utc"])
        want = EXPECTED_TYPE.get(t["scenario_type"])
        if t["scenario_type"] == "persistent_background_loss" and s <= start:
            want = None  # constant from the start: not detectable from district data
        hits = [o for o in opens if o["district_id"] == t["district_id"] and o["first_flagged_window_start"]
                and s - TOLERANCE <= ts(o["first_flagged_window_start"]) < e]
        matched.update(o["alert_id"] for o in hits)
        hit = min(hits, key=lambda o: o["window_end"]) if hits else None
        scenarios.append({
            "scenario_id": t["scenario_id"], "district_id": t["district_id"], "scenario_type": t["scenario_type"],
            "start_time_utc": t["start_time_utc"], "end_time_utc": t["end_time_utc"],
            "detectable": want is not None, "detected": hit is not None, "expected_alert_type": want,
            "alert_type": hit["alert_type"] if hit else None,
            "type_ok": (hit["alert_type"] == want) if hit and want else None,
            "delay_min": round((ts(hit["window_end"]) - s).total_seconds() / 60) if hit else None,
        })
    false_alarms = [o for o in opens if o["alert_id"] not in matched]

    dq = {(a["district_id"], a["window_start"][:16]) for a in alerts if a["alert_type"] == "data_quality"}
    expected_dq = set()
    for f in faults:
        if f["stream"] == "district" and f["fault_type"] == "null_value":
            district, minute = f["event_id"].split(":")[-2:]
            window = start + timedelta(minutes=int(minute) // 15 * 15)
            expected_dq.add((district, window.strftime("%Y-%m-%dT%H:%M")))

    detectable = [r for r in scenarios if r["detectable"]]
    return {
        "scenarios": scenarios,
        "summary": {
            "alerts": len(alerts),
            "incident_opens": len(opens),
            "scenarios_total": len(scenarios),
            "scenarios_detected": sum(r["detected"] for r in scenarios),
            "scenarios_detectable": len(detectable),
            "detectable_detected": sum(r["detected"] for r in detectable),
            "false_alarms": len(false_alarms),
            "precision": (len(opens) - len(false_alarms)) / len(opens) if opens else None,
            "dq_faults": len(expected_dq),
            "dq_alerted": len(expected_dq & dq),
            "dq_missed": len(expected_dq - dq),
            "dq_extra_alerts": len(dq - expected_dq),
        },
        "false_alarm_ids": [o["alert_id"] for o in false_alarms],
    }
