"""Offline evaluation of Flink district alerts against restricted truth.

Run AFTER detection. The truth is never an input to the job.

  python evaluate_alerts.py \
      --alerts /tmp/alerts.ndjson \
      --truth Generator/output/run-72b96af88a342375/restricted/scenario-truth.ndjson \
      --faults Generator/output/run-72b96af88a342375/restricted/fault-truth.ndjson

Incident matching: a truth scenario is DETECTED if an 'open' (or, for flow_level_shift,
'instant') alert for the same
district has first_flagged_window_start inside [scenario_start - 15 min, scenario_end).
Detection delay = open alert's window_end - scenario_start.
An 'open' incident alert that matches no scenario is a FALSE ALARM.
Data quality: each district null-pressure fault event is matched to the 15-minute
window containing it; that window must have a data_quality alert.
"""

import argparse
import json
from datetime import datetime, timedelta

EXPECTED_TYPE = {
    "burst": "flow_anomaly",
    "gradual_leak": "flow_anomaly",
    "demand_surge": "flow_anomaly",
    "supply_disturbance": "supply_disturbance",
    # A loss present from the very first minute has no "change" to detect (see job3c);
    # a loss that starts later is detectable as flow_level_shift.
    "persistent_background_loss": "flow_level_shift",
}


def ts(s):
    return datetime.fromisoformat(s.replace("Z", ""))


def lines(path):
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line.startswith("{"):
                yield json.loads(line)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--alerts", required=True)
    ap.add_argument("--truth", required=True)
    ap.add_argument("--faults")
    ap.add_argument("--run-start", default="2026-01-01T00:00:00")
    a = ap.parse_args()

    alerts = {r["alert_id"]: r for r in lines(a.alerts)}.values()  # replay-safe
    opens = [r for r in alerts
             if r["alert_type"] != "data_quality" and r["status"] in ("open", "instant")]
    truth = list(lines(a.truth))
    tol = timedelta(minutes=15)

    print(f"alerts: {len(alerts)} | incident opens: {len(opens)}\n")
    print(f"{'scenario':26} {'district':8} {'type':26} {'detected':9} {'type_ok':8} delay_min")
    matched = set()
    detected = detectable = 0
    for t in truth:
        s, e = ts(t["start_time_utc"]), ts(t["end_time_utc"])
        want = EXPECTED_TYPE.get(t["scenario_type"])
        if t["scenario_type"] == "persistent_background_loss" and ts(t["start_time_utc"]) <= ts(a.run_start):
            want = None  # constant from the start: not detectable from district data
        hits = [
            o for o in opens
            if o["district_id"] == t["district_id"]
            and o["first_flagged_window_start"]
            and s - tol <= ts(o["first_flagged_window_start"]) < e
        ]
        for o in hits:
            matched.add(o["alert_id"])
        hit = min(hits, key=lambda o: o["window_end"]) if hits else None
        if want is not None:
            detectable += 1
            detected += bool(hit)
        delay = round((ts(hit["window_end"]) - s).total_seconds() / 60) if hit else "-"
        type_ok = (hit["alert_type"] == want) if hit and want else "-"
        print(f"{t['scenario_id']:26} {t['district_id']:8} {t['scenario_type']:26} "
              f"{'yes' if hit else 'NO':9} {str(type_ok):8} {delay}")

    false_alarms = [o for o in opens if o["alert_id"] not in matched]
    print(f"\nrecall, all {len(truth)} scenarios:            {sum(1 for t in truth if any(o['district_id']==t['district_id'] and o['first_flagged_window_start'] and ts(t['start_time_utc'])-tol <= ts(o['first_flagged_window_start']) < ts(t['end_time_utc']) for o in opens))}/{len(truth)}")
    print(f"recall, detectable scope ({detectable}):       {detected}/{detectable}")
    print(f"false alarms (open incident alerts): {len(false_alarms)}")
    for o in false_alarms:
        print("  ", o["district_id"], o["alert_type"], o["window_start"])
    prec = (len(opens) - len(false_alarms)) / len(opens) if opens else float("nan")
    print(f"precision: {prec:.2f}")

    if a.faults:
        start = ts(a.run_start)
        dq = {(r["district_id"], r["window_start"][:16]) for r in alerts if r["alert_type"] == "data_quality"}
        expected = set()
        for f in lines(a.faults):
            if f["stream"] == "district" and f["fault_type"] == "null_value":
                did, minute = f["event_id"].split(":")[-2:]
                w = start + timedelta(minutes=int(minute) // 15 * 15)
                expected.add((did, w.strftime("%Y-%m-%dT%H:%M")))
        found = expected & dq
        print(f"\ndata quality: faults {len(expected)} | alerted {len(found)} | "
              f"missed {len(expected - found)} | extra alerts {len(dq - expected)}")
        for k in sorted(expected - found)[:10]:
            print("  missed:", k)


if __name__ == "__main__":
    main()
