"""Small, hand-checkable test for job3b_district_alerts.py.

  python job3b_test_cases.py make  > features_cases.ndjson
  python job3b_test_cases.py check alerts_out.ndjson

12 windows x 6 districts of 15-minute features (starting 2026-01-01T00:00).
Baseline: every district near 0.0012 m3/s and 40 m (within +-1.5% of peers).

  D001 w2-w4  flow x1.5, pressure normal  -> flow_anomaly open at w3, closed (last window w4)
  D002 w6     flow x1.5 for ONE window    -> no alert (needs 2 consecutive windows)
  D003 w3-w5  flow x0.5, pressure 10 m    -> supply_disturbance open at w4, closed (last window w5)
  D004 w7     pressure NULL (flagged)     -> data_quality null_pressure
  D005 w8     n_events = 14               -> data_quality incomplete_window

Only the 'must be exactly these' alerts are checked, so extra or missing alerts both fail.
"""

import json
import sys
from datetime import datetime, timedelta

BASE = datetime(2026, 1, 1)
N_WINDOWS = 12
DISTRICTS = [f"D{i:03d}" for i in range(1, 7)]


def ts(w):
    return (BASE + timedelta(minutes=15 * w)).strftime("%Y-%m-%dT%H:%M:%S")


def make_rows():
    rows = []
    for w in range(N_WINDOWS):
        for i, d in enumerate(DISTRICTS, 1):
            flow = 0.0012 * (1 + 0.005 * (i - 3))
            pressure = 40.0
            n_events, n_null, n_flag = 15, 0, 0
            if d == "D001" and 2 <= w <= 4:
                flow *= 1.5
            if d == "D002" and w == 6:
                flow *= 1.5
            if d == "D003" and 3 <= w <= 5:
                flow *= 0.5
                pressure = 10.0
            if d == "D004" and w == 7:
                pressure, n_null, n_flag = None, 1, 1
            if d == "D005" and w == 8:
                n_events = 14
            rows.append({
                "window_start": ts(w), "window_end": ts(w + 1), "district_id": d,
                "avg_flow_m3_s": flow, "inlet_volume_m3": flow * 900,
                "min_pressure_m": pressure, "avg_pressure_m": pressure,
                "n_events": n_events, "n_null_pressure": n_null, "n_flagged": n_flag,
            })
    return rows


# (district, alert_type, status, evidence window_start)
EXPECTED = {
    ("D001", "flow_anomaly", "open", ts(3)),
    ("D001", "flow_anomaly", "closed", ts(4)),
    ("D003", "supply_disturbance", "open", ts(4)),
    ("D003", "supply_disturbance", "closed", ts(5)),
    ("D004", "data_quality", "instant", ts(7)),
    ("D005", "data_quality", "instant", ts(8)),
}


def check(path):
    got, dup = set(), 0
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line.startswith("{"):
                continue
            r = json.loads(line)
            key = (r["district_id"], r["alert_type"], r["status"], r["window_start"][:19])
            dup += key in got
            got.add(key)
    print("alerts received:", len(got), "| duplicates:", dup)
    for k in sorted(EXPECTED - got):
        print("MISSING :", k)
    for k in sorted(got - EXPECTED):
        print("UNEXPECTED:", k)
    ok = got == EXPECTED
    print("RESULT:", "PASS" if ok else "FAIL")
    return 0 if ok else 1


if __name__ == "__main__":
    if sys.argv[1] == "make":
        for r in make_rows():
            print(json.dumps(r))
    else:
        raise SystemExit(check(sys.argv[2]))
