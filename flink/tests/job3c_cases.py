"""Hand-checkable test for job3c_level_shift.py.

  python flink/tests/job3c_cases.py make  > relative_cases.ndjson  # 12 days x 96 windows x 6 districts
  python flink/tests/job3c_cases.py check alerts_out.ndjson

Input is the 'relative' topic (rel_flow per district and 15-minute window), day 0 =
2026-01-01. Baseline rel_flow is 1.0 everywhere with cond = NULL.

  D005 days 8-10  rel_flow 1.05 (+5%)       -> ONE alert, on day 8 (days 9 and 10 are the same shift)
  D002 day 7      rel_flow 1.02 (+2%)       -> no alert (below the 3% threshold)
  D003 day 9      60 windows flagged (cond = flow_anomaly, rel 1.5), the other 36 at 1.05
                                            -> no alert (only 36 of 96 windows usable, minimum is 72)
  D001 day 10     rel_flow 0.95 (-5%)       -> no alert (only increases alert)

Day 11 only exists to close day 10 (the last day of a stream never closes).
Expected: exactly one alert.
"""

import json
import sys
from datetime import datetime, timedelta

BASE = datetime(2026, 1, 1)
DAYS = 12
DISTRICTS = [f"D{i:03d}" for i in range(1, 7)]


def ts(minutes):
    return (BASE + timedelta(minutes=minutes)).strftime("%Y-%m-%dT%H:%M:%S")


def make_rows():
    rows = []
    for day in range(DAYS):
        for w in range(96):
            start = day * 1440 + w * 15
            for d in DISTRICTS:
                rel, cond = 1.0, None
                if d == "D005" and 8 <= day <= 10:
                    rel = 1.05
                if d == "D002" and day == 7:
                    rel = 1.02
                if d == "D003" and day == 9:
                    if w < 60:
                        rel, cond = 1.5, "flow_anomaly"
                    else:
                        rel = 1.05
                if d == "D001" and day == 10:
                    rel = 0.95
                rows.append({
                    "window_start": ts(start), "window_end": ts(start + 15),
                    "district_id": d, "avg_flow_m3_s": 0.0012 * rel,
                    "min_pressure_m": 40.0, "rel_flow": rel, "rel_pressure": 1.0,
                    "cond": cond,
                })
    return rows


EXPECTED = {("D005", "flow_level_shift", "instant", ts(8 * 1440))}


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
    print("alerts received:", len(got), "| duplicate lines:", dup)
    for k in sorted(EXPECTED - got):
        print("MISSING   :", k)
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
