import json
import sys
from collections import Counter
from datetime import datetime

RAW = "Generator/output/run-bdc85c43085c4dc8/delivery/district-sensors.ndjson"
CLEAN = "/tmp/clean.ndjson"
FEATURES = "/tmp/features.ndjson"
EVENTS_PER_WINDOW = 15


def read_json_lines(path):
    # Console-consumer output can contain non-JSON lines, so keep only JSON objects.
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line.startswith("{"):
                yield json.loads(line)


def parse_ts(ts):
    return datetime.fromisoformat(ts.replace("Z", "+00:00"))


def window_key(ts):
    # Floor an event time to its 15-minute window, formatted as "YYYY-MM-DDTHH:MM".
    minute = int(ts[14:16]) // 15 * 15
    return f"{ts[:14]}{minute:02d}"


def check_order(path):
    # Count rows whose event time is older than the newest event seen so far.
    top = None
    total = back = 0
    worst = 0.0
    for r in read_json_lines(path):
        total += 1
        cur = parse_ts(r["event_time_utc"])
        if top is not None and cur < top:
            back += 1
            worst = max(worst, (top - cur).total_seconds())
        if top is None or cur > top:
            top = cur
    print(path)
    print(f"  rows: {total} | out-of-order rows: {back} | worst lateness (s): {worst}")


def report():
    check_order(CLEAN)

    feats = list(read_json_lines(FEATURES))
    counted = sum(r["n_events"] for r in feats)
    expected = len(feats) * EVENTS_PER_WINDOW
    print(f"\nfeatures windows: {len(feats)} | events counted: {counted} "
          f"| expected: {expected} | missing: {expected - counted}")
    dist = Counter(r["n_events"] for r in feats)
    print("n_events distribution:", dict(sorted(dist.items())))
    odd = [r for r in feats if r["n_events"] != EVENTS_PER_WINDOW]
    print("windows with n_events != 15:", len(odd),
          "| by district:", dict(Counter(r["district_id"] for r in odd)))

    # Null-pressure records in the clean topic versus the counts in the features topic.
    clean_nulls = Counter()
    ids = set()
    rows = 0
    for r in read_json_lines(CLEAN):
        rows += 1
        ids.add(r["event_id"])
        if r["pressure_m_h2o"] is None:
            clean_nulls[(r["district_id"], window_key(r["event_time_utc"]))] += 1

    feat_nulls = {(r["district_id"], r["window_start"][:16]): r["n_null_pressure"] for r in feats}
    print(f"\nclean rows: {rows} | unique event_ids: {len(ids)} "
          f"| null-pressure records: {sum(clean_nulls.values())}")
    print(f"null-pressure counted in features: {sum(feat_nulls.values())}")
    for key, n in sorted(clean_nulls.items()):
        got = feat_nulls.get(key)
        if got is None:
            print("  window not emitted:", key, "| nulls in clean:", n)
        elif got != n:
            print("  mismatch:", key, "| clean:", n, "| features:", got)


if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else "report"
    if mode == "order":
        for p in sys.argv[2:] or [RAW]:
            check_order(p)
    else:
        report()
