import json
import sys

import pandas as pd

FEATURES = "/tmp/features.ndjson"
THRESHOLDS = [0.05, 0.08, 0.10, 0.15, 0.20, 0.30]
DETAIL = float(sys.argv[1]) if len(sys.argv) > 1 else 0.10
PRESSURE_CUTS = [0.95, 0.90, 0.80, 0.50]


def load(path):
    rows = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line.startswith("{"):
                rows.append(json.loads(line))
    df = pd.DataFrame(rows)
    df["window_start"] = pd.to_datetime(df["window_start"])
    return df


def vs_peers(wide):
    # Each district divided by the median of the other districts in the same window.
    out = pd.DataFrame(index=wide.index, columns=wide.columns, dtype=float)
    for d in wide.columns:
        out[d] = wide[d] / wide.drop(columns=d).median(axis=1)
    return out


def runs(flag):
    # Consecutive True windows -> list of (start, end, length).
    grp = (flag != flag.shift()).cumsum()
    found = []
    for _, g in flag[flag].groupby(grp[flag]):
        found.append((g.index[0], g.index[-1], len(g)))
    return found


df = load(FEATURES)
print("feature rows:", len(df), "| districts:", df["district_id"].nunique(),
      "| windows:", df["window_start"].nunique())

flow = df.pivot(index="window_start", columns="district_id", values="avg_flow_m3_s").sort_index()
pres = df.pivot(index="window_start", columns="district_id", values="min_pressure_m").sort_index()
rel_flow = vs_peers(flow)
rel_pres = vs_peers(pres)

# 1) Distribution of relative flow over all district-windows
vals = rel_flow.stack()
pct = [0.001, 0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99, 0.999]
print("\nRelative flow (district / median of the others) - percentiles:")
print(vals.quantile(pct).round(3).to_string())
print("min:", round(vals.min(), 3), "| max:", round(vals.max(), 3))

# 2) Typical offset of each district (structural differences between districts)
print("\nMedian relative flow per district:")
print(rel_flow.median().round(3).to_string())

# 3) Threshold sensitivity on |relative flow - 1|
print("\nThreshold sensitivity (two-sided, |rel_flow - 1| > t):")
summary = []
for t in THRESHOLDS:
    flags = (rel_flow - 1).abs() > t
    all_runs = [(d, *r) for d in flags.columns for r in runs(flags[d])]
    summary.append({
        "t": t,
        "windows_above": int(flags.sum().sum()),
        "runs": len(all_runs),
        "one_window_runs": sum(1 for r in all_runs if r[3] == 1),
        "longest_run_windows": max((r[3] for r in all_runs), default=0),
        "districts": ",".join(sorted({r[0] for r in all_runs})),
    })
print(pd.DataFrame(summary).to_string(index=False))

# 4) Detail at the chosen threshold
flags = (rel_flow - 1).abs() > DETAIL
detail = []
for d in flags.columns:
    for start, end, n in runs(flags[d]):
        seg = rel_flow[d].loc[start:end]
        peak = seg.loc[(seg - 1).abs().idxmax()]
        pseg = rel_pres[d].loc[start:end]
        detail.append({
            "district": d,
            "start": start,
            "windows": n,
            "hours": n * 0.25,
            "peak_rel_flow": round(peak, 2),
            "min_rel_pressure": round(pseg.min(), 2) if pseg.notna().any() else None,
        })
print(f"\nRuns at threshold {DETAIL}:")
print(pd.DataFrame(detail).sort_values("start").to_string(index=False))

# 5) Pressure versus peers: how often does it drop at all?
pv = rel_pres.stack()
print("\nRelative pressure (min pressure / median of the others):")
for c in PRESSURE_CUTS:
    print(f"  windows below {c}: {int((pv < c).sum())}")
print("  min:", round(pv.min(), 3), "| windows with missing pressure:", int(pres.isna().sum().sum()))

# 6) Data-quality columns
print("\nData quality in features:")
print("  windows with n_events != 15:", int((df["n_events"] != 15).sum()))
print("  windows with null pressure:", int((df["n_null_pressure"] > 0).sum()))
print("  windows with flagged quality:", int((df["n_flagged"] > 0).sum()))
