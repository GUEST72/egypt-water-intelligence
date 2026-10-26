import pandas as pd

BASE = "Generator/output/run-bdc85c43085c4dc8/delivery/"

# Sensors: inlet volume per district per 15-minute window (window start as key)
s = pd.read_json(BASE + "district-sensors.ndjson", lines=True)
s = s[["district_id", "event_time_utc", "inlet_volume_m3"]]
s["win"] = pd.to_datetime(s["event_time_utc"]).dt.floor("15min")
inlet = s.groupby(["district_id", "win"])["inlet_volume_m3"].sum()
del s

# Meters: process chunk by chunk and keep only compact columns.
# event_id is replaced by a 64-bit hash so the long strings are not kept in memory.
parts, n_read = [], 0
reader = pd.read_json(BASE + "meter-readings.ndjson", lines=True, chunksize=100_000)
for ch in reader:
    n_read += len(ch)
    small = pd.DataFrame({
        "h": pd.util.hash_pandas_object(ch["event_id"], index=False).values,
        "district_id": ch["district_id"].astype("category"),
        "win": pd.to_datetime(ch["interval_start_utc"]).dt.floor("15min"),
        "consumed_volume_m3": ch["consumed_volume_m3"].astype("float64"),
    })
    parts.append(small)
    del ch, small

m = pd.concat(parts, ignore_index=True)
del parts
m["district_id"] = m["district_id"].astype(str)

# Deduplicate by event_id hash first, then count nulls (nulls stay null, not zero)
m = m.drop_duplicates(subset="h", keep="first")
print("meter rows read:", n_read,
      "| duplicates removed:", n_read - len(m),
      "| null consumption:", int(m["consumed_volume_m3"].isna().sum()))

metered = m.groupby(["district_id", "win"])["consumed_volume_m3"].sum()
del m

bal = pd.concat([inlet, metered], axis=1).dropna()
bal.columns = ["inlet_m3", "metered_m3"]
bal["ratio"] = bal["metered_m3"] / bal["inlet_m3"]
bal.to_csv("analysis/water_balance_15min.csv")

# Typical metered/inlet ratio per district (1.00 = every litre is metered)
base = bal.groupby("district_id")["ratio"].median()
print("\nMedian metered/inlet per district:")
print(base.round(3).to_string())

# Ratio inside each anomaly window versus the district's typical ratio
w = pd.read_csv("analysis/anomaly_windows.csv")
w["start_utc"] = pd.to_datetime(w["start_utc"], utc=True)
w["end_utc"] = pd.to_datetime(w["end_utc"], utc=True)

rows = []
for _, r in w.iterrows():
    d = bal.loc[r["district"]]
    sel = d[(d.index >= r["start_utc"]) & (d.index < r["end_utc"])]
    window_ratio = sel["metered_m3"].sum() / sel["inlet_m3"].sum()
    rows.append({
        "district": r["district"],
        "start_utc": r["start_utc"],
        "hours": r["hours"],
        "peak_rel_flow": r["peak_rel_flow"],
        "pressure_vs_median": r["pressure_vs_median"],
        "typical_ratio": round(base[r["district"]], 3),
        "window_ratio": round(window_ratio, 3),
        "unmetered_extra_pct": round((base[r["district"]] - window_ratio) * 100, 1),
    })
print()
print(pd.DataFrame(rows).to_string(index=False))
