import pandas as pd

PATH = "Generator/output/run-72b96af88a342375/delivery/district-sensors.ndjson"
THRESH = 0.10  # exploration value only, not a final detection threshold

cols = ["district_id", "event_time_utc", "inlet_flow_m3_s", "pressure_m_h2o"]
df = pd.read_json(PATH, lines=True)[cols]
df["t"] = pd.to_datetime(df["event_time_utc"])

q = (df.set_index("t")
       .groupby("district_id")[["inlet_flow_m3_s", "pressure_m_h2o"]]
       .resample("15min").mean())
flow = q["inlet_flow_m3_s"].unstack(0)
pres = q["pressure_m_h2o"].unstack(0)

# Each district relative to the median of the other districts at the same time
rel = pd.DataFrame(index=flow.index, columns=flow.columns, dtype=float)
for d in flow.columns:
    rel[d] = flow[d] / flow.drop(columns=d).median(axis=1)

# Constant offset per district (a permanent leak would show up here)
print("Median relative flow per district (1.00 = same as the others):")
print(rel.median().round(3).to_string(), "\n")

rel_n = rel / rel.median()
rows = []
for d in rel_n.columns:
    flag = (rel_n[d] - 1).abs() > THRESH
    run_id = (flag != flag.shift()).cumsum()
    for _, grp in rel_n[d][flag].groupby(run_id[flag]):
        start, end = grp.index[0], grp.index[-1] + pd.Timedelta(minutes=15)
        peak_value = grp.loc[(grp - 1).abs().idxmax()]
        p = pres[d].loc[grp.index]
        rows.append({
            "district": d,
            "start_utc": start,
            "end_utc": end,
            "hours": round((end - start).total_seconds() / 3600, 2),
            "peak_rel_flow": round(peak_value, 2),
            "min_pressure_m": round(p.min(), 1),
            "pressure_vs_median": round(p.min() / pres[d].median(), 2),
        })

out = pd.DataFrame(rows).sort_values("start_utc")
print(out.to_string(index=False))
out.to_csv("flink/analysis/anomaly_windows.csv", index=False)
