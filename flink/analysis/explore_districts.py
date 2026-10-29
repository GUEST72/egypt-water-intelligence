import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import os

PATH = "Generator/output/run-72b96af88a342375/delivery/district-sensors.ndjson"
cols = ["district_id", "event_time_utc", "inlet_flow_m3_s", "pressure_m_h2o"]

df = pd.read_json(PATH, lines=True)[cols]
df["t"] = pd.to_datetime(df["event_time_utc"])
print("rows:", len(df), "| null pressure:", df["pressure_m_h2o"].isna().sum())

hourly = (df.set_index("t")
            .groupby("district_id")[["inlet_flow_m3_s", "pressure_m_h2o"]]
            .resample("1h").mean())
flow = hourly["inlet_flow_m3_s"].unstack(0)
pres = hourly["pressure_m_h2o"].unstack(0)

hod = flow.index.hour
ratio = flow / flow.groupby(hod).transform("median")

os.makedirs("analysis", exist_ok=True)

plots = [
    ("flink/analysis/flow.png", flow, "Flow (m3/s)"),
    ("flink/analysis/pressure.png", pres, "Pressure (m H2O)"),
    ("flink/analysis/flow_ratio.png", ratio, "Flow Ratio")
]

for filename, data, ylabel in plots:
    plt.figure(figsize=(10, 5))
    data.plot(ax=plt.gca(), legend=False)
    plt.ylabel(ylabel)
    plt.xlabel("Time")
    plt.title(filename)
    plt.savefig(filename, bbox_inches="tight")
    plt.close()

print("saved: flink/analysis/flow.png, flink/analysis/pressure.png, flink/analysis/flow_ratio.png")
