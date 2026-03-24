import pandas as pd
import numpy as np

start = pd.Timestamp("2026-01-01 10:00:00")

rows = []

for i in range(600):  # 600 seconds = 10 minutes
    ts = start + pd.Timedelta(seconds=i)

    # ENGINE_TEMP base
    temp = 95 + np.random.normal(0, 1)

    # occasional overheating
    if 120 <= i <= 140 or 300 <= i <= 330:
        temp = 102 + np.random.normal(0, 1)

    # RPM base
    rpm = 1800 + np.random.normal(0, 50)

    # occasional high RPM
    if 125 <= i <= 135 or 305 <= i <= 315:
        rpm = 2200 + np.random.normal(0, 50)

    rows.append({"timestamp": ts, "signal": "ENGINE_TEMP", "value": temp})
    rows.append({"timestamp": ts, "signal": "RPM", "value": rpm})

df = pd.DataFrame(rows)

df.to_csv("sample_log.csv", index=False)

print("Log generated with", len(df), "rows")