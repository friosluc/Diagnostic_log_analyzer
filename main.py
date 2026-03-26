import json
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path
from parser import load_log
from alerts import detect_alerts_from_rules
from analysis import (
    detect_events_no_groupby,
    detect_composite_events,
    detect_event_sequence,
    compute_event_metrics,
    compute_event_rate,
    compute_rate_baseline,
    summarize,
    avg_rpm,
    count_engine_temp_over,
)
from config import (
    ENGINE_TEMP_THRESHOLD,
    RPM_THRESHOLD,
    MIN_EVENT_DURATION_SECONDS,
    COMPOSITE_MIN_OVERLAP_SECONDS,
    ALERT_RULES,
)
from reporting import build_system_summary, export_results

def add_duration_anomaly_flags(events_df):
    p90 = events_df.groupby("signal")["duration_s"].quantile(0.90)
    events_df = events_df.copy()
    events_df["p90_duration_s"] = events_df["signal"].map(p90)
    events_df["is_long_anomaly"] = events_df["duration_s"] >= events_df["p90_duration_s"]
    return events_df

def main():
    HERE = Path(__file__).resolve().parent
    csv_path = HERE / "sample_log.csv"

    df = load_log(csv_path)

    print(df.dtypes)

    report = summarize(df)
    report["avg_rpm"] = avg_rpm(df)
    report["engine_temp_over_100"] = count_engine_temp_over(df, ENGINE_TEMP_THRESHOLD)

    temp_events = detect_events_no_groupby(df, "ENGINE_TEMP", ENGINE_TEMP_THRESHOLD, MIN_EVENT_DURATION_SECONDS)
    report["engine_temp_over_100_events_no_groupby"] = len(temp_events)
    report["engine_temp_over_100_event_list"] = temp_events

    rpm_events = detect_events_no_groupby(df, "RPM", RPM_THRESHOLD, MIN_EVENT_DURATION_SECONDS)
    report["rpm_over_2000_events"] = len(rpm_events)
    report["rpm_over_2000_event_list"] = rpm_events

    print(json.dumps(report, indent=4))

    events_df = pd.DataFrame(temp_events + rpm_events)
    events_df["start"] = pd.to_datetime(events_df["start"])
    events_df["end"] = pd.to_datetime(events_df["end"])

    events_df = add_duration_anomaly_flags(events_df)
    print(events_df[["signal", "duration_s", "p90_duration_s", "is_long_anomaly"]])
    print(events_df.groupby("signal")["duration_s"].mean())
    print(events_df)

    # === Llama las gráficas ===
    plot_signal_with_events(df, events_df, "ENGINE_TEMP", ENGINE_TEMP_THRESHOLD)
    plot_signal_with_events(df, events_df, "RPM", RPM_THRESHOLD)

    composite_events = detect_composite_events(events_df, "ENGINE_TEMP", "RPM", "OVERHEAT_UNDER_LOAD")

    composite_df=pd.DataFrame(composite_events)
    print(composite_df)

    sequence_events = detect_event_sequence(events_df, "RPM", "ENGINE_TEMP", "HIGH_RPM_THEN_HIGH_TEMP", 3)

    sequence_df = pd.DataFrame(sequence_events)
    print(sequence_df)

    metrics_df = compute_event_metrics(events_df)
    print(metrics_df)
    print("Total events in system:", len(events_df))

    event_rate_df = compute_event_rate(events_df)
                        
    print(event_rate_df)

    baseline_df = compute_rate_baseline(event_rate_df)
    print(baseline_df)

    alerts_df = detect_alerts_from_rules(composite_df, ALERT_RULES)
    print(alerts_df)

    system_summary = build_system_summary(
        df,
        events_df,
        composite_df,
        sequence_df,
        alerts_df,
        metrics_df,
    )

    print(system_summary)

    export_results(system_summary, alerts_df, events_df)

def plot_signal_with_events(df: pd.DataFrame, events_df: pd.DataFrame, signal_name: str, threshold: float):
        # Filtrar señal y ordenar
        s = df[df["signal"] == signal_name].copy()
        if s.empty:
            print(f"No data for signal: {signal_name}")
            return

        s = s.sort_values("timestamp")

        # Asegurar datetime
        s["timestamp"] = pd.to_datetime(s["timestamp"])
        events_df = events_df.copy()
        events_df["start"] = pd.to_datetime(events_df["start"])
        events_df["end"] = pd.to_datetime(events_df["end"])

        # Filtrar eventos de esa señal
        ev = events_df[events_df["signal"] == signal_name].copy()

        # Plot
        plt.figure()
        plt.plot(s["timestamp"], s["value"])
        plt.axhline(threshold, linestyle="--")
        plt.title(f"{signal_name} with detected events")
        plt.xlabel("Time")
        plt.ylabel("Value")

        # Sombrear eventos
        for _, row in ev.iterrows():
            plt.axvspan(row["start"], row["end"], alpha=0.2)

        plt.tight_layout()
        plt.show()
if __name__ == "__main__":
    main()