import pandas as pd
from visualization import plot_signal_with_events
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

    temp_events = detect_events_no_groupby(df, "ENGINE_TEMP", ENGINE_TEMP_THRESHOLD, MIN_EVENT_DURATION_SECONDS)

    rpm_events = detect_events_no_groupby(df, "RPM", RPM_THRESHOLD, MIN_EVENT_DURATION_SECONDS)
    
    events_df = pd.DataFrame(temp_events + rpm_events)
    events_df["start"] = pd.to_datetime(events_df["start"])
    events_df["end"] = pd.to_datetime(events_df["end"])

    events_df = add_duration_anomaly_flags(events_df)
    print(events_df[["signal", "duration_s", "p90_duration_s", "is_long_anomaly"]])

    # === Llama las gráficas ===
    plot_signal_with_events(df, events_df, "ENGINE_TEMP", ENGINE_TEMP_THRESHOLD)
    plot_signal_with_events(df, events_df, "RPM", RPM_THRESHOLD)

    composite_events = detect_composite_events(events_df, "ENGINE_TEMP", "RPM", "OVERHEAT_UNDER_LOAD", COMPOSITE_MIN_OVERLAP_SECONDS)

    composite_df=pd.DataFrame(composite_events)

    sequence_events = detect_event_sequence(events_df, "RPM", "ENGINE_TEMP", "HIGH_RPM_THEN_HIGH_TEMP", 3)

    sequence_df = pd.DataFrame(sequence_events)

    metrics_df = compute_event_metrics(events_df)
    print("Total events in system:", len(events_df))

    event_rate_df = compute_event_rate(events_df)

    baseline_df = compute_rate_baseline(event_rate_df)

    alerts_df = detect_alerts_from_rules(composite_df, ALERT_RULES)

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

if __name__ == "__main__":
    main()