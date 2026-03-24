import json

def build_system_summary(
    df,
    events_df,
    composite_df,
    sequence_df,
    alerts_df,
    metrics_df
) -> dict:
    return {
        "total_rows": len(df),
        "total_signals": df["signal"].nunique(),
        "total_basic_events": len(events_df),
        "total_composite_events": len(composite_df),
        "total_sequence_events": len(sequence_df),
        "total_alerts": len(alerts_df),
        "low_alerts": int((alerts_df["severity"] == "LOW").sum()) if not alerts_df.empty else 0,
        "medium_alerts": int((alerts_df["severity"] == "MEDIUM").sum()) if not alerts_df.empty else 0,
        "high_alerts": int((alerts_df["severity"] == "HIGH").sum()) if not alerts_df.empty else 0,
        "total_abnormal_duration_s": float(events_df["duration_s"].sum()),
        "most_active_signal": metrics_df.sort_values("events", ascending=False).iloc[0]["signal"],
        "alerts_by_event": (alerts_df["event"].value_counts().to_dict() if not alerts_df.empty else {}),
    }

def export_results(
    system_summary: dict,
    alerts_df,
    events_df,
    output_prefix: str = "output"
):

    # summary
    with open(f"{output_prefix}_summary.json", "w") as f:
        json.dump(system_summary, f, indent=4)

    # alerts
    alerts_df.to_json(f"{output_prefix}_alerts.json", orient="records", date_format="iso")

    # events
    events_df.to_json(f"{output_prefix}_events.json", orient="records", date_format="iso")