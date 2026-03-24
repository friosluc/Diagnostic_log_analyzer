import pandas as pd
from config import LOW_MAX_DURATION, MEDIUM_MAX_DURATION

def classify_severity(duration_s: float) -> str:
    if duration_s < LOW_MAX_DURATION:
        return "LOW"
    elif duration_s <= MEDIUM_MAX_DURATION:
        return "MEDIUM"
    else:
        return "HIGH"


def detect_temporal_alerts(
    composite_df: pd.DataFrame,
    event_name: str,
    window_minutes: int = 3,
    min_count: int = 2,
    cooldown_minutes: int = 5
) -> pd.DataFrame:

    alerts = []

    df = composite_df[composite_df["event"] == event_name].copy()
    df = df.sort_values("start")

    last_alert_time = None

    for _, row in df.iterrows():
        start_time = row["start"]
        window_start = start_time - pd.Timedelta(minutes=window_minutes)

        window_events = df[(df["start"] >= window_start) & (df["start"] <= start_time)]

        if len(window_events) >= min_count:
            if last_alert_time is None or (start_time - last_alert_time) >= pd.Timedelta(minutes=cooldown_minutes):
                alerts.append({
                    "alert": f"{event_name}_FREQUENCY",
                    "trigger_time": start_time,
                    "count_in_window": len(window_events),
                    "window_minutes": window_minutes,
                    "cooldown_minutes": cooldown_minutes,
                    "severity": classify_severity(row["duration_s"])
                })
                last_alert_time = start_time

    return pd.DataFrame(alerts)


def detect_alerts_from_rules(composite_df: pd.DataFrame, rules: list[dict]) -> pd.DataFrame:
    all_alerts = []

    for rule in rules:
        event_name = rule["event"]
        rule_name = rule["name"]
        window_minutes = rule["window_minutes"]
        min_count = rule["min_count"]
        cooldown_minutes = rule["cooldown_minutes"]

        df = composite_df[composite_df["event"] == event_name].copy()
        df = df.sort_values("start")

        last_alert_time = None

        for _, row in df.iterrows():
            start_time = row["start"]
            window_start = start_time - pd.Timedelta(minutes=window_minutes)

            window_events = df[(df["start"] >= window_start) & (df["start"] <= start_time)]

            if len(window_events) >= min_count:
                if last_alert_time is None or (start_time - last_alert_time) >= pd.Timedelta(minutes=cooldown_minutes):
                    all_alerts.append({
                        "alert": f"{rule_name}_FREQUENCY",
                        "event": event_name,
                        "trigger_time": start_time,
                        "count_in_window": len(window_events),
                        "window_minutes": window_minutes,
                        "cooldown_minutes": cooldown_minutes,
                        "severity": classify_severity(row["duration_s"])
                    })

                    last_alert_time = start_time

    return pd.DataFrame(all_alerts)