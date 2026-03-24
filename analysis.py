import pandas as pd

# 1.- Imports

# 2.- Small helpers 
def summarize(df: pd.DataFrame) -> dict:
    return {
        "rows": int(len(df)),
        "signals": sorted(df["signal"].unique().tolist()),
    }

def count_engine_temp_over(df: pd.DataFrame, threshold: float = 100) -> int:
    hot = df[df["signal"] == "ENGINE_TEMP"]
    over = hot[hot["value"] > threshold]
    return int(len(over))

def avg_rpm(df: pd.DataFrame) -> float:
    rpm = df[df["signal"] == "RPM"]
    return float(rpm["value"].mean())


def engine_temp_events_over_threshold(df: pd.DataFrame, threshold=100, min_duration_seconds=2) -> int:
    temp = df[df["signal"] == "ENGINE_TEMP"].copy()
    if temp.empty:
        return 0

    temp = temp.sort_values("timestamp")
    temp["over"] = temp["value"] > threshold
    temp["over_prev"] = temp["over"].shift(1)

    # nuevo segmento si cambia el estado (False<->True) o si es la primera fila
    new_segment = (temp["over"] != temp["over_prev"]) | (temp["over_prev"].isna())
    temp["segment_id"] = new_segment.cumsum()

    events = 0
    for _, seg in temp[temp["over"]].groupby("segment_id"):
        start = seg["timestamp"].min()
        end = seg["timestamp"].max()
        duration = (end - start).total_seconds()
        if duration >= min_duration_seconds:
            events += 1

    return events

# 3.- Basic event detection

def events_over_threshold_no_groupby(df: pd.DataFrame, signal_name: str, threshold: float, min_duration_seconds: int = 2) -> int:
    s = df[df["signal"] == signal_name].copy()
    if s.empty:
        return 0

    s = s.sort_values("timestamp")
    s["over"] = s["value"] > threshold

    events = 0
    in_event = False
    event_start = None

    for ts, over in zip(s["timestamp"], s["over"]):
        if over and not in_event:
            in_event = True
            event_start = ts
        elif (not over) and in_event:
            duration = (ts - event_start).total_seconds()
            if duration >= min_duration_seconds:
                events += 1
            in_event = False
            event_start = None

    if in_event:
        last_ts = s["timestamp"].iloc[-1]
        duration = (last_ts - event_start).total_seconds()
        if duration >= min_duration_seconds:
            events += 1

    return events

def detect_events_no_groupby(df: pd.DataFrame, signal_name: str, threshold: float, min_duration_seconds: int = 2) -> list[dict]:
    s=df[df["signal"] == signal_name].copy()

    if s.empty:
        return []
    
    s=s.sort_values("timestamp")
    s["over"] = s["value"] > threshold

    events: list[dict] = []
    in_event = False
    event_start = None

    max_value = None
    sum_value = 0.0
    sample_count = 0

    for ts, over, value in zip(s["timestamp"], s["over"], s["value"]):
        if over and not in_event:
            #start event
            in_event = True
            event_start = ts

            max_value = value
            sum_value = float(value)
            sample_count = 1

        elif over and in_event:
            #the event continues
            if value > max_value:
                max_value=value
            sum_value += float(value)
            sample_count += 1
        
        elif not over and in_event:
            #end event
            duration = (ts - event_start).total_seconds()
            if duration >= min_duration_seconds:
                events.append({"signal": signal_name,
                               "start": event_start.isoformat(), 
                               "end": ts.isoformat(), 
                               "duration_s": duration, 
                               "max_value": float(max_value), 
                               "avg_value": float(sum_value/sample_count),
                               "samples": int(sample_count),
                               })
            in_event = False
            event_start = None
            max_value = None
            sum_value = 0.0
            sample_count = 0

    #if file ends and still in event
    if in_event:
        last_ts = s["timestamp"].iloc[-1]
        duration = (last_ts - event_start).total_seconds()
        if duration >= min_duration_seconds:
            events.append({
            "signal": signal_name,
            "start": event_start.isoformat(), 
            "end": last_ts.isoformat(), 
            "duration_s": duration, 
            "max_value": float(max_value), 
            "avg_value": float(sum_value/sample_count),
            "samples": int(sample_count),
            })

    return events

# 4.- Event relationships

def detect_composite_events(events_df: pd.DataFrame, signal_a: str, signal_b: str, composite_name: str, min_overlap_seconds: int = 1) -> list[dict]:
    a_events = events_df[events_df["signal"] == signal_a].copy()
    b_events = events_df[events_df["signal"] == signal_b].copy()

    composite_events = []

    for _, a in a_events.iterrows():
        for _, b in b_events.iterrows():
            start_a = a["start"]
            end_a = a["end"]

            start_b = b["start"]
            end_b = b["end"]

            overlap = (start_a <= end_b) and (start_b <= end_a)

            if overlap:
                overlap_start = max(start_a, start_b)
                overlap_end = min(end_a, end_b)
                duration = (overlap_end - overlap_start).total_seconds()

                if duration >= min_overlap_seconds:

                    composite_events.append({
                        "event": composite_name,
                        "signal_a": signal_a,
                        "signal_b": signal_b,
                        "start": overlap_start,
                        "end": overlap_end,
                        "duration_s": duration
                    })

    return composite_events
    
def detect_event_sequence(
        events_df: pd.DataFrame,
        first_signal: str,
        second_signal: str,
        sequence_name: str,
        max_gap_seconds: int
        ) -> list[dict]:
        first_events = events_df[events_df["signal"] == first_signal].copy()
        second_events = events_df[events_df["signal"] == second_signal].copy()

        sequence_events = []

        for _, a in first_events.iterrows():
            for _, b in second_events.iterrows():
                end_a = a["end"]
                start_b = b["start"]

                gap_s = (start_b - end_a).total_seconds()

                if 0 < gap_s <= max_gap_seconds:
                    sequence_events.append({
                        "event": sequence_name,
                        "first_signal": first_signal,
                        "second_signal": second_signal,
                        "first_end": end_a,
                        "second_start": start_b,
                        "gap_s": gap_s
                    })

        return sequence_events

# 5.- Metrics 

def compute_event_metrics(events_df: pd.DataFrame) -> pd.DataFrame:
    metrics = events_df.groupby("signal").agg(
        events = ("duration_s", "count"),
        avg_duration = ("duration_s", "mean"),
        max_duration = ("duration_s", "max"),
        min_duration = ("duration_s", "min"),
        total_duration = ("duration_s", "sum")
    )

    metrics = metrics.reset_index()

    return metrics

def compute_event_rate(events_df: pd.DataFrame) -> pd.DataFrame:

    df = events_df.copy()

    df["minute"] = df["start"].dt.floor("min")

    event_rate_df = (
        df.groupby(["signal", "minute"])
        .size()
        .reset_index(name="events_per_min")
    )

    event_rate_df = event_rate_df.sort_values(["signal", "minute"])

    event_rate_df["rolling_rate"] = (
        event_rate_df
        .groupby("signal")["events_per_min"]
        .rolling(window=3)
        .mean()
        .reset_index(level=0, drop=True)
    )

    return event_rate_df

def compute_rate_baseline(event_rate_df: pd.DataFrame) -> pd.DataFrame:
    baseline_df = event_rate_df.groupby("signal")["events_per_min"].agg(
        mean_rate="mean",
        std_rate="std",
    ).reset_index()

    baseline_df["threshold"] = baseline_df["mean_rate"]+3*baseline_df["std_rate"]

    return baseline_df
