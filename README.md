# Diagnostic Log Analyzer

A modular Python system for analyzing time-series telemetry data, detecting abnormal signal events, and generating rule-based alerts with severity classification.

---

## Overview

This project processes vehicle telemetry data (e.g., ENGINE_TEMP, RPM) and builds a structured pipeline to:

- Detect abnormal signal events based on configurable thresholds
- Identify relationships between signals (composite events)
- Analyze temporal patterns (event sequences)
- Compute metrics and event rates
- Trigger alerts using configurable rules with cooldown suppression
- Classify alert severity (LOW / MEDIUM / HIGH)
- Export structured results to JSON

---

## System Architecture

```
raw telemetry (CSV)
        ↓
   parser.py        → validates and loads data
        ↓
   analysis.py      → event detection, metrics, event rate, baseline
        ↓
   alerts.py        → rule-based alert engine (cooldown + severity)
        ↓
   reporting.py     → system summary + JSON export
        ↓
   main.py          → orchestrates the full pipeline
```

---

## Project Structure

```
diagnostic_log_analyzer/
│
├── config.py           # Thresholds, alert rules, severity bounds
├── parser.py           # CSV loader with validation
├── analysis.py         # Event detection, metrics, event rate, baseline
├── alerts.py           # Alert engine (rules, cooldown, severity)
├── reporting.py        # Summary generation and JSON export
├── main.py             # Pipeline orchestration
├── generate_log.py     # Synthetic telemetry data generator
└── sample_log.csv      # Sample input data
```

---

## Features

### Event Detection
- Threshold-based detection per signal (e.g., `ENGINE_TEMP > 100`)
- Minimum duration filtering to ignore noise
- Per-event metadata: `max_value`, `avg_value`, `duration_s`, `samples`

### Composite Events
- Detects overlapping signal events (e.g., high temp AND high RPM simultaneously → `OVERHEAT_UNDER_LOAD`)

### Temporal Analysis
- Event rate per minute
- Rolling averages
- Statistical baseline with 3σ anomaly threshold
- Sequence detection (event A followed by event B within a time gap)

### Alert Engine
- Configurable alert rules via `config.py`
- Sliding time-window event counting
- Cooldown suppression to avoid alert flooding
- Severity classification: `LOW` / `MEDIUM` / `HIGH`

### Reporting
- JSON export: events, alerts, system summary
- Aggregated metrics per signal
- Alert breakdown by severity and event type

---

## Example Output

**Alert:**
```json
{
  "alert": "OVERHEAT_UNDER_LOAD_FREQUENCY",
  "event": "OVERHEAT_UNDER_LOAD",
  "trigger_time": "2026-01-01T10:05:05",
  "count_in_window": 2,
  "window_minutes": 3,
  "cooldown_minutes": 5,
  "severity": "LOW"
}
```

**System Summary:**
```json
{
  "total_rows": 1200,
  "total_signals": 2,
  "total_basic_events": 4,
  "total_composite_events": 2,
  "total_sequence_events": 0,
  "total_alerts": 1,
  "low_alerts": 1,
  "medium_alerts": 0,
  "high_alerts": 0,
  "total_abnormal_duration_s": 74.0,
  "most_active_signal": "ENGINE_TEMP"
}
```

---

## Configuration

All system behavior is controlled via `config.py`:

```python
ENGINE_TEMP_THRESHOLD = 100
RPM_THRESHOLD = 2000
MIN_EVENT_DURATION_SECONDS = 2
LOW_MAX_DURATION = 15
MEDIUM_MAX_DURATION = 25

ALERT_RULES = [
    {
        "name": "OVERHEAT_UNDER_LOAD",
        "event": "OVERHEAT_UNDER_LOAD",
        "window_minutes": 3,
        "min_count": 2,
        "cooldown_minutes": 5
    }
]
```

---

## Key Concepts Practiced

- Time-series event detection (threshold + duration filtering)
- Sliding window pattern analysis
- Rule-based alerting with cooldown mechanisms
- Statistical baseline (mean + 3σ threshold)
- Modular system design with separation of concerns

---

## Roadmap

- [ ] Machine learning-based anomaly detection (classification using event features)
- [ ] Real-time streaming support
- [ ] Dashboard visualization
- [ ] Multi-signal correlation scoring

---

## Authorship

This project was built as part of a self-directed learning path in Python, data analysis, and intelligent systems.

Development was guided with the assistance of AI tools (ChatGPT, Claude) used as tutors and learning accelerators — all code was written, understood, and iterated on by the author.

**Learning path focus:** automotive diagnostics · data-driven decision systems · Python analytics pipelines
