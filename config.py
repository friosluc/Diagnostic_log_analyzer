ENGINE_TEMP_THRESHOLD = 100
RPM_THRESHOLD = 2000

MIN_EVENT_DURATION_SECONDS = 2

COMPOSITE_MIN_OVERLAP_SECONDS = 1

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