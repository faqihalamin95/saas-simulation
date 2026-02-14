import random
import pandas as pd
from datetime import timedelta
import copy


DUPLICATE_RATE = 0.02
LATE_EVENT_RATE = 0.05
NULL_FIELD_RATE = 0.01
TIME_SHIFT_RATE = 0.03

def inject_late_events(events, ts_field="event_timestamp_local"):
    """
    Shift ~3% events 1 bulan ke depan untuk mensimulasikan late-arriving data.
    """
    if not events:
        return events

    n = int(len(events) * LATE_EVENT_RATE)
    if n == 0:
        return events

    idxs = random.sample(range(len(events)), n)
    for i in idxs:
        events[i][ts_field] = events[i][ts_field] + pd.DateOffset(months=1)
    return events

def inject_duplicates(events: list) -> list:
    duplicated = []

    for event in events:
        duplicated.append(event)

        if random.random() < DUPLICATE_RATE:
            duplicated.append(copy.deepcopy(event))

    return duplicated


def inject_time_shift(events, ts_field="event_timestamp_local"):
    """
    Randomly shift ~3% events by ±12 hours to simulate time jitter.
    """
    if not events:
        return events

    n = int(len(events) * TIME_SHIFT_RATE)
    if n == 0:
        return events

    idxs = random.sample(range(len(events)), n)

    for i in idxs:
        events[i][ts_field] = events[i][ts_field] + timedelta(hours=random.randint(-12, 12))
    return events


def inject_null_fields(events: list) -> list:
    for event in events:
        if random.random() < NULL_FIELD_RATE:
            random_key = random.choice(list(event.keys()))
            event[random_key] = None
    return events

def apply_chaos(events):
    """
    Apply all chaos injections: late arriving, jitter, rename plan bulan ke-6.
    """
    events = inject_late_events(events, ts_field="event_timestamp_local")
    events = inject_time_shift(events, ts_field="event_timestamp_local")

    for event in events:
        if "plan" in event and event.get("batch_month") == "2024-06":
            if event["plan"] == "Pro":
                event["plan"] = "Pro Plus"

    return events

