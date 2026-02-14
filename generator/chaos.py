import copy
import random
from datetime import timedelta

import pandas as pd

from .config import CHAOS_EVENTS, get_month_index

DUPLICATE_RATE = 0.02
LATE_EVENT_RATE = 0.05
NULL_FIELD_RATE = 0.01
TIME_SHIFT_RATE = 0.03


def inject_late_events(events, ts_field="event_timestamp_local"):
    """
    Shift events 1 month forward to simulate late-arriving data.
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


def inject_duplicates(events: list, duplicate_rate: float = DUPLICATE_RATE) -> list:
    duplicated = []

    for event in events:
        duplicated.append(event)

        if random.random() < duplicate_rate:
            duplicated.append(copy.deepcopy(event))

    return duplicated


def inject_time_shift(events, ts_field="event_timestamp_local"):
    """
    Randomly shift events by ±12 hours to simulate time jitter.
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


def apply_chaos(events, current_month, dataset_name, ts_field="event_timestamp_local"):
    """
    Apply default chaos every month + scheduled monthly chaos from config.
    """
    if not events:
        return events

    events = inject_late_events(events, ts_field=ts_field)
    events = inject_time_shift(events, ts_field=ts_field)

    chaos_name = CHAOS_EVENTS.get(get_month_index(current_month))

    if chaos_name == "rename_plan":
        for event in events:
            if "plan" in event and event["plan"] == "Pro":
                event["plan"] = "Pro Plus"

    elif chaos_name == "add_column":
        for event in events:
            event["ingestion_source"] = "simulator_v2"

    elif chaos_name == "duplicate_payments" and dataset_name == "payments":
        events = inject_duplicates(events)

    elif chaos_name == "datatype_change" and dataset_name == "payments":
        for event in events:
            if "amount_usd" in event and event["amount_usd"] is not None:
                event["amount_usd"] = str(event["amount_usd"])

    return events
