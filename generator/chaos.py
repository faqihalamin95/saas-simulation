import copy
import random
import pandas as pd

from .config import CHAOS_EVENTS, LATE_ARRIVING_PROB, get_month_index

DUPLICATE_RATE = 0.02
LATE_EVENT_RATE = LATE_ARRIVING_PROB

def inject_late_events(events, ts_field="event_timestamp_utc"):
    """
    Shift a portion of events 1 month forward to simulate late-arriving data.
    (Contract: Late arriving - 1 mechanism only)
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
    """
    Simulate duplicate data spike.
    (Contract: 1 duplicate spike)
    """
    duplicated = []
    for event in events:
        duplicated.append(event)
        if random.random() < duplicate_rate:
            duplicated.append(copy.deepcopy(event))
    return duplicated

def apply_chaos(events, current_month, dataset_name, ts_field="event_timestamp_utc"):
    """
    Apply monthly chaos scenarios from blueprint.
    """
    if not events:
        return events

    # 1. Late Arriving (Always active mechanism)
    events = inject_late_events(events, ts_field=ts_field)

    # Check Scheduled Chaos
    chaos_name = CHAOS_EVENTS.get(get_month_index(current_month))

    # 2. Plan Rename (Month 6)
    if chaos_name == "rename_plan":
        for event in events:
            if "plan" in event and event["plan"] == "Pro":
                event["plan"] = "Pro Plus"

    # 3. Schema Change (Month 8 - Add Column)
    elif chaos_name == "add_column":
        for event in events:
            event["ingestion_source"] = "simulator_v2"
            event["promo_code"] = "Q3_LAUNCH"

    # 4. Duplicate Spike (Month 10)
    elif chaos_name == "duplicate_payments" and dataset_name == "payments":
        events = inject_duplicates(events)

    # 5. Datatype Change (Month 12)
    elif chaos_name == "datatype_change" and dataset_name == "payments":
        for event in events:
            if "amount_usd" in event and event["amount_usd"] is not None:
                event["amount_usd"] = str(event["amount_usd"])

    return events