import copy
import random
import pandas as pd

from .config import CHAOS_EVENTS, LATE_ARRIVING_PROB, get_month_index

DUPLICATE_RATE = 0.02
LATE_EVENT_RATE = LATE_ARRIVING_PROB

def inject_late_events(events, ts_field="event_timestamp_utc"):
    """
    Simulates data arriving later than the actual event time.
    Shifts a random subset of events 1 month into the future.
    
    Returns a NEW list without modifying the original.
    """
    if not events:
        return []

    # Create a deep copy to avoid mutating the original
    events_copy = copy.deepcopy(events)
    
    # Determine how many records will be delayed
    n = int(len(events_copy) * LATE_EVENT_RATE)
    if n == 0:
        return events_copy

    # Select random indices and apply the 1-month offset
    idxs = random.sample(range(len(events_copy)), n)
    for i in idxs:
        events_copy[i][ts_field] = events_copy[i][ts_field] + pd.DateOffset(months=1)

    return events_copy


def inject_duplicates(events: list, duplicate_rate: float = DUPLICATE_RATE) -> list:
    """
    Simulates a duplication bug (e.g., a retry logic error or upstream glitch).
    Returns a NEW list with duplicated records.
    """
    if not events:
        return []
    
    duplicated = []
    for event in events:
        # Always include the original
        duplicated.append(copy.deepcopy(event))
        # Randomly decide if this specific record should be duplicated
        if random.random() < duplicate_rate:
            duplicated.append(copy.deepcopy(event))
    
    return duplicated


def apply_chaos(events, current_month, dataset_name, ts_field="event_timestamp_utc"):
    """
    The main orchestrator that decides which 'Chaos Scenario' to trigger
    based on the current month in the simulation timeline.
    
    Returns a NEW list with chaos applied.
    """
    if not events:
        return []

    # Work on a copy to maintain immutability
    events = copy.deepcopy(events)
    
    # 1. LATENCY (Always On): Randomly delays a small % of data every month.
    events = inject_late_events(events, ts_field=ts_field)

    # Identify if there is a specific scheduled chaos event for this month
    chaos_name = CHAOS_EVENTS.get(get_month_index(current_month))

    # 2. DATA EVOLUTION: Renaming a categorical value (Simulates product change)
    if chaos_name == "rename_plan":
        for event in events:
            if "plan" in event and event["plan"] == "Pro":
                event["plan"] = "Pro Plus"

    # 3. SCHEMA CHANGE: Adding new unexpected columns (Simulates upstream API update)
    elif chaos_name == "add_column":
        for event in events:
            event["ingestion_source"] = "simulator_v2"
            event["promo_code"] = "Q3_LAUNCH"

    # 4. VOLUMETRIC ANOMALY: Injecting a spike of duplicate records
    elif chaos_name == "duplicate_payments" and dataset_name == "payments":
        events = inject_duplicates(events)

    # 5. TYPE MISMATCH: Changing a numeric field to a string (The "pipeline killer")
    elif chaos_name == "datatype_change" and dataset_name == "payments":
        for event in events:
            if "amount_usd" in event and event["amount_usd"] is not None:
                event["amount_usd"] = str(event["amount_usd"])

    return events