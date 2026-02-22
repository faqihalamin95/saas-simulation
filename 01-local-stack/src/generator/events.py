import numpy as np
import pandas as pd
import warnings
from faker import Faker
from .config import COUNTRIES, COUNTRY_TIMEZONE_MAP

fake = Faker()

def random_timestamp_in_month(month_start: pd.Timestamp) -> pd.Timestamp:
    """
    Calculates a random point in time within a specific month.
    Converts timestamps to Unix integers to allow random integer sampling.
    """
    start_ts = month_start.timestamp()
    # Find the boundary: start of the next month minus 1 second
    end_ts = (month_start + pd.DateOffset(months=1)).timestamp() - 1
    
    # Generate a random uniform integer between start and end
    random_ts = np.random.randint(start_ts, end_ts)
    return pd.to_datetime(random_ts, unit='s')

def local_to_utc(local_ts: pd.Timestamp, timezone_str: str) -> pd.Timestamp:
    try:
        return local_ts.tz_localize(
            timezone_str,
            nonexistent='shift_forward',
            ambiguous=False  # ← ganti dari 'infer' ke False
            # False = asumsikan waktu SETELAH DST "fall back" (winter time)
        ).tz_convert("UTC").tz_localize(None)
    except Exception as e:
        warnings.warn(
            f"[local_to_utc] Failed to localize timestamp '{local_ts}' "
            f"with timezone '{timezone_str}'. "
            f"Falling back to UTC. Reason: {e}",
            UserWarning,
            stacklevel=2
        )
        return local_ts.tz_localize("UTC").tz_localize(None)
        
def assign_country_timezone():
    """
    Randomly assign a country and its corresponding timezone.
    """
    country = np.random.choice(COUNTRIES)
    timezone = COUNTRY_TIMEZONE_MAP[country]
    return country, timezone

def generate_product_events(
    user_id: str,
    plan: str,
    current_month: pd.Timestamp,
    event_limit: int,
    timezone_str: str
):
    """
    Generate product usage events for a user in a given month.
    """
    if event_limit <= 0:
        return []

    # Randomize usage volume to make the data look realistic (not everyone uses it the same)
    usage_count = np.random.randint(1, event_limit + 1)
    events = []

    for _ in range(usage_count):
        local_ts = random_timestamp_in_month(current_month)
        utc_ts = local_to_utc(local_ts, timezone_str)

        events.append({
            "event_id": fake.uuid4(),
            "user_id": user_id,
            "event_type": "product_usage",
            "plan": plan,
            "event_timestamp_local": local_ts,
            "event_timestamp_utc": utc_ts,
            "batch_month": current_month.strftime("%Y-%m"),
        })

    return events

def generate_payment_timestamp(current_month: pd.Timestamp, timezone_str: str):
    """
    Generate payment timestamp (local + utc).
    """
    local_ts = random_timestamp_in_month(current_month)
    utc_ts = local_to_utc(local_ts, timezone_str)
    return local_ts, utc_ts