import numpy as np
import pandas as pd
from faker import Faker
from .config import COUNTRIES, COUNTRY_TIMEZONE_MAP

fake = Faker()

def random_timestamp_in_month(month_start: pd.Timestamp) -> pd.Timestamp:
    """
    Generate a random timestamp within the given month.
    """
    start_ts = month_start.timestamp()
    # End of month roughly (start of next month - 1 second)
    end_ts = (month_start + pd.DateOffset(months=1)).timestamp() - 1
    random_ts = np.random.randint(start_ts, end_ts)
    return pd.to_datetime(random_ts, unit='s')

def local_to_utc(local_ts: pd.Timestamp, timezone_str: str) -> pd.Timestamp:
    """
    Convert local timestamp to UTC, handling DST gaps and ambiguities.
    """
    try:
        # 'nonexistent' akan menggeser waktu yang tidak ada akibat DST shift
        # 'ambiguous' akan menangani waktu yang muncul 2x saat jam mundur
        return local_ts.tz_localize(
            timezone_str, 
            nonexistent='shift_forward', 
            ambiguous='infer'
        ).tz_convert("UTC").tz_localize(None)
    except Exception:
        # Fallback sederhana jika terjadi error tak terduga
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