import numpy as np
import pandas as pd
import pytz
from datetime import timedelta
from faker import Faker

from .config import COUNTRY_TIMEZONE_MAP

fake = Faker()


# =========================================================
# RANDOM TIMESTAMP GENERATOR (LOCAL TIME)
# =========================================================

def random_timestamp_in_month(current_month: pd.Timestamp) -> pd.Timestamp:
    """
    Generate random timestamp within given month (local naive).
    """
    start = current_month
    end = (current_month + pd.offsets.MonthEnd(1))

    random_days = np.random.randint(0, (end - start).days + 1)
    random_seconds = np.random.randint(0, 24 * 60 * 60)

    ts = start + timedelta(days=random_days, seconds=random_seconds)
    return ts


# =========================================================
# TIMEZONE ASSIGNMENT
# =========================================================

def assign_country_timezone():
    """
    Randomly assign country and timezone.
    """
    country = np.random.choice(list(COUNTRY_TIMEZONE_MAP.keys()))
    timezone_str = COUNTRY_TIMEZONE_MAP[country]
    return country, timezone_str


# =========================================================
# LOCAL → UTC CONVERSION
# =========================================================

def local_to_utc(local_ts: pd.Timestamp, timezone_str: str) -> pd.Timestamp:
    """
    Convert naive local timestamp to UTC.
    """
    tz = pytz.timezone(timezone_str)
    localized = tz.localize(local_ts)
    utc_ts = localized.astimezone(pytz.utc)
    return utc_ts


# =========================================================
# GENERATE PRODUCT USAGE EVENTS
# =========================================================

def generate_product_events(
    user_id: str,
    plan: str,
    current_month: pd.Timestamp,
    event_limit: int,
    timezone_str: str
):
    """
    Generate product usage events for a user in a given month.
    Returns list of dictionaries.
    """

    usage_count = np.random.randint(1, event_limit + 1)

    events = []

    for _ in range(usage_count):
        local_ts = random_timestamp_in_month(current_month)
        utc_ts = local_to_utc(local_ts, timezone_str)

        events.append({
            "event_id": fake.uuid4(),
            "user_id": user_id,
            "event_type": "product_usage",
            "event_timestamp_local": local_ts,
            "event_timestamp_utc": utc_ts,
            "batch_month": current_month.strftime("%Y-%m"),
        })

    return events


# =========================================================
# GENERATE PAYMENT TIMESTAMP
# =========================================================

def generate_payment_timestamp(current_month: pd.Timestamp, timezone_str: str):
    """
    Generate payment timestamp (local + utc).
    """
    local_ts = random_timestamp_in_month(current_month)
    utc_ts = local_to_utc(local_ts, timezone_str)

    return local_ts, utc_ts
