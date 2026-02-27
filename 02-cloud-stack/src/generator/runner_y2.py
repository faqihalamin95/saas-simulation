"""
runner_y2.py
────────────
Year 2 pipeline runner.

Usage
-----
Option A — carry over Y1 users from their snapshot parquet:
    python -m generator.runner_y2

Option B — start fresh with INITIAL_USERS_Y2 = 1339 dummy users
    (used when Y1 snapshot is unavailable):
    python -m generator.runner_y2 --no-carry-over
"""

import argparse
import glob
import os

import numpy as np
import pandas as pd

from .chaos_y2 import apply_chaos_y2
from .config_y2 import (
    BASE_OUTPUT_PATH_Y2,
    INITIAL_USERS_Y2,
    MONTH_RANGE_Y2,
    NEW_USERS_BY_MONTH,
    RANDOM_SEED,
    START_MONTH_Y2,
)
from .lifecycle_y2 import (
    carry_over_users_from_y1,
    generate_user_lifecycle_y2,
    generate_users_snapshot_y2,
)

# Writer is stateless — reuse the one from Y1 but point to Y2 output path
from .writer import write_parquet as _write_parquet_base


def write_parquet_y2(events, dataset_name, ts_field="event_timestamp_utc"):
    """Thin wrapper: writes to BASE_OUTPUT_PATH_Y2 instead of Y1 path."""
    import pandas as pd
    from datetime import datetime

    if not events:
        print(f"[{dataset_name}] No data to write.")
        return

    df = pd.DataFrame(events)

    if ts_field not in df.columns:
        alt = "event_timestamp_utc" if ts_field != "event_timestamp_utc" else "payment_timestamp_utc"
        if alt in df.columns:
            ts_field = alt
        else:
            raise ValueError(f"Timestamp field '{ts_field}' not found in events.")

    df[ts_field]    = pd.to_datetime(df[ts_field])
    df["event_date"] = df[ts_field].dt.date

    for event_date, group in df.groupby("event_date"):
        partition_path = os.path.join(BASE_OUTPUT_PATH_Y2, dataset_name, f"event_date={event_date}")
        os.makedirs(partition_path, exist_ok=True)
        file_name  = f"{dataset_name}_{int(datetime.now().timestamp())}_{event_date}.parquet"
        full_path  = os.path.join(partition_path, file_name)
        group.drop(columns=["event_date"]).to_parquet(full_path, index=False, engine="pyarrow")

    print(f"[{dataset_name}] Written {len(df)} records.")


def load_y1_snapshot() -> list:
    """
    Load the Y1 users snapshot parquet and return as list of dicts.
    Falls back to empty list if not found.
    """
    pattern = os.path.join("data", "raw", "users", "**", "*.parquet")
    files   = glob.glob(pattern, recursive=True)
    if not files:
        print("[runner_y2] WARNING: Y1 users snapshot not found. Will generate fresh users.")
        return []
    df = pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)
    print(f"[runner_y2] Loaded {len(df)} users from Y1 snapshot.")
    return df.to_dict(orient="records")


def run_pipeline_y2(carry_over: bool = True):
    np.random.seed(RANDOM_SEED)

    # ── Bootstrap initial user pool ──────────────────────
    if carry_over:
        y1_snapshot = load_y1_snapshot()
        if y1_snapshot:
            users = carry_over_users_from_y1(y1_snapshot, start_month=START_MONTH_Y2)
            print(f"[runner_y2] Carried over {len(users)} users from Y1.")
        else:
            users = generate_user_lifecycle_y2(INITIAL_USERS_Y2, start_month=START_MONTH_Y2)
            print(f"[runner_y2] Generated {len(users)} fresh initial users.")
    else:
        users = generate_user_lifecycle_y2(INITIAL_USERS_Y2, start_month=START_MONTH_Y2)
        print(f"[runner_y2] Generated {len(users)} fresh initial users (no carry-over).")

    # ── Monthly loop ─────────────────────────────────────
    for idx, current_month in enumerate(MONTH_RANGE_Y2):
        month_num = idx + 1   # 1-based

        # Add new users every month (volume from NEW_USERS_BY_MONTH)
        if idx > 0:
            lo, hi        = NEW_USERS_BY_MONTH.get(month_num, (50, 100))
            new_user_count = int(np.random.randint(lo, hi + 1))
            new_users      = generate_user_lifecycle_y2(new_user_count, start_month=current_month)
            users.extend(new_users)
            print(f"[{current_month.strftime('%Y-%m')}] Added new users: {new_user_count}")

        month_subs, month_pays, month_prods = [], [], []

        # Process lifecycle for every user
        for user in users:
            user.process_month(current_month)
            subs, pays, prods = user.collect_and_reset_monthly_events()
            month_subs.extend(subs)
            month_pays.extend(pays)
            month_prods.extend(prods)

        # Apply Y2 chaos
        month_subs = apply_chaos_y2(
            month_subs,
            current_month=current_month,
            dataset_name="subscription_events",
            ts_field="event_timestamp_utc",
        )
        month_prods = apply_chaos_y2(
            month_prods,
            current_month=current_month,
            dataset_name="product_events",
            ts_field="event_timestamp_utc",
        )
        month_pays = apply_chaos_y2(
            month_pays,
            current_month=current_month,
            dataset_name="payments",
            ts_field="payment_timestamp_utc",
        )

        # Write
        write_parquet_y2(month_subs,  "subscription_events", ts_field="event_timestamp_utc")
        write_parquet_y2(month_prods, "product_events",      ts_field="event_timestamp_utc")
        write_parquet_y2(month_pays,  "payments",            ts_field="payment_timestamp_utc")

        print(
            f"[{current_month.strftime('%Y-%m')}] "
            f"subs: {len(month_subs)}, pays: {len(month_pays)}, prods: {len(month_prods)}"
        )

        active  = sum(1 for u in users if u.status == "Active")
        churned = sum(1 for u in users if u.status == "Churned")
        print(
            f"[{current_month.strftime('%Y-%m')}] "
            f"Total: {len(users)} | Active: {active} | Churned: {churned}"
        )

    # ── Final snapshot ────────────────────────────────────
    write_parquet_y2(generate_users_snapshot_y2(users), "users", ts_field="created_at_utc")
    print("Y2 Pipeline Done.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--no-carry-over",
        action="store_true",
        help="Skip Y1 snapshot and generate fresh initial users instead.",
    )
    args  = parser.parse_args()
    run_pipeline_y2(carry_over=not args.no_carry_over)