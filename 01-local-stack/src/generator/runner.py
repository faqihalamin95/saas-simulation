import numpy as np
from .chaos import apply_chaos
from .config import (
    INITIAL_USERS,
    MAX_NEW_USERS,
    MIN_NEW_USERS,
    MONTH_RANGE,
    RANDOM_SEED,
)
from .lifecycle import generate_user_lifecycle, generate_users_snapshot
from .writer import write_parquet

def run_pipeline():
    np.random.seed(RANDOM_SEED)

    print(f"Generating initial users: {INITIAL_USERS}")
    users = generate_user_lifecycle(INITIAL_USERS, start_month=MONTH_RANGE[0])

    for idx, current_month in enumerate(MONTH_RANGE):

        # 1. Add New Users Monthly
        if idx > 0:
            new_users_count = int(np.random.randint(MIN_NEW_USERS, MAX_NEW_USERS + 1))
            new_users = generate_user_lifecycle(new_users_count, start_month=current_month)
            users.extend(new_users)
            print(f"[{current_month.strftime('%Y-%m')}] Added new users: {new_users_count}")

        month_subs, month_pays, month_prods = [], [], []

        # 2. Process Monthly Lifecycle for Each User
        for user in users:
            user.process_month(current_month)
            subs, pays, prods = user.collect_and_reset_monthly_events()
            month_subs.extend(subs)
            month_pays.extend(pays)
            month_prods.extend(prods)

        # 3. Apply Chaos
        month_subs = apply_chaos(
            month_subs,
            current_month=current_month,
            dataset_name="subscription_events",
            ts_field="event_timestamp_utc",
        )
        month_prods = apply_chaos(
            month_prods,
            current_month=current_month,
            dataset_name="product_events",
            ts_field="event_timestamp_utc",
        )
        month_pays = apply_chaos(
            month_pays,
            current_month=current_month,
            dataset_name="payments",
            ts_field="payment_timestamp_utc",
        )

        # 4. ✅ Tulis langsung per bulan, tidak numpuk di memory
        write_parquet(month_subs, "subscription_events", ts_field="event_timestamp_utc")
        write_parquet(month_prods, "product_events", ts_field="event_timestamp_utc")
        write_parquet(month_pays, "payments", ts_field="payment_timestamp_utc")

        print(
            f"[{current_month.strftime('%Y-%m')}] month events -> "
            f"subs: {len(month_subs)}, pays: {len(month_pays)}, prods: {len(month_prods)}"
        )

        active_users = sum(1 for u in users if u.status == "Active")
        churned_users = sum(1 for u in users if u.status == "Churned")
        print(
            f"[{current_month.strftime('%Y-%m')}] "
            f"Total users: {len(users)} | "
            f"Active: {active_users} | "
            f"Churned: {churned_users}"
        )

    # 5. Snapshot Users (tetap di akhir — ini memang hanya sekali)
    write_parquet(generate_users_snapshot(users), "users", ts_field="created_at_utc")

    print("Pipeline Done.")

if __name__ == "__main__":
    run_pipeline()