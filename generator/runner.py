import numpy as np

from .chaos import apply_chaos
from .config import (
    INITIAL_USERS,
    MAX_NEW_USERS,
    MIN_NEW_USERS,
    MONTH_RANGE,
    RANDOM_SEED,
)
from .lifecycle import generate_user_lifecycle
from .writer import write_parquet


def run_pipeline():
    np.random.seed(RANDOM_SEED)

    print(f"Generating initial users: {INITIAL_USERS}")
    users = generate_user_lifecycle(INITIAL_USERS, start_month=MONTH_RANGE[0])

    all_subs, all_pays, all_prods = [], [], []

    for idx, current_month in enumerate(MONTH_RANGE):
        if idx > 0:
            new_users = int(np.random.randint(MIN_NEW_USERS, MAX_NEW_USERS + 1))
            users.extend(generate_user_lifecycle(new_users, start_month=current_month))
            print(f"[{current_month.strftime('%Y-%m')}] Added new users: {new_users}")

        month_subs, month_pays, month_prods = [], [], []

        for user in users:
            user.process_month(current_month)
            subs, pays, prods = user.collect_and_reset_monthly_events()
            month_subs.extend(subs)
            month_pays.extend(pays)
            month_prods.extend(prods)

        month_subs = apply_chaos(
            month_subs,
            current_month=current_month,
            dataset_name="user_lifecycle",
            ts_field="event_timestamp_local",
        )
        month_prods = apply_chaos(
            month_prods,
            current_month=current_month,
            dataset_name="product_events",
            ts_field="event_timestamp_local",
        )
        month_pays = apply_chaos(
            month_pays,
            current_month=current_month,
            dataset_name="payments",
            ts_field="payment_timestamp_local",
        )

        all_subs.extend(month_subs)
        all_pays.extend(month_pays)
        all_prods.extend(month_prods)

        print(
            f"[{current_month.strftime('%Y-%m')}] month events -> "
            f"subs: {len(month_subs)}, pays: {len(month_pays)}, prods: {len(month_prods)}"
        )

    write_parquet(all_subs, "user_lifecycle")
    write_parquet(all_prods, "product_events")
    write_parquet(all_pays, "payments", ts_field="payment_timestamp_local")

    print("Done.")


if __name__ == "__main__":
    run_pipeline()
