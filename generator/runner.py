from .lifecycle import generate_user_lifecycle
from .chaos import apply_chaos
from .writer import write_parquet
from .config import INITIAL_USERS, START_MONTH


def run_pipeline():

    print("Generating user lifecycle...")
    users = generate_user_lifecycle(INITIAL_USERS)

    # ====================================================
    # PROCESS EACH MONTH
    # ====================================================
    # untuk sekarang bisa pakai single month START_MONTH
    # nanti bisa loop multi-bulan jika mau incremental

    all_subs, all_pays, all_prods = [], [], []

    for user in users:
        # jalankan monthly process
        user.process_month(START_MONTH)
        subs, pays, prods = user.collect_and_reset_monthly_events()
        all_subs.extend(subs)
        all_pays.extend(pays)
        all_prods.extend(prods)

    print("Applying chaos to lifecycle...")
    all_subs = apply_chaos(all_subs)

    print("Applying chaos to product events...")
    all_prods = apply_chaos(all_prods)

    print("Writing lifecycle to raw...")
    write_parquet(all_subs, "user_lifecycle")

    print("Writing product events to raw...")
    write_parquet(all_prods, "product_events")

    print("Writing payments to raw...")
    write_parquet(all_pays, "payments", ts_field="payment_timestamp_local")

    print("Done.")


if __name__ == "__main__":
    run_pipeline()
