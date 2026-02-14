import glob
import os

import pandas as pd

RAW_PATH = "raw"
OUTPUT_CSV = "raw/data_full_validation.csv"


def load_parquet_glob(pattern: str) -> pd.DataFrame:
    files = glob.glob(os.path.join(RAW_PATH, pattern), recursive=True)
    if not files:
        return pd.DataFrame()
    return pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)


subs = load_parquet_glob("user_lifecycle/**/*.parquet")
prods = load_parquet_glob("product_events/**/*.parquet")
pays = load_parquet_glob("payments/**/*.parquet")

if subs.empty or prods.empty or pays.empty:
    raise ValueError("Missing required raw datasets: user_lifecycle, product_events, or payments")

# ------------------------------
# 1. USER LIFECYCLE CHECKS
# ------------------------------
unique_users = subs["user_id"].nunique()
total_subs_events = len(subs)

trial_start = subs[subs["event_type"] == "trial_start"].shape[0]
trial_convert = subs[subs["event_type"] == "trial_convert"].shape[0]
trial_expire = subs[subs["event_type"] == "trial_expire"].shape[0]

canceled = subs[subs["event_type"] == "cancel"].shape[0]
upgrade_events = subs[subs["event_type"] == "upgrade"].shape[0]
downgrade_events = subs[subs["event_type"] == "downgrade"].shape[0]

month6_pro_plus = subs[(subs["batch_month"] == "2024-06") & (subs["plan"] == "Pro Plus")].shape[0]

subs["event_timestamp_local"] = pd.to_datetime(subs["event_timestamp_local"])
subs["event_month"] = subs["event_timestamp_local"].dt.strftime("%Y-%m")
late_arrivals = subs[subs["batch_month"] != subs["event_month"]].shape[0]

# ------------------------------
# 2. PAYMENT CHECKS
# ------------------------------
payment_counts = pays["status"].value_counts()
failed_payments = int(payment_counts.get("failed", 0))
success_payments = int(payment_counts.get("success", 0))

user_failed_3x = pays.groupby("user_id")["status"].apply(lambda x: (x == "failed").sum())
users_failed_3x = set(user_failed_3x[user_failed_3x >= 3].index)
canceled_users = set(subs.loc[subs["event_type"] == "cancel", "user_id"])
anomaly_failed_3x_count = len(users_failed_3x - canceled_users)

# ------------------------------
# 3. PRODUCT EVENTS CHECKS
# ------------------------------
limits = {"Free": 10, "Pro": 100, "Business": 300, "Trial": 0, "Pro Plus": 100}
prod_counts = prods.groupby(["user_id", "plan", "batch_month"]).size().reset_index(name="n_events")
exceed_limit = prod_counts[prod_counts.apply(lambda x: x["n_events"] > limits.get(x["plan"], 0), axis=1)]
exceed_limit_count = exceed_limit.shape[0]

print("=== FULL DATA VALIDATION ===")
print(f"Unique users: {unique_users}")
print(f"Total subscription events: {total_subs_events}")
print(f"Trial start: {trial_start}, convert: {trial_convert}, expire: {trial_expire}")
print(f"Canceled events: {canceled}")
print(f"Upgrade events: {upgrade_events}, Downgrade events: {downgrade_events}")
print(f"Month 6 Pro Plus events: {month6_pro_plus}")
print(f"Late arriving events: {late_arrivals}")

print("\n--- PAYMENTS ---")
print(f"Successful payments: {success_payments}, Failed payments: {failed_payments}")
print(f"Users failed 3x but never canceled: {anomaly_failed_3x_count}")

print("\n--- PRODUCT EVENTS ---")
print(f"Users exceeding plan limit: {exceed_limit_count}")

summary_df = pd.DataFrame({
    "unique_users": [unique_users],
    "total_subscription_events": [total_subs_events],
    "trial_start": [trial_start],
    "trial_convert": [trial_convert],
    "trial_expire": [trial_expire],
    "canceled": [canceled],
    "upgrade_events": [upgrade_events],
    "downgrade_events": [downgrade_events],
    "month6_pro_plus": [month6_pro_plus],
    "late_arrivals": [late_arrivals],
    "successful_payments": [success_payments],
    "failed_payments": [failed_payments],
    "anomaly_failed_3x": [anomaly_failed_3x_count],
    "exceed_product_limit": [exceed_limit_count],
})
summary_df.to_csv(OUTPUT_CSV, index=False)
print(f"\nFull validation CSV exported to {OUTPUT_CSV}")
