import glob
import os
import pandas as pd

RAW_PATH = "01-local-stack/data/raw"
OUTPUT_CSV = "01-local-stack/data/reports/full_validation.csv"

def load_parquet_glob(pattern: str) -> pd.DataFrame:
    files = glob.glob(os.path.join(RAW_PATH, pattern), recursive=True)
    if not files:
        return pd.DataFrame()
    return pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)

print("Loading datasets...")
subs = load_parquet_glob("subscription_events/**/*.parquet")
prods = load_parquet_glob("product_events/**/*.parquet")
pays = load_parquet_glob("payments/**/*.parquet")
users = load_parquet_glob("users/**/*.parquet")

if subs.empty or prods.empty or pays.empty or users.empty:
    raise ValueError("Missing required raw datasets. Run generator/runner.py first.")

# ------------------------------
# 1. SUBSCRIPTION EVENTS CHECKS
# ------------------------------
unique_users = users["user_id"].nunique()
total_subs_events = len(subs)

trial_start = subs[subs["event_type"] == "trial_start"].shape[0]
trial_convert = subs[subs["event_type"] == "trial_convert"].shape[0]
trial_expire = subs[subs["event_type"] == "trial_expire"].shape[0]
canceled = subs[subs["event_type"] == "cancel"].shape[0]
upgrade_events = subs[subs["event_type"] == "upgrade"].shape[0]
downgrade_events = subs[subs["event_type"] == "downgrade"].shape[0]

# Check Plan Rename Chaos (Month 6)
month6_pro_plus = subs[(subs["batch_month"] == "2024-06") & (subs["plan"] == "Pro Plus")].shape[0]

# Check Late Arriving Chaos
subs["event_timestamp_utc"] = pd.to_datetime(subs["event_timestamp_utc"])
subs["event_month"] = subs["event_timestamp_utc"].dt.strftime("%Y-%m")
late_arrivals = subs[subs["batch_month"] != subs["event_month"]].shape[0]

# ------------------------------
# 2. PAYMENT CHECKS
# ------------------------------
payment_counts = pays["status"].value_counts()
failed_payments = int(payment_counts.get("failed", 0))
success_payments = int(payment_counts.get("success", 0))

# Check 3x Fail Logic
user_failed_3x = pays.groupby("user_id")["status"].apply(lambda x: (x == "failed").sum())
users_failed_3x = set(user_failed_3x[user_failed_3x >= 3].index)
canceled_users = set(subs.loc[subs["event_type"] == "cancel", "user_id"])
# Anomaly if failed 3x but NOT canceled
anomaly_failed_3x_count = len(users_failed_3x - canceled_users)

# ------------------------------
# 3. PRODUCT EVENTS CHECKS
# ------------------------------
limits = {"Free": 10, "Pro": 100, "Business": 300, "Trial": 50, "Pro Plus": 100}
prod_counts = prods.groupby(["user_id", "plan", "batch_month"]).size().reset_index(name="n_events")
exceed_limit = prod_counts[prod_counts.apply(lambda x: x["n_events"] > limits.get(x["plan"], 0), axis=1)]
exceed_limit_count = exceed_limit.shape[0]

print("\n=== FULL DATA VALIDATION ===")
print(f"Unique users: {unique_users}")
print(f"Total subscription events: {total_subs_events}")
print(f"Trial start: {trial_start}, convert: {trial_convert}, expire: {trial_expire}")
print(f"Canceled events: {canceled}")
print(f"Upgrade events: {upgrade_events}, Downgrade events: {downgrade_events}")
print(f"Month 6 Pro Plus events (Chaos): {month6_pro_plus}")
print(f"Late arriving events (Chaos): {late_arrivals}")

print("\n--- PAYMENTS ---")
print(f"Successful: {success_payments}, Failed: {failed_payments}")
print(f"Anomaly: Users failed 3x but NOT canceled (Should be 0): {anomaly_failed_3x_count}")

print("\n--- PRODUCT EVENTS ---")
print(f"Users exceeding plan limit: {exceed_limit_count}")

summary_df = pd.DataFrame({
    "unique_users": [unique_users],
    "trial_convert": [trial_convert],
    "trial_expire": [trial_expire],
    "canceled": [canceled],
    "month6_pro_plus": [month6_pro_plus],
    "late_arrivals": [late_arrivals],
    "anomaly_failed_3x": [anomaly_failed_3x_count],
})
summary_df.to_csv(OUTPUT_CSV, index=False)
print(f"\nFull validation CSV exported to {OUTPUT_CSV}")