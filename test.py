import pandas as pd
import glob
import os
from datetime import datetime

RAW_PATH = "../raw"
OUTPUT_CSV = "../raw/data_full_validation.csv"

# ------------------------------
# Load all parquet files
# ------------------------------
subs = pd.concat([pd.read_parquet(f) for f in glob.glob(os.path.join(RAW_PATH, "user_lifecycle/**/*.parquet"), recursive=True)])
prods = pd.concat([pd.read_parquet(f) for f in glob.glob(os.path.join(RAW_PATH, "product_events/**/*.parquet"), recursive=True)])

# ------------------------------
# 1. USER LIFECYCLE CHECKS
# ------------------------------
unique_users = subs["user_id"].nunique()
total_subs_events = len(subs)

# 1a. Trial conversion vs expire
trial_start = subs[subs["event_type"]=="trial_start"].shape[0]
trial_convert = subs[subs["event_type"]=="trial_convert"].shape[0]
trial_expire = subs[subs["event_type"]=="trial_expire"].shape[0]

# 1b. Churn / cancel
canceled = subs[subs["event_type"]=="cancel"].shape[0]

# 1c. Plan transitions
upgrade_events = subs[subs["event_type"]=="upgrade"].shape[0]
downgrade_events = subs[subs["event_type"]=="downgrade"].shape[0]

# 1d. Month 6 rename Pro → Pro Plus
month6_pro_plus = subs[(subs["batch_month"]=="2024-06") & (subs["plan"]=="Pro Plus")].shape[0]

# 1e. Late arriving events (batch_month > event month)
# asumsi batch_month disimpan di subs["batch_month"] dan event month dari event_timestamp_local
subs["event_month"] = subs["event_timestamp_local"].dt.strftime("%Y-%m")
late_arrivals = subs[subs["batch_month"] != subs["event_month"]].shape[0]

# ------------------------------
# 2. PAYMENT CHECKS
# ------------------------------
if "status" in prods.columns:
    payment_counts = prods["status"].value_counts()
    failed_payments = payment_counts.get("failed", 0)
    success_payments = payment_counts.get("success", 0)

    # Users who failed 3x but still Active → should be 0
    user_failed_3x = prods.groupby("user_id")["status"].apply(lambda x: (x=="failed").sum())
    anomaly_failed_3x = user_failed_3x[(user_failed_3x>=3) & (subs.set_index("user_id").loc[user_failed_3x.index]["status"]=="Active")]
    anomaly_failed_3x_count = anomaly_failed_3x.shape[0]
else:
    failed_payments = success_payments = anomaly_failed_3x_count = 0

# ------------------------------
# 3. PRODUCT EVENTS CHECKS
# ------------------------------
# Max events per plan
limits = {"Free":10, "Pro":100, "Business":300, "Trial":0}
prod_counts = prods.groupby(["user_id","plan","batch_month"]).size().reset_index(name="n_events")
# Users exceeding plan limit
exceed_limit = prod_counts[prod_counts.apply(lambda x: x["n_events"] > limits.get(x["plan"],0), axis=1)]
exceed_limit_count = exceed_limit.shape[0]

# ------------------------------
# PRINT TERMINAL SUMMARY
# ------------------------------
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
print(f"Users failed 3x but still Active: {anomaly_failed_3x_count}")

print("\n--- PRODUCT EVENTS ---")
print(f"Users exceeding plan limit: {exceed_limit_count}")

# ------------------------------
# EXPORT CSV
# ------------------------------
summary_dict = {
    "unique_users":[unique_users],
    "total_subscription_events":[total_subs_events],
    "trial_start":[trial_start],
    "trial_convert":[trial_convert],
    "trial_expire":[trial_expire],
    "canceled":[canceled],
    "upgrade_events":[upgrade_events],
    "downgrade_events":[downgrade_events],
    "month6_pro_plus":[month6_pro_plus],
    "late_arrivals":[late_arrivals],
    "successful_payments":[success_payments],
    "failed_payments":[failed_payments],
    "anomaly_failed_3x":[anomaly_failed_3x_count],
    "exceed_product_limit":[exceed_limit_count]
}

summary_df = pd.DataFrame(summary_dict)
summary_df.to_csv(OUTPUT_CSV, index=False)
print(f"\nFull validation CSV exported to {OUTPUT_CSV}")
