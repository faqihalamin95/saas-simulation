import glob
import os
import pandas as pd

RAW_PATH = "data/raw"
OUTPUT_CSV = "data/reports/full_validation.csv"

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
unique_emails = users["email"].nunique()
duplicate_emails = len(users) - unique_emails
trial_start = subs[subs["event_type"] == "trial_start"].shape[0]
trial_convert = subs[subs["event_type"] == "trial_convert"].shape[0]
trial_expire = subs[subs["event_type"] == "trial_expire"].shape[0]
canceled = subs[subs["event_type"] == "cancel"].shape[0]
upgrade_events = subs[subs["event_type"] == "upgrade"].shape[0]
downgrade_events = subs[subs["event_type"] == "downgrade"].shape[0]
reactivation_events = subs[subs["event_type"] == "reactivate"].shape[0]
channel_dist = users["acquisition_channel"].value_counts(normalize=True).round(2)

# Check Late Arriving Chaos (Always On)
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
anomaly_failed_3x_count = len(users_failed_3x - canceled_users)

# ------------------------------
# 3. PRODUCT EVENTS CHECKS
# ------------------------------
limits = {"Free": 10, "Pro": 100, "Business": 300, "Trial": 50, "Pro Plus": 100}
prod_counts = prods.groupby(["user_id", "plan", "batch_month"]).size().reset_index(name="n_events")

# Vectorized approach (lebih efisien dari apply + lambda)
prod_counts["limit"] = prod_counts["plan"].map(limits).fillna(0)
exceed_limit = prod_counts[prod_counts["n_events"] > prod_counts["limit"]]
exceed_limit_count = exceed_limit.shape[0]

# ------------------------------
# 4. CHAOS EVENTS CHECKS
# ------------------------------

# Month 6: Plan Rename — "Pro" → "Pro Plus"
month6_pro_plus = subs[
    (subs["batch_month"] == "2024-06") & (subs["plan"] == "Pro Plus")
].shape[0]

# Month 8: Add Column — ingestion_source & promo_code
month8_subs = subs[subs["batch_month"] == "2024-08"]
has_ingestion_source = "ingestion_source" in month8_subs.columns
has_promo_code = "promo_code" in month8_subs.columns
month8_new_columns_ok = has_ingestion_source and has_promo_code

if month8_new_columns_ok:
    month8_ingestion_values = month8_subs["ingestion_source"].dropna().unique().tolist()
    month8_promo_values = month8_subs["promo_code"].dropna().unique().tolist()
else:
    month8_ingestion_values = []
    month8_promo_values = []

# Month 10: Duplicate Payments
month10_pays = pays[pays["batch_month"] == "2024-10"]
month10_total = len(month10_pays)
month10_unique = month10_pays["payment_id"].nunique()
month10_duplicates = month10_total - month10_unique

# Month 12: Datatype Change — amount_usd jadi string
month12_pays = pays[pays["batch_month"] == "2024-12"]
if not month12_pays.empty:
    month12_dtype = str(month12_pays["amount_usd"].dtype)
    month12_is_string = month12_dtype == "object"
else:
    month12_dtype = "N/A"
    month12_is_string = False

# ------------------------------
# PRINT RESULTS
# ------------------------------
print("\n=== FULL DATA VALIDATION ===")

print("\n--- SUBSCRIPTION EVENTS ---")
print(f"Unique users             : {unique_users}")
print(f"Duplicate emails         : {duplicate_emails} (should be 0)")
print(f"Total subscription events: {total_subs_events}")
print(f"Trial start              : {trial_start}")
print(f"Trial convert            : {trial_convert}")
print(f"Trial expire             : {trial_expire}")
print(f"Canceled events          : {canceled}")
print(f"Upgrade events           : {upgrade_events}")
print(f"Downgrade events         : {downgrade_events}")
print(f"Late arriving events     : {late_arrivals}")
print(f"Reactivation events      : {reactivation_events}")


print(f"\nAcquisition Channel Distribution:")
for channel, pct in channel_dist.items():
    print(f"  {channel:<20}: {pct:.0%}")

print("\n--- PAYMENTS ---")
print(f"Successful               : {success_payments}")
print(f"Failed                   : {failed_payments}")
print(f"Anomaly (failed 3x, not canceled — should be 0): {anomaly_failed_3x_count}")

print("\n--- PRODUCT EVENTS ---")
print(f"Users exceeding plan limit: {exceed_limit_count}")

print("\n--- CHAOS EVENTS ---")
print(f"[Month 6]  Pro Plus events (rename_plan)     : {month6_pro_plus} events")
print(f"[Month 8]  New columns detected (add_column) : ingestion_source={has_ingestion_source}, promo_code={has_promo_code}")
if month8_new_columns_ok:
    print(f"           ingestion_source values           : {month8_ingestion_values}")
    print(f"           promo_code values                 : {month8_promo_values}")
print(f"[Month 10] Duplicate payments                : {month10_duplicates} duplicates from {month10_total} records")
print(f"[Month 12] amount_usd dtype                  : {month12_dtype} | is_string={month12_is_string}")

# ------------------------------
# EXPORT SUMMARY CSV
# ------------------------------
os.makedirs(os.path.dirname(OUTPUT_CSV), exist_ok=True)

summary_df = pd.DataFrame({
    "unique_users": [unique_users],
    "duplicate_emails": [duplicate_emails],
    "total_subs_events": [total_subs_events],
    "trial_start": [trial_start],
    "trial_convert": [trial_convert],
    "trial_expire": [trial_expire],
    "canceled": [canceled],
    "upgrade_events": [upgrade_events],
    "downgrade_events": [downgrade_events],
    "reactivation_events": [reactivation_events],
    "late_arrivals": [late_arrivals],
    "success_payments": [success_payments],
    "failed_payments": [failed_payments],
    "anomaly_failed_3x": [anomaly_failed_3x_count],
    "exceed_limit_count": [exceed_limit_count],
    # Chaos events
    "chaos_month6_pro_plus": [month6_pro_plus],
    "chaos_month8_columns_ok": [month8_new_columns_ok],
    "chaos_month10_duplicates": [month10_duplicates],
    "chaos_month12_is_string": [month12_is_string],
})

summary_df.to_csv(OUTPUT_CSV, index=False)
print(f"\nFull validation CSV exported to {OUTPUT_CSV}")