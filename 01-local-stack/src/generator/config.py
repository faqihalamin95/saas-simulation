import pandas as pd

# =========================================================
# GLOBAL SETTINGS
# =========================================================
RANDOM_SEED = 42
START_MONTH = pd.Timestamp("2024-01-01")
END_MONTH = pd.Timestamp("2024-12-01")
MONTH_RANGE = pd.date_range(start=START_MONTH, end=END_MONTH, freq="MS")

INITIAL_USERS = 500
MIN_NEW_USERS = 50
MAX_NEW_USERS = 100

# =========================================================
# PRICING & LIMITS
# =========================================================
PLANS = ["Free", "Pro", "Business"]

PLAN_PRICES = {
    "Free": 0,
    "Pro": 15,
    "Business": 50,
    "Trial": 0,
    "Expired": 0,
    "Canceled": 0
}

EVENT_LIMITS = {
    "Free": 10,
    "Pro": 100,
    "Business": 300,
    "Trial": 50, # Trial users get some usage
    "Pro Plus": 100 # For Chaos Month 6
}

# =========================================================
# PROBABILITIES (STATE MACHINE)
# =========================================================
TRIAL_CONVERT_PROB = 0.4
CHURN_PROB = 0.05
UPGRADE_PROB = 0.10
DOWNGRADE_PROB = 0.05
PAYMENT_FAIL_PROB = 0.05
LATE_ARRIVING_PROB = 0.03
REACTIVATION_PROB = 0.08

# =========================================================
# TIMEZONE DISTRIBUTION
# =========================================================
COUNTRY_TIMEZONE_MAP = {
    "US": "America/New_York",
    "UK": "Europe/London",
    "DE": "Europe/Berlin",
    "IN": "Asia/Kolkata",
    "JP": "Asia/Tokyo",
    "BR": "America/Sao_Paulo",
}

COUNTRIES = list(COUNTRY_TIMEZONE_MAP.keys())

# =========================================================
# CHAOS INJECTION SCHEDULE (CONTRACT LOCKED)
# =========================================================
CHAOS_EVENTS = {
    6: "rename_plan",       # Month 6
    8: "add_column",        # Month 8
    10: "duplicate_payments", # Month 10
    12: "datatype_change",  # Month 12
}

# =========================================================
# OUTPUT CONFIG
# =========================================================
BASE_OUTPUT_PATH = "01-local-stack/data/raw"

SUBSCRIPTION_PATH = f"{BASE_OUTPUT_PATH}/subscription_events"
PAYMENTS_PATH = f"{BASE_OUTPUT_PATH}/payments"
PRODUCT_EVENTS_PATH = f"{BASE_OUTPUT_PATH}/product_events"
USERS_PATH = f"{BASE_OUTPUT_PATH}/users"

# =========================================================
# HELPER
# =========================================================
def get_month_index(current_month: pd.Timestamp) -> int:
    return (current_month.year - START_MONTH.year) * 12 + \
           (current_month.month - START_MONTH.month) + 1