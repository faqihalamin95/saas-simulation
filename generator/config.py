import pandas as pd

# =========================================================
# GLOBAL SIMULATION WINDOW
# =========================================================

START_MONTH = pd.to_datetime("2024-01-01")
END_MONTH = pd.to_datetime("2025-12-01")  # 24 months inclusive

MONTH_RANGE = pd.date_range(
    START_MONTH,
    END_MONTH,
    freq="MS"  # Month Start
)

# =========================================================
# RANDOM SEED (Deterministic Simulation)
# =========================================================

RANDOM_SEED = 42

# =========================================================
# USER GROWTH RULES
# =========================================================

INITIAL_USERS = 5000
MIN_NEW_USERS = 300
MAX_NEW_USERS = 800

# =========================================================
# PLANS & PRICING
# =========================================================

PLANS = ["Free", "Pro", "Business"]

PLAN_PRICES = {
    "Free": 0,
    "Pro": 15,
    "Business": 50,
}

# =========================================================
# PRODUCT USAGE LIMITS (Per Month)
# =========================================================

EVENT_LIMITS = {
    "Free": 10,
    "Pro": 100,
    "Business": 300,
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

# =========================================================
# TIMEZONE DISTRIBUTION (Multi-Region Simulation)
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
# CHAOS INJECTION SCHEDULE (Month Index Based)
# Month index starts from 1
# =========================================================

CHAOS_EVENTS = {
    6: "rename_plan",
    8: "add_column",
    10: "duplicate_payments",
    12: "datatype_change",
}

# =========================================================
# OUTPUT CONFIG
# =========================================================

BASE_OUTPUT_PATH = "raw"

SUBSCRIPTION_PATH = f"{BASE_OUTPUT_PATH}/user_lifecycle"
PAYMENTS_PATH = f"{BASE_OUTPUT_PATH}/payments"
PRODUCT_EVENTS_PATH = f"{BASE_OUTPUT_PATH}/product_events"
USERS_PATH = f"{BASE_OUTPUT_PATH}/users"

# =========================================================
# HELPER: GET MONTH INDEX
# =========================================================


def get_month_index(current_month: pd.Timestamp) -> int:
    """
    Returns month index starting from 1.
    Example:
        2024-01 -> 1
        2024-02 -> 2
    """
    return (current_month.year - START_MONTH.year) * 12 + \
           (current_month.month - START_MONTH.month) + 1
