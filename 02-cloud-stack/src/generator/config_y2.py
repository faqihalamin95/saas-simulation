import pandas as pd

# =========================================================
# YEAR 2 GLOBAL SETTINGS
# =========================================================
RANDOM_SEED = 84  # Different seed from Y1
START_MONTH_Y2 = pd.Timestamp("2025-01-01")
END_MONTH_Y2 = pd.Timestamp("2025-12-01")
MONTH_RANGE_Y2 = pd.date_range(start=START_MONTH_Y2, end=END_MONTH_Y2, freq="MS")

# Carry over the final user count from Year 1
INITIAL_USERS_Y2 = 1339

# ── Growth pattern ──────────────────────────────────────
# M1–M6  : baseline organic
# M7–M9  : Q3, slow climb (pre-viral buzz)
# M10–M12: Q4, viral 10x spike
NEW_USERS_BY_MONTH = {
    1:  (50,  100),   # M1  baseline
    2:  (50,  100),
    3:  (50,  100),
    4:  (60,  110),
    5:  (60,  110),
    6:  (70,  130),
    7:  (100, 200),   # M7  Q3 slow climb
    8:  (150, 300),
    9:  (200, 400),
    10: (800, 1500),  # M10 viral hits
    11: (600, 1200),
    12: (400, 900),
}

# =========================================================
# YEAR 2 PRICING & PLANS
# New names + new prices (price hike is part of the chaos)
# =========================================================
PLANS_Y2 = ["Starter", "Growth", "Enterprise"]

PLAN_PRICES_Y2 = {
    "Starter":   0,
    "Growth":    25,    # was Pro @ $15
    "Enterprise": 80,   # was Business @ $50
    "Trial":     0,
    "Expired":   0,
    "Canceled":  0,
}

EVENT_LIMITS_Y2 = {
    "Starter":    10,
    "Growth":    100,
    "Enterprise": 300,
    "Trial":      50,
}

# Dirty strings that slip through during plan migration
DIRTY_PLAN_VARIANTS = [
    "growth", "GROWTH", "Growht", "growth_plan", "Growth ",
    "Pro",         # old name leaked
    "starter",     # lowercase
    "Enterprise_v2",
]

# =========================================================
# ACQUISITION CHANNELS (referral weight goes up in Y2)
# =========================================================
ACQUISITION_CHANNELS = ["organic", "paid_ads", "referral", "email_campaign", "viral_share"]
ACQUISITION_CHANNEL_WEIGHTS = [0.25, 0.30, 0.25, 0.10, 0.10]

# =========================================================
# PROBABILITIES — same base as Y1
# =========================================================
TRIAL_CONVERT_PROB = 0.4
CHURN_PROB         = 0.05
UPGRADE_PROB       = 0.10
DOWNGRADE_PROB     = 0.05
PAYMENT_FAIL_PROB  = 0.05
LATE_ARRIVING_PROB = 0.03
REACTIVATION_PROB  = 0.08

# =========================================================
# LOCALE / TIMEZONE — same as Y1
# =========================================================
COUNTRY_FAKER_LOCALE = {
    "US": "en_US",
    "UK": "en_GB",
    "DE": "de_DE",
    "IN": "hi_IN",
    "JP": "ja_JP",
    "BR": "pt_BR",
}

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
# CHAOS SCHEDULE — YEAR 2
#
#  M3  → plan_migration   : plan rename + price change, dirty strings
#  M8  → referral_noise   : referral_code field appears, inconsistent format
#  M10 → viral_spike      : timestamp collision + null spike on plan
#  M12 → compounding      : referral chaos + null spike intensifies
# =========================================================
CHAOS_EVENTS_Y2 = {
    3:  "plan_migration",
    8:  "referral_noise",
    10: "viral_spike",
    12: "compounding",
}

# =========================================================
# OUTPUT — separate folder from Y1
# =========================================================
BASE_OUTPUT_PATH_Y2 = "data/raw_y2"

# =========================================================
# HELPER
# =========================================================
def get_month_index_y2(current_month: pd.Timestamp) -> int:
    return (current_month.year - START_MONTH_Y2.year) * 12 + \
           (current_month.month - START_MONTH_Y2.month) + 1