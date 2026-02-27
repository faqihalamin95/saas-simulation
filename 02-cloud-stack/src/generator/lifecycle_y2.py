import numpy as np
from faker import Faker
import pandas as pd

from .config_y2 import (
    PLANS_Y2,
    PLAN_PRICES_Y2,
    EVENT_LIMITS_Y2,
    TRIAL_CONVERT_PROB,
    CHURN_PROB,
    UPGRADE_PROB,
    DOWNGRADE_PROB,
    PAYMENT_FAIL_PROB,
    START_MONTH_Y2,
    REACTIVATION_PROB,
    COUNTRY_FAKER_LOCALE,
    ACQUISITION_CHANNELS,
    ACQUISITION_CHANNEL_WEIGHTS,
)

from .events import (          # reuse unchanged utility functions from Y1
    random_timestamp_in_month,
    local_to_utc,
    generate_product_events,
    generate_payment_timestamp,
    assign_country_timezone,
)

fake = Faker()


class UserLifecycleY2:
    """
    State machine for a single user — Year 2.
    Structurally identical to Y1 but uses Y2 plan names & prices.
    """

    def __init__(self, user_id, country, timezone_str, start_month,
                 carry_plan=None, carry_status=None):
        self.user_id      = user_id
        self.country      = country
        self.timezone_str = timezone_str
        self.created_at   = start_month

        # Carry-over state from Y1 if provided
        self.current_plan   = carry_plan   or "Trial"
        self.status         = carry_status or "Active"
        self.failed_payments = 0

        self.subscription_events = []
        self.payments            = []
        self.product_events      = []

        # Only fire trial_start for brand-new users (no carry-over plan)
        if carry_plan is None:
            self._add_subscription_event("trial_start", "Trial", start_month)

        locale     = COUNTRY_FAKER_LOCALE.get(country, "en_US")
        fake_local = Faker(locale)
        self.name  = fake_local.name()
        self.email = (
            f"{Faker('en_US').user_name()}_{self.user_id[:8]}"
            f"@{Faker('en_US').free_email_domain()}"
        )

        self.acquisition_channel = np.random.choice(
            ACQUISITION_CHANNELS,
            p=ACQUISITION_CHANNEL_WEIGHTS,
        )

    # ─────────────────────────────────────────────────────
    #  INTERNAL HELPERS
    # ─────────────────────────────────────────────────────

    def _add_subscription_event(self, event_type, plan, current_month):
        local_ts = random_timestamp_in_month(current_month)
        utc_ts   = local_to_utc(local_ts, self.timezone_str)
        self.subscription_events.append({
            "event_id":               fake.uuid4(),
            "user_id":                self.user_id,
            "event_type":             event_type,
            "plan":                   plan,
            "event_timestamp_local":  local_ts,
            "event_timestamp_utc":    utc_ts,
            "country":                self.country,
            "batch_month":            current_month.strftime("%Y-%m"),
        })

    def _add_payment(self, current_month, amount):
        local_ts, utc_ts = generate_payment_timestamp(current_month, self.timezone_str)
        is_success       = np.random.rand() > PAYMENT_FAIL_PROB

        if not is_success:
            self.failed_payments += 1
            status = "failed"
        else:
            self.failed_payments = 0
            status = "success"

        self.payments.append({
            "payment_id":              fake.uuid4(),
            "user_id":                 self.user_id,
            "amount_usd":              amount,
            "status":                  status,
            "attempt_number":          self.failed_payments if not is_success else 1,
            "payment_timestamp_local": local_ts,
            "payment_timestamp_utc":   utc_ts,
            "batch_month":             current_month.strftime("%Y-%m"),
        })

        if self.failed_payments >= 3:
            self.status       = "Churned"
            self.current_plan = "Canceled"
            self._add_subscription_event("cancel", "Canceled", current_month)

        return is_success

    # ─────────────────────────────────────────────────────
    #  PLAN TRANSITIONS  (Y2 plan names)
    # ─────────────────────────────────────────────────────

    def _maybe_upgrade_or_downgrade(self, current_month):
        if self.current_plan not in PLANS_Y2:
            return

        if np.random.rand() < UPGRADE_PROB:
            if self.current_plan == "Starter":
                new_plan = "Growth"
            elif self.current_plan == "Growth":
                new_plan = "Enterprise"
            else:
                return
            self.current_plan = new_plan
            self._add_subscription_event("upgrade", new_plan, current_month)

        elif np.random.rand() < DOWNGRADE_PROB:
            if self.current_plan == "Enterprise":
                new_plan = "Growth"
            elif self.current_plan == "Growth":
                new_plan = "Starter"
            else:
                return
            self.current_plan = new_plan
            self._add_subscription_event("downgrade", new_plan, current_month)

    # ─────────────────────────────────────────────────────
    #  MONTHLY PROCESS
    # ─────────────────────────────────────────────────────

    def process_month(self, current_month):
        if self.status != "Active":
            self._maybe_reactivate(current_month)
            return

        # TRIAL LOGIC
        if self.current_plan == "Trial":
            if np.random.rand() < TRIAL_CONVERT_PROB:
                self.current_plan = "Growth"   # Y2: converts to Growth (was Pro)
                self._add_subscription_event("trial_convert", "Growth", current_month)
            else:
                self.status       = "Churned"
                self.current_plan = "Expired"
                self._add_subscription_event("trial_expire", "Expired", current_month)
                return

        # PAYMENT LOGIC
        if self.current_plan in PLANS_Y2 and self.current_plan != "Starter":
            amount = PLAN_PRICES_Y2.get(self.current_plan, 0)
            if amount > 0:
                self._add_payment(current_month, amount)

        if self.status != "Active":
            return

        # RANDOM CHURN
        if np.random.rand() < CHURN_PROB:
            self.status       = "Churned"
            self.current_plan = "Canceled"
            self._add_subscription_event("cancel", "Canceled", current_month)
            return

        # PLAN CHANGE
        self._maybe_upgrade_or_downgrade(current_month)

        # PRODUCT USAGE
        if self.current_plan in PLANS_Y2 or self.current_plan == "Trial":
            limit        = EVENT_LIMITS_Y2.get(self.current_plan, 0)
            usage_events = generate_product_events(
                self.user_id,
                self.current_plan,
                current_month,
                limit,
                self.timezone_str,
            )
            self.product_events.extend(usage_events)

    # ─────────────────────────────────────────────────────
    #  EXPORT & CLEAR
    # ─────────────────────────────────────────────────────

    def collect_and_reset_monthly_events(self):
        subs  = self.subscription_events
        pays  = self.payments
        prods = self.product_events
        self.subscription_events = []
        self.payments            = []
        self.product_events      = []
        return subs, pays, prods

    def _maybe_reactivate(self, current_month):
        if self.status != "Churned" or self.current_plan != "Canceled":
            return
        if np.random.rand() < REACTIVATION_PROB:
            self.status          = "Active"
            self.current_plan    = "Starter"   # Y2: reactivates on Starter (was Free)
            self.failed_payments = 0
            self._add_subscription_event("reactivate", "Starter", current_month)


# ─────────────────────────────────────────────────────────
#  FACTORY FUNCTIONS
# ─────────────────────────────────────────────────────────

def generate_user_lifecycle_y2(n_users: int, start_month: pd.Timestamp = START_MONTH_Y2):
    """Generate brand-new Y2 users (for monthly intake)."""
    users = []
    for _ in range(n_users):
        user_id          = fake.uuid4()
        country, timezone = assign_country_timezone()
        users.append(UserLifecycleY2(user_id, country, timezone, start_month))
    return users


# Y1 plan → nearest Y2 plan (for carry-over mapping)
_CARRY_PLAN_MAP = {
    "Free":     "Starter",
    "Pro":      "Growth",
    "Pro Plus": "Growth",
    "Business": "Enterprise",
    "Trial":    "Trial",
    "Expired":  "Expired",
    "Canceled": "Canceled",
}

def carry_over_users_from_y1(y1_snapshot: list, start_month: pd.Timestamp = START_MONTH_Y2):
    """
    Convert a list of Y1 user snapshot dicts into Y2 UserLifecycleY2 objects.
    Maps old plan names to new ones.
    """
    users = []
    for row in y1_snapshot:
        mapped_plan = _CARRY_PLAN_MAP.get(row.get("current_plan"), "Starter")
        user = UserLifecycleY2(
            user_id     = row["user_id"],
            country     = row["country"],
            timezone_str= row["timezone"],
            start_month = start_month,
            carry_plan  = mapped_plan,
            carry_status= row.get("current_status", "Active"),
        )
        # Preserve original name / email / channel instead of re-generating
        user.name                = row.get("name", user.name)
        user.email               = row.get("email", user.email)
        user.acquisition_channel = row.get("acquisition_channel", user.acquisition_channel)
        users.append(user)
    return users


def generate_users_snapshot_y2(users):
    """Snapshot of users for the Y2 dimension table."""
    from .events import random_timestamp_in_month
    snapshot = []
    for user in users:
        created_at_utc = local_to_utc(
            random_timestamp_in_month(user.created_at),
            user.timezone_str,
        )
        snapshot.append({
            "user_id":              user.user_id,
            "name":                 user.name,
            "email":                user.email,
            "acquisition_channel":  user.acquisition_channel,
            "country":              user.country,
            "timezone":             user.timezone_str,
            "current_status":       user.status,
            "current_plan":         user.current_plan,
            "created_at_utc":       created_at_utc,
        })
    return snapshot