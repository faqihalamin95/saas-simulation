import locale
import numpy as np
from faker import Faker
import pandas as pd

from .config import (
    PLANS,
    PLAN_PRICES,
    EVENT_LIMITS,
    TRIAL_CONVERT_PROB,
    CHURN_PROB,
    UPGRADE_PROB,
    DOWNGRADE_PROB,
    PAYMENT_FAIL_PROB,
    START_MONTH,
    REACTIVATION_PROB,
    COUNTRY_FAKER_LOCALE, 
    ACQUISITION_CHANNELS, 
    ACQUISITION_CHANNEL_WEIGHTS,
)

from .events import (
    random_timestamp_in_month,
    local_to_utc,
    generate_product_events,
    generate_payment_timestamp,
    assign_country_timezone,
)

fake = Faker()

class UserLifecycle:
    """
    State machine for a single user.
    """

    def __init__(self, user_id, country, timezone_str, start_month):
        self.user_id = user_id
        self.country = country
        self.timezone_str = timezone_str
        self.created_at = start_month  # Tracking creation for snapshot

        self.current_plan = "Trial"
        self.status = "Active"
        self.failed_payments = 0

        # store monthly outputs (cleared after write)
        self.subscription_events = []
        self.payments = []
        self.product_events = []

        # initial trial start event
        self._add_subscription_event(
            event_type="trial_start",
            plan="Trial",
            current_month=start_month
        )

        # Personalize name/email based on country for more realism
        locale = COUNTRY_FAKER_LOCALE.get(country, "en_US")
        fake_local = Faker(locale)
        self.name = fake_local.name()
        self.email = f"{Faker('en_US').user_name()}_{self.user_id[:8]}@{Faker('en_US').free_email_domain()}"

        # Assign acquisition channel based on weighted probabilities
        self.acquisition_channel = np.random.choice(
            ACQUISITION_CHANNELS, 
            p=ACQUISITION_CHANNEL_WEIGHTS
        )

    # =========================================================
    # INTERNAL EVENT HELPERS
    # =========================================================

    def _add_subscription_event(self, event_type, plan, current_month):
        local_ts = random_timestamp_in_month(current_month)
        utc_ts = local_to_utc(local_ts, self.timezone_str)

        self.subscription_events.append({
            "event_id": fake.uuid4(),
            "user_id": self.user_id,
            "event_type": event_type,
            "plan": plan,
            "event_timestamp_local": local_ts,
            "event_timestamp_utc": utc_ts,
            "country": self.country,
            "batch_month": current_month.strftime("%Y-%m"),
        })

    def _add_payment(self, current_month, amount):
        local_ts, utc_ts = generate_payment_timestamp(
            current_month,
            self.timezone_str
        )

        is_success = np.random.rand() > PAYMENT_FAIL_PROB

        if not is_success:
            self.failed_payments += 1
            status = "failed"
        else:
            self.failed_payments = 0
            status = "success"

        self.payments.append({
            "payment_id": fake.uuid4(),
            "user_id": self.user_id,
            "amount_usd": amount,
            "status": status,
            "attempt_number": self.failed_payments if not is_success else 1,
            "payment_timestamp_local": local_ts,
            "payment_timestamp_utc": utc_ts,
            "batch_month": current_month.strftime("%Y-%m"),
        })

        # Auto cancel after 3 failures
        if self.failed_payments >= 3:
            self.status = "Churned"
            self.current_plan = "Canceled"
            self._add_subscription_event(
                event_type="cancel",
                plan="Canceled",
                current_month=current_month
            )

        return is_success

    # =========================================================
    # PLAN TRANSITIONS
    # =========================================================

    def _maybe_upgrade_or_downgrade(self, current_month):
        if self.current_plan not in PLANS:
            return

        if np.random.rand() < UPGRADE_PROB:
            if self.current_plan == "Free":
                new_plan = "Pro"
            elif self.current_plan == "Pro":
                new_plan = "Business"
            else:
                return
            self.current_plan = new_plan
            self._add_subscription_event("upgrade", new_plan, current_month)

        elif np.random.rand() < DOWNGRADE_PROB:
            if self.current_plan == "Business":
                new_plan = "Pro"
            elif self.current_plan == "Pro":
                new_plan = "Free"
            else:
                return
            self.current_plan = new_plan
            self._add_subscription_event("downgrade", new_plan, current_month)

    # =========================================================
    # MONTHLY PROCESS
    # =========================================================

    def process_month(self, current_month):
        """
        Main monthly state transition.
        """

        # chance to reactivate if churned
        if self.status != "Active":
            self._maybe_reactivate(current_month)
            return

        # --- TRIAL LOGIC ---
        if self.current_plan == "Trial":
            # Simulate trial ending this month
            if np.random.rand() < TRIAL_CONVERT_PROB:
                self.current_plan = "Pro"
                self._add_subscription_event(
                    "trial_convert",
                    "Pro",
                    current_month
                )
            else:
                self.status = "Churned"
                self.current_plan = "Expired"
                self._add_subscription_event(
                    "trial_expire",
                    "Expired",
                    current_month
                )
                return

        # --- PAYMENT LOGIC ---
        if self.current_plan in PLANS and self.current_plan != "Free":
            amount = PLAN_PRICES.get(self.current_plan, 0)
            if amount > 0:
                success = self._add_payment(current_month, amount)
                if not success:
                    # Payment failed, skip usage for this simulation tick if strictly blocked?
                    # Contract says: fail + retry + auto cancel. 
                    # We let it slide here until 3x fails trigger cancel.
                    pass
        
        # If user was canceled by payment logic above, stop.
        if self.status != "Active":
            return

        # --- RANDOM CHURN ---
        if np.random.rand() < CHURN_PROB:
            self.status = "Churned"
            self.current_plan = "Canceled"
            self._add_subscription_event(
                "cancel",
                "Canceled",
                current_month
            )
            return

        # --- PLAN CHANGE ---
        self._maybe_upgrade_or_downgrade(current_month)

        # --- PRODUCT USAGE ---
        # Allow usage if active (even if payment failed this month, usually grace period exists)
        if self.current_plan in PLANS or self.current_plan == "Trial":
            limit = EVENT_LIMITS.get(self.current_plan, 0)
            usage_events = generate_product_events(
                self.user_id,
                self.current_plan,
                current_month,
                limit,
                self.timezone_str
            )
            self.product_events.extend(usage_events)

    # =========================================================
    # EXPORT & CLEAR (Memory Safe)
    # =========================================================

    def collect_and_reset_monthly_events(self):
        subs = self.subscription_events
        pays = self.payments
        prods = self.product_events

        self.subscription_events = []
        self.payments = []
        self.product_events = []

        return subs, pays, prods
    
    def _maybe_reactivate(self, current_month):
        """
        Gives churned users a chance to come back.
        Only users who explicitly canceled (not expired trials) can reactivate.
        They restart on the Free plan.
        """
        if self.status != "Churned":
            return
        if self.current_plan != "Canceled":
            return  # Expired trial tidak bisa reaktivasi

        if np.random.rand() < REACTIVATION_PROB:
            self.status = "Active"
            self.current_plan = "Free"
            self.failed_payments = 0  # Reset payment failure counter
            self._add_subscription_event(
                event_type="reactivate",
                plan="Free",
                current_month=current_month
            )

def generate_user_lifecycle(n_users: int, start_month: pd.Timestamp = START_MONTH):
    """
    Generate list of UserLifecycle instances with initial country & timezone assigned.
    """
    users = []
    for _ in range(n_users):
        user_id = fake.uuid4()
        country, timezone = assign_country_timezone()
        user = UserLifecycle(user_id, country, timezone, start_month)
        users.append(user)
    return users

def generate_users_snapshot(users):
    """
    Generate a snapshot of users for the dimension table.
    """
    snapshot = []
    for user in users:
        # Create a rough UTC created_at based on their start month
        created_at_utc = local_to_utc(
            random_timestamp_in_month(user.created_at), 
            user.timezone_str
        )
        
        snapshot.append({
            "user_id": user.user_id,
            "name": user.name,
            "email": user.email,
            "acquisition_channel": user.acquisition_channel,
            "country": user.country,
            "timezone": user.timezone_str,
            "current_status": user.status,
            "current_plan": user.current_plan,
            "created_at_utc": created_at_utc
        })
    return snapshot