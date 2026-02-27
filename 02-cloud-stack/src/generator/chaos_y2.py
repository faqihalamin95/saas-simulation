import copy
import random
import pandas as pd

from .config_y2 import CHAOS_EVENTS_Y2, LATE_ARRIVING_PROB, DIRTY_PLAN_VARIANTS, get_month_index_y2

DUPLICATE_RATE  = 0.02
LATE_EVENT_RATE = LATE_ARRIVING_PROB

# Old plan → new plan mapping (used in migration chaos)
Y1_TO_Y2_PLAN_MAP = {
    "Free":     "Starter",
    "Pro":      "Growth",
    "Pro Plus": "Growth",   # Y1 chaos renamed Pro → Pro Plus; both map to Growth
    "Business": "Enterprise",
}


# ──────────────────────────────────────────────────────────
#  ALWAYS-ON INJECTORS
# ──────────────────────────────────────────────────────────

def inject_late_events(events, ts_field="event_timestamp_utc"):
    """Always-on: randomly delay a small % of events by 1 month."""
    if not events:
        return []
    events_copy = copy.deepcopy(events)
    n = int(len(events_copy) * LATE_EVENT_RATE)
    if n == 0:
        return events_copy
    idxs = random.sample(range(len(events_copy)), n)
    for i in idxs:
        events_copy[i][ts_field] = events_copy[i][ts_field] + pd.DateOffset(months=1)
    return events_copy


def inject_duplicates(events: list, duplicate_rate: float = DUPLICATE_RATE) -> list:
    """Simulate retry logic / upstream glitch duplicates."""
    if not events:
        return []
    duplicated = []
    for event in events:
        duplicated.append(copy.deepcopy(event))
        if random.random() < duplicate_rate:
            duplicated.append(copy.deepcopy(event))
    return duplicated


# ──────────────────────────────────────────────────────────
#  CHAOS 1 — PLAN MIGRATION (M3, always-on after that)
#  Plan names AND prices changed. Migration script was messy:
#  60% clean rename, 20% old name stays, 20% dirty variant
# ──────────────────────────────────────────────────────────

def inject_plan_migration(events, dirty_rate=0.20, stale_rate=0.20):
    """
    Simulates a messy plan rename + price migration.
    - clean   (60%): properly renamed to new plan name
    - stale   (20%): old Y1 name leaked through
    - dirty   (20%): typo / casing variant slipped through
    """
    events = copy.deepcopy(events)
    for event in events:
        if "plan" not in event or event["plan"] is None:
            continue
        roll = random.random()
        if roll < (1 - dirty_rate - stale_rate):
            # Clean path
            event["plan"] = Y1_TO_Y2_PLAN_MAP.get(event["plan"], event["plan"])
        elif roll < (1 - dirty_rate):
            # Stale — old name stays, nothing to do
            pass
        else:
            # Dirty variant
            event["plan"] = random.choice(DIRTY_PLAN_VARIANTS)
    return events


# ──────────────────────────────────────────────────────────
#  CHAOS 2 — REFERRAL CODE NOISE (M8+)
#  Viral referral campaigns launch. New field `referral_code`
#  appears but format is all over the place.
# ──────────────────────────────────────────────────────────

_REFERRAL_FORMATS = ["REF-{}", "ref_{}", "Ref{}", "{}", None, "N/A", "", "ORGANIC"]

def inject_referral_noise(events, noise_rate=0.35):
    """
    Add an inconsistently formatted `referral_code` field.
    Breaks GROUP BY referral_code — same campaign, 5 different keys.
    """
    events = copy.deepcopy(events)
    for event in events:
        if random.random() < noise_rate:
            fmt = random.choice(_REFERRAL_FORMATS)
            if fmt is None or fmt in ("N/A", "", "ORGANIC"):
                event["referral_code"] = fmt
            else:
                code = "".join(random.choices("ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789", k=6))
                event["referral_code"] = fmt.format(code)
    return events


# ──────────────────────────────────────────────────────────
#  CHAOS 3 — VIRAL SPIKE ARTIFACTS (M10)
#  Ingestion layer batches events at scale → timestamp collision.
#  Onboarding pipeline overwhelmed → plan field goes null.
# ──────────────────────────────────────────────────────────

def inject_timestamp_collision(events, ts_field="event_timestamp_utc", collision_rate=0.10):
    """
    At viral scale the ingestion layer flushes batches at once.
    Multiple events share the exact same timestamp.
    Breaks window functions and event ordering.
    """
    events = copy.deepcopy(events)
    if len(events) < 2:
        return events
    n = int(len(events) * collision_rate)
    if n < 2:
        return events
    idxs = random.sample(range(len(events)), n)
    anchor_ts = events[idxs[0]][ts_field]
    for i in idxs:
        events[i][ts_field] = anchor_ts
    return events


def inject_null_spike(events, field="plan", null_rate=0.12):
    """
    Onboarding pipeline overwhelmed during viral surge.
    Some records written before plan assignment completes → null plan.
    Silently breaks plan-level revenue/usage metrics.
    """
    events = copy.deepcopy(events)
    for event in events:
        if field in event and random.random() < null_rate:
            event[field] = None
    return events


# ──────────────────────────────────────────────────────────
#  MAIN ORCHESTRATOR
# ──────────────────────────────────────────────────────────

def apply_chaos_y2(events, current_month, dataset_name, ts_field="event_timestamp_utc"):
    """
    Year 2 chaos orchestrator.

    Always-on
    ─────────
    • Late arriving events (every month)
    • Plan migration noise (from M3 onward — once migration runs it never fully heals)

    Scheduled
    ─────────
    M3  plan_migration : migration script executes, dirty plan strings appear
    M8  referral_noise : referral campaign launches, noisy referral_code field
    M10 viral_spike    : timestamp collision + null spike on plan
    M12 compounding    : referral noise peaks + null spike intensifies
    """
    if not events:
        return []

    events = copy.deepcopy(events)
    month_idx = get_month_index_y2(current_month)

    # ── Always-on ───────────────────────────────────────────
    events = inject_late_events(events, ts_field=ts_field)

    # Plan migration noise kicks in from M3 and persists
    if month_idx >= 3:
        events = inject_plan_migration(events)

    # ── Scheduled ───────────────────────────────────────────
    chaos_name = CHAOS_EVENTS_Y2.get(month_idx)

    if chaos_name == "plan_migration":
        # M3: migration is extra dirty on the day it runs
        events = inject_plan_migration(events, dirty_rate=0.30, stale_rate=0.25)

    elif chaos_name == "referral_noise":
        # M8: referral campaign goes live
        events = inject_referral_noise(events, noise_rate=0.40)

    elif chaos_name == "viral_spike":
        # M10: viral hit — batch flush artifacts + onboarding overwhelmed
        events = inject_timestamp_collision(events, ts_field=ts_field, collision_rate=0.12)
        events = inject_null_spike(events, field="plan", null_rate=0.15)
        events = inject_referral_noise(events, noise_rate=0.50)  # referral explodes

    elif chaos_name == "compounding":
        # M12: all problems compound
        events = inject_null_spike(events, field="plan", null_rate=0.10)
        events = inject_referral_noise(events, noise_rate=0.55)
        if dataset_name == "payments":
            events = inject_duplicates(events, duplicate_rate=0.04)

    return events