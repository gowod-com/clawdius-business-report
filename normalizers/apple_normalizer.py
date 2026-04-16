"""Apple report normalizer.

Converts raw Apple TSV rows into canonical NormalizedSubscriptionEvent records.

Apple SUBSCRIPTION_EVENT report key columns:
- Event Date, Event, Subscription Name, Subscription Apple ID,
  Subscription Group ID, Standard Subscription Duration, Subscription Offer Type,
  Marketing Opt-in Duration, Customer Price, Customer Currency,
  Country, State, Previous Subscription Name, Previous Subscription Apple ID,
  Days Before Canceling, Cancellation Reason, Days After Expiration,
  Preserved Pricing, Proceeds (USD), Preserved Pricing Proceeds (USD),
  Developer Proceeds, Developer Proceeds Currency, Preserved Pricing Developer Proceeds,
  Client, Device, Subscription Offer Name, Subscriber ID, Subscription Purchase Date

Apple SUBSCRIPTION (SUBSCRIBER) report key columns:
- Account ID, App Apple ID, App Name, App Bundle ID, Subscription Name,
  Subscription Apple ID, Subscription Group ID, Standard Subscription Duration,
  Promotional Offer Identifier, Subscription Offer Type, Subscriber ID,
  Subscription Purchase Date, Subscription, Status
"""
import json
import logging
import datetime
from typing import Optional

from storage.models import EventType, Platform, PlanInterval

logger = logging.getLogger(__name__)

# Apple event type → canonical EventType
APPLE_EVENT_MAP = {
    "Subscribe": EventType.NEW_SUBSCRIPTION,
    "Resubscribe": EventType.NEW_SUBSCRIPTION,
    "Cancel": EventType.CANCELLATION,
    "Voluntary Cancel": EventType.CANCELLATION,
    "Billing Retry": EventType.UNKNOWN,
    "Billing Cancel": EventType.CANCELLATION,
    "Crossgrade (Downgrade)": EventType.PLAN_CHANGE,
    "Crossgrade (Upgrade)": EventType.PLAN_CHANGE,
    "Upgrade": EventType.PLAN_CHANGE,
    "Downgrade": EventType.PLAN_CHANGE,
    "Price Increase Consent": EventType.UNKNOWN,
    "Expiration": EventType.CANCELLATION,
    "Free Trial": EventType.UNKNOWN,
}

APPLE_DURATION_TO_INTERVAL = {
    "1 Month": PlanInterval.MONTHLY,
    "2 Months": PlanInterval.MONTHLY,
    "3 Months": PlanInterval.MONTHLY,
    "6 Months": PlanInterval.MONTHLY,
    "1 Year": PlanInterval.YEARLY,
    "1 Week": PlanInterval.MONTHLY,
    "2 Weeks": PlanInterval.MONTHLY,
}


def _parse_float(val: str, default: float = 0.0) -> float:
    try:
        return float(val) if val else default
    except (ValueError, TypeError):
        return default


def _parse_date(val: str) -> Optional[datetime.datetime]:
    if not val:
        return None
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.datetime.strptime(val.strip(), fmt)
        except ValueError:
            continue
    return None


def normalize_subscription_event_row(row: dict, report_date: str, ingestion_run_id: str) -> Optional[dict]:
    """
    Normalize a SUBSCRIPTION_EVENT TSV row.
    Returns a dict matching NormalizedSubscriptionEvent fields or None if skip.
    """
    event_str = row.get("Event", "").strip()
    if not event_str:
        return None

    event_type = APPLE_EVENT_MAP.get(event_str, EventType.UNKNOWN)

    # Skip renewals — they don't count as new subscriptions
    # "Renewal" is the word Apple uses for auto-renew; map to UNKNOWN to filter
    if event_str in ("Renewal", "Auto-Renew Enabled", "Auto-Renew Disabled"):
        return None

    duration = row.get("Standard Subscription Duration", "").strip()
    plan_interval = APPLE_DURATION_TO_INTERVAL.get(duration, PlanInterval.UNKNOWN)

    # Gross amount = Customer Price (in customer currency, but we use proceeds as proxy)
    gross_amount = _parse_float(row.get("Customer Price", "0"))
    currency = row.get("Customer Currency", "USD").strip() or "USD"
    country = row.get("Country", "UNKNOWN").strip() or "UNKNOWN"

    event_ts = _parse_date(row.get("Event Date", ""))

    sub_id = row.get("Subscription Apple ID", "").strip()
    customer_id = row.get("Subscriber ID", "").strip()

    # MRR for event rows: use Developer Proceeds / 12 for yearly, as-is for monthly
    dev_proceeds = _parse_float(row.get("Proceeds (USD)", "0"))
    if plan_interval == PlanInterval.YEARLY:
        mrr_amount = dev_proceeds / 12.0
    else:
        mrr_amount = dev_proceeds

    return {
        "ingestion_run_id": ingestion_run_id,
        "source": "apple",
        "report_date": report_date,
        "event_timestamp": event_ts,
        "platform": Platform.IOS.value,
        "country": country,
        "currency": currency,
        "plan_interval": plan_interval.value,
        "event_type": event_type,
        "gross_amount": gross_amount,
        "subscription_external_id": sub_id,
        "customer_external_id": customer_id,
        "mrr_amount": mrr_amount,
        "is_active_snapshot": False,
        "extra_metadata": json.dumps({
            "event_str": event_str,
            "subscription_name": row.get("Subscription Name", ""),
            "duration": duration,
        }),
    }


def normalize_subscriber_row(row: dict, report_date: str, ingestion_run_id: str) -> Optional[dict]:
    """
    Normalize a SUBSCRIPTION (SUBSCRIBER) TSV row — used for active snapshot.
    Returns a dict matching NormalizedSubscriptionSnapshot fields or None if skip.
    """
    status = row.get("Status", "").strip()
    # Only include active subscriptions for the snapshot
    if status not in ("Active", "active", "1"):
        return None

    duration = row.get("Standard Subscription Duration", "").strip()
    plan_interval = APPLE_DURATION_TO_INTERVAL.get(duration, PlanInterval.UNKNOWN)

    sub_id = row.get("Subscription Apple ID", "").strip()
    customer_id = row.get("Subscriber ID", "").strip()

    if not sub_id:
        return None

    # Apple subscriber report doesn't have direct pricing info per row
    # We default mrr_amount to 0 — should be enriched from event reports
    return {
        "ingestion_run_id": ingestion_run_id,
        "source": "apple",
        "snapshot_date": report_date,
        "platform": Platform.IOS.value,
        "country": "UNKNOWN",
        "currency": "USD",
        "plan_interval": plan_interval.value,
        "subscription_external_id": sub_id,
        "customer_external_id": customer_id,
        "mrr_amount": 0.0,
        "gross_price": 0.0,
    }
