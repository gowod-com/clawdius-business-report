"""Stripe normalizer.

Converts raw Stripe invoices and subscriptions into canonical events.

Rules:
- Renewals (billing_reason=subscription_cycle) do NOT count as new subscriptions
- monthly→yearly upgrade = PLAN_CHANGE, not NEW_SUBSCRIPTION
- Country = card country if available, else UNKNOWN
- MRR: monthly=price, yearly=price/12
"""
import json
import logging
import datetime
from typing import Optional

from storage.models import EventType, Platform, PlanInterval

logger = logging.getLogger(__name__)


def _parse_float(val, default: float = 0.0) -> float:
    try:
        return float(val) if val is not None else default
    except (ValueError, TypeError):
        return default


def _infer_plan_interval(invoice: dict) -> PlanInterval:
    """Infer plan interval from subscription plan or invoice lines."""
    try:
        lines = invoice.get("lines", {}).get("data", [])
        for line in lines:
            plan = line.get("plan") or {}
            interval = plan.get("interval", "")
            if interval == "year":
                return PlanInterval.YEARLY
            if interval == "month":
                return PlanInterval.MONTHLY
            price = line.get("price") or {}
            recurring = price.get("recurring") or {}
            interval = recurring.get("interval", "")
            if interval == "year":
                return PlanInterval.YEARLY
            if interval == "month":
                return PlanInterval.MONTHLY
    except Exception:
        pass
    return PlanInterval.UNKNOWN


def _infer_event_type(invoice: dict, was_previously_active: bool = False) -> EventType:
    """
    Infer the canonical event type from a Stripe invoice.
    billing_reason values: subscription_create, subscription_cycle, subscription_update,
                           manual, upcoming, subscription_threshold
    """
    billing_reason = invoice.get("billing_reason", "")

    if billing_reason == "subscription_create":
        return EventType.NEW_SUBSCRIPTION

    if billing_reason == "subscription_cycle":
        # Renewal — do not count as new subscription
        return EventType.UNKNOWN

    if billing_reason == "subscription_update":
        # Could be upgrade/downgrade
        if was_previously_active:
            return EventType.PLAN_CHANGE
        return EventType.NEW_SUBSCRIPTION

    # Cancellations are not invoices — they're subscription status changes
    return EventType.UNKNOWN


def normalize_invoice(
    invoice: dict,
    report_date: str,
    ingestion_run_id: str,
    was_previously_active: bool = False,
) -> Optional[dict]:
    """
    Normalize a Stripe paid invoice into a canonical event.
    Returns None for renewals and non-subscription invoices.
    """
    invoice_id = invoice.get("id", "")
    if not invoice_id:
        return None

    # Only process subscription invoices
    subscription_id = invoice.get("subscription")
    if isinstance(subscription_id, dict):
        subscription_id = subscription_id.get("id", "")
    if not subscription_id:
        return None

    event_type = _infer_event_type(invoice, was_previously_active)

    # Skip renewals
    if event_type == EventType.UNKNOWN:
        billing_reason = invoice.get("billing_reason", "")
        if billing_reason == "subscription_cycle":
            return None

    plan_interval = _infer_plan_interval(invoice)
    country = invoice.get("_country", "UNKNOWN") or "UNKNOWN"
    currency = (invoice.get("currency") or "usd").upper()

    # Amount in cents → dollars
    amount_paid = _parse_float(invoice.get("amount_paid", 0)) / 100.0
    gross_amount = amount_paid

    # MRR calculation
    if plan_interval == PlanInterval.YEARLY:
        mrr_amount = gross_amount / 12.0
    elif plan_interval == PlanInterval.MONTHLY:
        mrr_amount = gross_amount
    else:
        mrr_amount = gross_amount  # fallback

    event_ts_raw = invoice.get("created")
    event_ts = datetime.datetime.utcfromtimestamp(event_ts_raw) if event_ts_raw else None

    customer_id = invoice.get("customer")
    if isinstance(customer_id, dict):
        customer_id = customer_id.get("id", "")

    return {
        "ingestion_run_id": ingestion_run_id,
        "source": "stripe",
        "report_date": report_date,
        "event_timestamp": event_ts,
        "platform": Platform.STRIPE.value,
        "country": country,
        "currency": currency,
        "plan_interval": plan_interval.value,
        "event_type": event_type,
        "gross_amount": gross_amount,
        "subscription_external_id": subscription_id,
        "customer_external_id": str(customer_id) if customer_id else None,
        "mrr_amount": mrr_amount,
        "is_active_snapshot": False,
        "extra_metadata": json.dumps({
            "invoice_id": invoice_id,
            "billing_reason": invoice.get("billing_reason"),
            "status": invoice.get("status"),
        }),
    }


def normalize_active_subscription(
    sub: dict,
    report_date: str,
    ingestion_run_id: str,
) -> Optional[dict]:
    """
    Normalize a Stripe active subscription for MRR snapshot.
    """
    sub_id = sub.get("id", "")
    if not sub_id:
        return None

    # Determine plan interval and pricing
    items = sub.get("items", {}).get("data", [])
    plan_interval = PlanInterval.UNKNOWN
    gross_price = 0.0
    currency = "USD"

    for item in items:
        price = item.get("price") or {}
        recurring = price.get("recurring") or {}
        interval = recurring.get("interval", "")
        if interval == "year":
            plan_interval = PlanInterval.YEARLY
        elif interval == "month":
            plan_interval = PlanInterval.MONTHLY

        unit_amount = _parse_float(price.get("unit_amount", 0)) / 100.0
        qty = item.get("quantity", 1) or 1
        gross_price += unit_amount * qty
        currency = (price.get("currency") or "usd").upper()
        break  # Use first item for simplicity

    if plan_interval == PlanInterval.YEARLY:
        mrr_amount = gross_price / 12.0
    else:
        mrr_amount = gross_price

    country = sub.get("_country", "UNKNOWN") or "UNKNOWN"
    customer_id = sub.get("customer")
    if isinstance(customer_id, dict):
        customer_id = customer_id.get("id", "")

    return {
        "ingestion_run_id": ingestion_run_id,
        "source": "stripe",
        "snapshot_date": report_date,
        "platform": Platform.STRIPE.value,
        "country": country,
        "currency": currency,
        "plan_interval": plan_interval.value,
        "subscription_external_id": sub_id,
        "customer_external_id": str(customer_id) if customer_id else None,
        "mrr_amount": mrr_amount,
        "gross_price": gross_price,
    }


def normalize_cancellation(
    sub: dict,
    report_date: str,
    ingestion_run_id: str,
) -> Optional[dict]:
    """
    Normalize a Stripe canceled subscription into a CANCELLATION event.
    Used when processing subscriptions that changed to 'canceled' status on report_date.
    """
    sub_id = sub.get("id", "")
    if not sub_id:
        return None

    canceled_at = sub.get("canceled_at")
    event_ts = datetime.datetime.utcfromtimestamp(canceled_at) if canceled_at else None

    country = sub.get("_country", "UNKNOWN") or "UNKNOWN"
    currency = "USD"
    plan_interval = PlanInterval.UNKNOWN

    items = sub.get("items", {}).get("data", [])
    for item in items:
        price = item.get("price") or {}
        recurring = price.get("recurring") or {}
        interval = recurring.get("interval", "")
        if interval == "year":
            plan_interval = PlanInterval.YEARLY
        elif interval == "month":
            plan_interval = PlanInterval.MONTHLY
        currency = (price.get("currency") or "usd").upper()
        break

    customer_id = sub.get("customer")
    if isinstance(customer_id, dict):
        customer_id = customer_id.get("id", "")

    return {
        "ingestion_run_id": ingestion_run_id,
        "source": "stripe",
        "report_date": report_date,
        "event_timestamp": event_ts,
        "platform": Platform.STRIPE.value,
        "country": country,
        "currency": currency,
        "plan_interval": plan_interval.value,
        "event_type": EventType.CANCELLATION,
        "gross_amount": 0.0,
        "subscription_external_id": sub_id,
        "customer_external_id": str(customer_id) if customer_id else None,
        "mrr_amount": 0.0,
        "is_active_snapshot": False,
        "extra_metadata": json.dumps({
            "cancel_at_period_end": sub.get("cancel_at_period_end"),
            "cancellation_details": sub.get("cancellation_details"),
        }),
    }
