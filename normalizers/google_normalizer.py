"""Google Play normalizer.

Converts raw Google voided purchases and subscription data into canonical events.
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


def _parse_millis(ms_str) -> Optional[datetime.datetime]:
    try:
        ms = int(ms_str)
        return datetime.datetime.utcfromtimestamp(ms / 1000)
    except (TypeError, ValueError):
        return None


def _infer_plan_interval(purchase_data: dict) -> PlanInterval:
    """Infer plan interval from subscription offer or product ID."""
    offer_details = purchase_data.get("latestOrderId", "") or ""
    product_id = purchase_data.get("productId", "") or ""
    # Heuristic: product IDs often contain "monthly" or "yearly"
    combined = (offer_details + product_id).lower()
    if "year" in combined or "annual" in combined:
        return PlanInterval.YEARLY
    if "month" in combined:
        return PlanInterval.MONTHLY
    return PlanInterval.UNKNOWN


def normalize_voided_purchase(purchase: dict, report_date: str, ingestion_run_id: str) -> Optional[dict]:
    """
    Normalize a Google voided purchase (cancellation) event.
    """
    order_id = purchase.get("orderId", purchase.get("purchaseToken", ""))
    if not order_id:
        return None

    voided_ms = purchase.get("voidedTimeMillis")
    event_ts = _parse_millis(voided_ms)

    # Google voided purchases include kind field
    kind = purchase.get("kind", "")
    is_subscription = "subscriptionPurchase" in kind

    if not is_subscription:
        # Skip one-time purchases
        return None

    country = purchase.get("countryCode", "UNKNOWN") or "UNKNOWN"

    # Price info from voidedPurchase is limited; use priceAmountMicros if available
    price_micros = purchase.get("priceAmountMicros", 0)
    currency = purchase.get("priceCurrencyCode", "USD") or "USD"
    gross_amount = _parse_float(price_micros) / 1_000_000

    return {
        "ingestion_run_id": ingestion_run_id,
        "source": "google",
        "report_date": report_date,
        "event_timestamp": event_ts,
        "platform": Platform.ANDROID.value,
        "country": country,
        "currency": currency,
        "plan_interval": PlanInterval.UNKNOWN.value,
        "event_type": EventType.CANCELLATION,
        "gross_amount": gross_amount,
        "subscription_external_id": order_id,
        "customer_external_id": purchase.get("purchaseToken", ""),
        "mrr_amount": 0.0,
        "is_active_snapshot": False,
        "extra_metadata": json.dumps({
            "voidedReason": purchase.get("voidedReason"),
            "voidedSource": purchase.get("voidedSource"),
            "kind": kind,
        }),
    }


def normalize_subscription_purchase(purchase: dict, report_date: str, ingestion_run_id: str) -> Optional[dict]:
    """
    Normalize a Google subscription purchase for MRR snapshot.
    """
    purchase_token = purchase.get("purchaseToken", "")
    if not purchase_token:
        return None

    plan_interval = _infer_plan_interval(purchase)
    country = purchase.get("countryCode", "UNKNOWN") or "UNKNOWN"

    price_micros = purchase.get("priceAmountMicros", 0)
    currency = purchase.get("priceCurrencyCode", "USD") or "USD"
    gross_price = _parse_float(price_micros) / 1_000_000

    if plan_interval == PlanInterval.YEARLY:
        mrr_amount = gross_price / 12.0
    else:
        mrr_amount = gross_price

    return {
        "ingestion_run_id": ingestion_run_id,
        "source": "google",
        "snapshot_date": report_date,
        "platform": Platform.ANDROID.value,
        "country": country,
        "currency": currency,
        "plan_interval": plan_interval.value,
        "subscription_external_id": purchase_token,
        "customer_external_id": purchase.get("obfuscatedExternalAccountId", ""),
        "mrr_amount": mrr_amount,
        "gross_price": gross_price,
    }
