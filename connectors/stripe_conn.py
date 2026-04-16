"""Stripe connector.

Uses the stripe Python SDK to fetch invoices and subscriptions.
- Invoices: filtered by created date range for J-1
- Subscriptions: active snapshot for MRR
- No webhooks — polling only
"""
import json
import logging
import time
import datetime
from typing import Iterator, Dict, Any, Optional

import config

logger = logging.getLogger(__name__)


class StripeConnectorError(Exception):
    pass


def _get_stripe():
    """Return initialized stripe module."""
    if not config.is_stripe_configured():
        raise StripeConnectorError("Stripe API key not configured.")
    try:
        import stripe
        stripe.api_key = config.STRIPE_API_KEY
        return stripe
    except ImportError:
        raise StripeConnectorError("stripe package not installed. Run: pip install stripe")


def _auto_paginate(list_fn, max_retries: int = 3, backoff: float = 2.0, **kwargs) -> Iterator[Any]:
    """Auto-paginate a Stripe list endpoint with retry."""
    stripe = _get_stripe()
    starting_after = None

    while True:
        call_kwargs = dict(kwargs)
        if starting_after:
            call_kwargs["starting_after"] = starting_after

        for attempt in range(max_retries):
            try:
                page = list_fn(**call_kwargs)
                break
            except stripe.error.RateLimitError as e:
                wait = backoff * (2 ** attempt)
                logger.warning(f"Stripe rate limit. Waiting {wait}s: {e}")
                time.sleep(wait)
            except stripe.error.APIConnectionError as e:
                wait = backoff * (2 ** attempt)
                logger.warning(f"Stripe connection error. Waiting {wait}s: {e}")
                time.sleep(wait)
            except stripe.error.StripeError as e:
                raise StripeConnectorError(f"Stripe API error: {e}")
        else:
            raise StripeConnectorError(f"Stripe API exhausted retries.")

        for item in page.data:
            yield item

        if not page.has_more:
            break
        starting_after = page.data[-1].id


def _get_card_country_from_invoice(invoice: Dict[str, Any]) -> str:
    """
    Extract card country from an already-expanded invoice dict.
    Uses only data already fetched — NO extra API calls.
    Priority:
      1. invoice.payment_method_details.card.country (set on paid invoices)
      2. customer.invoice_settings.default_payment_method.card.country (expanded)
      3. customer.default_source card country (legacy)
      4. UNKNOWN
    """
    try:
        # 1. payment_method_details on the invoice itself (most reliable, no extra call)
        pmd = invoice.get("payment_method_details") or {}
        if isinstance(pmd, dict):
            card = pmd.get("card") or {}
            if isinstance(card, dict) and card.get("country"):
                return card["country"]

        # 2. charge object if already expanded (some invoice layouts include it)
        charge = invoice.get("charge")
        if isinstance(charge, dict):
            pm_details = charge.get("payment_method_details") or {}
            card = pm_details.get("card") or {}
            if isinstance(card, dict) and card.get("country"):
                return card["country"]
            billing = charge.get("billing_details") or {}
            addr = billing.get("address") or {}
            if addr.get("country"):
                return addr["country"]

        # 3. Customer default payment method (expanded)
        customer = invoice.get("customer")
        if isinstance(customer, dict):
            inv_settings = customer.get("invoice_settings") or {}
            dpm = inv_settings.get("default_payment_method")
            if isinstance(dpm, dict):
                card = dpm.get("card") or {}
                if isinstance(card, dict) and card.get("country"):
                    return card["country"]
            # legacy: default_source
            src = customer.get("default_source")
            if isinstance(src, dict) and src.get("country"):
                return src["country"]

    except Exception as exc:
        logger.debug(f"Could not get card country from invoice: {exc}")

    return "UNKNOWN"


def fetch_invoices(report_date: str) -> Iterator[Dict[str, Any]]:
    """
    Fetch all paid invoices for J-1.
    report_date: YYYY-MM-DD (= J-1)
    Yields enriched invoice dicts.
    """
    if not config.is_stripe_configured():
        logger.warning("Stripe connector not configured, skipping.")
        return

    try:
        stripe = _get_stripe()
    except StripeConnectorError as e:
        logger.error(f"Stripe connector error: {e}")
        return

    date_obj = datetime.date.fromisoformat(report_date)
    start_ts = int(datetime.datetime(date_obj.year, date_obj.month, date_obj.day, 0, 0, 0).timestamp())
    end_ts = int(datetime.datetime(date_obj.year, date_obj.month, date_obj.day, 23, 59, 59).timestamp())

    total = 0
    logger.info(f"Fetching Stripe invoices for {report_date}")

    try:
        for invoice in _auto_paginate(
            stripe.Invoice.list,
            status="paid",
            created={"gte": start_ts, "lte": end_ts},
            limit=100,
            expand=["data.subscription", "data.customer", "data.charge"],
        ):
            total += 1
            raw = invoice.to_dict() if hasattr(invoice, "to_dict") else dict(invoice)
            country = _get_card_country_from_invoice(raw)
            raw["_country"] = country
            yield raw
    except StripeConnectorError as e:
        logger.error(f"Stripe invoice fetch failed: {e}")
        return

    logger.info(f"Stripe: {total} paid invoices for {report_date}")


def fetch_active_subscriptions(report_date: str) -> Iterator[Dict[str, Any]]:
    """
    Fetch all active subscriptions for MRR snapshot.
    report_date: YYYY-MM-DD (= J-1)
    """
    if not config.is_stripe_configured():
        logger.warning("Stripe connector not configured, skipping.")
        return

    try:
        stripe = _get_stripe()
    except StripeConnectorError as e:
        logger.error(f"Stripe connector error: {e}")
        return

    total = 0
    logger.info(f"Fetching Stripe active subscriptions for MRR snapshot ({report_date})")

    try:
        for sub in _auto_paginate(
            stripe.Subscription.list,
            status="active",
            limit=100,
            expand=["data.customer", "data.latest_invoice.charge"],
        ):
            total += 1
            raw = sub.to_dict() if hasattr(sub, "to_dict") else dict(sub)
            # Get country from latest invoice (already expanded, no extra API call)
            country = "UNKNOWN"
            try:
                latest_inv = raw.get("latest_invoice")
                if latest_inv and isinstance(latest_inv, dict):
                    country = _get_card_country_from_invoice(latest_inv)
            except Exception:
                pass
            raw["_country"] = country
            yield raw
    except StripeConnectorError as e:
        logger.error(f"Stripe active subscription fetch failed: {e}")
        return

    logger.info(f"Stripe: {total} active subscriptions for MRR snapshot")


def was_customer_active_before(stripe_module, customer_id: str, before_ts: int) -> bool:
    """
    Check if a customer had an active subscription before the given timestamp.
    Used to distinguish upgrades from new subscriptions.
    """
    try:
        subs = stripe_module.Subscription.list(
            customer=customer_id,
            limit=100,
        )
        for sub in subs.auto_paging_iter():
            # If the subscription was created before the timestamp, customer was already a subscriber
            if sub.get("created", 0) < before_ts:
                return True
        return False
    except Exception as exc:
        logger.debug(f"Could not check prior subscription for customer {customer_id}: {exc}")
        return False
