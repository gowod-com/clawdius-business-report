"""Tests for Stripe normalizer."""
import pytest
import datetime
from normalizers.stripe_normalizer import normalize_invoice, normalize_active_subscription, normalize_cancellation
from storage.models import EventType, PlanInterval


def make_invoice(**overrides):
    base = {
        "id": "in_test_123",
        "subscription": "sub_test_456",
        "billing_reason": "subscription_create",
        "status": "paid",
        "amount_paid": 999,  # $9.99 in cents
        "currency": "usd",
        "created": 1744675200,  # 2026-04-15 00:00:00 UTC
        "customer": "cus_test_789",
        "_country": "FR",
        "lines": {
            "data": [
                {
                    "price": {
                        "recurring": {"interval": "month"},
                        "currency": "usd",
                        "unit_amount": 999,
                    },
                    "quantity": 1,
                }
            ]
        },
    }
    base.update(overrides)
    return base


def make_subscription(**overrides):
    base = {
        "id": "sub_test_456",
        "customer": "cus_test_789",
        "_country": "US",
        "items": {
            "data": [
                {
                    "price": {
                        "recurring": {"interval": "month"},
                        "currency": "usd",
                        "unit_amount": 999,
                    },
                    "quantity": 1,
                }
            ]
        },
    }
    base.update(overrides)
    return base


class TestStripeNormalizer:

    def test_new_subscription(self):
        inv = make_invoice(billing_reason="subscription_create")
        result = normalize_invoice(inv, "2026-04-15", "run1")
        assert result is not None
        assert result["event_type"] == EventType.NEW_SUBSCRIPTION
        assert result["platform"] == "Stripe"
        assert result["country"] == "FR"
        assert result["gross_amount"] == pytest.approx(9.99)
        assert result["plan_interval"] == "monthly"
        assert result["mrr_amount"] == pytest.approx(9.99)

    def test_renewal_is_skipped(self):
        inv = make_invoice(billing_reason="subscription_cycle")
        result = normalize_invoice(inv, "2026-04-15", "run1")
        assert result is None

    def test_upgrade_event(self):
        inv = make_invoice(billing_reason="subscription_update")
        result = normalize_invoice(inv, "2026-04-15", "run1", was_previously_active=True)
        assert result is not None
        assert result["event_type"] == EventType.PLAN_CHANGE

    def test_upgrade_first_sub(self):
        """Update billing_reason on first sub = NEW_SUBSCRIPTION."""
        inv = make_invoice(billing_reason="subscription_update")
        result = normalize_invoice(inv, "2026-04-15", "run1", was_previously_active=False)
        assert result is not None
        assert result["event_type"] == EventType.NEW_SUBSCRIPTION

    def test_yearly_mrr(self):
        inv = make_invoice(
            billing_reason="subscription_create",
            amount_paid=9900,  # $99.00 yearly
            lines={
                "data": [
                    {
                        "price": {
                            "recurring": {"interval": "year"},
                            "currency": "usd",
                            "unit_amount": 9900,
                        },
                        "quantity": 1,
                    }
                ]
            },
        )
        result = normalize_invoice(inv, "2026-04-15", "run1")
        assert result is not None
        assert result["plan_interval"] == "yearly"
        assert result["gross_amount"] == pytest.approx(99.0)
        assert result["mrr_amount"] == pytest.approx(8.25)  # 99 / 12

    def test_no_subscription_id_skipped(self):
        inv = make_invoice(subscription=None)
        result = normalize_invoice(inv, "2026-04-15", "run1")
        assert result is None

    def test_unknown_country_fallback(self):
        inv = make_invoice(_country=None)
        result = normalize_invoice(inv, "2026-04-15", "run1")
        assert result is not None
        assert result["country"] == "UNKNOWN"

    def test_active_subscription_mrr(self):
        sub = make_subscription()
        result = normalize_active_subscription(sub, "2026-04-15", "run1")
        assert result is not None
        assert result["mrr_amount"] == pytest.approx(9.99)
        assert result["gross_price"] == pytest.approx(9.99)
        assert result["plan_interval"] == "monthly"

    def test_cancellation(self):
        sub = {
            "id": "sub_cancel_001",
            "customer": "cus_test_789",
            "_country": "US",
            "canceled_at": 1744675200,
            "items": {
                "data": [
                    {
                        "price": {
                            "recurring": {"interval": "month"},
                            "currency": "usd",
                            "unit_amount": 999,
                        },
                        "quantity": 1,
                    }
                ]
            },
        }
        result = normalize_cancellation(sub, "2026-04-15", "run1")
        assert result is not None
        assert result["event_type"] == EventType.CANCELLATION
        assert result["gross_amount"] == 0.0
