"""Tests for Google Play normalizer."""
import pytest
from normalizers.google_normalizer import normalize_voided_purchase, normalize_subscription_purchase
from storage.models import EventType, Platform


def make_voided_purchase(**overrides):
    base = {
        "orderId": "GPA.3388-1234-5678",
        "purchaseToken": "token_abc123",
        "kind": "androidpublisher#subscriptionPurchase",
        "voidedTimeMillis": "1744675200000",
        "countryCode": "DE",
        "priceAmountMicros": "9990000",
        "priceCurrencyCode": "EUR",
        "voidedReason": 0,
        "voidedSource": 0,
    }
    base.update(overrides)
    return base


class TestGoogleNormalizer:

    def test_cancellation_basic(self):
        purchase = make_voided_purchase()
        result = normalize_voided_purchase(purchase, "2026-04-15", "run1")
        assert result is not None
        assert result["event_type"] == EventType.CANCELLATION
        assert result["platform"] == Platform.ANDROID.value
        assert result["country"] == "DE"
        assert result["gross_amount"] == pytest.approx(9.99)

    def test_non_subscription_skipped(self):
        purchase = make_voided_purchase(kind="androidpublisher#productPurchase")
        result = normalize_voided_purchase(purchase, "2026-04-15", "run1")
        assert result is None

    def test_missing_order_id_skipped(self):
        purchase = make_voided_purchase(orderId="", purchaseToken="")
        result = normalize_voided_purchase(purchase, "2026-04-15", "run1")
        assert result is None

    def test_unknown_country_fallback(self):
        purchase = make_voided_purchase(countryCode=None)
        result = normalize_voided_purchase(purchase, "2026-04-15", "run1")
        assert result is not None
        assert result["country"] == "UNKNOWN"

    def test_subscription_snapshot(self):
        purchase = {
            "purchaseToken": "token_xyz",
            "priceAmountMicros": "9990000",
            "priceCurrencyCode": "EUR",
            "countryCode": "FR",
            "productId": "com.gowod.premium.monthly",
        }
        result = normalize_subscription_purchase(purchase, "2026-04-15", "run1")
        assert result is not None
        assert result["plan_interval"] == "monthly"
        assert result["mrr_amount"] == pytest.approx(9.99)

    def test_yearly_plan_interval(self):
        purchase = {
            "purchaseToken": "token_zzz",
            "priceAmountMicros": "79990000",
            "priceCurrencyCode": "EUR",
            "countryCode": "US",
            "productId": "com.gowod.premium.yearly",
        }
        result = normalize_subscription_purchase(purchase, "2026-04-15", "run1")
        assert result is not None
        assert result["plan_interval"] == "yearly"
        assert result["mrr_amount"] == pytest.approx(79.99 / 12)
