"""Tests for Apple normalizer."""
import pytest
from normalizers.apple_normalizer import normalize_subscription_event_row, normalize_subscriber_row
from storage.models import EventType


def make_event_row(**overrides):
    base = {
        "Event Date": "2026-04-15",
        "Event": "Subscribe",
        "Subscription Name": "GOWOD Premium",
        "Subscription Apple ID": "sub_apple_123",
        "Standard Subscription Duration": "1 Month",
        "Customer Price": "9.99",
        "Customer Currency": "EUR",
        "Country": "FR",
        "Proceeds (USD)": "7.00",
        "Subscriber ID": "cust_abc",
    }
    base.update(overrides)
    return base


class TestAppleNormalizer:

    def test_new_subscription(self):
        row = make_event_row(Event="Subscribe")
        result = normalize_subscription_event_row(row, "2026-04-15", "run1")
        assert result is not None
        assert result["event_type"] == EventType.NEW_SUBSCRIPTION
        assert result["platform"] == "iOS"
        assert result["country"] == "FR"
        assert result["plan_interval"] == "monthly"
        assert result["gross_amount"] == 9.99
        assert result["mrr_amount"] == pytest.approx(7.0)

    def test_cancellation(self):
        row = make_event_row(Event="Cancel")
        result = normalize_subscription_event_row(row, "2026-04-15", "run1")
        assert result is not None
        assert result["event_type"] == EventType.CANCELLATION

    def test_yearly_interval(self):
        row = make_event_row(
            Event="Subscribe",
            **{"Standard Subscription Duration": "1 Year", "Proceeds (USD)": "84.00"}
        )
        result = normalize_subscription_event_row(row, "2026-04-15", "run1")
        assert result is not None
        assert result["plan_interval"] == "yearly"
        assert result["mrr_amount"] == pytest.approx(7.0)  # 84 / 12

    def test_renewal_is_skipped(self):
        row = make_event_row(Event="Renewal")
        result = normalize_subscription_event_row(row, "2026-04-15", "run1")
        assert result is None

    def test_auto_renew_skipped(self):
        row = make_event_row(Event="Auto-Renew Enabled")
        result = normalize_subscription_event_row(row, "2026-04-15", "run1")
        assert result is None

    def test_empty_event(self):
        row = make_event_row(Event="")
        result = normalize_subscription_event_row(row, "2026-04-15", "run1")
        assert result is None

    def test_upgrade_event(self):
        row = make_event_row(Event="Crossgrade (Upgrade)")
        result = normalize_subscription_event_row(row, "2026-04-15", "run1")
        assert result is not None
        assert result["event_type"] == EventType.PLAN_CHANGE

    def test_subscriber_active(self):
        row = {
            "Subscription Apple ID": "sub_apple_999",
            "Standard Subscription Duration": "1 Year",
            "Status": "Active",
            "Subscriber ID": "cust_xyz",
        }
        result = normalize_subscriber_row(row, "2026-04-15", "run1")
        assert result is not None
        assert result["snapshot_date"] == "2026-04-15"
        assert result["plan_interval"] == "yearly"

    def test_subscriber_inactive_skipped(self):
        row = {
            "Subscription Apple ID": "sub_apple_999",
            "Standard Subscription Duration": "1 Month",
            "Status": "Expired",
            "Subscriber ID": "cust_xyz",
        }
        result = normalize_subscriber_row(row, "2026-04-15", "run1")
        assert result is None
