"""Tests for Slack message formatter."""
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from storage.models import (
    Base, DailyBusinessMetrics, DailyBusinessMetricsByPlatform,
    DailyBusinessMetricsByCountry,
)
from delivery.slack_formatter import build_slack_message


@pytest.fixture
def session():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    s = Session()
    yield s
    s.close()


def seed_data(session, report_date):
    session.add(DailyBusinessMetrics(
        report_date=report_date,
        new_subscriptions=42,
        cancellations=5,
        net_new_premiums=37,
        gross_sales=1234.56,
        mrr=8900.00,
    ))
    session.add(DailyBusinessMetricsByPlatform(
        report_date=report_date,
        platform="iOS",
        new_subscriptions=25,
        cancellations=3,
        net_new_premiums=22,
        gross_sales=700.00,
        mrr=5000.00,
    ))
    session.add(DailyBusinessMetricsByPlatform(
        report_date=report_date,
        platform="Stripe",
        new_subscriptions=17,
        cancellations=2,
        net_new_premiums=15,
        gross_sales=534.56,
        mrr=3900.00,
    ))
    session.add(DailyBusinessMetricsByCountry(
        report_date=report_date,
        country="FR",
        new_subscriptions=20,
        cancellations=2,
        net_new_premiums=18,
        gross_sales=500.00,
        mrr=3000.00,
    ))
    session.add(DailyBusinessMetricsByCountry(
        report_date=report_date,
        country="US",
        new_subscriptions=10,
        cancellations=1,
        net_new_premiums=9,
        gross_sales=300.00,
        mrr=2000.00,
    ))
    session.commit()


class TestSlackFormatter:

    def test_full_message_structure(self, session):
        report_date = "2026-04-15"
        seed_data(session, report_date)
        msg = build_slack_message(session, report_date)

        assert "📊 *Business Report — 2026-04-15*" in msg
        assert "*🌍 Global*" in msg
        assert "Gross Sales: $1,235" in msg
        assert "New Subscriptions: 42" in msg
        assert "Cancellations: 5" in msg
        assert "Net New Premiums: +37" in msg
        assert "MRR: $8,900" in msg

    def test_platform_section(self, session):
        report_date = "2026-04-15"
        seed_data(session, report_date)
        msg = build_slack_message(session, report_date)

        assert "📱 iOS" in msg
        assert "💳 Stripe" in msg

    def test_country_section(self, session):
        report_date = "2026-04-15"
        seed_data(session, report_date)
        msg = build_slack_message(session, report_date)

        assert "🗺 Top Countries" in msg
        assert "FR:" in msg
        assert "US:" in msg

    def test_warnings(self, session):
        report_date = "2026-04-15"
        seed_data(session, report_date)
        msg = build_slack_message(session, report_date, warnings=["Apple data missing"])

        assert "⚠️ Apple data missing" in msg

    def test_no_data_graceful(self, session):
        msg = build_slack_message(session, "2026-01-01")
        assert "📊 *Business Report — 2026-01-01*" in msg
        assert "No data" in msg

    def test_negative_net(self, session):
        session.add(DailyBusinessMetrics(
            report_date="2026-04-20",
            new_subscriptions=2,
            cancellations=5,
            net_new_premiums=-3,
            gross_sales=100.0,
            mrr=500.0,
        ))
        session.commit()
        msg = build_slack_message(session, "2026-04-20")
        assert "Net New Premiums: -3" in msg
