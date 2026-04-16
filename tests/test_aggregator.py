"""Tests for daily aggregator."""
import pytest
import datetime
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from storage.models import (
    Base, NormalizedSubscriptionEvent, NormalizedSubscriptionSnapshot,
    DailyBusinessMetrics, DailyBusinessMetricsByPlatform,
    DailyBusinessMetricsByCountry, EventType,
)
from aggregators.daily_aggregator import compute_and_store


@pytest.fixture
def session():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    s = Session()
    yield s
    s.close()


def add_event(session, report_date, platform, country, event_type, gross_amount=0.0):
    event = NormalizedSubscriptionEvent(
        ingestion_run_id="test_run",
        source="stripe",
        report_date=report_date,
        platform=platform,
        country=country,
        event_type=event_type,
        gross_amount=gross_amount,
        mrr_amount=0.0,
        currency="USD",
        plan_interval="monthly",
        is_active_snapshot=False,
    )
    session.add(event)
    session.flush()


def add_snapshot(session, snapshot_date, platform, country, mrr_amount):
    snap = NormalizedSubscriptionSnapshot(
        ingestion_run_id="test_run",
        source="stripe",
        snapshot_date=snapshot_date,
        platform=platform,
        country=country,
        subscription_external_id=f"sub_{platform}_{country}_{mrr_amount}",
        mrr_amount=mrr_amount,
        gross_price=mrr_amount,
        currency="USD",
        plan_interval="monthly",
    )
    session.add(snap)
    session.flush()


class TestDailyAggregator:

    def test_basic_aggregation(self, session):
        report_date = "2026-04-15"
        add_event(session, report_date, "iOS", "FR", EventType.NEW_SUBSCRIPTION, 9.99)
        add_event(session, report_date, "iOS", "DE", EventType.NEW_SUBSCRIPTION, 9.99)
        add_event(session, report_date, "Stripe", "US", EventType.CANCELLATION, 0.0)
        add_snapshot(session, report_date, "iOS", "FR", 9.99)
        add_snapshot(session, report_date, "Stripe", "US", 20.0)
        session.commit()

        result = compute_and_store(session, report_date)

        global_row = session.query(DailyBusinessMetrics).filter_by(report_date=report_date).first()
        assert global_row is not None
        assert global_row.new_subscriptions == 2
        assert global_row.cancellations == 1
        assert global_row.net_new_premiums == 1
        assert global_row.gross_sales == pytest.approx(19.98)
        assert global_row.mrr == pytest.approx(29.99)

    def test_idempotency(self, session):
        report_date = "2026-04-16"
        add_event(session, report_date, "iOS", "FR", EventType.NEW_SUBSCRIPTION, 9.99)
        session.commit()

        compute_and_store(session, report_date)
        # Run again — should replace, not duplicate
        compute_and_store(session, report_date)

        count = session.query(DailyBusinessMetrics).filter_by(report_date=report_date).count()
        assert count == 1

    def test_by_platform(self, session):
        report_date = "2026-04-17"
        add_event(session, report_date, "iOS", "FR", EventType.NEW_SUBSCRIPTION, 9.99)
        add_event(session, report_date, "Android", "DE", EventType.NEW_SUBSCRIPTION, 8.99)
        add_event(session, report_date, "Stripe", "US", EventType.CANCELLATION, 0.0)
        session.commit()

        compute_and_store(session, report_date)

        ios_row = session.query(DailyBusinessMetricsByPlatform).filter_by(
            report_date=report_date, platform="iOS"
        ).first()
        assert ios_row is not None
        assert ios_row.new_subscriptions == 1
        assert ios_row.gross_sales == pytest.approx(9.99)

        android_row = session.query(DailyBusinessMetricsByPlatform).filter_by(
            report_date=report_date, platform="Android"
        ).first()
        assert android_row is not None
        assert android_row.new_subscriptions == 1

    def test_no_data(self, session):
        """Aggregator should handle empty data gracefully."""
        report_date = "2026-04-18"
        compute_and_store(session, report_date)

        global_row = session.query(DailyBusinessMetrics).filter_by(report_date=report_date).first()
        assert global_row is not None
        assert global_row.new_subscriptions == 0
        assert global_row.cancellations == 0
        assert global_row.gross_sales == 0.0
        assert global_row.mrr == 0.0
