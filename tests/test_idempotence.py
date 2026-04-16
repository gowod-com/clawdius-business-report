"""Tests for idempotence and deduplication."""
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import IntegrityError

from storage.models import (
    Base, NormalizedSubscriptionEvent, NormalizedSubscriptionSnapshot,
    DailyBusinessMetrics, EventType,
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


class TestIdempotence:

    def test_double_aggregation_no_duplicate(self, session):
        """Running aggregator twice produces exactly one global metrics row."""
        report_date = "2026-04-15"
        session.add(NormalizedSubscriptionEvent(
            ingestion_run_id="run1",
            source="stripe",
            report_date=report_date,
            platform="Stripe",
            country="FR",
            event_type=EventType.NEW_SUBSCRIPTION,
            gross_amount=9.99,
            mrr_amount=9.99,
            currency="USD",
            plan_interval="monthly",
            is_active_snapshot=False,
        ))
        session.commit()

        compute_and_store(session, report_date)
        compute_and_store(session, report_date)

        count = session.query(DailyBusinessMetrics).filter_by(report_date=report_date).count()
        assert count == 1

    def test_snapshot_deduplication(self, session):
        """Same snapshot inserted twice should not create duplicates."""
        snap1 = NormalizedSubscriptionSnapshot(
            ingestion_run_id="run1",
            source="stripe",
            snapshot_date="2026-04-15",
            platform="Stripe",
            country="US",
            subscription_external_id="sub_dedup_test",
            mrr_amount=9.99,
            gross_price=9.99,
            currency="USD",
            plan_interval="monthly",
        )
        session.add(snap1)
        session.commit()

        # Try inserting duplicate
        snap2 = NormalizedSubscriptionSnapshot(
            ingestion_run_id="run2",
            source="stripe",
            snapshot_date="2026-04-15",
            platform="Stripe",
            country="US",
            subscription_external_id="sub_dedup_test",
            mrr_amount=9.99,
            gross_price=9.99,
            currency="USD",
            plan_interval="monthly",
        )
        session.add(snap2)
        try:
            session.commit()
            # If no error, check count
        except IntegrityError:
            session.rollback()

        count = session.query(NormalizedSubscriptionSnapshot).filter_by(
            subscription_external_id="sub_dedup_test"
        ).count()
        assert count == 1

    def test_backfill_multiple_dates(self, session):
        """Backfilling multiple dates produces one metrics row per date."""
        for date in ["2026-04-13", "2026-04-14", "2026-04-15"]:
            session.add(NormalizedSubscriptionEvent(
                ingestion_run_id=f"run_{date}",
                source="stripe",
                report_date=date,
                platform="Stripe",
                country="FR",
                event_type=EventType.NEW_SUBSCRIPTION,
                gross_amount=9.99,
                mrr_amount=9.99,
                currency="USD",
                plan_interval="monthly",
                is_active_snapshot=False,
                subscription_external_id=f"sub_{date}",
            ))
        session.commit()

        for date in ["2026-04-13", "2026-04-14", "2026-04-15"]:
            compute_and_store(session, date)

        count = session.query(DailyBusinessMetrics).count()
        assert count == 3

        row = session.query(DailyBusinessMetrics).filter_by(report_date="2026-04-14").first()
        assert row.new_subscriptions == 1
