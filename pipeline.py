"""Main ingestion pipeline.

Orchestrates:
1. Fetch raw data from connectors
2. Store in raw tables (with deduplication)
3. Normalize into canonical events
4. Aggregate KPIs
5. Format and post Slack message
"""
import json
import logging
import datetime
import uuid
from typing import List, Optional

from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError

import config
from storage.db import get_session, init_db
from storage.models import (
    RawAppleReport,
    RawGoogleOrder,
    RawStripeInvoice,
    RawStripeSubscription,
    NormalizedSubscriptionEvent,
    NormalizedSubscriptionSnapshot,
    EventType,
)
from connectors import apple, google, stripe_conn, slack_conn
from normalizers import apple_normalizer, google_normalizer, stripe_normalizer
from aggregators.daily_aggregator import compute_and_store
from delivery.slack_formatter import build_slack_message

logger = logging.getLogger(__name__)


def _make_run_id(report_date: str) -> str:
    return f"{report_date}_{uuid.uuid4().hex[:8]}"


def _safe_add(session: Session, obj) -> bool:
    """Try to add an object; ignore unique constraint violations."""
    try:
        session.add(obj)
        session.flush()
        return True
    except IntegrityError:
        session.rollback()
        return False


def _upsert_normalized_event(session: Session, data: dict) -> bool:
    """Insert normalized event, skip on duplicate (idempotent)."""
    # Check if already exists
    existing = (
        session.query(NormalizedSubscriptionEvent)
        .filter_by(
            source=data["source"],
            report_date=data["report_date"],
            subscription_external_id=data.get("subscription_external_id"),
            event_type=data["event_type"],
        )
        .first()
    )
    if existing:
        return False

    event = NormalizedSubscriptionEvent(**data)
    try:
        session.add(event)
        session.flush()
        return True
    except IntegrityError:
        session.rollback()
        return False


def _upsert_snapshot(session: Session, data: dict) -> bool:
    """Insert normalized snapshot, skip on duplicate."""
    existing = (
        session.query(NormalizedSubscriptionSnapshot)
        .filter_by(
            source=data["source"],
            snapshot_date=data["snapshot_date"],
            subscription_external_id=data["subscription_external_id"],
        )
        .first()
    )
    if existing:
        return False

    snap = NormalizedSubscriptionSnapshot(**data)
    try:
        session.add(snap)
        session.flush()
        return True
    except IntegrityError:
        session.rollback()
        return False


# ---------------------------------------------------------------------------
# Apple ingestion
# ---------------------------------------------------------------------------

def ingest_apple(session: Session, report_date: str, run_id: str) -> List[str]:
    """Ingest Apple data. Returns list of warning strings."""
    warnings = []

    if not config.is_apple_configured():
        warnings.append("Apple connector not configured — iOS data missing.")
        return warnings

    # Subscription event report → events
    event_count = 0
    try:
        for item in apple.fetch_subscription_event_report(report_date):
            raw = RawAppleReport(
                ingestion_run_id=run_id,
                report_date=report_date,
                report_type="SUBSCRIPTION_EVENT",
                row_hash=item["row_hash"],
                raw_tsv_row=item["raw_tsv_row"],
            )
            _safe_add(session, raw)

            norm = apple_normalizer.normalize_subscription_event_row(
                item["row_data"], report_date, run_id
            )
            if norm:
                _upsert_normalized_event(session, norm)
                event_count += 1
    except Exception as exc:
        warnings.append(f"Apple SUBSCRIPTION_EVENT fetch failed: {exc}")
        logger.error(f"Apple event fetch error: {exc}", exc_info=True)

    # Subscriber report → snapshot
    snap_count = 0
    try:
        for item in apple.fetch_subscription_report(report_date):
            raw = RawAppleReport(
                ingestion_run_id=run_id,
                report_date=report_date,
                report_type="SUBSCRIPTION",
                row_hash=item["row_hash"],
                raw_tsv_row=item["raw_tsv_row"],
            )
            _safe_add(session, raw)

            snap = apple_normalizer.normalize_subscriber_row(
                item["row_data"], report_date, run_id
            )
            if snap:
                _upsert_snapshot(session, snap)
                snap_count += 1
    except Exception as exc:
        warnings.append(f"Apple SUBSCRIPTION fetch failed: {exc}")
        logger.error(f"Apple subscriber fetch error: {exc}", exc_info=True)

    session.commit()
    logger.info(f"Apple ingestion: {event_count} events, {snap_count} snapshots")
    return warnings


# ---------------------------------------------------------------------------
# Google ingestion
# ---------------------------------------------------------------------------

def ingest_google(session: Session, report_date: str, run_id: str) -> List[str]:
    """Ingest Google Play data. Returns list of warning strings."""
    warnings = []

    if not config.is_google_configured():
        warnings.append("Google connector not configured — Android data missing.")
        return warnings

    cancel_count = 0
    try:
        for purchase in google.fetch_voided_purchases(report_date):
            raw = RawGoogleOrder(
                ingestion_run_id=run_id,
                order_id=purchase["order_id"],
                report_date=report_date,
                raw_json=json.dumps(purchase["raw_data"]),
            )
            _safe_add(session, raw)

            norm = google_normalizer.normalize_voided_purchase(
                purchase["raw_data"], report_date, run_id
            )
            if norm:
                _upsert_normalized_event(session, norm)
                cancel_count += 1
    except Exception as exc:
        warnings.append(f"Google voided purchases fetch failed: {exc}")
        logger.error(f"Google fetch error: {exc}", exc_info=True)

    session.commit()
    logger.info(f"Google ingestion: {cancel_count} cancellations")
    return warnings


# ---------------------------------------------------------------------------
# Stripe ingestion
# ---------------------------------------------------------------------------

def ingest_stripe(session: Session, report_date: str, run_id: str) -> List[str]:
    """Ingest Stripe data. Returns list of warning strings."""
    warnings = []

    if not config.is_stripe_configured():
        warnings.append("Stripe connector not configured — Stripe data missing.")
        return warnings

    # Track customer IDs seen to detect upgrades
    invoice_count = 0
    try:
        for invoice in stripe_conn.fetch_invoices(report_date):
            invoice_id = invoice.get("id", "")

            raw = RawStripeInvoice(
                ingestion_run_id=run_id,
                invoice_id=invoice_id,
                report_date=report_date,
                raw_json=json.dumps(invoice, default=str),
            )
            _safe_add(session, raw)

            norm = stripe_normalizer.normalize_invoice(
                invoice, report_date, run_id
            )
            if norm:
                _upsert_normalized_event(session, norm)
                invoice_count += 1
    except Exception as exc:
        warnings.append(f"Stripe invoice fetch failed: {exc}")
        logger.error(f"Stripe invoice error: {exc}", exc_info=True)

    # Active subscriptions for MRR snapshot
    snap_count = 0
    try:
        for sub in stripe_conn.fetch_active_subscriptions(report_date):
            sub_id = sub.get("id", "")

            raw = RawStripeSubscription(
                ingestion_run_id=run_id,
                subscription_id=sub_id,
                report_date=report_date,
                raw_json=json.dumps(sub, default=str),
            )
            _safe_add(session, raw)

            snap = stripe_normalizer.normalize_active_subscription(
                sub, report_date, run_id
            )
            if snap:
                _upsert_snapshot(session, snap)
                snap_count += 1
    except Exception as exc:
        warnings.append(f"Stripe subscription snapshot failed: {exc}")
        logger.error(f"Stripe snapshot error: {exc}", exc_info=True)

    session.commit()
    logger.info(f"Stripe ingestion: {invoice_count} invoice events, {snap_count} snapshots")
    return warnings


# ---------------------------------------------------------------------------
# Main pipeline entry point
# ---------------------------------------------------------------------------

def run_pipeline(report_date: str, dry_run: bool = False) -> bool:
    """
    Run the full pipeline for a given report_date (YYYY-MM-DD).
    Returns True on success.
    """
    logger.info(f"=== Pipeline START: report_date={report_date}, dry_run={dry_run} ===")
    init_db()
    session = get_session()
    run_id = _make_run_id(report_date)
    all_warnings = []

    try:
        # Ingest all sources
        all_warnings.extend(ingest_apple(session, report_date, run_id))
        all_warnings.extend(ingest_google(session, report_date, run_id))
        all_warnings.extend(ingest_stripe(session, report_date, run_id))

        # Aggregate
        compute_and_store(session, report_date)

        # Format Slack message
        message = build_slack_message(session, report_date, warnings=all_warnings or None)

        if dry_run:
            logger.info("Dry run — Slack message:\n" + message)
            print("\n" + "="*60)
            print(message)
            print("="*60 + "\n")
        else:
            success = slack_conn.post_message(message)
            if not success:
                logger.error("Failed to post Slack message.")
                return False

        logger.info(f"=== Pipeline DONE: {report_date} ===")
        return True

    except Exception as exc:
        logger.error(f"Pipeline failed: {exc}", exc_info=True)
        return False
    finally:
        session.close()
