"""Daily business metrics aggregator.

Reads from normalized_subscription_events and normalized_subscription_snapshots,
computes KPIs, and writes to daily_business_metrics* tables.
Idempotent: deletes and re-inserts for the given date.
"""
import logging
import datetime
from collections import defaultdict
from typing import Dict, List, Tuple

from sqlalchemy.orm import Session

from storage.models import (
    NormalizedSubscriptionEvent,
    NormalizedSubscriptionSnapshot,
    DailyBusinessMetrics,
    DailyBusinessMetricsByPlatform,
    DailyBusinessMetricsByCountry,
    DailyBusinessMetricsByPlatformCountry,
    EventType,
)

logger = logging.getLogger(__name__)


def _zero_metrics() -> dict:
    return {
        "new_subscriptions": 0,
        "cancellations": 0,
        "gross_sales": 0.0,
        "mrr": 0.0,
    }


def compute_and_store(session: Session, report_date: str) -> dict:
    """
    Aggregate KPIs for report_date from normalized data.
    Returns summary dict.
    """
    logger.info(f"Aggregating KPIs for {report_date}")

    # -----------------------------------------------------------------------
    # Load events for this date
    # -----------------------------------------------------------------------
    events: List[NormalizedSubscriptionEvent] = (
        session.query(NormalizedSubscriptionEvent)
        .filter(NormalizedSubscriptionEvent.report_date == report_date)
        .all()
    )

    # Load MRR snapshot for this date
    snapshots: List[NormalizedSubscriptionSnapshot] = (
        session.query(NormalizedSubscriptionSnapshot)
        .filter(NormalizedSubscriptionSnapshot.snapshot_date == report_date)
        .all()
    )

    # -----------------------------------------------------------------------
    # Compute global metrics
    # -----------------------------------------------------------------------
    global_metrics = _zero_metrics()
    by_platform: Dict[str, dict] = defaultdict(_zero_metrics)
    by_country: Dict[str, dict] = defaultdict(_zero_metrics)
    by_platform_country: Dict[Tuple[str, str], dict] = defaultdict(_zero_metrics)

    for event in events:
        platform = event.platform
        country = event.country

        if event.event_type == EventType.NEW_SUBSCRIPTION:
            global_metrics["new_subscriptions"] += 1
            by_platform[platform]["new_subscriptions"] += 1
            by_country[country]["new_subscriptions"] += 1
            by_platform_country[(platform, country)]["new_subscriptions"] += 1

        elif event.event_type == EventType.CANCELLATION:
            global_metrics["cancellations"] += 1
            by_platform[platform]["cancellations"] += 1
            by_country[country]["cancellations"] += 1
            by_platform_country[(platform, country)]["cancellations"] += 1

        # Gross sales = all paid invoices (new subs + renewals), excluding cancellations
        # UNKNOWN events with gross_amount > 0 are renewals (billing_reason=subscription_cycle)
        if event.event_type != EventType.CANCELLATION and (event.gross_amount or 0.0) > 0:
            global_metrics["gross_sales"] += event.gross_amount or 0.0
            by_platform[platform]["gross_sales"] += event.gross_amount or 0.0
            by_country[country]["gross_sales"] += event.gross_amount or 0.0
            by_platform_country[(platform, country)]["gross_sales"] += event.gross_amount or 0.0

    # MRR from snapshots
    for snap in snapshots:
        platform = snap.platform
        country = snap.country
        mrr = snap.mrr_amount or 0.0

        global_metrics["mrr"] += mrr
        by_platform[platform]["mrr"] += mrr
        by_country[country]["mrr"] += mrr
        by_platform_country[(platform, country)]["mrr"] += mrr

    now = datetime.datetime.utcnow()

    # -----------------------------------------------------------------------
    # Idempotent upsert — delete old rows first
    # -----------------------------------------------------------------------
    session.query(DailyBusinessMetrics).filter_by(report_date=report_date).delete()
    session.query(DailyBusinessMetricsByPlatform).filter_by(report_date=report_date).delete()
    session.query(DailyBusinessMetricsByCountry).filter_by(report_date=report_date).delete()
    session.query(DailyBusinessMetricsByPlatformCountry).filter_by(report_date=report_date).delete()

    # Global
    net = global_metrics["new_subscriptions"] - global_metrics["cancellations"]
    session.add(DailyBusinessMetrics(
        report_date=report_date,
        new_subscriptions=global_metrics["new_subscriptions"],
        cancellations=global_metrics["cancellations"],
        net_new_premiums=net,
        gross_sales=round(global_metrics["gross_sales"], 2),
        mrr=round(global_metrics["mrr"], 2),
        computed_at=now,
    ))

    # By platform
    for platform, m in by_platform.items():
        net_p = m["new_subscriptions"] - m["cancellations"]
        session.add(DailyBusinessMetricsByPlatform(
            report_date=report_date,
            platform=platform,
            new_subscriptions=m["new_subscriptions"],
            cancellations=m["cancellations"],
            net_new_premiums=net_p,
            gross_sales=round(m["gross_sales"], 2),
            mrr=round(m["mrr"], 2),
            computed_at=now,
        ))

    # By country
    for country, m in by_country.items():
        net_c = m["new_subscriptions"] - m["cancellations"]
        session.add(DailyBusinessMetricsByCountry(
            report_date=report_date,
            country=country,
            new_subscriptions=m["new_subscriptions"],
            cancellations=m["cancellations"],
            net_new_premiums=net_c,
            gross_sales=round(m["gross_sales"], 2),
            mrr=round(m["mrr"], 2),
            computed_at=now,
        ))

    # By platform+country
    for (platform, country), m in by_platform_country.items():
        net_pc = m["new_subscriptions"] - m["cancellations"]
        session.add(DailyBusinessMetricsByPlatformCountry(
            report_date=report_date,
            platform=platform,
            country=country,
            new_subscriptions=m["new_subscriptions"],
            cancellations=m["cancellations"],
            net_new_premiums=net_pc,
            gross_sales=round(m["gross_sales"], 2),
            mrr=round(m["mrr"], 2),
            computed_at=now,
        ))

    session.commit()
    logger.info(
        f"Aggregation done for {report_date}: "
        f"new={global_metrics['new_subscriptions']}, "
        f"cancel={global_metrics['cancellations']}, "
        f"gross=${global_metrics['gross_sales']:.2f}, "
        f"mrr=${global_metrics['mrr']:.2f}"
    )

    return {
        "report_date": report_date,
        "global": global_metrics,
        "by_platform": dict(by_platform),
        "by_country": dict(by_country),
    }
