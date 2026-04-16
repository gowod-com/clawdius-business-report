"""Slack message formatter for the daily business report."""
import logging
from typing import List, Optional

from sqlalchemy.orm import Session

from storage.models import (
    DailyBusinessMetrics,
    DailyBusinessMetricsByPlatform,
    DailyBusinessMetricsByCountry,
)

logger = logging.getLogger(__name__)

TOP_N_COUNTRIES = 10


def _fmt_currency(amount: float) -> str:
    if amount >= 1_000:
        return f"${amount:,.0f}"
    return f"${amount:.2f}"


def _platform_emoji(platform: str) -> str:
    return {
        "iOS": "📱",
        "Android": "🤖",
        "Stripe": "💳",
    }.get(platform, "🔷")


def build_slack_message(
    session: Session,
    report_date: str,
    warnings: Optional[List[str]] = None,
) -> str:
    """
    Build the Slack message from aggregated metrics.
    Returns formatted Slack markdown text.
    """
    lines = [f"📊 *Business Report — {report_date}*", ""]

    # Global metrics
    global_row = session.query(DailyBusinessMetrics).filter_by(report_date=report_date).first()

    if global_row:
        lines.append("*🌍 Global*")
        lines.append(f"• Gross Sales: {_fmt_currency(global_row.gross_sales)}")
        lines.append(f"• New Subscriptions: {global_row.new_subscriptions}")
        lines.append(f"• Cancellations: {global_row.cancellations}")
        lines.append(f"• Net New Premiums: {global_row.net_new_premiums:+d}")
        lines.append(f"• MRR: {_fmt_currency(global_row.mrr)}")
    else:
        lines.append("*🌍 Global* — _No data_")

    lines.append("")

    # Per-platform metrics
    platform_rows = (
        session.query(DailyBusinessMetricsByPlatform)
        .filter_by(report_date=report_date)
        .order_by(DailyBusinessMetricsByPlatform.platform)
        .all()
    )

    if platform_rows:
        for row in platform_rows:
            emoji = _platform_emoji(row.platform)
            lines.append(f"*{emoji} {row.platform}*")
            lines.append(f"• Gross Sales: {_fmt_currency(row.gross_sales)}")
            lines.append(f"• New Subscriptions: {row.new_subscriptions}")
            lines.append(f"• Cancellations: {row.cancellations}")
            lines.append(f"• Net New Premiums: {row.net_new_premiums:+d}")
            lines.append(f"• MRR: {_fmt_currency(row.mrr)}")
            lines.append("")
    else:
        lines.append("_No per-platform data_")
        lines.append("")

    # Top countries
    country_rows = (
        session.query(DailyBusinessMetricsByCountry)
        .filter_by(report_date=report_date)
        .order_by(DailyBusinessMetricsByCountry.gross_sales.desc())
        .limit(TOP_N_COUNTRIES)
        .all()
    )

    if country_rows:
        lines.append(f"*🗺 Top Countries* (top {TOP_N_COUNTRIES})")
        for row in country_rows:
            lines.append(
                f"• {row.country}: "
                f"{row.new_subscriptions} new / "
                f"{row.cancellations} cancel / "
                f"{_fmt_currency(row.gross_sales)} gross / "
                f"{_fmt_currency(row.mrr)} MRR"
            )
    else:
        lines.append("_No country data_")

    # Warnings
    if warnings:
        lines.append("")
        for w in warnings:
            lines.append(f"⚠️ {w}")

    return "\n".join(lines)
