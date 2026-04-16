"""SQLAlchemy ORM models for raw and normalized storage."""
import enum
import datetime
from sqlalchemy import (
    Column, String, Integer, Float, Boolean, DateTime, Text, Enum as SAEnum,
    UniqueConstraint, Index
)
from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    pass


# ---------------------------------------------------------------------------
# Raw tables
# ---------------------------------------------------------------------------

class RawAppleReport(Base):
    __tablename__ = "raw_apple_reports"

    id = Column(Integer, primary_key=True, autoincrement=True)
    ingestion_run_id = Column(String(64), nullable=False)
    report_date = Column(String(10), nullable=False)       # YYYY-MM-DD
    report_type = Column(String(64), nullable=False)       # SUBSCRIPTION / SUBSCRIPTION_EVENT
    row_hash = Column(String(64), nullable=False)
    raw_tsv_row = Column(Text, nullable=False)
    ingested_at = Column(DateTime, default=datetime.datetime.utcnow)

    __table_args__ = (
        UniqueConstraint("ingestion_run_id", "row_hash", name="uq_apple_run_hash"),
        Index("ix_raw_apple_date", "report_date"),
    )


class RawGoogleOrder(Base):
    __tablename__ = "raw_google_orders"

    id = Column(Integer, primary_key=True, autoincrement=True)
    ingestion_run_id = Column(String(64), nullable=False)
    order_id = Column(String(256), nullable=False)
    report_date = Column(String(10), nullable=False)
    raw_json = Column(Text, nullable=False)
    ingested_at = Column(DateTime, default=datetime.datetime.utcnow)

    __table_args__ = (
        UniqueConstraint("ingestion_run_id", "order_id", name="uq_google_order_run"),
        Index("ix_raw_google_order_date", "report_date"),
    )


class RawGoogleSubscription(Base):
    __tablename__ = "raw_google_subscriptions"

    id = Column(Integer, primary_key=True, autoincrement=True)
    ingestion_run_id = Column(String(64), nullable=False)
    purchase_token = Column(String(512), nullable=False)
    report_date = Column(String(10), nullable=False)
    raw_json = Column(Text, nullable=False)
    ingested_at = Column(DateTime, default=datetime.datetime.utcnow)

    __table_args__ = (
        UniqueConstraint("ingestion_run_id", "purchase_token", name="uq_google_sub_run"),
        Index("ix_raw_google_sub_date", "report_date"),
    )


class RawStripeInvoice(Base):
    __tablename__ = "raw_stripe_invoices"

    id = Column(Integer, primary_key=True, autoincrement=True)
    ingestion_run_id = Column(String(64), nullable=False)
    invoice_id = Column(String(256), nullable=False)
    report_date = Column(String(10), nullable=False)
    raw_json = Column(Text, nullable=False)
    ingested_at = Column(DateTime, default=datetime.datetime.utcnow)

    __table_args__ = (
        UniqueConstraint("ingestion_run_id", "invoice_id", name="uq_stripe_invoice_run"),
        Index("ix_raw_stripe_invoice_date", "report_date"),
    )


class RawStripeSubscription(Base):
    __tablename__ = "raw_stripe_subscriptions"

    id = Column(Integer, primary_key=True, autoincrement=True)
    ingestion_run_id = Column(String(64), nullable=False)
    subscription_id = Column(String(256), nullable=False)
    report_date = Column(String(10), nullable=False)
    raw_json = Column(Text, nullable=False)
    ingested_at = Column(DateTime, default=datetime.datetime.utcnow)

    __table_args__ = (
        UniqueConstraint("ingestion_run_id", "subscription_id", name="uq_stripe_sub_run"),
        Index("ix_raw_stripe_sub_date", "report_date"),
    )


# ---------------------------------------------------------------------------
# Normalized tables
# ---------------------------------------------------------------------------

class EventType(enum.Enum):
    NEW_SUBSCRIPTION = "NEW_SUBSCRIPTION"
    CANCELLATION = "CANCELLATION"
    ACTIVE_SNAPSHOT = "ACTIVE_SNAPSHOT"
    PLAN_CHANGE = "PLAN_CHANGE"
    UNKNOWN = "UNKNOWN"


class Platform(enum.Enum):
    IOS = "iOS"
    ANDROID = "Android"
    STRIPE = "Stripe"
    UNKNOWN = "UNKNOWN"


class PlanInterval(enum.Enum):
    MONTHLY = "monthly"
    YEARLY = "yearly"
    UNKNOWN = "unknown"


class NormalizedSubscriptionEvent(Base):
    __tablename__ = "normalized_subscription_events"

    id = Column(Integer, primary_key=True, autoincrement=True)
    ingestion_run_id = Column(String(64), nullable=False)
    source = Column(String(32), nullable=False)             # apple / google / stripe
    report_date = Column(String(10), nullable=False)        # YYYY-MM-DD
    event_timestamp = Column(DateTime, nullable=True)
    platform = Column(String(16), nullable=False)           # iOS / Android / Stripe
    country = Column(String(8), nullable=False, default="UNKNOWN")
    currency = Column(String(8), nullable=False, default="USD")
    plan_interval = Column(String(16), nullable=False, default="unknown")
    event_type = Column(SAEnum(EventType), nullable=False)
    gross_amount = Column(Float, nullable=False, default=0.0)
    subscription_external_id = Column(String(512), nullable=True)
    customer_external_id = Column(String(512), nullable=True)
    mrr_amount = Column(Float, nullable=False, default=0.0)
    is_active_snapshot = Column(Boolean, nullable=False, default=False)
    extra_metadata = Column(Text, nullable=True)            # JSON blob
    created_at = Column(DateTime, default=datetime.datetime.utcnow)

    __table_args__ = (
        UniqueConstraint(
            "source", "report_date", "subscription_external_id", "event_type",
            name="uq_norm_event"
        ),
        Index("ix_norm_event_date", "report_date"),
        Index("ix_norm_event_platform", "platform"),
    )


class NormalizedSubscriptionSnapshot(Base):
    """Active subscription snapshot at end of J-1 for MRR calculation."""
    __tablename__ = "normalized_subscription_snapshots"

    id = Column(Integer, primary_key=True, autoincrement=True)
    ingestion_run_id = Column(String(64), nullable=False)
    source = Column(String(32), nullable=False)
    snapshot_date = Column(String(10), nullable=False)
    platform = Column(String(16), nullable=False)
    country = Column(String(8), nullable=False, default="UNKNOWN")
    currency = Column(String(8), nullable=False, default="USD")
    plan_interval = Column(String(16), nullable=False, default="unknown")
    subscription_external_id = Column(String(512), nullable=False)
    customer_external_id = Column(String(512), nullable=True)
    mrr_amount = Column(Float, nullable=False, default=0.0)
    gross_price = Column(Float, nullable=False, default=0.0)
    created_at = Column(DateTime, default=datetime.datetime.utcnow)

    __table_args__ = (
        UniqueConstraint(
            "source", "snapshot_date", "subscription_external_id",
            name="uq_snapshot"
        ),
        Index("ix_snapshot_date", "snapshot_date"),
    )


# ---------------------------------------------------------------------------
# Aggregated metrics tables
# ---------------------------------------------------------------------------

class DailyBusinessMetrics(Base):
    """Global daily metrics."""
    __tablename__ = "daily_business_metrics"

    id = Column(Integer, primary_key=True, autoincrement=True)
    report_date = Column(String(10), nullable=False, unique=True)
    new_subscriptions = Column(Integer, nullable=False, default=0)
    cancellations = Column(Integer, nullable=False, default=0)
    net_new_premiums = Column(Integer, nullable=False, default=0)
    gross_sales = Column(Float, nullable=False, default=0.0)
    mrr = Column(Float, nullable=False, default=0.0)
    computed_at = Column(DateTime, default=datetime.datetime.utcnow)


class DailyBusinessMetricsByPlatform(Base):
    __tablename__ = "daily_business_metrics_by_platform"

    id = Column(Integer, primary_key=True, autoincrement=True)
    report_date = Column(String(10), nullable=False)
    platform = Column(String(16), nullable=False)
    new_subscriptions = Column(Integer, nullable=False, default=0)
    cancellations = Column(Integer, nullable=False, default=0)
    net_new_premiums = Column(Integer, nullable=False, default=0)
    gross_sales = Column(Float, nullable=False, default=0.0)
    mrr = Column(Float, nullable=False, default=0.0)
    computed_at = Column(DateTime, default=datetime.datetime.utcnow)

    __table_args__ = (
        UniqueConstraint("report_date", "platform", name="uq_metrics_platform"),
    )


class DailyBusinessMetricsByCountry(Base):
    __tablename__ = "daily_business_metrics_by_country"

    id = Column(Integer, primary_key=True, autoincrement=True)
    report_date = Column(String(10), nullable=False)
    country = Column(String(8), nullable=False)
    new_subscriptions = Column(Integer, nullable=False, default=0)
    cancellations = Column(Integer, nullable=False, default=0)
    net_new_premiums = Column(Integer, nullable=False, default=0)
    gross_sales = Column(Float, nullable=False, default=0.0)
    mrr = Column(Float, nullable=False, default=0.0)
    computed_at = Column(DateTime, default=datetime.datetime.utcnow)

    __table_args__ = (
        UniqueConstraint("report_date", "country", name="uq_metrics_country"),
    )


class DailyBusinessMetricsByPlatformCountry(Base):
    __tablename__ = "daily_business_metrics_by_platform_country"

    id = Column(Integer, primary_key=True, autoincrement=True)
    report_date = Column(String(10), nullable=False)
    platform = Column(String(16), nullable=False)
    country = Column(String(8), nullable=False)
    new_subscriptions = Column(Integer, nullable=False, default=0)
    cancellations = Column(Integer, nullable=False, default=0)
    net_new_premiums = Column(Integer, nullable=False, default=0)
    gross_sales = Column(Float, nullable=False, default=0.0)
    mrr = Column(Float, nullable=False, default=0.0)
    computed_at = Column(DateTime, default=datetime.datetime.utcnow)

    __table_args__ = (
        UniqueConstraint("report_date", "platform", "country", name="uq_metrics_platform_country"),
    )
