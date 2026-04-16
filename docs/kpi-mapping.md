# KPI Mapping — Platform Details & Edge Cases

## Overview

This document explains how each KPI is derived from each platform's data, and how edge cases are handled.

---

## Apple App Store

### Data Sources

| Report | API Endpoint | Type | Usage |
|---|---|---|---|
| SUBSCRIPTION_EVENT | `/v1/salesReports` | SUBSCRIPTION_EVENT/SUMMARY | Events (new, cancel, upgrade) |
| SUBSCRIPTION (SUBSCRIBER) | `/v1/salesReports` | SUBSCRIPTION/SUBSCRIBER | Active snapshot |

### Event Mapping

| Apple `Event` | Canonical EventType |
|---|---|
| Subscribe | NEW_SUBSCRIPTION |
| Resubscribe | NEW_SUBSCRIPTION |
| Cancel | CANCELLATION |
| Voluntary Cancel | CANCELLATION |
| Billing Cancel | CANCELLATION |
| Expiration | CANCELLATION |
| Crossgrade (Upgrade) | PLAN_CHANGE |
| Crossgrade (Downgrade) | PLAN_CHANGE |
| Upgrade | PLAN_CHANGE |
| Downgrade | PLAN_CHANGE |
| **Renewal** | ⛔ SKIPPED |
| Auto-Renew Enabled | ⛔ SKIPPED |
| Auto-Renew Disabled | ⛔ SKIPPED |
| Billing Retry | UNKNOWN |

### KPI Computation

| KPI | Apple Source |
|---|---|
| New Subscriptions | Events with `Event = Subscribe or Resubscribe` |
| Cancellations | Events with `Event = Cancel, Voluntary Cancel, Billing Cancel, Expiration` |
| Gross Sales | `Customer Price` column for new/plan_change events |
| MRR | `Proceeds (USD)` for SUBSCRIBER rows (active). Yearly ÷ 12. |
| Country | `Country` column (2-letter ISO) |
| Plan Interval | `Standard Subscription Duration` → monthly (≤6mo) / yearly (1 Year) |

### Edge Cases

- **Free trials**: Mapped to UNKNOWN (excluded from New Subscriptions counts)
- **Upgrades/crossgrades**: PLAN_CHANGE, not NEW_SUBSCRIPTION — no double-counting
- **J-1 alignment**: Apple reports are dated by `Event Date` in the report
- **Missing country**: Defaults to "UNKNOWN"

---

## Google Play

### Data Sources

| API | Usage |
|---|---|
| `purchases.voidedpurchases.list` | Cancellations (voided subscription purchases) |
| `purchases.subscriptionsv2.get` | Per-subscription enrichment (on-demand) |

### Limitations (V1)

- **New subscriptions**: Not available via polling API without Pub/Sub integration. Requires [Real-time Developer Notifications](https://developer.android.com/google/play/billing/rtdn-reference).
- **Active snapshot**: Not available without Pub/Sub. Android MRR is approximated from events.
- **V2 recommendation**: Integrate Google Cloud Pub/Sub for subscription lifecycle events.

### Event Mapping

| Google Event | Canonical EventType |
|---|---|
| Voided subscription purchase | CANCELLATION |
| One-time product void | ⛔ SKIPPED |

### KPI Computation

| KPI | Google Source |
|---|---|
| Cancellations | Voided subscription purchases |
| Gross Sales | `priceAmountMicros ÷ 1,000,000` |
| Country | `countryCode` field |
| Plan Interval | Inferred from `productId` (contains "monthly" or "yearly") |

---

## Stripe

### Data Sources

| API | Usage |
|---|---|
| `Invoice.list(status=paid, created=...)` | Revenue events (new subs, upgrades) |
| `Subscription.list(status=active)` | MRR snapshot |
| `PaymentIntent.retrieve(expand=payment_method)` | Card country |

### Billing Reason Mapping

| Stripe `billing_reason` | Canonical EventType |
|---|---|
| `subscription_create` | NEW_SUBSCRIPTION |
| `subscription_cycle` | ⛔ SKIPPED (renewal) |
| `subscription_update` + prior active | PLAN_CHANGE |
| `subscription_update` + no prior | NEW_SUBSCRIPTION |
| `manual` | UNKNOWN |

### KPI Computation

| KPI | Stripe Source |
|---|---|
| New Subscriptions | Invoices with `billing_reason = subscription_create` |
| Cancellations | Subscriptions with status=canceled on J-1 (via `canceled_at` date) |
| Gross Sales | `amount_paid ÷ 100` (cents to dollars) |
| MRR | Active subscriptions: monthly=price, yearly=price÷12 |
| Country | `payment_method.card.country` → customer default PM → "UNKNOWN" |
| Plan Interval | `price.recurring.interval` (month/year) |

### Edge Cases

- **monthly → yearly upgrade**: `billing_reason = subscription_update` + customer already had active sub → PLAN_CHANGE
- **Trials**: `amount_paid = 0` → gross_sales = 0, still counts as NEW_SUBSCRIPTION
- **Multi-item subscriptions**: Only first item used for pricing (GOWOD uses single-item plans)
- **Currency**: Stripe stores amounts in currency's smallest unit. USD = cents, EUR = cents, JPY = yen (no conversion).

---

## MRR Calculation

MRR (Monthly Recurring Revenue) is a **snapshot at the end of J-1**:

```
MRR = Σ(active subscriptions) where:
  - monthly plan: MRR_contribution = monthly_price
  - yearly plan:  MRR_contribution = yearly_price / 12
```

**Not included in MRR:**
- Trials (amount = 0)
- Paused subscriptions
- Past-due subscriptions

---

## Deduplication Rules

| Table | Unique Key |
|---|---|
| `raw_apple_reports` | `ingestion_run_id + row_hash` |
| `raw_google_orders` | `ingestion_run_id + order_id` |
| `raw_stripe_invoices` | `ingestion_run_id + invoice_id` |
| `raw_stripe_subscriptions` | `ingestion_run_id + subscription_id` |
| `normalized_subscription_events` | `source + report_date + subscription_external_id + event_type` |
| `normalized_subscription_snapshots` | `source + snapshot_date + subscription_external_id` |

---

## J-1 Alignment

All sources use J-1 (yesterday) as the report date:

| Source | J-1 Definition |
|---|---|
| Apple | Report date parameter = J-1 |
| Google | `startTime/endTime` = J-1 00:00 to 23:59 UTC |
| Stripe | `created[gte/lte]` = J-1 00:00 to 23:59 UTC |

Backfill uses the same logic for each historical date.
