# clawdius-business-report

Daily business KPI report for GOWOD — consolidates Apple App Store, Google Play, and Stripe subscription data into a single Slack report.

## Features

- 📊 **Daily KPIs**: New subscriptions, cancellations, net new premiums, gross sales, MRR
- 🌍 **Segmentation**: Global, by platform (iOS/Android/Stripe), by country, by plan interval
- 🔄 **Backfill support**: Run for any historical date range with idempotence
- 🛡️ **Robust**: Retry logic, deduplication, graceful degradation per source
- 📱 **Slack delivery**: Clean, business-oriented message format

## Architecture

```
clawdius-business-report/
├── connectors/          # API clients (Apple, Google, Stripe, Slack)
├── storage/             # SQLite via SQLAlchemy (raw + normalized tables)
├── normalizers/         # Platform → canonical event model
├── aggregators/         # KPI computation (daily_aggregator.py)
├── delivery/            # Slack message formatting
├── tests/               # Unit tests (fully mocked)
├── docs/                # KPI mapping and edge cases
├── run_report.py        # Entry point
├── pipeline.py          # Orchestration
└── config.py            # Env-based configuration
```

### Data Flow

```
APIs → Connectors → Raw SQLite tables
                  → Normalizers → normalized_subscription_events
                                → normalized_subscription_snapshots
                  → Aggregator  → daily_business_metrics*
                  → Formatter   → Slack message → #business-report
```

## Setup

### Prerequisites

- Python 3.12+
- Pip

### Installation

```bash
cd clawdius-business-report
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### Configuration

Copy `.env.example` to `.env` and fill in your credentials:

```bash
cp .env.example .env
```

Required env vars:

| Variable | Source | Description |
|---|---|---|
| `APPLE_KEY_ID` | App Store Connect → Keys | Private key ID |
| `APPLE_ISSUER_ID` | App Store Connect → Keys | Issuer ID |
| `APPLE_PRIVATE_KEY` | Downloaded `.p8` file content | ES256 private key (PEM) |
| `APPLE_VENDOR_NUMBER` | App Store Connect → Payments | Vendor number |
| `GOOGLE_SERVICE_ACCOUNT_JSON` | GCP console | Path to service account JSON |
| `GOOGLE_PACKAGE_NAME` | Google Play Console | App package name |
| `STRIPE_API_KEY` | Stripe Dashboard → API Keys | Secret key (sk_live_...) |
| `SLACK_BOT_TOKEN` | Slack App settings | Bot token (xoxb-...) |
| `SLACK_CHANNEL_ID` | Slack channel | Channel ID |

## Usage

```bash
# Run for yesterday (J-1)
python run_report.py

# Run for a specific date
python run_report.py --date 2026-04-15

# Backfill a date range (idempotent)
python run_report.py --backfill --from 2026-04-01 --to 2026-04-15

# Dry run (print to stdout, don't post to Slack)
python run_report.py --dry-run
python run_report.py --date 2026-04-15 --dry-run
```

## KPI Definitions

| KPI | Definition |
|---|---|
| **New Subscriptions** | First-time subscriptions started on J-1. Renewals excluded. |
| **Cancellations** | Subscriptions canceled/expired on J-1. |
| **Net New Premiums** | New Subscriptions − Cancellations |
| **Gross Sales** | Sum of customer-facing invoice amounts on J-1. Renewals included in Apple/Google. |
| **MRR** | Snapshot of active subscription value at end of J-1. Monthly=price, Yearly=price÷12. |

See `docs/kpi-mapping.md` for platform-specific details and edge cases.

## Running Tests

```bash
pytest tests/ -v
```

## Slack Output Format

```
📊 *Business Report — 2026-04-15*

*🌍 Global*
• Gross Sales: $1,234
• New Subscriptions: 42
• Cancellations: 5
• Net New Premiums: +37
• MRR: $8,900

*📱 iOS*
• Gross Sales: $700
• ...

*💳 Stripe*
• ...

*🗺 Top Countries* (top 10)
• FR: 20 new / 2 cancel / $500 gross / $3,000 MRR
• US: 10 new / 1 cancel / $300 gross / $2,000 MRR

⚠️ [warnings if any source failed]
```

## Database

SQLite database at `data/business_report.db` (auto-created).

Key tables:
- `raw_apple_reports`, `raw_google_orders`, `raw_stripe_invoices`, `raw_stripe_subscriptions` — raw ingestion
- `normalized_subscription_events` — canonical events
- `normalized_subscription_snapshots` — MRR snapshots
- `daily_business_metrics` — global aggregates
- `daily_business_metrics_by_platform` — per-platform aggregates
- `daily_business_metrics_by_country` — per-country aggregates

## Notes

- All times are UTC
- Backfill is fully idempotent (safe to re-run)
- Each source fails independently — partial data is reported with warnings
- `.env` is gitignored — never commit credentials
