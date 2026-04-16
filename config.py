"""Central configuration for clawdius-business-report."""
import os
from pathlib import Path
from dotenv import load_dotenv

# Load .env from project root
_ROOT = Path(__file__).parent
load_dotenv(_ROOT / ".env")


def _require(key: str) -> str:
    val = os.getenv(key)
    if not val:
        raise EnvironmentError(f"Required env var '{key}' is not set.")
    return val


def _optional(key: str, default: str = "") -> str:
    return os.getenv(key, default)


# ---------------------------------------------------------------------------
# Apple App Store Connect
# ---------------------------------------------------------------------------
APPLE_KEY_ID = _optional("APPLE_KEY_ID")
APPLE_ISSUER_ID = _optional("APPLE_ISSUER_ID")
APPLE_PRIVATE_KEY = _optional("APPLE_PRIVATE_KEY")  # PEM / p8 content
APPLE_VENDOR_NUMBER = _optional("APPLE_VENDOR_NUMBER")

# ---------------------------------------------------------------------------
# Google Play
# ---------------------------------------------------------------------------
GOOGLE_SERVICE_ACCOUNT_JSON = _optional("GOOGLE_SERVICE_ACCOUNT_JSON")
GOOGLE_PACKAGE_NAME = _optional("GOOGLE_PACKAGE_NAME")

# ---------------------------------------------------------------------------
# Stripe
# ---------------------------------------------------------------------------
STRIPE_API_KEY = _optional("STRIPE_API_KEY")

# ---------------------------------------------------------------------------
# Slack
# ---------------------------------------------------------------------------
SLACK_BOT_TOKEN = _optional("SLACK_BOT_TOKEN")
SLACK_CHANNEL_ID = _optional("SLACK_CHANNEL_ID", "C0ATYL0UH7A")

# ---------------------------------------------------------------------------
# General
# ---------------------------------------------------------------------------
REPORT_TIMEZONE = _optional("REPORT_TIMEZONE", "UTC")
DATABASE_URL = _optional("DATABASE_URL", "sqlite:///data/business_report.db")
LOG_LEVEL = _optional("LOG_LEVEL", "INFO")


def is_apple_configured() -> bool:
    return all([APPLE_KEY_ID, APPLE_ISSUER_ID, APPLE_PRIVATE_KEY, APPLE_VENDOR_NUMBER])


def is_google_configured() -> bool:
    return all([GOOGLE_SERVICE_ACCOUNT_JSON, GOOGLE_PACKAGE_NAME])


def is_stripe_configured() -> bool:
    return bool(STRIPE_API_KEY)


def is_slack_configured() -> bool:
    return bool(SLACK_BOT_TOKEN)
