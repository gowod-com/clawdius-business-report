"""Google Play Developer API connector.

Uses google-api-python-client + google-auth with a service account.
Scope: https://www.googleapis.com/auth/androidpublisher
"""
import json
import logging
import time
import datetime
from typing import Iterator, Dict, Any, Optional

import config

logger = logging.getLogger(__name__)

ANDROID_PUBLISHER_SCOPE = "https://www.googleapis.com/auth/androidpublisher"


class GoogleConnectorError(Exception):
    pass


def _build_service():
    """Build and return the googleapiclient service for androidpublisher."""
    try:
        from google.oauth2 import service_account
        from googleapiclient.discovery import build
    except ImportError:
        raise GoogleConnectorError(
            "google-api-python-client and google-auth are required. "
            "Run: pip install google-api-python-client google-auth"
        )

    if not config.is_google_configured():
        raise GoogleConnectorError("Google connector not configured.")

    credentials = service_account.Credentials.from_service_account_file(
        config.GOOGLE_SERVICE_ACCOUNT_JSON,
        scopes=[ANDROID_PUBLISHER_SCOPE],
    )
    service = build("androidpublisher", "v3", credentials=credentials, cache_discovery=False)
    return service


def _retry(fn, max_retries: int = 3, backoff: float = 2.0):
    """Execute fn with retries on transient errors."""
    for attempt in range(max_retries):
        try:
            return fn()
        except Exception as exc:
            err_str = str(exc)
            # 429 or 5xx from googleapiclient
            if "429" in err_str or "500" in err_str or "503" in err_str:
                wait = backoff * (2 ** attempt)
                logger.warning(f"Google API transient error: {exc}. Retrying in {wait}s")
                time.sleep(wait)
            elif attempt < max_retries - 1:
                wait = backoff * (2 ** attempt)
                logger.warning(f"Google API error: {exc}. Retrying in {wait}s")
                time.sleep(wait)
            else:
                raise GoogleConnectorError(f"Google API failed after {max_retries} attempts: {exc}")
    raise GoogleConnectorError("Google API exhausted retries.")


def fetch_voided_purchases(report_date: str) -> Iterator[Dict[str, Any]]:
    """
    Fetch voided purchases (cancellations) for a given date.
    report_date: YYYY-MM-DD
    """
    if not config.is_google_configured():
        logger.warning("Google connector not configured, skipping.")
        return

    try:
        service = _build_service()
    except GoogleConnectorError as e:
        logger.error(f"Google connector error: {e}")
        return

    package_name = config.GOOGLE_PACKAGE_NAME
    # Convert date to epoch ms
    date_obj = datetime.date.fromisoformat(report_date)
    start_ms = int(datetime.datetime(date_obj.year, date_obj.month, date_obj.day, 0, 0, 0).timestamp() * 1000)
    end_ms = int(datetime.datetime(date_obj.year, date_obj.month, date_obj.day, 23, 59, 59).timestamp() * 1000)

    page_token = None
    total = 0

    while True:
        kwargs = {
            "packageName": package_name,
            "startTime": start_ms,
            "endTime": end_ms,
            "maxResults": 1000,
        }
        if page_token:
            kwargs["token"] = page_token

        def _call():
            return service.purchases().voidedpurchases().list(**kwargs).execute()

        try:
            result = _retry(_call)
        except GoogleConnectorError as e:
            logger.error(f"Failed to fetch Google voided purchases: {e}")
            return

        for item in result.get("voidedPurchases", []):
            total += 1
            yield {
                "order_id": item.get("orderId", item.get("purchaseToken", "")),
                "raw_data": item,
            }

        page_token = result.get("nextPageToken")
        if not page_token:
            break

    logger.info(f"Google: {total} voided purchases for {report_date}")


def fetch_active_subscriptions(report_date: str) -> Iterator[Dict[str, Any]]:
    """
    Fetch active subscriptions for MRR snapshot.
    Uses purchases.subscriptionsv2 — requires knowing purchase tokens.
    In practice, this is done via Google Cloud Pub/Sub or Realtime Developer
    Notifications. Here we use a best-effort approach via the monetization API.

    For V1, we return an empty iterator with a warning if Pub/Sub is not set up.
    The MRR snapshot for Google is approximated from normalized events.
    """
    if not config.is_google_configured():
        logger.warning("Google connector not configured, skipping.")
        return

    logger.warning(
        "Google Play active subscription snapshot requires Pub/Sub integration. "
        "Skipping for V1. MRR for Android will be approximated from events."
    )
    return


def fetch_subscription_purchases(purchase_token: str) -> Optional[Dict[str, Any]]:
    """
    Fetch details for a specific subscription purchase by token.
    Used for enriching individual events.
    """
    if not config.is_google_configured():
        return None

    try:
        service = _build_service()
    except GoogleConnectorError as e:
        logger.error(f"Google connector error: {e}")
        return None

    def _call():
        return (
            service.purchases()
            .subscriptionsv2()
            .get(
                packageName=config.GOOGLE_PACKAGE_NAME,
                token=purchase_token,
            )
            .execute()
        )

    try:
        return _retry(_call)
    except GoogleConnectorError as e:
        logger.error(f"Failed to fetch Google subscription {purchase_token}: {e}")
        return None
