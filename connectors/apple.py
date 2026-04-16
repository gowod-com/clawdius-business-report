"""Apple App Store Connect connector.

Fetches Sales/Subscription reports using the App Store Connect REST API.
Auth: JWT (ES256), 20-min expiry, signed with the p8 private key.
Reports are gzipped TSV files.
"""
import gzip
import hashlib
import io
import json
import logging
import time
import datetime
from typing import Iterator, Dict, Any

import requests
import jwt

import config

logger = logging.getLogger(__name__)

ASC_BASE_URL = "https://api.appstoreconnect.apple.com"
JWT_AUDIENCE = "appstoreconnect-v1"
JWT_EXPIRY_SECS = 20 * 60  # 20 minutes


class AppleConnectorError(Exception):
    pass


def _load_private_key() -> str:
    """
    Load the Apple private key. Supports two formats:
    - APPLE_PRIVATE_KEY_FILE: path to a .p8 file
    - APPLE_PRIVATE_KEY: inline PEM content (supports \\n-escaped or multi-line)
    """
    import os
    key_file = os.getenv("APPLE_PRIVATE_KEY_FILE", "")
    if key_file:
        key_path = key_file.strip()
        if not os.path.isabs(key_path):
            key_path = str(config._ROOT / key_path)
        with open(key_path, "r") as f:
            return f.read().strip()

    # Inline key from config (already has \n -> newline replacement applied in config.py)
    key = config.APPLE_PRIVATE_KEY
    if not key:
        raise AppleConnectorError("No Apple private key configured. Set APPLE_PRIVATE_KEY_FILE or APPLE_PRIVATE_KEY.")
    return key.strip()


def _make_jwt() -> str:
    """Generate a signed JWT for App Store Connect API."""
    now = int(time.time())
    payload = {
        "iss": config.APPLE_ISSUER_ID,
        "iat": now,
        "exp": now + JWT_EXPIRY_SECS,
        "aud": JWT_AUDIENCE,
    }
    private_key = _load_private_key()
    token = jwt.encode(
        payload,
        private_key,
        algorithm="ES256",
        headers={"kid": config.APPLE_KEY_ID},
    )
    return token


def _get_headers() -> Dict[str, str]:
    return {"Authorization": f"Bearer {_make_jwt()}"}


def _fetch_with_retry(url: str, params: Dict, max_retries: int = 3, backoff: float = 2.0) -> requests.Response:
    """GET request with exponential backoff on 429/5xx."""
    for attempt in range(max_retries):
        try:
            resp = requests.get(url, headers=_get_headers(), params=params, timeout=60)
            if resp.status_code == 429:
                retry_after = float(resp.headers.get("Retry-After", backoff * (2 ** attempt)))
                logger.warning(f"Apple API rate limited. Waiting {retry_after}s")
                time.sleep(retry_after)
                continue
            if resp.status_code >= 500:
                wait = backoff * (2 ** attempt)
                logger.warning(f"Apple API server error {resp.status_code}. Waiting {wait}s")
                time.sleep(wait)
                continue
            return resp
        except requests.RequestException as exc:
            if attempt < max_retries - 1:
                wait = backoff * (2 ** attempt)
                logger.warning(f"Apple request failed: {exc}. Retrying in {wait}s")
                time.sleep(wait)
            else:
                raise AppleConnectorError(f"Apple API request failed after {max_retries} attempts: {exc}")
    raise AppleConnectorError(f"Apple API exhausted retries for {url}")


def _parse_tsv_report(gzip_content: bytes) -> Iterator[Dict[str, str]]:
    """Decompress and parse a gzipped TSV report into row dicts."""
    with gzip.GzipFile(fileobj=io.BytesIO(gzip_content)) as gz:
        text = gz.read().decode("utf-8")
    lines = text.splitlines()
    if not lines:
        return
    headers = lines[0].split("\t")
    for line in lines[1:]:
        if not line.strip():
            continue
        values = line.split("\t")
        yield dict(zip(headers, values))


def fetch_subscription_report(report_date: str) -> Iterator[Dict[str, Any]]:
    """
    Fetch the SUBSCRIPTION (SUBSCRIBER subtype) report for a given date.
    report_date: YYYY-MM-DD
    Yields dicts of {row_hash, raw_tsv_row, row_data}.
    """
    if not config.is_apple_configured():
        logger.warning("Apple connector not configured, skipping.")
        return

    params = {
        "filter[reportType]": "SUBSCRIPTION",
        "filter[reportSubType]": "SUBSCRIBER",
        "filter[frequency]": "DAILY",
        "filter[reportDate]": report_date,
        "filter[vendorNumber]": config.APPLE_VENDOR_NUMBER,
        "filter[version]": "1_3",
    }

    url = f"{ASC_BASE_URL}/v1/salesReports"
    logger.info(f"Fetching Apple subscription report for {report_date}")
    resp = _fetch_with_retry(url, params)

    if resp.status_code == 404:
        logger.warning(f"Apple: No subscription report found for {report_date}")
        return
    if resp.status_code != 200:
        logger.error(f"Apple subscription report error {resp.status_code}: {resp.text[:500]}")
        return

    rows = list(_parse_tsv_report(resp.content))
    logger.info(f"Apple: {len(rows)} subscription rows for {report_date}")

    for row in rows:
        raw_str = "\t".join(row.values())
        row_hash = hashlib.sha256(raw_str.encode()).hexdigest()
        yield {
            "row_hash": row_hash,
            "raw_tsv_row": raw_str,
            "row_data": row,
        }


def fetch_subscription_event_report(report_date: str) -> Iterator[Dict[str, Any]]:
    """
    Fetch the SUBSCRIPTION_EVENT report for a given date.
    Yields dicts of {row_hash, raw_tsv_row, row_data}.
    """
    if not config.is_apple_configured():
        logger.warning("Apple connector not configured, skipping.")
        return

    params = {
        "filter[reportType]": "SUBSCRIPTION_EVENT",
        "filter[reportSubType]": "SUMMARY",
        "filter[frequency]": "DAILY",
        "filter[reportDate]": report_date,
        "filter[vendorNumber]": config.APPLE_VENDOR_NUMBER,
        "filter[version]": "1_3",
    }

    url = f"{ASC_BASE_URL}/v1/salesReports"
    logger.info(f"Fetching Apple subscription event report for {report_date}")
    resp = _fetch_with_retry(url, params)

    if resp.status_code == 404:
        logger.warning(f"Apple: No subscription event report found for {report_date}")
        return
    if resp.status_code != 200:
        logger.error(f"Apple event report error {resp.status_code}: {resp.text[:500]}")
        return

    rows = list(_parse_tsv_report(resp.content))
    logger.info(f"Apple: {len(rows)} subscription event rows for {report_date}")

    for row in rows:
        raw_str = "\t".join(row.values())
        row_hash = hashlib.sha256(raw_str.encode()).hexdigest()
        yield {
            "row_hash": row_hash,
            "raw_tsv_row": raw_str,
            "row_data": row,
        }
