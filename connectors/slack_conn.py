"""Slack connector for posting business reports."""
import logging
from typing import Optional

import config

logger = logging.getLogger(__name__)


class SlackConnectorError(Exception):
    pass


def post_message(text: str, channel: Optional[str] = None) -> bool:
    """
    Post a message to Slack.
    Returns True on success, False on failure.
    """
    if not config.is_slack_configured():
        logger.warning("Slack connector not configured, message not sent.")
        return False

    try:
        from slack_sdk import WebClient
        from slack_sdk.errors import SlackApiError
    except ImportError:
        raise SlackConnectorError("slack-sdk not installed. Run: pip install slack-sdk")

    target_channel = channel or config.SLACK_CHANNEL_ID
    client = WebClient(token=config.SLACK_BOT_TOKEN)

    try:
        response = client.chat_postMessage(
            channel=target_channel,
            text=text,
            mrkdwn=True,
        )
        logger.info(f"Slack message posted to {target_channel}, ts={response['ts']}")
        return True
    except SlackApiError as e:
        logger.error(f"Slack API error: {e.response['error']}")
        return False
    except Exception as e:
        logger.error(f"Unexpected Slack error: {e}")
        return False
