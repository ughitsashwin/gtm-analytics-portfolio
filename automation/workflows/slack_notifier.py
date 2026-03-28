# automation/workflows/slack_notifier.py
#
# WHAT THIS DOES:
# Sends formatted messages to Slack via an Incoming Webhook.
# A webhook is just a URL — when you POST JSON to it, Slack displays it
# as a message in the channel you configured.
#
# WHY NOT USE THE SLACK API DIRECTLY?
# The full Slack API requires OAuth, bot tokens, and more setup.
# Incoming Webhooks are simpler and sufficient for one-way notifications
# like automated reports. We use the full API only if we need two-way
# interaction (e.g. a Slack bot that answers questions).

import os
import json
import urllib.request
from dotenv import load_dotenv

load_dotenv()


def post_to_slack(text: str, title: str = None) -> bool:
    """
    Post a message to Slack via Incoming Webhook.

    Slack supports "Block Kit" for rich formatting — we use a simple
    header block + text block pattern here.

    Args:
        text:  The message body (plain text or markdown)
        title: Optional bold header shown above the message

    Returns:
        True if successful, False if failed
    """
    webhook_url = os.environ.get("SLACK_WEBHOOK_URL")
    if not webhook_url:
        print("ERROR: SLACK_WEBHOOK_URL not found in .env")
        return False

    # Slack Block Kit — a structured JSON format for rich messages.
    # "blocks" is a list of UI components that Slack renders.
    # We use two block types:
    #   "header" — large bold text at the top
    #   "section" — regular text body (supports markdown with mrkdwn)
    blocks = []

    if title:
        blocks.append({
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": title,
                "emoji": True
            }
        })

    # Split long messages into chunks — Slack has a 3000 char limit per block
    # We chunk at 2900 to leave a safe margin
    chunk_size = 2900
    chunks = [text[i:i+chunk_size] for i in range(0, len(text), chunk_size)]

    for chunk in chunks:
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",    # mrkdwn = Slack's markdown flavour
                "text": chunk
            }
        })

    # Add a divider at the bottom for visual separation
    blocks.append({"type": "divider"})

    payload = json.dumps({"blocks": blocks}).encode("utf-8")

    # Send via urllib (built-in) — no extra dependencies needed for a POST
    req = urllib.request.Request(
        webhook_url,
        data=payload,
        headers={"Content-Type": "application/json"}
    )

    try:
        with urllib.request.urlopen(req) as response:
            if response.status == 200:
                print(f"Posted to Slack successfully")
                return True
            else:
                print(f"Slack returned status {response.status}")
                return False
    except Exception as e:
        print(f"Failed to post to Slack: {e}")
        return False