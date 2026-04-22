"""
utils/slack_alert.py
--------------------
Slack notifications for Flow B pipeline via incoming webhook.

Setup (one-time)
----------------
1. Go to: https://api.slack.com/apps
2. Create App → Incoming Webhooks → Activate → Add to Workspace
3. Choose your channel → Copy webhook URL
4. Add to .env:
    SLACK_ENABLED=true
    SLACK_WEBHOOK_URL=https://hooks.slack.com/services/XXX/YYY/ZZZ

Usage in backfill_runner.py
----------------------------
from utils.slack_alert import slack_notify

slack_notify(status="success", month="Jan-21", processed=262943,
             failed=137, duration="1:48:40")

slack_notify(status="failure", month="Jan-21", error=exc,
             duration="1:31:49")
"""

import json
import logging
import os
import traceback
import urllib.request
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv

_PROJECT_ROOT = Path(__file__).parent.parent.resolve()
load_dotenv(dotenv_path=_PROJECT_ROOT / ".env", override=False)

logger = logging.getLogger(__name__)

SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL", "").strip()
SLACK_ENABLED     = os.getenv("SLACK_ENABLED", "false").lower() == "true"


def slack_notify(
    status: str,
    month: str,
    processed: int = 0,
    failed: int = 0,
    warnings: int = 0,
    duration=None,
    error: Exception = None,
    flow: str = "Flow B",  # "Flow A" or "Flow B"
) -> None:
    """
    Send pipeline status to Slack.

    Parameters
    ----------
    status    : 'success' or 'failure'
    month     : e.g. 'Jan-21'
    processed : files successfully written to DB
    failed    : files that crashed, no data saved
    warnings  : files with ixt2 warnings, data saved
    duration  : how long the month took
    error     : Exception on failure
    """
    if not SLACK_ENABLED:
        return
    if not SLACK_WEBHOOK_URL:
        logger.warning("Slack skipped — SLACK_WEBHOOK_URL not set in .env")
        return

    total        = processed + failed
    rate         = f"{processed/total*100:.2f}%" if total > 0 else "N/A"
    duration_str = str(duration) if duration else "N/A"

    if status == "success":
        color        = "#1a7a3c"
        header       = f"✅ *CH Pipeline Completed — {flow} | {month}*"
        status_label = "✅ SUCCESS"

        fields = [
            {"title": "Flow",            "value": flow,             "short": True},
            {"title": "Month",           "value": month,            "short": True},
            {"title": "Status",          "value": status_label,     "short": True},
            {"title": "Files Processed", "value": f"{processed:,}", "short": True},
            {"title": "Files Failed",    "value": f"{failed:,}",    "short": True},
            {"title": "Files Warning",   "value": f"{warnings:,}",  "short": True},
            {"title": "Success Rate",    "value": rate,             "short": True},
            {"title": "Duration",        "value": duration_str,     "short": True},
        ]

        notes = []
        if failed > 0:
            notes.append(f"⚠️ *{failed:,} files failed* — no data saved, check email for details")
        if warnings > 0:
            notes.append(f"🔔 *{warnings:,} files had warnings* — data saved, minor fields skipped")
        extra = "\n".join(notes)

    else:
        color        = "#c0392b"
        header       = f"❌ *CH Pipeline FAILED — {flow} | {month}*"
        status_label = "❌ FAILED"

        err_type = type(error).__name__ if error else "Unknown"
        err_msg  = str(error)[:300]     if error else "No details"

        # Last 3 lines of traceback — skips Python 3.13 ~~~^^^ highlight lines
        tb_lines = traceback.format_exc().strip().split("\n")
        clean    = [l for l in tb_lines if l.strip() and not all(c in "~^ " for c in l.strip())]
        tb_short = "\n".join(clean[-3:]) if len(clean) >= 3 else "\n".join(clean)

        fields = [
            {"title": "Flow",          "value": flow,           "short": True},
            {"title": "Month",         "value": month,          "short": True},
            {"title": "Status",        "value": status_label,   "short": True},
            {"title": "Duration",      "value": duration_str,   "short": True},
            {"title": "Error Type",    "value": err_type,       "short": True},
            {"title": "Error Message", "value": err_msg,        "short": False},
        ]
        extra = f"*Traceback (last 3 lines):*\n```{tb_short}```"

    payload = {
        "text": header,
        "attachments": [
            {
                "color":  color,
                "fields": fields,
                "footer": "Companies House Data Pipeline",
                "ts":     int(__import__("time").time()),
            }
        ],
    }

    if extra:
        payload["attachments"].append({"color": color, "text": extra})

    try:
        data = json.dumps(payload).encode("utf-8")
        req  = urllib.request.Request(
            SLACK_WEBHOOK_URL,
            data=data,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            result = resp.read().decode()
            if result == "ok":
                logger.info("Slack notification sent [%s]", month)
            else:
                logger.warning("Slack unexpected response: %s", result)
    except Exception as exc:
        logger.warning("Slack notification failed (pipeline continues): %s", exc)
