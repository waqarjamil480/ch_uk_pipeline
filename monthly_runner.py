"""
monthly_runner.py
-----------------
Monthly automation runner for Flow B — Accounts Data.

Run this every month via Task Scheduler (Windows) or cron (Linux/Mac).
It processes ONLY the current month — not the full backfill.

How it works
------------
1. Detect current month from today's date (e.g. April 2026 → Apr-26)
2. Check pipeline_tracking_uk_ch — if already 'completed', skip and exit
3. If not completed, download + process + load current month only
4. Send email alert on success or failure

Usage
-----
    python monthly_runner.py

Schedule (Windows Task Scheduler)
----------------------------------
    Program : python
    Arguments: C:\\path\\to\\monthly_runner.py
    Trigger  : Monthly, day 7 of each month at 06:00 AM
               (CH publishes within 5 working days of month end)

Why day 7?
----------
Companies House publishes each month's data within 5 working days
of the previous month end. Running on the 7th guarantees the file
is always available before the script runs.

Example schedule:
    Jan run  → 7th Feb → processes February 2026 data
    Feb run  → 7th Mar → processes March 2026 data
    ...
"""

import logging
import sys
from datetime import date, datetime
from logging.handlers import RotatingFileHandler
from pathlib import Path

# ── Project path setup ────────────────────────────────────────────────────
_HERE = Path(__file__).parent.resolve()
sys.path.insert(0, str(_HERE))

from config import (
    build_accounts_url, zip_filename, month_label,
    NUM_WORKERS, BATCH_SIZE, PROJECT_ROOT,
    DOWNLOAD_DIR, EXTRACT_DIR,
)
from utils.db_wrapper import DBWrapper
from utils.setup_db import setup_database
from utils.email_alert import notify
from backfill_runner import (
    process_month,
    get_completed_months,
    _db_fail_month,
)

# ── Logging ───────────────────────────────────────────────────────────────
_LOG_FILE = PROJECT_ROOT / "monthly_runner.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    handlers=[
        RotatingFileHandler(
            str(_LOG_FILE), maxBytes=5*1024*1024, backupCount=3, encoding="utf-8"
        ),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)


def get_current_month() -> tuple:
    """
    Return (year, month) for the month that should be processed today.

    CH publishes data within 5 working days of month end.
    We always process the PREVIOUS month's data (it's already published).

    Example:
      Running on 7th April 2026 → process March 2026 data
      Running on 7th May  2026  → process April 2026 data
    """
    today = date.today()
    # Previous month
    if today.month == 1:
        return today.year - 1, 12
    return today.year, today.month - 1


def main():
    run_start = datetime.now()
    run_id    = run_start.strftime("%Y%m%d_%H%M%S")

    logger.info("=" * 65)
    logger.info("FLOW B — MONTHLY RUNNER  [run_id: %s]", run_id)
    logger.info("  Running on   : %s", date.today().strftime("%d %B %Y"))
    logger.info("=" * 65)

    # ── Detect which month to process ────────────────────────────────
    year, month = get_current_month()
    label = month_label(year, month)
    url   = build_accounts_url(year, month)

    logger.info("  Target month : %s  (%04d-%02d)", label, year, month)
    logger.info("  Download URL : %s", url)

    # ── Ensure DB tables exist ────────────────────────────────────────
    db = DBWrapper()
    try:
        with db.conn.cursor() as cur:
            required = [
                'financials_uk_ch', 'directors_uk_ch', 'reports_uk_ch',
                'processed_files_uk_ch', 'pipeline_tracking_uk_ch'
            ]
            cur.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_name = ANY(%s)", (required,)
            )
            existing = {row[0] for row in cur.fetchall()}
            missing  = [t for t in required if t not in existing]
            if missing:
                logger.info("Missing tables: %s — running setup …", missing)
                setup_database()
    finally:
        db.close()

    # ── Check if already completed ────────────────────────────────────
    db = DBWrapper()
    try:
        completed = get_completed_months(db)
    finally:
        db.close()

    if label in completed:
        logger.info(
            "✅ %s already completed — nothing to do.", label
        )
        logger.info(
            "   Next run: 7th %s",
            date(year if month < 12 else year + 1,
                 month % 12 + 1, 7).strftime("%B %Y")
        )
        return 0

    # ── Process the current month ─────────────────────────────────────
    logger.info("Processing %s …", label)
    success = process_month(year, month)

    duration = datetime.now() - run_start

    if success:
        logger.info("")
        logger.info("=" * 65)
        logger.info("✅  Monthly run COMPLETE  |  month=%s  |  time=%s", label, duration)
        logger.info("=" * 65)
        return 0
    else:
        logger.error("")
        logger.error("=" * 65)
        logger.error("❌  Monthly run FAILED   |  month=%s  |  time=%s", label, duration)
        logger.error("   Re-run this script to retry.")
        logger.error("=" * 65)
        return 1


if __name__ == "__main__":
    sys.exit(main())
