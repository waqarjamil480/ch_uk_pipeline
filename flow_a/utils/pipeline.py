"""
flow_a/utils/pipeline.py
-------------------------
Flow A — Company Data CSV pipeline orchestrator.

Loading approach: COPY → staging → metadata_uk_ch (fast, no workers)
----------------------------------------------------------------
Flow A does NOT use:
  - parallel workers (8 workers) — that is Flow B only
  - batch DB writes (500 rows at a time) — that is Flow B only

Flow A uses:
  - Single ZIP download (BasicCompanyDataAsOneFile)
  - COPY into UNLOGGED staging table (fast bulk load)
  - Atomic UPSERT: staging → metadata_uk_ch
  - Snapshot integrity gate (≥95% row count check)

Sequence
--------
 1. Auto-resolve URL for current month
 2. Idempotency check
 3. Record start in pipeline_tracking_uk_ch
 4. Download ZIP
 5. TRUNCATE staging
 6. COPY all rows into staging (single stream, no workers)
 7. Snapshot integrity gate
 8. INSERT INTO metadata_uk_ch SELECT FROM staging ON CONFLICT DO UPDATE
 9. TRUNCATE staging (cleanup)
10. Console summary
11. Delete ZIP
12. Update pipeline_tracking_uk_ch
13. Send email + Slack
"""

import logging
import re
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional
import io
import psycopg2

from flow_a.utils.db import DatabaseWrapper
from flow_a.utils.helpers import (
    Config, resolve_download_url, download_single,
    cleanup_zips, stream_rows_from_zip, write_report,
)

logger = logging.getLogger(__name__)

SOURCE_NAME  = "company_data"
_MONTH_NAMES = ["","January","February","March","April","May","June",
                "July","August","September","October","November","December"]


def _batch_id(year, month):
    return f"{SOURCE_NAME}_{year:04d}-{month:02d}-01"


def _ch_upload_from_filename(filename):
    match = re.search(r"(\d{4})-(\d{2})-\d{2}", filename)
    if match:
        y, m = int(match.group(1)), int(match.group(2))
        return f"{_MONTH_NAMES[m]}-{y}"
    return filename


class SnapshotIntegrityError(Exception):
    pass


def run(config: Config, db: DatabaseWrapper) -> None:
    """Execute Flow A end-to-end."""
    from utils.email_alert import notify as email_notify
    from utils.slack_alert  import slack_notify

    pipeline_start = datetime.utcnow()

    # Step 1: Resolve URL
    url, year, month = resolve_download_url(
        force_year=config.force_year,
        force_month=config.force_month,
    )
    batch_id  = _batch_id(year, month)
    zip_fname = url.split("/")[-1]
    ch_upload = _ch_upload_from_filename(zip_fname)

    logger.info("━" * 60)
    logger.info("Flow A — Company Data CSV")
    logger.info("Batch ID   : %s", batch_id)
    logger.info("ch_upload  : %s", ch_upload)
    logger.info("URL        : %s", url)
    logger.info("Mode       : COPY → staging → metadata_uk_ch")
    logger.info("━" * 60)

    # Step 2: Idempotency
    if db.is_batch_completed(batch_id):
        logger.info("Batch '%s' already completed — skipping.", batch_id)
        return

    # Step 3: Start tracking
    db.tracking_start(batch_id, SOURCE_NAME,
                      ch_upload=ch_upload, zip_filename=zip_fname)

    zip_paths = []
    rows_staged = rows_upserted = 0
    snapshot_check_result = ""

    try:
        # Step 4: Download
        zip_path  = download_single(url, config.download_dir)
        zip_paths = [zip_path]

        # Step 5: Truncate staging
        db.truncate_staging()

        # Step 6: COPY into staging (single stream — no workers)
        logger.info("Loading CSV into staging via COPY …")
        rows_staged, file_stats = _copy_to_staging(config, db, zip_path)
        logger.info("Staging loaded: %d rows", rows_staged)

        # Step 7: Snapshot integrity gate
        passed, snapshot_check_result = db.assert_full_snapshot()
        if not passed:
            raise SnapshotIntegrityError(snapshot_check_result)

        # Step 8: Atomic UPSERT staging → metadata_uk_ch
        logger.info("Swapping staging → metadata_uk_ch (may take several minutes) …")
        rows_upserted = db.swap_staging_to_metadata()

        # Step 9: Truncate staging after success
        db.truncate_staging()

        # Step 10: Summary
        elapsed = datetime.utcnow() - pipeline_start
        _print_summary(file_stats, rows_staged, rows_upserted,
                       snapshot_check_result, elapsed)

        # Step 11: Cleanup ZIP
        if config.cleanup_downloads:
            cleanup_zips(zip_paths)

        # Step 12: Tracking
        db.tracking_complete(batch_id, files_processed=1, files_failed=0)

        # Step 13: Email + Slack
        stats = {
            "rows_staged":    rows_staged,
            "rows_upserted":  rows_upserted,
            "snapshot_check": snapshot_check_result,
        }
        email_notify(
            status="success", month=ch_upload,
            processed=rows_upserted, failed=0,
            duration=elapsed, flow="Flow A",
            extra_stats={
                "rows_staged":    rows_staged,
                "rows_upserted":  rows_upserted,
                "snapshot_check": snapshot_check_result,
            },
        )
        slack_notify(
            status="success", month=ch_upload,
            processed=rows_upserted, failed=0,
            duration=elapsed, flow="Flow A",
        )
        logger.info("✅ Flow A complete. batch=%s  elapsed=%s", batch_id, elapsed)

    except SnapshotIntegrityError as exc:
        elapsed = datetime.utcnow() - pipeline_start
        logger.error("SNAPSHOT INTEGRITY FAILURE: %s", exc)
        _safe_truncate(db)
        db.tracking_fail(batch_id)
        email_notify(status="failure", month=ch_upload,
                     duration=elapsed, error=exc, flow="Flow A")
        slack_notify(status="failure", month=ch_upload,
                     duration=elapsed, error=exc, flow="Flow A")
        raise

    except Exception as exc:
        elapsed = datetime.utcnow() - pipeline_start
        logger.error("Flow A failed: %s", exc)
        _safe_truncate(db)
        logger.info("ZIP kept for retry.")
        db.tracking_fail(batch_id)
        email_notify(status="failure", month=ch_upload,
                     duration=elapsed, error=exc, flow="Flow A")
        slack_notify(status="failure", month=ch_upload,
                     duration=elapsed, error=exc, flow="Flow A")
        raise RuntimeError(f"Flow A failed [{batch_id}]: {exc}") from exc


def _safe_truncate(db):
    try:
        db.truncate_staging()
    except Exception:
        pass


def _copy_to_staging(config, db, zip_path):
    """
    Stream CSV rows and COPY into staging.
    Single stream — no parallel workers (Flow B only uses workers).
    """
    row_count  = 0
    status     = "Success"
    start      = datetime.utcnow()

    buf = db.new_row_buffer()
    try:
        for batch in stream_rows_from_zip(zip_path, batch_size=config.batch_size):
            buf.add_batch(batch)
            row_count += len(batch)
            if row_count % 500_000 == 0:
                logger.info("  … %d rows staged so far", row_count)
        buf.close()
    except Exception as exc:
        logger.error("COPY error: %s", exc)
        status = "Failed"
        raise

    file_stats = [{
        "file_name":    zip_path.name,
        "row_count":    row_count,
        "status":       status,
        "processed_at": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
    }]
    return row_count, file_stats


def _print_summary(file_stats, rows_staged, rows_upserted, snapshot_check, elapsed):
    sep = "=" * 65
    print(f"\n{sep}")
    print("  FLOW A — COMPANIES HOUSE CSV LOAD SUMMARY")
    print(sep)
    for s in file_stats:
        print(f"  {s['file_name']:<44} {s['row_count']:>10,}  {s['status']}")
    print(sep)
    print(f"  {'Rows staged via COPY':44} {rows_staged:>10,}")
    print(f"  {'Snapshot integrity check':44} {snapshot_check:>10}")
    print(f"  {'Rows upserted to metadata_uk_ch':44} {rows_upserted:>10,}")
    print(f"  {'Total execution time':44} {str(elapsed):>10}")
    print(f"{sep}\n")
