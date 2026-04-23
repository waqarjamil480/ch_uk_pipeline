"""
utils/verification.py
---------------------
Post-pipeline data verification for Flow B.

Checks per month:
  1. Pipeline tracking record (pipeline_tracking_uk_ch)
  2. Row counts — financials, directors, reports
  3. NULL check on critical fields
  4. Duplicate check on unique-constrained tables

Called from:
  backfill_runner.py → run_verification(label)   passes month label explicitly
  run_pipeline.py    → run_verification()         auto-detects latest completed month
"""

import logging
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent))

from utils.db_wrapper import DBWrapper

logger = logging.getLogger(__name__)


def run_verification(ch_upload: str = None) -> bool:
    """
    Parameters
    ----------
    ch_upload : str, optional
        Month label e.g. 'Jan-21'.
        If None, auto-detects the most recently completed month
        from pipeline_tracking_uk_ch.
    """
    db = DBWrapper()

    try:
        # ── Resolve month label ───────────────────────────────────────
        if not ch_upload:
            with db.conn.cursor() as cur:
                cur.execute("""
                    SELECT ch_upload
                    FROM   pipeline_tracking_uk_ch
                    WHERE  status = 'completed'
                    ORDER  BY completed_at DESC
                    LIMIT  1
                """)
                row = cur.fetchone()
            if not row:
                logger.warning("Verification skipped — no completed month in pipeline_tracking_uk_ch")
                return True
            ch_upload = row[0]

        logger.info("=" * 60)
        logger.info(" VERIFYING PIPELINE DATA FOR: %s", ch_upload)
        logger.info("=" * 60)

        with db.conn.cursor() as cur:

            # ── 1. Pipeline tracking record ───────────────────────────
            logger.info("1. PIPELINE EXECUTION STATUS")
            cur.execute("""
                SELECT status, files_processed, files_failed,
                       started_at, completed_at
                FROM   pipeline_tracking_uk_ch
                WHERE  ch_upload = %s
                ORDER  BY started_at DESC
                LIMIT  1
            """, (ch_upload,))
            row = cur.fetchone()
            if row:
                status, processed, failed, started, completed = row
                logger.info("   Status          : %s", status)
                logger.info("   Files processed : %s", processed)
                logger.info("   Files failed    : %s", failed)
                logger.info("   Started         : %s", started)
                logger.info("   Completed       : %s", completed)
                if completed and started:
                    logger.info("   Duration        : %s", completed - started)
            else:
                logger.warning("   No tracking record found for %s", ch_upload)

            # ── 2. Row counts ─────────────────────────────────────────
            logger.info("\n2. DATA VOLUME CHECK")

            cur.execute("SELECT COUNT(*) FROM financials_uk_ch WHERE ch_upload = %s", (ch_upload,))
            financials_count = cur.fetchone()[0]
            logger.info("   Financial records : %s", f"{financials_count:,}")

            cur.execute("SELECT COUNT(DISTINCT company_number) FROM financials_uk_ch WHERE ch_upload = %s", (ch_upload,))
            companies_count = cur.fetchone()[0]
            logger.info("   Unique companies  : %s", f"{companies_count:,}")

            cur.execute("SELECT COUNT(*) FROM directors_uk_ch WHERE ch_upload = %s", (ch_upload,))
            directors_count = cur.fetchone()[0]
            logger.info("   Director records  : %s", f"{directors_count:,}")

            cur.execute("SELECT COUNT(*) FROM reports_uk_ch WHERE ch_upload = %s", (ch_upload,))
            reports_count = cur.fetchone()[0]
            logger.info("   Report records    : %s", f"{reports_count:,}")

            if financials_count == 0:
                logger.error("   CRITICAL: No financial records loaded for %s!", ch_upload)
                return False
            logger.info("   Data volume OK")

            # ── 3. NULL check on critical fields ──────────────────────
            logger.info("\n3. DATA QUALITY CHECK (NULL VALUES)")
            cur.execute("""
                SELECT
                    COUNT(*) FILTER (WHERE company_number IS NULL) AS null_company,
                    COUNT(*) FILTER (WHERE metric         IS NULL) AS null_metric,
                    COUNT(*) FILTER (WHERE value          IS NULL) AS null_value
                FROM financials_uk_ch
                WHERE ch_upload = %s
            """, (ch_upload,))
            null_company, null_metric, null_value = cur.fetchone()

            if null_company > 0:
                logger.warning("   %d records missing company_number", null_company)
            if null_metric > 0:
                logger.warning("   %d records missing metric", null_metric)
            if null_value > 0:
                logger.warning("   %d records missing value", null_value)
            if null_company == 0 and null_metric == 0 and null_value == 0:
                logger.info("   No critical NULL values")

            # ── 4. Duplicate check ────────────────────────────────────
            logger.info("\n4. DUPLICATE CHECK")

            cur.execute("""
                SELECT COUNT(*) FROM (
                    SELECT company_number, director_name
                    FROM   directors_uk_ch
                    WHERE  ch_upload = %s
                    GROUP  BY company_number, director_name
                    HAVING COUNT(*) > 1
                ) dup
            """, (ch_upload,))
            dup_directors = cur.fetchone()[0]
            if dup_directors > 0:
                logger.warning("   %d duplicate director records", dup_directors)
            else:
                logger.info("   No duplicate director records")

            cur.execute("""
                SELECT COUNT(*) FROM (
                    SELECT company_number, section
                    FROM   reports_uk_ch
                    WHERE  ch_upload = %s
                    GROUP  BY company_number, section
                    HAVING COUNT(*) > 1
                ) dup
            """, (ch_upload,))
            dup_reports = cur.fetchone()[0]
            if dup_reports > 0:
                logger.warning("   %d duplicate report records", dup_reports)
            else:
                logger.info("   No duplicate report records")

        logger.info("\n" + "=" * 60)
        logger.info(" VERIFICATION COMPLETE — %s", ch_upload)
        logger.info("=" * 60)
        return True

    except Exception as exc:
        logger.error("Verification failed: %s", exc, exc_info=True)
        return False
    finally:
        db.close()


if __name__ == "__main__":
    success = run_verification()
    sys.exit(0 if success else 1)
