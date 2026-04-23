


"""
backfill_runner.py
------------------
Flow B — Accounts Data (iXBRL) sequential backfill.

How it works
------------
Processes one month at a time, strictly in order (oldest first).
Each month goes through this exact sequence:

  1.  Check if already completed → skip if yes
  2.  Verify the CH URL exists (HEAD request)
  3.  Download ZIP into  <project>/data/downloads/
  4.  Extract HTML files into  <project>/data/extracted/<YYYY-MM>/
  5.  Filter out already-processed files (idempotency)
  6.  Parse HTML files in parallel  (NUM_WORKERS)
  7.  Write to DB in batches of 500 rows
  8.  Retry failed files (3 attempts)
  9.  Mark month as 'completed' in pipeline_tracking_uk_ch
  10. DELETE all extracted HTML files and the extract subfolder
  11. DELETE the downloaded ZIP file
  → Only then move to the next month

After ALL months finish:
  12. Run verification

Why one month at a time?
-------------------------
- Disk space: each ZIP is ~1.8 GB. Processing one at a time means
  you only ever need ~1.8 GB free, not 64 × 1.8 GB.
- Safety: if step 5-8 fail, only that month's data is at risk.
  The tracking table lets you resume from exactly where it failed.
- Correctness: directors_uk_ch and reports_uk_ch use UPSERT (latest wins).
  Processing oldest-first guarantees the most recent data survives.

Testing
-------
Set TEST_MONTHS=2 in .env → processes only Jan 2021 + Feb 2021.
Set TEST_MONTHS=0 for full backfill.

Resume
------
Already-completed months are skipped automatically.
Just run the script again after any interruption.
"""

import glob
import logging
import shutil
import sys
import urllib.request
from datetime import datetime, date
from utils.worker_pool import parallel_imap_unordered
from pathlib import Path
from typing import List, Set, Tuple

from config import (
    build_accounts_url, zip_filename, month_label,
    BACKFILL_START_YEAR, BACKFILL_START_MONTH,
    TEST_MONTHS, NUM_WORKERS, BATCH_SIZE,
    DOWNLOAD_DIR, EXTRACT_DIR, PROJECT_ROOT,
)
from utils.db_wrapper import DBWrapper
from utils.setup_db import setup_database
from utils.email_alert import notify
from utils.slack_alert import slack_notify
from run_pipeline import (
    process_file, process_batch, retry_failed_files, run_verification,
)

# ---------------------------------------------------------------------------
# Logging — writes to both console and backfill.log in project folder
# ---------------------------------------------------------------------------
_LOG_FILE = PROJECT_ROOT / "backfill.log"
from logging.handlers import RotatingFileHandler as _RFH
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    handlers=[
        _RFH(str(_LOG_FILE), maxBytes=10*1024*1024, backupCount=3, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Month sequence generator
# ---------------------------------------------------------------------------

def generate_months(start_year: int, start_month: int, limit: int = 0) -> List[Tuple[int, int]]:
    """
    Return (year, month) tuples from start up to today, oldest first.
    If limit > 0, return only the first *limit* months (test mode).
    """
    today  = date.today()
    months = []
    y, m   = start_year, start_month

    while (y, m) <= (today.year, today.month):
        months.append((y, m))
        m += 1
        if m > 12:
            m = 1
            y += 1

    return months[:limit] if limit > 0 else months


# ---------------------------------------------------------------------------
# Completed-months lookup
# ---------------------------------------------------------------------------

def get_completed_months(db: DBWrapper) -> Set[str]:
    """
    Return ch_upload labels that are already fully processed.
    Only 'completed' months are skipped — 'failed' months are retried.
    """
    with db.conn.cursor() as cur:
        cur.execute(
            "SELECT ch_upload FROM pipeline_tracking_uk_ch WHERE status = 'completed'"
        )
        return {row[0] for row in cur.fetchall()}


# ---------------------------------------------------------------------------
# URL check
# ---------------------------------------------------------------------------

def url_exists(url: str, timeout: int = 15) -> bool:
    """HEAD request — True if HTTP 200 or 206."""
    try:
        req = urllib.request.Request(url, method="HEAD")
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status in (200, 206)
    except Exception:
        return False


# ---------------------------------------------------------------------------
# Download
# ---------------------------------------------------------------------------

def _check_disk_space(path: Path, required_gb: float = 5.0) -> None:
    """
    Check that enough free disk space exists before downloading.
    Each accounts ZIP is ~1.8 GB + extraction needs ~2x space.
    Raises RuntimeError if free space is below required_gb.
    """
    import shutil
    free_gb = shutil.disk_usage(path).free / (1024 ** 3)
    logger.info("  Disk space: %.1f GB free (need %.1f GB)", free_gb, required_gb)
    if free_gb < required_gb:
        raise RuntimeError(
            f"Insufficient disk space: {free_gb:.1f} GB free, "
            f"need at least {required_gb:.1f} GB. "
            f"Free up space and re-run — the pipeline will resume from here."
        )


def download_zip(url: str, dest: Path) -> None:
    """
    Stream download with 10 MB chunks.
    Logs progress every 100 MB.
    Uses a .tmp staging file — partial downloads never leave corrupt ZIPs.

    Skip logic
    ----------
    Skips the download if:
      1. The file already exists at the exact expected path, OR
      2. A file with the same name already exists anywhere inside
         DOWNLOAD_DIR (handles cases where you already have the ZIP
         from a previous manual download with a slightly different path).
    """
    # Check 1: exact path
    if dest.exists():
        logger.info("    ZIP already exists, skipping download: %s", dest.name)
        return

    # Check 2: same filename already in DOWNLOAD_DIR or PROJECT_ROOT
    search_dirs = [DOWNLOAD_DIR, PROJECT_ROOT]
    for search_dir in search_dirs:
        existing = list(search_dir.glob(dest.name))
        if existing:
            found = existing[0]
            if found != dest:
                logger.info(
                    "    ZIP found at %s — copying to downloads folder.",
                    found,
                )
                import shutil
                shutil.copy2(str(found), str(dest))
            logger.info("    ZIP already exists, skipping download: %s", dest.name)
            return

    # Recreate the downloads folder if it was deleted after previous month
    dest.parent.mkdir(parents=True, exist_ok=True)

    tmp = dest.with_suffix(".tmp")
    chunk = 10 * 1024 * 1024       # 10 MB
    log_every = 100 * 1024 * 1024  # log every 100 MB

    logger.info("    Downloading → %s", dest)
    try:
        with urllib.request.urlopen(url) as resp, open(tmp, "wb") as fh:
            total      = int(resp.headers.get("Content-Length", 0))
            downloaded = 0
            next_log   = log_every
            while True:
                data = resp.read(chunk)
                if not data:
                    break
                fh.write(data)
                downloaded += len(data)
                if downloaded >= next_log:
                    pct = (downloaded / total * 100) if total else 0
                    logger.info(
                        "    %.0f / %.0f MB  (%.0f%%)",
                        downloaded / 1e6, total / 1e6, pct,
                    )
                    next_log += log_every
        tmp.rename(dest)
        logger.info(
            "    Download complete: %s  (%.0f MB)",
            dest.name, dest.stat().st_size / 1e6,
        )
    except Exception:
        if tmp.exists():
            tmp.unlink()
        raise


# ---------------------------------------------------------------------------
# Extract
# ---------------------------------------------------------------------------

def extract_zip(zip_path: Path, extract_to: Path) -> None:
    """Extract ZIP into extract_to directory with progress logging."""
    import zipfile
    extract_to.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(zip_path, "r") as zf:
        members   = zf.infolist()
        total     = len(members)
        log_every = max(1, total // 10)
        logger.info("    Extracting %d files ...", total)
        for i, member in enumerate(members, 1):
            zf.extract(member, extract_to)
            if i % log_every == 0 or i == total:
                logger.info(
                    "    Extraction: %d / %d  (%d%%)",
                    i, total, i * 100 // total,
                )
    logger.info("    Extracted to: %s", extract_to)


# ---------------------------------------------------------------------------
# Cleanup — called after EACH month completes
# ---------------------------------------------------------------------------

def delete_month_files(zip_path: Path, extract_path: Path) -> None:
    """
    Delete ALL files for a completed month:
      1. The extracted HTML folder and everything in it
      2. The downloaded ZIP file

    Called only after a month is fully processed and marked 'completed'.
    This keeps disk usage to ~1.8 GB at any time regardless of how many
    months are in the backfill.
    """
    # Delete extracted HTML folder
    if extract_path.exists():
        shutil.rmtree(extract_path, ignore_errors=True)
        logger.info("    Deleted extracted folder: %s", extract_path)

    # Delete the ZIP file
    if zip_path.exists():
        zip_path.unlink()
        logger.info("    Deleted ZIP: %s", zip_path.name)

    # Remove downloads dir if now empty
    if DOWNLOAD_DIR.exists() and not any(DOWNLOAD_DIR.iterdir()):
        DOWNLOAD_DIR.rmdir()
        logger.info("    Removed empty downloads folder.")

    # Remove extracted dir if now empty
    if EXTRACT_DIR.exists() and not any(EXTRACT_DIR.iterdir()):
        EXTRACT_DIR.rmdir()
        logger.info("    Removed empty extracted folder.")


# ---------------------------------------------------------------------------
# DB tracking helpers
# ---------------------------------------------------------------------------

def _db_start_month(db: DBWrapper, label: str, zip_name: str) -> None:
    """
    Record month as started in pipeline_tracking_uk_ch.
    batch_id  = 'accounts_Jan-21'  (source prefix + ch_upload label)
    source    = 'accounts'         (derived from ZIP name starting with 'Accounts_')
    started_at  = now
    completed_at = NULL (set when month finishes)
    loaded_at    = now (updated on every change)
    """
    batch_id = f"accounts_{label}"
    with db.conn.cursor() as cur:
        cur.execute("""
            INSERT INTO pipeline_tracking_uk_ch
                (batch_id, ch_upload, source, zip_filename,
                 status, files_processed, files_failed,
                 started_at, completed_at, loaded_at)
            VALUES (%s, %s, 'accounts', %s,
                    'running', 0, 0,
                    %s, NULL, NOW())
            ON CONFLICT (batch_id) DO UPDATE SET
                status       = 'running',
                started_at   = EXCLUDED.started_at,
                completed_at = NULL,
                loaded_at    = NOW()
        """, (batch_id, label, zip_name, datetime.now()))
    db.conn.commit()


def _db_complete_month(db: DBWrapper, label: str, ok: int, failed: int) -> None:
    """
    Mark month as completed in pipeline_tracking_uk_ch.
    files_processed = successfully parsed + written to DB
    files_failed    = permanently failed after all retries
    completed_at    = now (overall month end time)
    loaded_at       = now (last update time)
    """
    batch_id = f"accounts_{label}"
    with db.conn.cursor() as cur:
        cur.execute("""
            UPDATE pipeline_tracking_uk_ch
            SET files_processed = %s,
                files_failed    = %s,
                completed_at    = %s,
                status          = 'completed',
                loaded_at       = NOW()
            WHERE batch_id = %s
        """, (ok, failed, datetime.now(), batch_id))
    db.conn.commit()


def _db_fail_month(db: DBWrapper, label: str, ok: int, failed: int) -> None:
    """
    Mark month as failed in pipeline_tracking_uk_ch.
    Month will be retried on next run of backfill_runner.py.
    """
    batch_id = f"accounts_{label}"
    with db.conn.cursor() as cur:
        cur.execute("""
            UPDATE pipeline_tracking_uk_ch
            SET files_processed = %s,
                files_failed    = %s,
                completed_at    = %s,
                status          = 'failed',
                loaded_at       = NOW()
            WHERE batch_id = %s
        """, (ok, failed, datetime.now(), batch_id))
    db.conn.commit()


# ---------------------------------------------------------------------------
# Single-month processor — the core of the backfill
# ---------------------------------------------------------------------------

def process_month(year: int, month: int) -> bool:
    """
    Process one complete month end-to-end:
      download → extract → parse → DB write → verify → delete all files

    Returns True if the month completed successfully.
    Opens and closes its own DB connection so each month is independent.
    """
    label        = month_label(year, month)
    url          = build_accounts_url(year, month)
    zip_name     = zip_filename(year, month)
    zip_path     = DOWNLOAD_DIR / zip_name
    extract_path = EXTRACT_DIR  / f"{year}-{month:02d}"

    # Reject folder: <project>/rejected/YYYY_MM/
    # Failed HTML files are copied here for inspection
    reject_dir   = PROJECT_ROOT / "rejected" / f"{year}_{month:02d}"

    month_start  = datetime.now()

    logger.info("")
    logger.info("┌" + "─" * 68)
    logger.info("│  Month  : %s  (%04d-%02d)", label, year, month)
    logger.info("│  URL    : %s", url)
    logger.info("│  ZIP    : %s", zip_path)
    logger.info("│  Reject : %s", reject_dir)
    logger.info("└" + "─" * 68)

    # ── Step 1: Verify URL ────────────────────────────────────────────
    logger.info("  [1/9] Checking URL …")
    if not url_exists(url):
        logger.warning("  URL not yet available — skipping %s", label)
        return False

    db = DBWrapper()
    try:
        # ── Step 2: Download ──────────────────────────────────────────
        logger.info("  [2/9] Downloading ZIP …")
        download_zip(url, zip_path)

        # ── Step 3: Extract (skip if already extracted) ──────────────
        logger.info("  [3/9] Extracting HTML files …")
        existing_html = list(extract_path.glob("**/*.html")) if extract_path.exists() else []
        if existing_html:
            logger.info(
                "  Extract folder already has %d HTML files — skipping extraction.",
                len(existing_html),
            )
        else:
            extract_zip(zip_path, extract_path)

        # ── Step 4: Collect HTML files ────────────────────────────────
        logger.info("  [4/9] Collecting HTML files …")
        html_files = glob.glob(str(extract_path / "**" / "*.html"), recursive=True)
        logger.info("        Found: %d HTML files", len(html_files))

        # Verify every file actually exists on disk and is readable.
        # In some environments (Docker volume mounts, NFS, Windows ↔ Linux)
        # glob returns paths that were extracted but not fully flushed.
        missing_files = [f for f in html_files if not Path(f).exists()]
        if missing_files:
            logger.warning(
                "  ⚠️  %d HTML paths returned by glob do NOT exist on disk — "
                "these will fail to parse. This usually means the extraction "
                "was incomplete (disk full, permissions, or volume mount issue).",
                len(missing_files),
            )
            for mf in missing_files[:10]:   # log first 10 only
                logger.warning("    Missing: %s", mf)
            # Remove ghost paths so they are not queued for processing
            html_files = [f for f in html_files if Path(f).exists()]
            logger.info(
                "        After removing missing paths: %d HTML files to process",
                len(html_files),
            )

        if not html_files:
            logger.warning("  No HTML files found — skipping %s", label)
            delete_month_files(zip_path, extract_path)
            return False

        # ── Step 5: Filter already-processed ─────────────────────────
        logger.info("  [5/9] Filtering already-processed files …")
        with db.conn.cursor() as cur:
            # Check (source_file, ch_upload) together — not just source_file.
            # Same filename in a different month = different record = process it.
            # e.g. Prod224_0086_02084294_20200229 in Jan-21 and Feb-21 ZIPs
            # are two separate records and both should be processed.
            cur.execute(
                "SELECT source_file FROM processed_files_uk_ch WHERE ch_upload = %s",
                (label,)
            )
            already_done = {row[0] for row in cur.fetchall()}

        new_files = [f for f in html_files if Path(f).stem not in already_done]
        logger.info(
            "        New: %d  Already processed: %d  (for month: %s)",
            len(new_files), len(html_files) - len(new_files), label,
        )

        if not new_files:
            logger.info("  All files already processed — marking complete.")
            _db_start_month(db, label, zip_name)
            _db_complete_month(db, label, 0, 0)
            delete_month_files(zip_path, extract_path)
            return True

        # ── Step 6: Record start ──────────────────────────────────────
        _db_start_month(db, label, zip_name)

        # ── Step 7: Parse + write in batches of 500 ───────────────────
        logger.info(
            "  [6/9] Parsing %d files with %d workers, batch_size=%d …",
            len(new_files), NUM_WORKERS, BATCH_SIZE,
        )
        results_buffer = []
        all_failed     = []
        processed_count = 0

        # Pass (filepath, label) tuples so ch_upload is always the
        # ZIP month (e.g. 'Jan-21') — never None, never wrong.
        # Handles old-format filenames like Prod224_0086_... where
        # derive_ch_upload() returns None.
        file_args = [(f, label) for f in new_files]

        for result in parallel_imap_unordered(
            process_file, file_args,
            n_workers=NUM_WORKERS, chunksize=50,
        ):
            results_buffer.append(result)
            processed_count += 1

            if processed_count % BATCH_SIZE == 0:
                pct = processed_count / len(new_files) * 100
                logger.info(
                    "        Batch DB write: %d / %d files  (%.1f%%)",
                    processed_count, len(new_files), pct,
                )
                failed = process_batch(results_buffer, label, reject_dir=str(reject_dir))
                all_failed.extend(failed)
                results_buffer = []

        # Flush last partial batch
        if results_buffer:
            failed = process_batch(results_buffer, label, reject_dir=str(reject_dir))
            all_failed.extend(failed)

        ok_count = len(new_files) - len(all_failed)
        logger.info(
            "        Parse done: %d ok  %d failed",
            ok_count, len(all_failed),
        )

        # ── Step 8: Retry failed files ────────────────────────────────
        if all_failed:
            logger.info("  [7/9] Retrying %d failed files …", len(all_failed))
            all_failed = retry_failed_files(all_failed, label, retries=3, delay=5, reject_dir=str(reject_dir))
            ok_count   = len(new_files) - len(all_failed)

        # ── Step 9: Mark complete ─────────────────────────────────────
        # Count files_failed from processed_files_uk_ch table — this is the
        # authoritative source. It counts ALL permanently failed files
        # including ones that failed in retry AND ones that failed during
        # initial processing, regardless of whether all_failed list is
        # accurate (e.g. Windows [Errno 22] errors can cause list issues).
        logger.info("  [8/9] Marking month complete in tracking table …")

        # Count from processed_files_uk_ch WHERE ch_upload = label AND
        # processed_at >= month_start so we only count THIS run,
        # not files from previous runs of the same month.
        with db.conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM processed_files_uk_ch "
                "WHERE ch_upload = %s AND status = 'failed' "
                "AND processed_at >= %s",
                (label, month_start)
            )
            actual_failed = cur.fetchone()[0]
            cur.execute(
                "SELECT COUNT(*) FROM processed_files_uk_ch "
                "WHERE ch_upload = %s AND status = 'success' "
                "AND processed_at >= %s",
                (label, month_start)
            )
            actual_ok = cur.fetchone()[0]

        logger.info(
            "        files_processed=%d  files_failed=%d  "
            "(from processed_files_uk_ch table — this run only)",
            actual_ok, actual_failed,
        )

        # If ALL files failed (ok=0, failed>0) mark month as failed
        # so it retries on next run rather than being skipped forever.
        # A month is only 'completed' if at least some files succeeded
        # OR there were genuinely no new files to process.
        if actual_ok == 0 and actual_failed > 0:
            logger.warning(
                "  All %d files failed — marking month as 'failed' "
                "(will retry on next run)", actual_failed,
            )
            _db_fail_month(db, label, actual_ok, actual_failed)
        else:
            _db_complete_month(db, label, actual_ok, actual_failed)

        # ── Step 10: Delete all files for this month ───────────────────
        logger.info("  [9/9] Deleting downloaded ZIP and extracted files …")
        delete_month_files(zip_path, extract_path)

        duration = datetime.now() - month_start

        # Query failed files from DB to attach to email
        failed_file_rows = []
        if actual_failed > 0:
            with db.conn.cursor() as cur:
                cur.execute("""
                    SELECT source_file, ch_upload, comment
                    FROM processed_files_uk_ch
                    WHERE ch_upload = %s
                    AND status = 'failed'
                    AND processed_at >= %s
                    ORDER BY processed_at
                """, (label, month_start))
                failed_file_rows = [
                    {
                        "source_file": row[0],
                        "ch_upload":   row[1],
                        "comment":     row[2],
                    }
                    for row in cur.fetchall()
                ]
            logger.info(
                "  Attaching %d failed file records to email",
                len(failed_file_rows),
            )

        # Email status must match DB status:
        # all files failed → email shows FAILURE
        # at least some succeeded → email shows SUCCESS
        if actual_ok == 0 and actual_failed > 0:
            logger.error(
                "  ❌  %s  ALL FAILED  |  ok=%d  failed=%d  |  time=%s",
                label, actual_ok, actual_failed, duration,
            )
            notify(
                status="failure",
                month=label,
                processed=actual_ok,
                failed=actual_failed,
                duration=duration,
                error=Exception(f"All {actual_failed} files failed to parse."),
                failed_files=failed_file_rows if failed_file_rows else None,
            )
            slack_notify(
                status="failure",
                month=label,
                processed=actual_ok,
                failed=actual_failed,
                duration=duration,
                error=Exception(f"All {actual_failed} files failed to parse."),
            )
            return False

        logger.info(
            "  ✅  %s  COMPLETE  |  ok=%d  failed=%d  |  time=%s",
            label, actual_ok, actual_failed, duration,
        )
        notify(
            status="success",
            month=label,
            flow="Flow B",
            processed=actual_ok,
            failed=actual_failed,
            duration=duration,
            failed_files=failed_file_rows if failed_file_rows else None,
        )
        slack_notify(
            status="success",
            month=label,
            flow="Flow B",
            processed=actual_ok,
            failed=actual_failed,
            warnings=actual_warnings if "actual_warnings" in dir() else 0,
            duration=duration,
        )
        return True

    except Exception as exc:
        import traceback
        logger.error("  ❌  %s FAILED: %s", label, exc)
        logger.error(traceback.format_exc())

        # Mark failed in DB so the month can be retried next run
        try:
            _db_fail_month(db, label, 0, len(html_files) if 'html_files' in dir() else 0)
        except Exception:
            pass
        notify(
            status="failure",
            month=label,
            flow="Flow B",
            processed=0,
            failed=0,
            duration=datetime.now() - month_start,
            error=exc,
        )
        slack_notify(
            status="failure",
            month=label,
            flow="Flow B",
            duration=datetime.now() - month_start,
            error=exc,
        )

        # On failure: delete extracted HTML files (free disk space)
        # but KEEP the ZIP so retry doesn't need to re-download 2GB
        logger.info("  Cleaning up after failure …")
        logger.info("  Deleting extracted HTML files (freeing disk) …")
        if extract_path.exists():
            import shutil
            shutil.rmtree(extract_path, ignore_errors=True)
            logger.info("  Deleted extracted folder: %s", extract_path)
        if zip_path.exists():
            logger.info("  ZIP kept for retry (no re-download needed): %s", zip_path.name)
        else:
            logger.info("  ZIP not found — will re-download on retry.")
        return False

    finally:
        db.close()


# ---------------------------------------------------------------------------
# Main — sequential backfill loop
# ---------------------------------------------------------------------------

def main():
    run_start = datetime.now()
    run_id    = run_start.strftime("%Y%m%d_%H%M%S")

    logger.info("=" * 70)
    logger.info("FLOW B — ACCOUNTS BACKFILL  [run_id: %s]", run_id)
    logger.info("  Project dir  : %s", PROJECT_ROOT)
    logger.info("  Download dir : %s", DOWNLOAD_DIR)
    logger.info("  Extract dir  : %s", EXTRACT_DIR)
    logger.info("  Log file     : %s", _LOG_FILE)
    logger.info(
        "  Mode         : %s",
        f"TEST — first {TEST_MONTHS} months only" if TEST_MONTHS else "FULL backfill",
    )
    logger.info("=" * 70)

    # ── Ensure DB tables exist ────────────────────────────────────────
    db = DBWrapper()
    try:
        with db.conn.cursor() as cur:
            # Check ALL required tables exist — not just one.
            # This handles the case where some tables were dropped manually.
            required_tables = [
                'financials_uk_ch', 'directors_uk_ch', 'reports_uk_ch',
                'processed_files_uk_ch', 'pipeline_tracking_uk_ch'
            ]
            cur.execute("""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_name = ANY(%s)
            """, (required_tables,))
            existing = {row[0] for row in cur.fetchall()}
            missing  = [t for t in required_tables if t not in existing]

            if missing:
                logger.info(
                    "Missing tables detected: %s — running setup_database() …",
                    missing,
                )
                setup_database()
                logger.info("Database setup complete.")
            else:
                logger.info("All pipeline tables exist — skipping setup.")
    finally:
        db.close()

    # ── Generate month list ───────────────────────────────────────────
    all_months = generate_months(
        BACKFILL_START_YEAR, BACKFILL_START_MONTH, limit=TEST_MONTHS
    )

    # ── Find already-completed months ─────────────────────────────────
    db = DBWrapper()
    try:
        completed = get_completed_months(db)
    finally:
        db.close()

    pending = [(y, m) for y, m in all_months if month_label(y, m) not in completed]

    logger.info("")
    logger.info("Month plan:")
    logger.info("  Total months : %d", len(all_months))
    logger.info("  Completed    : %d  (will be skipped)", len(all_months) - len(pending))
    logger.info("  Pending      : %d  (will be processed)", len(pending))
    logger.info("")
    for i, (y, m) in enumerate(pending, 1):
        logger.info("  [%d] %s", i, month_label(y, m))

    if not pending:
        logger.info("✅ All months already completed. Nothing to do.")
        return

    # ── Process months ONE BY ONE ─────────────────────────────────────
    # Each month is fully completed (incl. file deletion) before the next starts.
    results = {"ok": 0, "failed": 0, "skipped": 0}

    for idx, (year, month) in enumerate(pending, 1):
        label = month_label(year, month)
        logger.info(
            "\n[Month %d / %d] → %s", idx, len(pending), label
        )

        try:
            success = process_month(year, month)
        except KeyboardInterrupt:
            logger.warning(
                "\n⚠️  Interrupted by user at %s. "
                "Month marked as failed — re-run to resume from here.",
                label,
            )
            # Mark current month failed so it retries next run
            try:
                db = DBWrapper()
                _db_fail_month(db, label, 0, 0)
                db.close()
            except Exception:
                pass
            logger.info("Safe to re-run: python backfill_runner.py")
            break

        if success:
            results["ok"] += 1
            logger.info("[Month %d / %d] → %s  ✅ DONE", idx, len(pending), label)
        else:
            results["failed"] += 1
            logger.error("[Month %d / %d] → %s  ❌ FAILED", idx, len(pending), label)

        # Brief pause between months (let DB settle, avoid hammering CH)
        if idx < len(pending):
            logger.info("Pausing 5 seconds before next month …")
            import time
            time.sleep(5)

    # ── Final summary ─────────────────────────────────────────────────
    total_duration = datetime.now() - run_start

    logger.info("")
    logger.info("=" * 70)
    logger.info("FLOW B COMPLETE  [run_id: %s]", run_id)
    logger.info("  Months processed : %d / %d", results["ok"], len(pending))
    logger.info("  Months failed    : %d", results["failed"])
    logger.info("  Total duration   : %s", total_duration)
    logger.info("  Log saved to     : %s", _LOG_FILE)
    logger.info("=" * 70)

    # ── Verification on last processed month ──────────────────────────
    if results["ok"] > 0:
        logger.info("\nRunning data verification …")
        if run_verification():
            logger.info("✅ Verification passed.")
        else:
            logger.warning("⚠️  Verification found issues — check logs above.")


if __name__ == "__main__":
    main()