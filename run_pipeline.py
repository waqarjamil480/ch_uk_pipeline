# run_pipeline.py
import os
import sys
import glob
import csv
import tempfile
import logging
from pathlib import Path
from utils.worker_pool import parallel_imap_unordered
from datetime import datetime
import time

# Add parent directory to path so we can import modules
parent_dir = Path(__file__).parent.parent
sys.path.insert(0, str(parent_dir))

from test_pipeline import parse_one_file
from config import *
from utils.db_wrapper import DBWrapper
from utils.pipeline_utils import download_zip, extract_zip
from utils.setup_db import setup_database
# from utils.email_alert import notify
# -----------------------------
# Logging
# -----------------------------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('pipeline.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


# -----------------------------
# DATABASE CHECK
# -----------------------------
def check_database_ready():
    """Check if database tables exist"""
    try:
        db = DBWrapper()
        with db.conn.cursor() as cur:
            cur.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = 'financials_uk_ch'
                )
            """)
            exists = cur.fetchone()[0]
        db.close()
        return exists
    except Exception as e:
        logger.warning(f"Database connection error: {e}")
        return False

# -----------------------------
# PROCESS FILE
# -----------------------------
def process_file(args):
    """
    Parse a single file safely.

    Accepts either:
      - filepath (str)           — ch_upload derived from filename
      - (filepath, ch_upload)    — ch_upload passed explicitly (preferred)
        This handles files with old batch codes like '0086' where
        derive_ch_upload() returns None. The ch_upload always comes
        from which ZIP the file was in, not the filename.
    """
    if isinstance(args, tuple):
        filepath, ch_upload_override = args
    else:
        filepath = args
        ch_upload_override = None

    try:
        import warnings as _warnings
        # Capture UserWarnings from ixbrlparse (e.g. ixt2:datemonthdayyearen)
        # These mean the file parsed successfully but with partial issues.
        # We store them as status='warning' in processed_files_uk_ch.
        captured_warnings = []
        with _warnings.catch_warnings(record=True) as w:
            _warnings.simplefilter("always")
            result = parse_one_file(filepath)
            if w:
                # Only capture ixt2 format warnings — these are the ones
                # where ixbrlparse could not parse a specific field format.
                # e.g. "Format ixt2:datemonthdayyearen not implemented"
                # Ignore other warnings (deprecation, resource, etc)
                captured_warnings = [
                    str(warning.message) for warning in w
                    if issubclass(warning.category, UserWarning)
                    and "ixt" in str(warning.message).lower()
                ]

        # ALWAYS set ch_upload from the ZIP month label (ch_upload_override).
        # Never trust the filename-derived value — it can be None for old
        # format files like Prod224_0086_... or simply wrong.
        # The ZIP month (e.g. 'Jan-21') is the authoritative source.
        if result and ch_upload_override:
            for key in ["financials_uk_ch", "directors_uk_ch", "text_sections"]:
                for row in result.get(key, []):
                    row["ch_upload"] = ch_upload_override  # always override

        # UserWarnings from ixbrlparse = harmless — one field skipped,
        # everything else extracted fine. Data IS saved to DB.
        # We record warnings in processed_files_uk_ch as status='warning'
        # so you have a full audit trail of which files had issues.
        if captured_warnings:
            filename = Path(filepath).name
            warning_detail = (
                f"FILE: {filename}\n"
                f"Parsed with {len(captured_warnings)} warning(s) — "
                f"data SAVED (only flagged fields were skipped):\n"
                + "\n".join(f"  [{i+1}] {w}" for i, w in enumerate(captured_warnings))
            )
            # Return result WITH data so it gets saved to DB
            # warning_detail passed so process_batch logs it separately
            return (filepath, result, None, captured_warnings, warning_detail)

        return (filepath, result, None, [], None)
    except Exception as e:
        import traceback
        # Extract clean error — works on Python 3.9 AND 3.13.
        # Python 3.13 adds ~~~^^^ highlighting lines which pollute last_two.
        # We skip those and extract: file location + source line + error.
        tb_text = traceback.format_exc().strip()
        lines   = tb_text.split('\n')

        # Skip pure highlight lines (only ~, ^, spaces — Python 3.13 only)
        non_highlight = [
            l for l in lines
            if l.strip() and not all(c in '~^ ' for c in l.strip())
        ]

        error_line  = non_highlight[-1] if non_highlight else str(e)
        source_line = non_highlight[-2].strip() if len(non_highlight) >= 2 else ""
        file_lines  = [l for l in lines if l.strip().startswith('File "')]
        file_loc    = file_lines[-1].strip() if file_lines else ""

        filename   = Path(filepath).name
        full_error = (
            f"FILE: {filename}\n"
            f"{file_loc}\n"
            f"{source_line}\n"
            f"{error_line}"
        )
        return (filepath, None, full_error, [])  # empty warnings on hard failure

# -----------------------------
# FILTER NEW FILES
# -----------------------------
def filter_new_files(db, files):
    with db.conn.cursor() as cur:
        cur.execute("SELECT source_file FROM processed_files_uk_ch")
        processed = set(row[0] for row in cur.fetchall())

    new_files = [
        f for f in files
        if os.path.splitext(os.path.basename(f))[0] not in processed
    ]
    logger.info(f"Filtered new files: {len(new_files)} / {len(files)}")
    return new_files

# -----------------------------
# PROCESS BATCH
# -----------------------------
def process_batch(file_results, ch_upload, reject_dir=None):
    """
    Process a batch of parsed file results and write to database.

    Failed files are:
      1. Recorded in processed_files_uk_ch table with status='failed'
         and the full error message in the comment column.
      2. Moved to reject_dir/<YYYY_MM>/ folder if reject_dir is provided.

    No log files are written — all info is in the DB.
    """
    import shutil
    db = DBWrapper()
    financials_uk_ch, directors_uk_ch, reports_uk_ch = [], [], []
    failed_files  = []   # list of (filepath, source_file_stem, error_msg)
    warning_files = []   # list of (filepath, source_file_stem, warning_msg)
    source_files  = set()

    for item in file_results:
        # 5-tuple: (filepath, None, None, warnings_list, warning_detail) — warning file
        # 4-tuple: (filepath, result, None, [])                          — success
        # 4-tuple: (filepath, None, error, [])                           — failed
        # 3-tuple: (filepath, result, error)                             — legacy
        if len(item) == 5:
            filepath, result, error, parse_warnings, warning_detail = item
        elif len(item) == 4:
            filepath, result, error, parse_warnings = item
            warning_detail = None
        else:
            filepath, result, error = item
            parse_warnings = []
            warning_detail = None

        # ── Hard failure — crashed entirely, no data extracted ────────
        if error:
            source_stem = Path(filepath).stem
            failed_files.append((filepath, source_stem, error))
            continue

        # ── Warning — parsed with minor issues, data IS saved to DB ──
        # UserWarning means one field was skipped, rest is fine.
        # Data written to DB normally, but also logged as 'warning'
        # in processed_files_uk_ch so you have a full audit trail.
        if parse_warnings and warning_detail:
            source_stem = Path(filepath).stem
            warning_files.append((filepath, source_stem, warning_detail))
            # DO NOT continue — fall through to save data to DB below

        # ── Success — no warnings, data written to DB ─────────────────
        if not result:
            source_stem = Path(filepath).stem
            failed_files.append((filepath, source_stem, "Empty result — no data parsed"))
            continue

        # Track source files — only add to source_files if NOT a warning file
        # Warning files get status='warning' via insert_warning_files, not 'success'
        is_warning_file = bool(parse_warnings and warning_detail)
        if not is_warning_file:
            for section in ["financials_uk_ch", "directors_uk_ch", "text_sections"]:
                if section in result:
                    for row in result[section]:
                        if "source_file" in row:
                            source_files.add(row["source_file"])

        for f in result.get("financials_uk_ch", []):
            financials_uk_ch.append((
                f["company_number"], f["metric"], f["value"], f["period"],
                f["account_closing_date_last_year"], f["fiscal_period_new"],
                f["is_consolidated"], f["data_scope"], f["filing_date"],
                f["source_file"],
                ch_upload          # ← always use the ZIP month parameter
            ))                     #   never f["ch_upload"] which can be None
        for d in result.get("directors_uk_ch", []):
            directors_uk_ch.append((
                d["company_number"], d["director_name"],
                d["filing_date"], d["source_file"],
                ch_upload          # ← always use the ZIP month parameter
            ))
        for r in result.get("text_sections", []):
            reports_uk_ch.append((
                r["company_number"], r["company_name"],
                r["section"], r["text"], r["filing_date"],
                r["source_file"],
                ch_upload          # ← always use the ZIP month parameter
            ))

    # Deduplicate
    directors_uk_ch = list({(d[0], d[1]): d for d in directors_uk_ch}.values())
    reports_uk_ch   = list({(r[0], r[2]): r for r in reports_uk_ch}.values())

    tmpfile_path = None
    try:
        if financials_uk_ch:
            with tempfile.NamedTemporaryFile(mode='w+', newline='', delete=False, suffix='.csv') as tmpfile:
                writer = csv.writer(tmpfile)
                writer.writerows(financials_uk_ch)
                tmpfile_path = tmpfile.name
            db.copy_financials_from_csv(tmpfile_path)

        if directors_uk_ch:
            db.upsert_directors(directors_uk_ch)
        if reports_uk_ch:
            db.upsert_reports(reports_uk_ch)

        # Record successful files
        if source_files:
            db.insert_processed_files(list(source_files), ch_upload)

        # Record warning files in processed_files_uk_ch as status='warning'
        # Data WAS saved to DB — warning just means one field was skipped
        if warning_files:
            db.insert_warning_files(
                [(stem, ch_upload, msg) for _, stem, msg in warning_files]
            )
            logger.info(
                "%d files had warnings (minor field issues) — "
                "data saved, logged as status='warning'",
                len(warning_files),
            )
            # Copy warning files to warnings/YYYY_MM/ folder for inspection
            if reject_dir:
                try:
                    import shutil as _shutil
                    reject_path  = Path(reject_dir)
                    # e.g. rejected/2021_01 → warnings/2021_01
                    warning_path = reject_path.parent.parent / "warnings" / reject_path.name
                    warning_path.mkdir(parents=True, exist_ok=True)
                    for filepath, _, _ in warning_files:
                        dest = warning_path / Path(filepath).name
                        _shutil.copy2(filepath, dest)
                        logger.info("  Copied to warnings: %s", dest)
                except Exception as we:
                    logger.warning("Could not copy to warnings folder: %s", we)

        # Record failed files in processed_files_uk_ch table + move to reject folder
        for filepath, source_stem, error_msg in failed_files:
            # 1. Record in DB with full error
            db.insert_failed_file(source_stem, ch_upload, error_msg)

            # 2. Move to reject folder if specified
            if reject_dir:
                try:
                    reject_path = Path(reject_dir)
                    reject_path.mkdir(parents=True, exist_ok=True)
                    dest = reject_path / Path(filepath).name
                    if Path(filepath).exists():
                        shutil.copy2(filepath, dest)
                        logger.debug("Rejected file moved to: %s", dest)
                    else:
                        placeholder = dest.with_suffix(".MISSING.txt")
                        placeholder.write_text(
                            f"Original file not found on disk: {filepath}\n"
                            f"Parse error: {error_msg}\n",
                            encoding="utf-8",
                        )
                        logger.warning(
                            "Source file missing for reject copy — wrote placeholder: %s",
                            placeholder.name,
                        )
                except Exception as move_err:
                    logger.warning("Could not move rejected file %s: %s", filepath, move_err)

        if failed_files:
            logger.warning(
                "%d files failed — recorded in processed_files_uk_ch table with status='failed'",
                len(failed_files),
            )

    except Exception as e:
        logger.error(f"Database write error: {e}")
        raise
    finally:
        db.close()
        if tmpfile_path and os.path.exists(tmpfile_path):
            os.remove(tmpfile_path)

    return [f[0] for f in failed_files]

# -----------------------------
# RETRY FAILED FILES
# -----------------------------
def retry_failed_files(failed_files, ch_upload, retries=3, delay=5, reject_dir=None):
    """
    Retry failed files up to `retries` times after all files are parsed.

    Flow
    ----
    Attempt 1: retry all failed files
      → succeeded  → process_batch() → DB status='success'  ✅
      → still fail → track last error, move to attempt 2
    Attempt 2: retry remaining
      → succeeded  → process_batch() → DB status='success'  ✅
      → still fail → track last error, move to attempt 3
    Attempt 3: retry remaining
      → succeeded  → process_batch() → DB status='success'  ✅
      → still fail → permanently failed:
                       DB status='failed', comment=last error
                       file copied to reject folder

    Key fixes vs previous version
    ------------------------------
    1. process_file() called with (filepath, ch_upload) tuple —
       so ch_upload is always correct even for old-format filenames
       like Prod224_0086_... where derive_ch_upload() returns None.

    2. Last error tracked through the loop —
       no extra 4th parse attempt just to get the error message.

    3. DB always reflects FINAL outcome:
       - Recovered on retry  → status='success'
       - Permanent failure   → status='failed' + full error in comment
    """
    import shutil

    logger.info(
        f"Starting retry: {len(failed_files)} failed files, "
        f"max {retries} attempts, {delay}s between attempts"
    )

    # Track last known error per filepath so we don't re-parse just for error msg
    last_errors = {fp: "Unknown error" for fp in failed_files}

    for attempt in range(1, retries + 1):
        logger.info(
            f"Retry attempt {attempt}/{retries} — "
            f"{len(failed_files)} files remaining"
        )
        still_failing = []

        for filepath in failed_files:
            # Pass (filepath, ch_upload) tuple — fixes ch_upload=None for
            # old-format filenames like Prod224_0086_02084294_20200229
            # process_file returns 5-tuple: (filepath, result, error, warnings, warning_detail)
            # Unpack all 5 — ignore warnings in retry (already logged on first attempt)
            _ret = process_file((filepath, ch_upload))
            fp, result, error = _ret[0], _ret[1], _ret[2]

            if error or not result:
                # Still failing — track the latest error message
                last_errors[filepath] = error or "Empty result — no data parsed"
                still_failing.append(filepath)
                logger.warning(
                    f"  Retry {attempt}/{retries} failed: "
                    f"{Path(filepath).name} → {last_errors[filepath]}"
                )
            else:
                # ✅ Succeeded on this retry attempt
                # process_batch writes data and updates DB to status='success'
                process_batch([(fp, result, None)], ch_upload)
                logger.info(
                    f"  Retry {attempt}/{retries} SUCCEEDED: "
                    f"{Path(filepath).name} → DB updated to status='success'"
                )

        if not still_failing:
            logger.info(
                f"All {len(failed_files)} files recovered successfully "
                f"on retry attempt {attempt}."
            )
            return []

        failed_files = still_failing

        # Wait before next attempt (not after the last one)
        if attempt < retries:
            logger.info(
                f"  {len(failed_files)} files still failing. "
                f"Waiting {delay}s before attempt {attempt + 1}..."
            )
            time.sleep(delay)

    # ── All retries exhausted ─────────────────────────────────────────
    # Record each permanently failed file in DB and copy to reject folder
    logger.warning(
        f"{len(failed_files)} files permanently failed "
        f"after {retries} retry attempts."
    )

    db = DBWrapper()
    try:
        for filepath in failed_files:
            source_stem = Path(filepath).stem

            # Build final error message from last known error (no extra parse)
            # Store only the actual error — clean and readable.
            # No "Permanently failed after N retry attempts" prefix.
            error_msg = last_errors.get(filepath, 'Unknown error')

            # Update DB: status='failed', comment=full error
            db.insert_failed_file(source_stem, ch_upload, error_msg)
            logger.warning(
                f"  Permanent failure recorded: {source_stem} | {error_msg[:100]}"
            )

            # Copy failed file to reject folder for inspection
            if reject_dir:
                try:
                    reject_path = Path(reject_dir)
                    reject_path.mkdir(parents=True, exist_ok=True)
                    dest = reject_path / Path(filepath).name
                    if Path(filepath).exists():
                        shutil.copy2(filepath, dest)
                        logger.info(f"  Copied to reject folder: {dest}")
                    else:
                        # File no longer on disk (extraction issue or already cleaned up).
                        # Write a placeholder .txt so you know which file failed.
                        placeholder = dest.with_suffix(".MISSING.txt")
                        placeholder.write_text(
                            f"Original file not found on disk: {filepath}\n"
                            f"Parse error: {error_msg}\n",
                            encoding="utf-8",
                        )
                        logger.warning(
                            f"  Source file missing — wrote placeholder: {placeholder.name}"
                        )
                except Exception as move_err:
                    logger.warning(
                        f"  Could not copy to reject folder: {move_err}"
                    )
    finally:
        db.close()

    return failed_files

# -----------------------------
# CLEANUP
# -----------------------------
def cleanup_files(directory):
    try:
        for root, _, files in os.walk(directory):
            for f in files:
                os.remove(os.path.join(root, f))
        logger.info(f"Cleaned up files in {directory}")
    except Exception as e:
        logger.error(f"Cleanup error: {e}")

# -----------------------------
# VERIFICATION
# -----------------------------
def run_verification():
    try:
        from utils.verification import run_verification as verify
        return verify()
    except ImportError:
        logger.warning("verification.py not found, skipping verification")
        return True

# -----------------------------
# MAIN
# -----------------------------
def main():
    """
    Single-month runner — always processes the CURRENT month automatically.

    Run this every month on a schedule (cron / Task Scheduler):
        python run_pipeline.py

    It will:
      - Detect the current month from today's date
      - Build the correct CH download URL automatically
      - Skip if this month has already been completed (idempotent)

    No hardcoded months. No config changes needed between runs.
    """
    from config import build_accounts_url, zip_filename, month_label
    from datetime import date

    # ── Auto-detect current month from today's date ───────────────────
    today          = date.today()
    year           = today.year
    month          = today.month
    PIPELINE_MONTH = month_label(year, month)          # e.g. "Apr-26"
    DOWNLOAD_URL   = build_accounts_url(year, month)   # e.g. ".../Accounts_Monthly_Data-April2026.zip"
    ZIP_PATH       = str(Path(__file__).parent / "data" / "downloads" / zip_filename(year, month))
    EXTRACT_DIR    = str(Path(__file__).parent / "data" / "extracted" / f"{year}-{month:02d}")
    # ─────────────────────────────────────────────────────────────────

    start_time = datetime.now()
    run_id = start_time.strftime("%Y%m%d_%H%M%S")

    logger.info("="*80)
    logger.info(f"PIPELINE RUN STARTED - {run_id}")
    logger.info(f"Month: {PIPELINE_MONTH}")
    logger.info("="*80)

    # Step 1: Database setup
    logger.info("Step 1: Checking database setup...")
    if not check_database_ready():
        setup_database()
        logger.info("Database setup completed!")

    # Step 2: Check previous pipeline
    db = DBWrapper()
    already_processed = False
    try:
        with db.conn.cursor() as cur:
            cur.execute("SELECT status FROM pipeline_tracking_uk_ch WHERE ch_upload=%s", (PIPELINE_MONTH,))
            result = cur.fetchone()
            if result and result[0] == 'completed':
                logger.info(f"Pipeline already completed for {PIPELINE_MONTH}. Skipping.")
                already_processed = True
    finally:
        db.close()

    if already_processed:
        run_verification()
        return

    # Step 3: Download and extract
    logger.info("Step 3: Downloading and extracting data...")
    Path(ZIP_PATH).parent.mkdir(parents=True, exist_ok=True)
    download_zip(DOWNLOAD_URL, ZIP_PATH)
    extract_zip(ZIP_PATH, EXTRACT_DIR)

    # Step 4: Collect files
    logger.info("Step 4: Collecting HTML files...")
    files = glob.glob(f"{EXTRACT_DIR}/**/*.html", recursive=True)
    logger.info(f"Total files found: {len(files)}")
    if not files:
        logger.warning("No HTML files found to process")
        cleanup_files(EXTRACT_DIR)
        return

    # Step 5: Filter already processed
    db = DBWrapper()
    try:
        files = filter_new_files(db, files)
        db.insert_pipeline_run((
            PIPELINE_MONTH, Path(ZIP_PATH).name, 0, 0, datetime.now(), None, "running"
        ))
    finally:
        db.close()

    if not files:
        logger.info("No new files to process")
        cleanup_files(EXTRACT_DIR)
        return

    # Step 6: Process files in parallel
    logger.info("Step 6: Processing files in parallel...")
    results_buffer = []
    total_failed_files = []
    processed_count = 0

    for result in parallel_imap_unordered(
        process_file, files,
        n_workers=NUM_WORKERS, chunksize=50,
    ):
        results_buffer.append(result)
        processed_count += 1
        if processed_count % BATCH_SIZE == 0:
            logger.info(f"Processed {processed_count}/{len(files)} files")
            failed_files = process_batch(results_buffer, PIPELINE_MONTH)
            total_failed_files.extend(failed_files)
            results_buffer = []

    if results_buffer:
        failed_files = process_batch(results_buffer, PIPELINE_MONTH)
        total_failed_files.extend(failed_files)

    if total_failed_files:
        total_failed_files = retry_failed_files(total_failed_files, PIPELINE_MONTH)

    # Step 7: Update tracking
    db = DBWrapper()
    try:
        with db.conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM processed_files_uk_ch")
            total_processed = cur.fetchone()[0]
            cur.execute("""
                UPDATE pipeline_tracking_uk_ch
                SET files_processed=%s, files_failed=%s,
                    completed_at=%s, status='completed'
                WHERE ch_upload=%s
            """, (total_processed, len(total_failed_files), datetime.now(), PIPELINE_MONTH))
    finally:
        db.close()

    # Step 8: Cleanup
    cleanup_files(EXTRACT_DIR)

    # Step 9: Summary
    duration = datetime.now() - start_time
    logger.info("="*60)
    logger.info("PIPELINE COMPLETED")
    logger.info(f"   Month    : {PIPELINE_MONTH}")
    logger.info(f"   Processed: {total_processed}")
    logger.info(f"   Failed   : {len(total_failed_files)}")
    logger.info(f"   Duration : {duration}")
    logger.info("="*60)

    run_verification()


if __name__ == "__main__":
    main()