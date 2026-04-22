

# utils/setup_db.py
import sys
import logging
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from utils.db_wrapper import DBWrapper

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def setup_database():
    """
    Create all pipeline tables and indexes.
    Safe to run multiple times — uses IF NOT EXISTS throughout.
    """
    db = DBWrapper()
    logger.info("Creating tables...")

    try:
        with db.conn.cursor() as cur:

            # ── FINANCIALS — APPEND ONLY ──────────────────────────────
            cur.execute("""
                CREATE TABLE IF NOT EXISTS financials_uk_ch (
                    company_number                TEXT,
                    metric                        TEXT,
                    value                         NUMERIC,
                    period                        TEXT,
                    account_closing_date_last_year DATE,
                    fiscal_period_new             INT,
                    is_consolidated               TEXT,
                    data_scope                    TEXT,
                    filing_date                   DATE,
                    source_file                   TEXT,
                    ch_upload                     TEXT,
                    loaded_at                     TIMESTAMP DEFAULT NOW()
                );
            """)

            # ── DIRECTORS — UPSERT ────────────────────────────────────
            cur.execute("""
                CREATE TABLE IF NOT EXISTS directors_uk_ch (
                    company_number TEXT,
                    director_name  TEXT,
                    filing_date    DATE,
                    source_file    TEXT,
                    ch_upload      TEXT,
                    loaded_at      TIMESTAMP DEFAULT NOW(),
                    UNIQUE(company_number, director_name)
                );
            """)

            # ── REPORTS — UPSERT ──────────────────────────────────────
            cur.execute("""
                CREATE TABLE IF NOT EXISTS reports_uk_ch (
                    company_number TEXT,
                    company_name   TEXT,
                    section        TEXT,
                    text           TEXT,
                    filing_date    DATE,
                    source_file    TEXT,
                    ch_upload      TEXT,
                    loaded_at      TIMESTAMP DEFAULT NOW(),
                    UNIQUE(company_number, section)
                );
            """)

            # ── PROCESSED FILES — checkpoint table ────────────────────
            # PRIMARY KEY is (source_file, ch_upload) so same filename
            # in different months is treated as a separate record
            cur.execute("""
                CREATE TABLE IF NOT EXISTS processed_files_uk_ch (
                    source_file  TEXT      NOT NULL,
                    ch_upload    TEXT      NOT NULL,
                    status       TEXT      NOT NULL DEFAULT 'success',
                    comment      TEXT,
                    processed_at TIMESTAMP DEFAULT NOW(),
                    PRIMARY KEY (source_file, ch_upload)
                );
            """)

            # ── PIPELINE TRACKING ─────────────────────────────────────
            # Replaces ch_pipeline_runs.
            # One row per monthly batch — tracks overall run status,
            # timing, and file counts.
            #
            # Columns:
            #   id              — auto-increment
            #   batch_id        — unique e.g. 'accounts_Jan-21'
            #   ch_upload       — month label e.g. 'Jan-21'
            #   source          — always 'accounts' for Flow B
            #                     (derived from ZIP name starting with 'Accounts_')
            #   zip_filename    — e.g. 'Accounts_Monthly_Data-January2021.zip'
            #   status          — 'running' / 'completed' / 'failed'
            #   files_processed — count of successfully processed files
            #   files_failed    — count of permanently failed files
            #   started_at      — when this month's processing started
            #   completed_at    — when this month finished (NULL while running)
            #   loaded_at       — last time this row was updated
            cur.execute("""
                CREATE TABLE IF NOT EXISTS pipeline_tracking_uk_ch (
                    id              SERIAL      PRIMARY KEY,
                    batch_id        TEXT        NOT NULL UNIQUE,
                    ch_upload       TEXT        NOT NULL,
                    source          TEXT        NOT NULL DEFAULT 'accounts',
                    zip_filename    TEXT,
                    status          TEXT        NOT NULL DEFAULT 'running',
                    files_processed INTEGER     DEFAULT 0,
                    files_failed    INTEGER     DEFAULT 0,
                    started_at      TIMESTAMP,
                    completed_at    TIMESTAMP,
                    loaded_at       TIMESTAMP   DEFAULT NOW()
                );
            """)

            # ── INDEXES ───────────────────────────────────────────────
            logger.info("Creating indexes...")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_financials_company   ON financials_uk_ch(company_number);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_financials_ch_upload ON financials_uk_ch(ch_upload);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_directors_company    ON directors_uk_ch(company_number);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_reports_company      ON reports_uk_ch(company_number);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_processed_ch_upload  ON processed_files_uk_ch(ch_upload);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_processed_status     ON processed_files_uk_ch(status);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_tracking_batch_id    ON pipeline_tracking_uk_ch(batch_id);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_tracking_ch_upload   ON pipeline_tracking_uk_ch(ch_upload);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_tracking_status      ON pipeline_tracking_uk_ch(status);")

            # ── UPGRADE: add columns if upgrading from old schema ─────
            cur.execute("""
                DO $$
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1 FROM information_schema.columns
                        WHERE table_name='processed_files_uk_ch' AND column_name='status'
                    ) THEN
                        ALTER TABLE processed_files_uk_ch ADD COLUMN status TEXT NOT NULL DEFAULT 'success';
                    END IF;
                    IF NOT EXISTS (
                        SELECT 1 FROM information_schema.columns
                        WHERE table_name='processed_files_uk_ch' AND column_name='comment'
                    ) THEN
                        ALTER TABLE processed_files_uk_ch ADD COLUMN comment TEXT;
                    END IF;
                END$$;
            """)

        db.conn.commit()
        logger.info("All tables and indexes created successfully!")
        return True

    except Exception as e:
        db.conn.rollback()
        logger.error(f"Database setup error: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        db.close()


if __name__ == "__main__":
    success = setup_database()
    sys.exit(0 if success else 1)