"""
flow_a/main.py
--------------
Flow A — Company Data CSV Pipeline entry point.

Run directly : python flow_a/main.py
Run via master: python run.py --flow-a
"""
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from config import (
    DB_DSN, BATCH_SIZE, CLEANUP_DOWNLOADS,
    MAX_RETRIES, RETRY_DELAY_SECONDS, LOG_LEVEL,
    FORCE_YEAR, FORCE_MONTH, PROJECT_ROOT,
)
from flow_a.utils.db import DatabaseWrapper
from flow_a.utils.helpers import Config, setup_logging, retry
from flow_a.utils.pipeline import run as run_pipeline

logger = logging.getLogger(__name__)


def main() -> int:
    setup_logging(level=LOG_LEVEL)

    logger.info("=" * 60)
    logger.info("Companies House — Flow A: Company Data CSV")
    logger.info("=" * 60)

    # Build config from unified config.py
    config = Config(
        db_dsn            = DB_DSN,
        batch_size        = BATCH_SIZE,
        parse_workers     = 1,   # Flow A: single stream, no workers
        db_workers        = 1,   # Flow A: single stream, no workers
        cleanup_downloads = CLEANUP_DOWNLOADS,
        max_retries       = MAX_RETRIES,
        retry_delay_seconds = RETRY_DELAY_SECONDS,
        log_level         = LOG_LEVEL,
        force_year        = FORCE_YEAR,
        force_month       = FORCE_MONTH,
        download_dir      = str(PROJECT_ROOT / "data" / "downloads"),
        report_dir        = str(PROJECT_ROOT / "reports_uk_ch"),
    )

    logger.info("  db              : %s", DB_DSN.split("@")[-1])
    logger.info("  batch_size      : %d", config.batch_size)
    logger.info("  cleanup_downloads: %s", config.cleanup_downloads)
    logger.info("  max_retries     : %d", config.max_retries)
    if FORCE_YEAR and FORCE_MONTH:
        logger.info("  forced month    : %04d-%02d", FORCE_YEAR, FORCE_MONTH)
    else:
        logger.info("  month           : auto-detected")

    db = DatabaseWrapper(dsn=DB_DSN)
    try:
        db.connect()
        db.create_tables()
    except Exception as exc:
        logger.error("Database init failed: %s", exc)
        return 1

    @retry(max_attempts=MAX_RETRIES, delay_seconds=RETRY_DELAY_SECONDS)
    def _run():
        run_pipeline(config=config, db=db)

    try:
        _run()
        logger.info("Flow A finished successfully.")
        return 0
    except Exception as exc:
        logger.error("Flow A failed: %s", exc)
        return 1
    finally:
        db.disconnect()


if __name__ == "__main__":
    sys.exit(main())
