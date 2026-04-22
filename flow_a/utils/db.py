import io
"""
flow_a/utils/db.py
------------------
All PostgreSQL interaction for Flow A — Company Data (CSV).
Single source of truth: _STAGING_COLUMNS list drives DDL, COPY, and UPSERT.
"""
import logging
import psycopg2
from contextlib import contextmanager
from typing import Any, Dict, List, Tuple

logger = logging.getLogger(__name__)
SNAPSHOT_TOLERANCE = 0.95

# ── Single source of truth — all 26 data columns ─────────────────────────
_STAGING_COLUMNS = [
    "company_number",
    "company_name",
    "address_line_1",
    "address_line_2",
    "post_town",
    "county",
    "country",
    "postcode",
    "care_of",
    "po_box",
    "company_category",
    "company_status",
    "country_of_origin",
    "dissolution_date",
    "incorporation_date",
    "accounts_ref_day",
    "accounts_ref_month",
    "accounts_next_due_date",
    "accounts_last_made_up",
    "accounts_category",
    "returns_next_due_date",
    "returns_last_made_up",
    "sic_code_1",
    "sic_code_2",
    "sic_code_3",
    "sic_code_4",
]

# ── DDL (built from _STAGING_COLUMNS — no manual column lists) ───────────
def _col_ddl(cols):
    return ",\n".join(f"    {c:<24} TEXT" for c in cols)

DDL_METADATA = f"""
CREATE TABLE IF NOT EXISTS metadata_uk_ch (
    company_number          TEXT        NOT NULL,
{_col_ddl([c for c in _STAGING_COLUMNS if c != "company_number"])},
    loaded_at               TIMESTAMP   DEFAULT NOW(),
    CONSTRAINT pk_metadata PRIMARY KEY (company_number)
);
"""

DDL_STAGING = f"""
CREATE UNLOGGED TABLE IF NOT EXISTS metadata_staging_uk_ch (
{_col_ddl(_STAGING_COLUMNS)}
);
"""

DDL_TRACKING = """
CREATE TABLE IF NOT EXISTS pipeline_tracking_uk_ch (
    id              SERIAL      PRIMARY KEY,
    batch_id        TEXT        NOT NULL UNIQUE,
    ch_upload       TEXT,
    source          TEXT        NOT NULL,
    zip_filename    TEXT,
    status          TEXT        NOT NULL DEFAULT 'running',
    files_processed INTEGER     DEFAULT 0,
    files_failed    INTEGER     DEFAULT 0,
    started_at      TIMESTAMP,
    completed_at    TIMESTAMP,
    loaded_at       TIMESTAMP   DEFAULT NOW()
);
"""

DDL_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_metadata_company_number ON metadata_uk_ch (company_number);",
    "CREATE INDEX IF NOT EXISTS idx_metadata_status ON metadata_uk_ch (company_status);",
    "CREATE INDEX IF NOT EXISTS idx_tracking_batch_id ON pipeline_tracking_uk_ch (batch_id);",
    "CREATE INDEX IF NOT EXISTS idx_tracking_source ON pipeline_tracking_uk_ch (source);",
]

# ── COPY + UPSERT SQL ─────────────────────────────────────────────────────
_COLS_SQL   = ", ".join(_STAGING_COLUMNS)
_COPY_SQL   = (
    f"COPY metadata_staging_uk_ch ({_COLS_SQL}) "
    "FROM STDIN WITH (FORMAT TEXT, DELIMITER E'\\t', NULL '')"
)
_UPDATE_SET = ",\n    ".join(
    f"{c} = EXCLUDED.{c}"
    for c in _STAGING_COLUMNS if c != "company_number"
)
SQL_UPSERT_FROM_STAGING = f"""
INSERT INTO metadata_uk_ch ({_COLS_SQL}, loaded_at)
SELECT {_COLS_SQL}, NOW()
FROM metadata_staging_uk_ch
ON CONFLICT (company_number) DO UPDATE SET
    {_UPDATE_SET},
    loaded_at = NOW();
"""

# ── Row serialiser ────────────────────────────────────────────────────────
def _row_to_tsv(row: Dict[str, Any]) -> str:
    fields = []
    for col in _STAGING_COLUMNS:
        val = row.get(col)
        if val is None:
            fields.append("")
        else:
            fields.append(
                str(val)
                .replace("\\", "\\\\")
                .replace("\t", " ")
                .replace("\n", " ")
                .replace("\r", "")
            )
    return "\t".join(fields)

# ── RowBuffer — auto-flushes every 50k rows ───────────────────────────────
COPY_FLUSH_ROWS = 50_000

class RowBuffer:
    def __init__(self, conn, flush_every: int = COPY_FLUSH_ROWS):
        self._conn        = conn
        self._flush_every = flush_every
        self._lines: List[str] = []
        self._total       = 0

    def add_batch(self, rows: List[Dict[str, Any]]) -> None:
        for row in rows:
            self._lines.append(_row_to_tsv(row))
            if len(self._lines) >= self._flush_every:
                self._flush()

    def close(self) -> int:
        if self._lines:
            self._flush()
        return self._total

    def _flush(self) -> None:
        if not self._lines:
            return
        data = ("\n".join(self._lines) + "\n").encode("utf-8", errors="replace")
        buf  = io.BytesIO(data)
        try:
            self._conn.cursor().copy_expert(_COPY_SQL, buf)
            self._conn.commit()
        except Exception:
            self._conn.rollback()
            raise
        finally:
            self._total += len(self._lines)
            self._lines  = []

    @property
    def row_count(self) -> int:
        return self._total

# ── DatabaseWrapper ───────────────────────────────────────────────────────
class DatabaseWrapper:

    def __init__(self, dsn: str):
        self._dsn  = dsn
        self._conn = None

    def connect(self) -> None:
        logger.info("Connecting to PostgreSQL …")
        self._conn = psycopg2.connect(self._dsn)
        self._conn.autocommit = False
        logger.info("Connected.")

    def disconnect(self) -> None:
        if self._conn and not self._conn.closed:
            self._conn.close()
            logger.info("Database connection closed.")

    @contextmanager
    def transaction(self):
        try:
            yield self._conn
            self._conn.commit()
        except Exception:
            self._conn.rollback()
            raise

    def create_tables(self) -> None:
        logger.info("Ensuring pipeline tables exist …")
        with self.transaction():
            cur = self._conn.cursor()
            cur.execute(DDL_METADATA)
            cur.execute(DDL_STAGING)
            cur.execute(DDL_TRACKING)
            for idx in DDL_INDEXES:
                cur.execute(idx)
        logger.info("Tables and indexes ready.")

    def truncate_staging(self) -> None:
        logger.info("Truncating staging table …")
        with self.transaction():
            self._conn.cursor().execute("TRUNCATE TABLE metadata_staging_uk_ch;")
        logger.info("Staging cleared.")

    def new_row_buffer(self) -> "RowBuffer":
        return RowBuffer(self._conn)

    def get_staging_count(self) -> int:
        cur = self._conn.cursor()
        cur.execute("SELECT COUNT(*) FROM metadata_staging_uk_ch;")
        count = cur.fetchone()[0]
        logger.info("Staging count: %d", count)
        return count

    def assert_full_snapshot(self, tolerance: float = SNAPSHOT_TOLERANCE) -> Tuple[bool, str]:
        staging  = self.get_staging_count()
        previous = self.get_metadata_count()
        if staging == 0:
            reason = "ABORTED: staging is empty — download or parse failed."
            logger.error(reason)
            return False, reason
        if previous > 0:
            ratio = staging / previous
            if ratio < tolerance:
                reason = (
                    f"ABORTED: staging has {staging:,} rows but previous "
                    f"metadata_uk_ch had {previous:,} ({ratio:.1%} < {tolerance:.0%}). "
                    "Partial download suspected. Metadata unchanged."
                )
                logger.error(reason)
                return False, reason
            logger.info("Snapshot check passed: staging=%d  prev=%d  ratio=%.1f%%",
                        staging, previous, ratio * 100)
        else:
            logger.info("First load — staging=%d rows.", staging)
        return True, "OK"

    def swap_staging_to_metadata(self) -> int:
        logger.info("Swapping staging → metadata_uk_ch (UPSERT) …")
        with self.transaction():
            self._conn.cursor().execute(SQL_UPSERT_FROM_STAGING)
        count = self.get_metadata_count()
        logger.info("Swap complete. metadata_uk_ch rows: %d", count)
        return count

    def get_metadata_count(self) -> int:
        cur = self._conn.cursor()
        cur.execute("SELECT COUNT(*) FROM metadata_uk_ch;")
        return cur.fetchone()[0]

    def tracking_start(self, batch_id, source, ch_upload=None, zip_filename=None):
        sql = """
            INSERT INTO pipeline_tracking_uk_ch
                (batch_id, ch_upload, source, zip_filename,
                 status, files_processed, files_failed,
                 started_at, completed_at, loaded_at)
            VALUES (%s, %s, %s, %s, 'running', 0, 0, NOW(), NULL, NOW())
            ON CONFLICT (batch_id) DO UPDATE SET
                ch_upload    = EXCLUDED.ch_upload,
                zip_filename = EXCLUDED.zip_filename,
                status       = 'running',
                started_at   = NOW(),
                loaded_at    = NOW();
        """
        with self.transaction():
            self._conn.cursor().execute(sql, (batch_id, ch_upload, source, zip_filename))
        logger.info("Tracking started: %s  ch_upload=%s", batch_id, ch_upload)

    def tracking_complete(self, batch_id, files_processed=0, files_failed=0):
        with self.transaction():
            self._conn.cursor().execute("""
                UPDATE pipeline_tracking_uk_ch
                SET status='completed', files_processed=%s, files_failed=%s,
                    completed_at=NOW(), loaded_at=NOW()
                WHERE batch_id=%s;
            """, (files_processed, files_failed, batch_id))
        logger.info("Tracking completed: %s", batch_id)

    def tracking_fail(self, batch_id, files_processed=0, files_failed=0):
        with self.transaction():
            self._conn.cursor().execute("""
                UPDATE pipeline_tracking_uk_ch
                SET status='failed', files_processed=%s, files_failed=%s,
                    completed_at=NOW(), loaded_at=NOW()
                WHERE batch_id=%s;
            """, (files_processed, files_failed, batch_id))
        logger.warning("Tracking failed: %s", batch_id)

    def is_batch_completed(self, batch_id):
        cur = self._conn.cursor()
        cur.execute(
            "SELECT 1 FROM pipeline_tracking_uk_ch "
            "WHERE batch_id=%s AND status='completed' LIMIT 1;",
            (batch_id,),
        )
        return cur.fetchone() is not None
