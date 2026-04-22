"""
utils/db_wrapper.py
-------------------
DB wrapper for Flow B.
Connection details come from Airflow connection 'staging_pg_ch_connection_w1'
via config.py — no credentials hardcoded here.
"""

import psycopg2
import sys
from psycopg2.extras import execute_values
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))
from config import DB_CONFIG


class DBWrapper:

    def __init__(self):
        self.conn = psycopg2.connect(**DB_CONFIG)
        self.conn.autocommit = False   # explicit commits — safer for batch writes

    def close(self):
        self.conn.close()

    # ─────────────────────────────────────────
    # GENERIC BATCH INSERT
    # ─────────────────────────────────────────
    def insert_batch(self, table, columns, rows, conflict_clause=None):
        if not rows:
            return

        query = f"""
            INSERT INTO {table} ({','.join(columns)})
            VALUES %s
        """

        if conflict_clause:
            query += f" {conflict_clause}"

        with self.conn.cursor() as cur:
            execute_values(cur, query, rows)
        self.conn.commit()

    # ─────────────────────────────────────────
    # FINANCIALS → APPEND ONLY
    # ─────────────────────────────────────────
    def insert_financials(self, rows):
        cols = [
            "company_number", "metric", "value", "period",
            "account_closing_date_last_year", "fiscal_period_new",
            "is_consolidated", "data_scope", "filing_date",
            "source_file", "ch_upload"
        ]
        self.insert_batch("financials_uk_ch", cols, rows)

    # ─────────────────────────────────────────
    # UPSERT TABLES
    # ─────────────────────────────────────────
    def upsert_directors(self, rows):
        cols = ["company_number", "director_name", "filing_date", "source_file", "ch_upload"]
        conflict = """
        ON CONFLICT (company_number, director_name)
        DO UPDATE SET
            filing_date = EXCLUDED.filing_date,
            source_file = EXCLUDED.source_file,
            ch_upload = EXCLUDED.ch_upload
        """
        self.insert_batch("directors_uk_ch", cols, rows, conflict)

    def upsert_reports(self, rows):
        cols = ["company_number", "company_name", "section", "text",
                "filing_date", "source_file", "ch_upload"]
        conflict = """
        ON CONFLICT (company_number, section)
        DO UPDATE SET
            company_name = EXCLUDED.company_name,
            text = EXCLUDED.text,
            filing_date = EXCLUDED.filing_date,
            source_file = EXCLUDED.source_file,
            ch_upload = EXCLUDED.ch_upload
        """
        self.insert_batch("reports_uk_ch", cols, rows, conflict)

    # ─────────────────────────────────────────
    # PIPELINE TRACKING
    # ─────────────────────────────────────────
    def insert_pipeline_run(self, row):
        query = """
        INSERT INTO pipeline_tracking_uk_ch (
            batch_id, ch_upload, source, zip_filename,
            files_processed, files_failed,
            started_at, completed_at, status
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (batch_id) DO NOTHING
        """
        with self.conn.cursor() as cur:
            cur.execute(query, row)
        self.conn.commit()

    def copy_financials_from_csv(self, csv_path):
        with self.conn.cursor() as cur:
            cur.copy_expert(
                """
                COPY financials_uk_ch (
                    company_number, metric, value, period,
                    account_closing_date_last_year, fiscal_period_new,
                    is_consolidated, data_scope, filing_date,
                    source_file, ch_upload
                ) FROM STDIN WITH CSV
                """,
                open(csv_path, 'r')
            )

    from psycopg2.extras import execute_values

    def insert_processed_files(self, source_files, ch_upload):
        with self.conn.cursor() as cur:
            execute_values(
                cur,
                """
                INSERT INTO processed_files_uk_ch (source_file, ch_upload, status, comment, processed_at)
                VALUES %s
                ON CONFLICT (source_file, ch_upload) DO UPDATE SET
                    status       = EXCLUDED.status,
                    comment      = EXCLUDED.comment,
                    processed_at = EXCLUDED.processed_at
                """,
                [(sf, ch_upload, 'success', 'Processed successfully', __import__('datetime').datetime.now())
                 for sf in source_files]
            )
        self.conn.commit()

    def insert_failed_file(self, source_file, ch_upload, error_message):
        with self.conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO processed_files_uk_ch (source_file, ch_upload, status, comment, processed_at)
                VALUES (%s, %s, 'failed', %s, NOW())
                ON CONFLICT (source_file, ch_upload) DO UPDATE SET
                    status       = EXCLUDED.status,
                    comment      = EXCLUDED.comment,
                    processed_at = EXCLUDED.processed_at
                """,
                (source_file, ch_upload, str(error_message)[:2000])
            )
        self.conn.commit()

    def insert_warning_files(self, warning_rows):
        with self.conn.cursor() as cur:
            execute_values(
                cur,
                """
                INSERT INTO processed_files_uk_ch
                    (source_file, ch_upload, status, comment, processed_at)
                VALUES %s
                ON CONFLICT (source_file, ch_upload) DO UPDATE SET
                    status       = EXCLUDED.status,
                    comment      = EXCLUDED.comment,
                    processed_at = EXCLUDED.processed_at
                """,
                [
                    (source_file, ch_upload, 'warning', comment[:2000],
                     __import__('datetime').datetime.now())
                    for source_file, ch_upload, comment in warning_rows
                ]
            )
        self.conn.commit()

    def get_total_processed_files(self, month):
        with self.conn.cursor() as cur:
            cur.execute(
                "SELECT files_processed FROM pipeline_tracking_uk_ch WHERE ch_upload = %s",
                (month,)
            )
            result = cur.fetchone()
        return result[0] if result and result[0] is not None else 0
