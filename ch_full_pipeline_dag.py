"""
dags/full_pipeline_dag.py
--------------------------
Airflow DAG: full_pipeline

Two tasks:
  task_flow_a  — Company Data CSV  (metadata_uk_ch)
  task_flow_b  — Accounts iXBRL   (financials_uk_ch, directors_uk_ch, reports_uk_ch)

Flow A runs first; Flow B starts only after Flow A succeeds.

Connections used (configured in Airflow UI → Admin → Connections):
  staging_pg_ch_connection_w1  → PostgreSQL
  smtp_w1_default              → SMTP/Email
    Extra field must contain:  {"receiver_email": "you@example.com"}

All other settings read from .env inside the project folder.

Setup:
  1. Copy/extract this project into your Airflow volume, e.g.:
       Docker:  /opt/airflow/ch_pipeline/
  2. Copy this DAG file to your Airflow dags folder, e.g.:
       Docker:  /opt/airflow/dags/full_pipeline_dag.py
  3. Set CH_PIPELINE_PATH env var in your docker-compose.yaml (or Airflow config):
       CH_PIPELINE_PATH=/opt/airflow/ch_pipeline
  4. Install requirements inside the Airflow container:
       pip install -r /opt/airflow/ch_pipeline/requirements.txt
"""

import logging
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator

# ── Project root — update this path to match your Docker volume mount ─────
PROJECT_PATH = Path(os.getenv("CH_PIPELINE_PATH", "/opt/airflow/dags/ch_pipeline_airflow"))

logger = logging.getLogger(__name__)


# =============================================================================
# Task callables
# =============================================================================

def run_flow_a(**context):
    """
    Flow A — Downloads Company Data CSV and loads it into metadata_uk_ch.
    Equivalent to:  python run.py --flow-a
    """
    if str(PROJECT_PATH) not in sys.path:
        sys.path.insert(0, str(PROJECT_PATH))

    from config import (
        DB_DSN, BATCH_SIZE, CLEANUP_DOWNLOADS,
        MAX_RETRIES, RETRY_DELAY_SECONDS, LOG_LEVEL,
        FORCE_YEAR, FORCE_MONTH, PROJECT_ROOT,
    )
    from flow_a.utils.db import DatabaseWrapper
    from flow_a.utils.helpers import Config, retry
    from flow_a.utils.pipeline import run as run_pipeline

    logging.basicConfig(
        level=getattr(logging, LOG_LEVEL, logging.INFO),
        format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    config = Config(
        db_dsn              = DB_DSN,
        batch_size          = BATCH_SIZE,
        parse_workers       = 1,
        db_workers          = 1,
        cleanup_downloads   = CLEANUP_DOWNLOADS,
        max_retries         = MAX_RETRIES,
        retry_delay_seconds = RETRY_DELAY_SECONDS,
        log_level           = LOG_LEVEL,
        force_year          = FORCE_YEAR,
        force_month         = FORCE_MONTH,
        download_dir        = str(PROJECT_ROOT / "data" / "downloads"),
        report_dir          = str(PROJECT_ROOT / "reports_uk_ch"),
    )

    logger.info("=" * 60)
    logger.info("Flow A — Company Data CSV (metadata_uk_ch)")
    logger.info("DB: %s", DB_DSN.split("@")[-1])
    logger.info("=" * 60)

    db = DatabaseWrapper(dsn=DB_DSN)
    try:
        db.connect()
        db.create_tables()
    except Exception as exc:
        raise RuntimeError(f"Flow A — database init failed: {exc}") from exc

    @retry(max_attempts=MAX_RETRIES, delay_seconds=RETRY_DELAY_SECONDS)
    def _run():
        run_pipeline(config=config, db=db)

    try:
        _run()
        logger.info("✅  Flow A finished successfully.")
    except Exception as exc:
        logger.error("❌  Flow A failed: %s", exc)
        raise
    finally:
        db.disconnect()


def run_flow_b(**context):
    """
    Flow B — Sequential iXBRL accounts backfill.
    Equivalent to:  python run.py --flow-b
    """
    if str(PROJECT_PATH) not in sys.path:
        sys.path.insert(0, str(PROJECT_PATH))

    from backfill_runner import main as flow_b_main

    logger.info("=" * 60)
    logger.info("Flow B — Accounts iXBRL (financials / directors / reports)")
    logger.info("=" * 60)

    try:
        flow_b_main()
        logger.info("✅  Flow B finished successfully.")
    except Exception as exc:
        logger.error("❌  Flow B failed: %s", exc)
        raise


# =============================================================================
# DAG
# =============================================================================

default_args = {
    "owner":            "airflow",
    "depends_on_past":  False,
    "retries":          1,
    "retry_delay":      timedelta(minutes=5),
    "email_on_failure": False,   # pipeline sends its own rich email via smtp_w1_default
    "email_on_retry":   False,
}

with DAG(
    dag_id="company_house_uk_full_pipeline",
    description="Companies House — Flow A (CSV metadata) → Flow B (iXBRL accounts)",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule=None,          # manual trigger; set e.g. "0 2 1 * *" for 1st of month at 02:00
    catchup=False,
    tags=["companies_house", "pipeline"],
) as dag:

    task_flow_a = PythonOperator(
        task_id="task_flow_a",
        python_callable=run_flow_a,
        execution_timeout=timedelta(hours=6),
    )

    task_flow_b = PythonOperator(
        task_id="task_flow_b",
        python_callable=run_flow_b,
        execution_timeout=timedelta(hours=48),
    )

    # Dependency: Flow A must succeed before Flow B starts
    task_flow_a >> task_flow_b
