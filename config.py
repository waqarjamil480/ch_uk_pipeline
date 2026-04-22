"""
config.py — Unified config for Flow A + Flow B

DB credentials   → Airflow connection: staging_pg_ch_connection_w1
SMTP credentials → Airflow connection: smtp_w1_default
All other settings → .env (loaded here)
"""
import os
from pathlib import Path
from dotenv import load_dotenv

_HERE = Path(__file__).parent.resolve()
load_dotenv(dotenv_path=_HERE / ".env", override=True)


def _get_db_config():
    """
    Pull DB connection details from Airflow connection 'staging_pg_ch_connection_w1'.
    Falls back to env vars for local (non-Airflow) runs.
    """
    try:
        from airflow.hooks.base import BaseHook
        conn = BaseHook.get_connection("staging_pg_ch_connection_w1")
        return {
            "host":     conn.host,
            "port":     int(conn.port) if conn.port else 5432,
            "dbname":   conn.schema,
            "user":     conn.login,
            "password": conn.password,
        }
    except Exception:
        return {
            "host":     os.getenv("DB_HOST",     "localhost"),
            "port":     int(os.getenv("DB_PORT", "5432")),
            "dbname":   os.getenv("DB_NAME",     ""),
            "user":     os.getenv("DB_USER",     ""),
            "password": os.getenv("DB_PASSWORD", ""),
        }


def _get_smtp_config():
    """
    Pull SMTP details from Airflow connection 'smtp_w1_default'.
    Falls back to env vars for local runs.

    Airflow SMTP connection fields:
      host     = SMTP server  (e.g. smtp.gmail.com)
      port     = SMTP port    (e.g. 587)
      login    = sender email
      password = app password / SMTP password
      extra    = JSON, e.g. {"receiver_email": "you@example.com"}
    """
    try:
        from airflow.hooks.base import BaseHook
        import json
        conn = BaseHook.get_connection("smtp_w1_default")
        extra = {}
        if conn.extra:
            try:
                extra = json.loads(conn.extra)
            except Exception:
                pass
        return {
            "smtp_server":     conn.host or "smtp.gmail.com",
            "smtp_port":       int(conn.port) if conn.port else 587,
            "sender_email":    conn.login or "",
            "sender_password": conn.password or "",
            "receiver_email":  extra.get("receiver_email", os.getenv("RECEIVER_EMAIL", "")),
        }
    except Exception:
        return {
            "smtp_server":     os.getenv("SMTP_SERVER",     "smtp.gmail.com"),
            "smtp_port":       int(os.getenv("SMTP_PORT",   "587")),
            "sender_email":    os.getenv("SENDER_EMAIL",    ""),
            "sender_password": os.getenv("SENDER_PASSWORD", ""),
            "receiver_email":  os.getenv("RECEIVER_EMAIL",  ""),
        }


# ── Database (from Airflow connection) ───────────────────────────────────
_db = _get_db_config()
DB_HOST     = _db["host"]
DB_PORT     = _db["port"]
DB_NAME     = _db["dbname"]
DB_USER     = _db["user"]
DB_PASSWORD = _db["password"]

DB_CONFIG = {"host": DB_HOST, "port": DB_PORT,
             "dbname": DB_NAME, "user": DB_USER, "password": DB_PASSWORD}

# DSN string for Flow A
DB_DSN = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# ── SMTP (from Airflow connection) ────────────────────────────────────────
_smtp = _get_smtp_config()
SMTP_SERVER     = _smtp["smtp_server"]
SMTP_PORT       = _smtp["smtp_port"]
SENDER_EMAIL    = _smtp["sender_email"]
SENDER_PASSWORD = _smtp["sender_password"]
RECEIVER_EMAIL  = _smtp["receiver_email"]

# ── Paths ─────────────────────────────────────────────────────────────────
PROJECT_ROOT = _HERE
DOWNLOAD_DIR = PROJECT_ROOT / "data" / "downloads"
EXTRACT_DIR  = PROJECT_ROOT / "data" / "extracted"
DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
EXTRACT_DIR.mkdir(parents=True, exist_ok=True)

# ── Flow A settings (from .env) ───────────────────────────────────────────
CLEANUP_DOWNLOADS   = os.getenv("CLEANUP_DOWNLOADS",       "true").lower() == "true"
MAX_RETRIES         = int(os.getenv("MAX_RETRIES",         "3"))
RETRY_DELAY_SECONDS = int(os.getenv("RETRY_DELAY_SECONDS", "3"))
LOG_LEVEL           = os.getenv("LOG_LEVEL",               "INFO").upper()
FORCE_YEAR          = int(os.getenv("FORCE_YEAR"))  if os.getenv("FORCE_YEAR")  else None
FORCE_MONTH         = int(os.getenv("FORCE_MONTH")) if os.getenv("FORCE_MONTH") else None

# ── Flow B settings (from .env) ───────────────────────────────────────────
NUM_WORKERS          = int(os.getenv("NUM_WORKERS",          "8"))
BATCH_SIZE           = int(os.getenv("BATCH_SIZE",           "500"))
BACKFILL_START_YEAR  = int(os.getenv("BACKFILL_START_YEAR",  "2021"))
BACKFILL_START_MONTH = int(os.getenv("BACKFILL_START_MONTH", "1"))
TEST_MONTHS          = int(os.getenv("TEST_MONTHS",          "0"))

# ── Email (enabled flag from .env; credentials from Airflow connection) ───
EMAIL_ENABLED = os.getenv("EMAIL_ENABLED", "false").lower() == "true"

# ── Slack (from .env) ─────────────────────────────────────────────────────
SLACK_ENABLED     = os.getenv("SLACK_ENABLED",     "false").lower() == "true"
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL", "")

# ── Flow B URL helpers ────────────────────────────────────────────────────
_MONTH_NAMES = ["","January","February","March","April","May","June",
                "July","August","September","October","November","December"]
_CH_BASE = "https://download.companieshouse.gov.uk"

def build_accounts_url(year, month):
    return f"{_CH_BASE}/Accounts_Monthly_Data-{_MONTH_NAMES[month]}{year}.zip"

def zip_filename(year, month):
    return f"Accounts_Monthly_Data-{_MONTH_NAMES[month]}{year}.zip"

def month_label(year, month):
    return f"{_MONTH_NAMES[month][:3]}-{str(year)[2:]}"
