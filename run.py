"""
run.py — Master runner for both flows.

Usage
-----
    python run.py              ← Flow A then Flow B
    python run.py --flow-a     ← Flow A only
    python run.py --flow-b     ← Flow B only
"""
import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path

_HERE = Path(__file__).parent.resolve()
sys.path.insert(0, str(_HERE))

from config import LOG_LEVEL

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(str(_HERE / "pipeline.log"), encoding="utf-8"),
    ],
)
logger = logging.getLogger(__name__)


def run_flow_a() -> bool:
    logger.info("=" * 65)
    logger.info("▶  FLOW A — Company Data CSV (metadata_uk_ch table)")
    logger.info("=" * 65)
    try:
        from flow_a.main import main as flow_a_main
        result = flow_a_main()
        ok = result == 0
        logger.info("%s  Flow A %s", "✅" if ok else "❌", "SUCCESS" if ok else "FAILED")
        return ok
    except Exception as exc:
        logger.error("❌  Flow A crashed: %s", exc, exc_info=True)
        return False


def run_flow_b() -> bool:
    logger.info("=" * 65)
    logger.info("▶  FLOW B — Accounts iXBRL (financials_uk_ch, directors_uk_ch, reports_uk_ch)")
    logger.info("=" * 65)
    try:
        from backfill_runner import main as flow_b_main
        flow_b_main()
        logger.info("✅  Flow B SUCCESS")
        return True
    except Exception as exc:
        logger.error("❌  Flow B crashed: %s", exc, exc_info=True)
        return False


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--flow-a", action="store_true")
    parser.add_argument("--flow-b", action="store_true")
    args = parser.parse_args()

    run_a = args.flow_a or (not args.flow_a and not args.flow_b)
    run_b = args.flow_b or (not args.flow_a and not args.flow_b)

    start = datetime.now()
    logger.info("=" * 65)
    logger.info("  Companies House Pipeline — Master Runner")
    logger.info("  %s", start.strftime("%Y-%m-%d %H:%M:%S"))
    logger.info("  Running: %s",
        "Flow A → Flow B" if (run_a and run_b)
        else "Flow A only" if run_a else "Flow B only")
    logger.info("=" * 65)

    a_ok = run_flow_a() if run_a else True
    b_ok = run_flow_b() if run_b else True

    elapsed = datetime.now() - start
    logger.info("")
    logger.info("=" * 65)
    logger.info("  DONE  |  total time: %s", elapsed)
    logger.info("  Flow A : %s", "✅ SUCCESS" if a_ok else "❌ FAILED")
    logger.info("  Flow B : %s", "✅ SUCCESS" if b_ok else "❌ FAILED")
    logger.info("=" * 65)
    return 0 if (a_ok and b_ok) else 1


if __name__ == "__main__":
    sys.exit(main())
