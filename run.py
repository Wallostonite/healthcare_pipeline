# ================================================================
# run.py — Module 03 Entry Point
# ================================================================
# python run.py
# python run.py --skip-demos
#
# WHAT HAPPENS:
#   1. Demo SQL concepts (basics, aggregation, joins) in the terminal
#   2. Run the full extraction query (05_extract_raw_data.sql)
#   3. Save raw-data.csv to data/
# ================================================================

import sys, pathlib, argparse
_root = pathlib.Path(__file__).resolve().parent
if str(_root) not in sys.path:
    sys.path.insert(0, str(_root))

from config import INDUSTRY, DB_AVAILABLE, logger
from src.query_runner  import SQLQueryRunner
from src.data_extractor import DataExtractor


def main() -> None:
    parser = argparse.ArgumentParser(description="Healthcare Schema Extraction Pipeline")
    parser.add_argument("--skip-demos", action="store_true", help="Skip interactive SQL demos")
    args = parser.parse_args()
    
    logger.info("=" * 60)
    logger.info(f"  HEALTHCARE SCHEMA — SQL AND POSTGRESQL")
    logger.info(f"  Industry: {INDUSTRY}")
    logger.info(f"  DB Available: {DB_AVAILABLE}")
    logger.info("=" * 60)

    # ── PART 1: SQL Demonstrations ──────────────────────────────────
    if not args.skip_demos:
        runner = SQLQueryRunner()

        print("\n── DEMO 1: Basic SELECT and WHERE")
        runner.demo_basics()

        print("\n── DEMO 2: GROUP BY and Aggregation")
        runner.demo_aggregation()

        print("\n── DEMO 3: JOIN — patients with billing")  # ✅ Updated label
        runner.demo_joins()

    # ── PART 2: Production Extraction ──────────────────────────────
    logger.info("\n[EXTRACT] Starting production extraction...")
    try:
        extractor = DataExtractor()
        extractor.extract().save().report()
    except Exception as e:
        logger.error(f"[EXTRACT] Fatal error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()