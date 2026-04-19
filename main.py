"""
main.py — Pipeline orchestrator.

Run this file to execute the full ETL pipeline:
    python main.py

Optional flags:
    --no-extract   Skip the extract step (reuse existing raw files)
    --no-analyze   Skip the analytical query / export step
"""

import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime

from config import LOG_DIR, RAW_DIR
from pipeline.extract import extract_all
from pipeline.transform import transform
from pipeline.load import load
from analysis.queries import run_analysis


# ── Logging setup ─────────────────────────────────────────────────────────────

def setup_logging() -> None:
    os.makedirs(LOG_DIR, exist_ok=True)
    log_file = os.path.join(LOG_DIR, "pipeline.log")

    fmt = "[%(asctime)s] %(levelname)-5s — %(message)s"
    datefmt = "%Y-%m-%d %H:%M:%S"

    logging.basicConfig(
        level=logging.INFO,
        format=fmt,
        datefmt=datefmt,
        handlers=[
            logging.FileHandler(log_file, encoding="utf-8"),
            logging.StreamHandler(sys.stdout),
        ],
    )


# ── Helpers ───────────────────────────────────────────────────────────────────

def load_raw_from_disk() -> list[dict]:
    """Fall-back: read today's saved raw JSON files instead of calling the API."""
    payloads  = []
    date_str  = datetime.utcnow().strftime("%Y%m%d")
    if not os.path.isdir(RAW_DIR):
        return payloads
    for fname in os.listdir(RAW_DIR):
        if fname.endswith(f"_{date_str}.json"):
            with open(os.path.join(RAW_DIR, fname), encoding="utf-8") as fh:
                payloads.append(json.load(fh))
    return payloads


# ── Pipeline ──────────────────────────────────────────────────────────────────

def run_pipeline(skip_extract: bool = False, skip_analyze: bool = False) -> None:
    logger = logging.getLogger(__name__)
    start  = time.perf_counter()

    logger.info("=" * 60)
    logger.info("Starting ETL pipeline  (%s UTC)", datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"))
    logger.info("=" * 60)

    # ── Extract ──────────────────────────────────────────────────────────────
    if skip_extract:
        logger.info("EXTRACT skipped — loading raw files from disk.")
        raw = load_raw_from_disk()
    else:
        raw = extract_all()

    if not raw:
        logger.error("No data extracted — aborting pipeline.")
        sys.exit(1)

    total_records = sum(
        len(p.get("hourly", {}).get("time", [])) for p in raw
    )
    logger.info("Extracted %d hourly records across %d cities.", total_records, len(raw))

    # ── Transform ─────────────────────────────────────────────────────────────
    df = transform(raw)
    if df.empty:
        logger.error("Transform produced an empty DataFrame — aborting.")
        sys.exit(1)

    # ── Load ──────────────────────────────────────────────────────────────────
    inserted = load(df)
    logger.info("Inserted %d new rows into the database.", inserted)

    # ── Analyze ───────────────────────────────────────────────────────────────
    if skip_analyze:
        logger.info("ANALYZE skipped.")
    else:
        results = run_analysis()
        logger.info("Analysis complete — %d queries exported.", len(results))

    elapsed = time.perf_counter() - start
    logger.info("Pipeline complete in %.1fs.", elapsed)
    logger.info("=" * 60)


# ── CLI ───────────────────────────────────────────────────────────────────────

def main() -> None:
    setup_logging()

    parser = argparse.ArgumentParser(description="Weather ETL Pipeline")
    parser.add_argument(
        "--no-extract", action="store_true",
        help="Skip API extraction; reuse today's raw JSON files",
    )
    parser.add_argument(
        "--no-analyze", action="store_true",
        help="Skip the analytical query / export step",
    )
    args = parser.parse_args()

    run_pipeline(
        skip_extract=args.no_extract,
        skip_analyze=args.no_analyze,
    )


if __name__ == "__main__":
    main()
