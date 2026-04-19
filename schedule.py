"""
schedule.py — Optional: run the pipeline on a recurring schedule.

Usage:
    python schedule.py

The pipeline will run immediately and then repeat every N hours as
configured by SCHEDULE_INTERVAL_HOURS in config.py.

Press Ctrl-C to stop.
"""

import logging
import sys
import time

import schedule as _schedule

from config import SCHEDULE_INTERVAL_HOURS
from main import run_pipeline, setup_logging

logger = logging.getLogger(__name__)


def job() -> None:
    try:
        run_pipeline()
    except SystemExit as exc:
        # run_pipeline calls sys.exit(1) on fatal errors — catch and log
        logger.error("Pipeline exited with code %s — will retry next interval.", exc.code)


def main() -> None:
    setup_logging()
    logger.info(
        "Scheduler started — running every %d hour(s). Press Ctrl-C to stop.",
        SCHEDULE_INTERVAL_HOURS,
    )

    # Run immediately on start, then on schedule
    job()
    _schedule.every(SCHEDULE_INTERVAL_HOURS).hours.do(job)

    try:
        while True:
            _schedule.run_pending()
            time.sleep(60)
    except KeyboardInterrupt:
        logger.info("Scheduler stopped by user.")
        sys.exit(0)


if __name__ == "__main__":
    main()
