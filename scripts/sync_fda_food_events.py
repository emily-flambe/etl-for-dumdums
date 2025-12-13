#!/usr/bin/env python3
"""
Sync FDA food adverse events to BigQuery.

Usage:
    python scripts/sync_fda_food_events.py           # Incremental (90 days)
    python scripts/sync_fda_food_events.py --full    # Full sync (10 years)
"""

import argparse
import logging
import os
import sys

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv

load_dotenv()

from lib.source import run_sync
from sources.fda_food_events import (
    FDAFoodEventsSource,
    DEFAULT_LOOKBACK_DAYS,
    FULL_LOOKBACK_DAYS,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Sync FDA food adverse events to BigQuery"
    )
    parser.add_argument(
        "--full",
        action="store_true",
        help=f"Full sync ({FULL_LOOKBACK_DAYS} days) instead of incremental ({DEFAULT_LOOKBACK_DAYS} days)",
    )
    args = parser.parse_args()

    lookback_days = FULL_LOOKBACK_DAYS if args.full else DEFAULT_LOOKBACK_DAYS
    logger.info(f"Starting FDA food events sync (lookback: {lookback_days} days)...")

    run_sync(FDAFoodEventsSource(lookback_days=lookback_days))

    logger.info("FDA food events sync complete!")
