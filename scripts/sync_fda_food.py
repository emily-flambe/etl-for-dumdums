#!/usr/bin/env python3
"""Sync FDA Food Enforcement data to BigQuery."""

import argparse
import logging
import os
import sys

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Load .env for local development (no-op if not present)
from dotenv import load_dotenv
load_dotenv()

from lib.source import run_sync
from sources.fda_food import (
    FDAFoodEnforcementSource,
    DEFAULT_START_DATE,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

# Full sync goes back to 2012 (dataset start)
FULL_START_DATE = "2012-01-01"

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Sync FDA Food Enforcement data to BigQuery")
    parser.add_argument(
        "--full",
        action="store_true",
        help=f"Full sync (from {FULL_START_DATE}, not just {DEFAULT_START_DATE})",
    )
    parser.add_argument(
        "--start-date",
        type=str,
        default=None,
        help=f"Custom start date (YYYY-MM-DD format, default: {DEFAULT_START_DATE})",
    )
    args = parser.parse_args()

    # Determine start date
    if args.start_date:
        start_date = args.start_date
    elif args.full:
        start_date = FULL_START_DATE
    else:
        start_date = DEFAULT_START_DATE

    # Sync FDA food enforcement recalls
    print(f"Syncing FDA food enforcement recalls (from {start_date})...")
    run_sync(FDAFoodEnforcementSource(start_date=start_date))

    print("FDA food enforcement sync complete!")
