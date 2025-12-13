#!/usr/bin/env python3
"""Sync Iowa Liquor Sales data to BigQuery."""

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
from sources.iowa_liquor import (
    IowaLiquorSalesSource,
    DEFAULT_LOOKBACK_DAYS,
    FULL_LOOKBACK_DAYS,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Sync Iowa Liquor Sales data to BigQuery")
    parser.add_argument(
        "--full",
        action="store_true",
        help=f"Full sync ({FULL_LOOKBACK_DAYS} days, not just recent {DEFAULT_LOOKBACK_DAYS} days)",
    )
    args = parser.parse_args()

    lookback = FULL_LOOKBACK_DAYS if args.full else DEFAULT_LOOKBACK_DAYS

    print(f"Syncing Iowa Liquor Sales (lookback: {lookback} days)...")
    run_sync(IowaLiquorSalesSource(lookback_days=lookback))

    print("Iowa Liquor Sales sync complete!")
