#!/usr/bin/env python3
"""Sync Oura data to BigQuery."""

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
from sources.oura import OuraSleepSource, OuraReadinessSource, OuraActivitySource

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Sync Oura data to BigQuery")
    parser.add_argument(
        "--full",
        action="store_true",
        help="Full sync (365 days, not just recent 7 days)",
    )
    args = parser.parse_args()

    # Determine lookback based on full flag
    lookback_days = 365 if args.full else 7

    # Sync all Oura sources
    print(f"Syncing Oura data (lookback: {lookback_days} days)...")
    run_sync(OuraSleepSource(lookback_days=lookback_days))
    run_sync(OuraReadinessSource(lookback_days=lookback_days))
    run_sync(OuraActivitySource(lookback_days=lookback_days))
    print("Oura sync complete!")
