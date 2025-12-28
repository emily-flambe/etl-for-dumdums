#!/usr/bin/env python3
"""Sync Oura data to BigQuery."""

import argparse
import logging

from dotenv import load_dotenv

from lib.source import run_sync
from sources.oura import (
    OuraActivitySource,
    OuraReadinessSource,
    OuraSleepSessionSource,
    OuraSleepSource,
)

# Load .env for local development (no-op if not present)
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Sync Oura data to BigQuery")
    parser.add_argument(
        "--full",
        action="store_true",
        help="Full sync (all historical data, not just recent 7 days)",
    )
    args = parser.parse_args()

    # Determine lookback based on full flag
    # None = no limit, fetch all historical data
    lookback_days = None if args.full else 7

    # Sync all Oura sources
    lookback_msg = f"{lookback_days} days" if lookback_days else "all history"
    print(f"Syncing Oura data ({lookback_msg})...")
    run_sync(OuraSleepSource(lookback_days=lookback_days))
    run_sync(OuraSleepSessionSource(lookback_days=lookback_days))
    run_sync(OuraReadinessSource(lookback_days=lookback_days))
    run_sync(OuraActivitySource(lookback_days=lookback_days))
    print("Oura sync complete!")
