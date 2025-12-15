#!/usr/bin/env python3
"""Sync stock price data to BigQuery."""

import argparse
import logging

from dotenv import load_dotenv

from lib.source import run_sync
from sources.stocks import StockPricesSource

# Load .env for local development (no-op if not present)
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Sync stock prices to BigQuery")
    parser.add_argument(
        "--full",
        action="store_true",
        help="Full sync (5 years of history, not just 30 days)",
    )
    args = parser.parse_args()

    # Determine lookback based on full flag
    # 5 years = ~1825 days, use 1900 to be safe
    lookback_days = 1900 if args.full else 30

    print(f"Syncing stock prices (lookback: {lookback_days} days)...")
    run_sync(StockPricesSource(lookback_days=lookback_days))
    print("Stock sync complete!")
