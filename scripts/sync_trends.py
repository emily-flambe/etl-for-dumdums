#!/usr/bin/env python3
"""Sync Google Trends data to BigQuery."""

import argparse
import logging

from dotenv import load_dotenv

from lib.source import run_sync
from sources.trends import GoogleTrendsSource

# Load .env for local development (no-op if not present)
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Sync Google Trends data to BigQuery")
    parser.add_argument(
        "--timeframe",
        default="today 3-m",
        help="Pytrends timeframe (default: 'today 3-m' for last 3 months)",
    )
    parser.add_argument(
        "--geo",
        default="US",
        help="Two-letter country code or empty for worldwide (default: 'US')",
    )
    args = parser.parse_args()

    print(f"Syncing Google Trends data (timeframe: {args.timeframe}, geo: {args.geo})...")
    run_sync(GoogleTrendsSource(timeframe=args.timeframe, geo=args.geo))
    print("Google Trends sync complete!")
