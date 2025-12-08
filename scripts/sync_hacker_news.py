#!/usr/bin/env python3
"""Sync Hacker News data to BigQuery."""

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
from sources.hacker_news import (
    HNStoriesSource,
    HNCommentsSource,
    DEFAULT_LOOKBACK_DAYS,
    DEFAULT_COMMENTS_LOOKBACK_DAYS,
    FULL_LOOKBACK_DAYS,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Sync Hacker News data to BigQuery")
    parser.add_argument(
        "--full",
        action="store_true",
        help=f"Full sync ({FULL_LOOKBACK_DAYS} days, not just recent {DEFAULT_LOOKBACK_DAYS} days)",
    )
    args = parser.parse_args()

    # Determine lookback based on full flag
    stories_lookback = FULL_LOOKBACK_DAYS if args.full else DEFAULT_LOOKBACK_DAYS
    comments_lookback = FULL_LOOKBACK_DAYS if args.full else DEFAULT_COMMENTS_LOOKBACK_DAYS

    # Sync Hacker News stories
    print(f"Syncing Hacker News stories (lookback: {stories_lookback} days)...")
    run_sync(HNStoriesSource(lookback_days=stories_lookback))

    # Sync Hacker News comments (top-level only, for sentiment analysis)
    # Comments use shorter lookback due to Cloudflare AI rate limits
    print(f"Syncing Hacker News comments (lookback: {comments_lookback} days)...")
    run_sync(HNCommentsSource(lookback_days=comments_lookback))

    print("Hacker News sync complete!")
