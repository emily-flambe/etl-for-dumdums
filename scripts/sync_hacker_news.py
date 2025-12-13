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
    FULL_COMMENTS_LOOKBACK_DAYS,
    DEFAULT_TOP_STORIES_PER_DAY,
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
    parser.add_argument(
        "--top-stories",
        type=int,
        default=DEFAULT_TOP_STORIES_PER_DAY,
        help=f"Top N stories by activity per day to fetch comments from (default: {DEFAULT_TOP_STORIES_PER_DAY})",
    )
    args = parser.parse_args()

    # Determine lookback based on full flag
    stories_lookback = FULL_LOOKBACK_DAYS if args.full else DEFAULT_LOOKBACK_DAYS
    comments_lookback = FULL_COMMENTS_LOOKBACK_DAYS if args.full else DEFAULT_COMMENTS_LOOKBACK_DAYS

    # Sync Hacker News stories
    print(f"Syncing Hacker News stories (lookback: {stories_lookback} days)...")
    run_sync(HNStoriesSource(lookback_days=stories_lookback))

    # Sync Hacker News comments from top stories by activity
    # Limited to top N stories per day to stay within Cloudflare AI rate limits
    print(f"Syncing HN comments from top {args.top_stories} stories/day (lookback: {comments_lookback} days)...")
    run_sync(HNCommentsSource(lookback_days=comments_lookback, top_stories_per_day=args.top_stories))

    print("Hacker News sync complete!")
