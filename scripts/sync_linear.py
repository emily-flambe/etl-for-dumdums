#!/usr/bin/env python3
"""Sync Linear data to BigQuery."""

import argparse
import logging

from dotenv import load_dotenv

from lib.source import run_sync
from sources.linear import (
    LinearCyclesSource,
    LinearIssuesSource,
    LinearUsersSource,
)

# Load .env for local development (no-op if not present)
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Sync Linear data to BigQuery")
    parser.add_argument(
        "--full",
        action="store_true",
        help="Full sync (all issues, not just recent)",
    )
    args = parser.parse_args()

    # Sync dimension tables first
    run_sync(LinearUsersSource())
    run_sync(LinearCyclesSource())

    # Then sync fact table (issues with FKs to users and cycles)
    if args.full:
        run_sync(LinearIssuesSource(full_sync=True))
    else:
        run_sync(LinearIssuesSource(lookback_days=7))
