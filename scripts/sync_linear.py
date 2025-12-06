#!/usr/bin/env python3
"""Sync Linear data to BigQuery."""

import logging
import os
import sys

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Load .env for local development (no-op if not present)
from dotenv import load_dotenv
load_dotenv()

from lib.source import run_sync
from sources.linear import LinearCyclesSource, LinearIssuesSource

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

if __name__ == "__main__":
    # Sync cycles first (dimension table)
    run_sync(LinearCyclesSource())

    # Then sync issues (fact table with cycle_id FK)
    run_sync(LinearIssuesSource(lookback_days=7))
