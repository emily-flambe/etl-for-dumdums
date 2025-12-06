#!/usr/bin/env python3
"""Sync Linear issues to BigQuery."""

import logging
import os
import sys

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from lib.source import run_sync
from sources.linear import LinearIssuesSource

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

if __name__ == "__main__":
    source = LinearIssuesSource(lookback_days=7)
    run_sync(source)
