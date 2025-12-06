"""
Base source abstraction for ETL workflows.

Provides a simple pattern for implementing data sources that sync to BigQuery.
"""

import logging
from abc import ABC, abstractmethod
from typing import Any

from google.cloud import bigquery

from lib import bigquery as bq

logger = logging.getLogger(__name__)


class Source(ABC):
    """
    Base class for data sources.

    Subclasses must define:
        - table_id: Target BigQuery table name
        - primary_key: Column used for merge/upsert matching
        - schema: List of BigQuery SchemaField definitions
        - fetch(): Retrieve raw data from the source API
        - transform(): Convert raw data to BigQuery row format
    """

    table_id: str
    primary_key: str
    schema: list[bigquery.SchemaField]

    @abstractmethod
    def fetch(self) -> list[dict[str, Any]]:
        """Fetch raw data from the source API."""
        pass

    @abstractmethod
    def transform(self, raw_data: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Transform raw API data to BigQuery row format."""
        pass


def run_sync(source: Source, full_refresh: bool = False) -> None:
    """
    Run a sync for the given source.

    Args:
        source: Source instance to sync
        full_refresh: If True, use WRITE_TRUNCATE instead of merge
    """
    logger.info(f"Starting sync for {source.__class__.__name__}...")

    # Fetch and transform
    raw_data = source.fetch()

    if not raw_data:
        logger.info("No data returned from source")
        return

    rows = source.transform(raw_data)
    logger.info(f"Transformed {len(rows)} rows")

    # Load to BigQuery
    client = bq.get_client()

    if full_refresh:
        bq.load_table(client, source.table_id, rows, source.schema)
    else:
        bq.merge_table(client, source.table_id, rows, source.schema, source.primary_key)

    logger.info(f"Sync complete for {source.__class__.__name__}")
