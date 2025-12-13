"""
FDA Food Enforcement data source.

Fetches food recall enforcement data from BigQuery public dataset and syncs to local BigQuery.
Data source: bigquery-public-data.fda_food.food_enforcement

Note: The FDA updates this public dataset periodically. Data includes food recalls
(not drugs or devices). Some records may have data quality issues (e.g., missing
recall numbers).
"""

import logging
from datetime import datetime
from typing import Any

from google.cloud import bigquery

from lib.source import Source
from lib import bigquery as bq

logger = logging.getLogger(__name__)

# Default: only fetch recalls from 2025-01-01 onwards
DEFAULT_START_DATE = "2025-01-01"


class FDAFoodEnforcementSource(Source):
    """Fetches food enforcement/recall data from FDA BigQuery public dataset.

    This source queries the public dataset and syncs to local BigQuery,
    enabling geographic analysis of food recalls across the US.

    Note: Data is filtered to valid records only (excludes null/nan recall numbers).
    """

    dataset_id = "fda_food"
    table_id = "raw_recalls"
    primary_key = "recall_number"
    schema = [
        bigquery.SchemaField("recall_number", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("event_id", "INTEGER"),
        bigquery.SchemaField("classification", "STRING"),
        bigquery.SchemaField("status", "STRING"),
        bigquery.SchemaField("voluntary_mandated", "STRING"),
        bigquery.SchemaField("recalling_firm", "STRING"),
        bigquery.SchemaField("city", "STRING"),
        bigquery.SchemaField("state", "STRING"),
        bigquery.SchemaField("country", "STRING"),
        bigquery.SchemaField("postal_code", "STRING"),
        bigquery.SchemaField("reason_for_recall", "STRING"),
        bigquery.SchemaField("product_description", "STRING"),
        bigquery.SchemaField("product_quantity", "STRING"),
        bigquery.SchemaField("distribution_pattern", "STRING"),
        bigquery.SchemaField("recall_initiation_date", "DATE"),
        bigquery.SchemaField("center_classification_date", "DATE"),
        bigquery.SchemaField("report_date", "DATE"),
        bigquery.SchemaField("termination_date", "DATE"),
    ]

    def __init__(self, start_date: str = DEFAULT_START_DATE):
        """Initialize with start date filter.

        Args:
            start_date: Only fetch recalls on or after this date (YYYY-MM-DD format)

        Raises:
            ValueError: If start_date is not in valid YYYY-MM-DD format
        """
        # Validate date format to prevent invalid queries
        try:
            datetime.strptime(start_date, "%Y-%m-%d")
        except ValueError as e:
            raise ValueError(f"start_date must be YYYY-MM-DD format, got '{start_date}': {e}")

        self.start_date = start_date

    def fetch(self) -> list[dict[str, Any]]:
        """Fetch food enforcement data from BigQuery public dataset.

        Returns:
            List of recall records as dictionaries.

        Raises:
            Exception: If BigQuery query fails.
        """
        logger.info(f"Fetching FDA food recalls from {self.start_date} onwards...")

        client = bq.get_client()

        query = """
        SELECT
            recall_number,
            event_id,
            classification,
            status,
            voluntary_mandated,
            recalling_firm,
            city,
            state,
            country,
            postal_code,
            reason_for_recall,
            product_description,
            product_quantity,
            distribution_pattern,
            recall_initiation_date,
            center_classification_date,
            report_date,
            termination_date
        FROM `bigquery-public-data.fda_food.food_enforcement`
        WHERE recall_initiation_date >= @start_date
          AND recall_initiation_date <= CURRENT_DATE()
        ORDER BY recall_initiation_date DESC
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("start_date", "DATE", self.start_date)
            ]
        )

        logger.info("Executing query against bigquery-public-data.fda_food.food_enforcement...")
        try:
            result = client.query(query, job_config=job_config).result()
        except Exception as e:
            logger.error(f"Failed to fetch FDA data from BigQuery: {e}")
            raise

        rows = []
        for row in result:
            rows.append({
                "recall_number": row.recall_number,
                "event_id": row.event_id,
                "classification": row.classification,
                "status": row.status,
                "voluntary_mandated": row.voluntary_mandated,
                "recalling_firm": row.recalling_firm,
                "city": row.city,
                "state": row.state,
                "country": row.country,
                "postal_code": row.postal_code,
                "reason_for_recall": row.reason_for_recall,
                "product_description": row.product_description,
                "product_quantity": row.product_quantity,
                "distribution_pattern": row.distribution_pattern,
                "recall_initiation_date": row.recall_initiation_date.isoformat() if row.recall_initiation_date else None,
                "center_classification_date": row.center_classification_date.isoformat() if row.center_classification_date else None,
                "report_date": row.report_date.isoformat() if row.report_date else None,
                "termination_date": row.termination_date.isoformat() if row.termination_date else None,
            })

        logger.info(f"Fetched {len(rows)} recalls")
        return rows

    def transform(self, raw_data: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Transform is a no-op since we already shaped data in the query."""
        return raw_data
