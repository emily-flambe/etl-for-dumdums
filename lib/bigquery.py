"""
Shared BigQuery utilities for ETL workflows.

Provides common functionality for authenticating to BigQuery and
managing datasets/tables across all sync scripts.
"""

import base64
import json
import logging
import os
from typing import Any

from google.cloud import bigquery

logger = logging.getLogger(__name__)

# Default dataset for raw data ingestion
DEFAULT_DATASET = "raw_data"
DEFAULT_LOCATION = "US"


def get_client() -> bigquery.Client:
    """
    Create a BigQuery client from environment variables.

    Expects:
        GCP_SA_KEY: Base64-encoded service account JSON
        GCP_PROJECT_ID: Target GCP project ID

    Returns:
        Authenticated BigQuery client

    Raises:
        ValueError: If required environment variables are not set
    """
    gcp_sa_key_b64 = os.environ.get("GCP_SA_KEY")
    project_id = os.environ.get("GCP_PROJECT_ID")

    if not gcp_sa_key_b64:
        raise ValueError("GCP_SA_KEY environment variable is not set")
    if not project_id:
        raise ValueError("GCP_PROJECT_ID environment variable is not set")

    sa_key_json = base64.b64decode(gcp_sa_key_b64).decode("utf-8")
    sa_info = json.loads(sa_key_json)

    return bigquery.Client.from_service_account_info(sa_info, project=project_id)


def ensure_dataset_exists(
    client: bigquery.Client,
    dataset_id: str = DEFAULT_DATASET,
    location: str = DEFAULT_LOCATION,
) -> bigquery.Dataset:
    """
    Ensure a dataset exists, creating it if necessary.

    Args:
        client: Authenticated BigQuery client
        dataset_id: Dataset name (default: raw_data)
        location: Dataset location (default: US)

    Returns:
        The existing or newly created Dataset
    """
    dataset_ref = client.dataset(dataset_id)

    try:
        dataset = client.get_dataset(dataset_ref)
        logger.debug(f"Dataset {dataset_id} already exists")
        return dataset
    except Exception:
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = location
        dataset = client.create_dataset(dataset)
        logger.info(f"Created dataset {dataset_id} in {location}")
        return dataset


def load_table(
    client: bigquery.Client,
    table_id: str,
    rows: list[dict[str, Any]],
    schema: list[bigquery.SchemaField],
    dataset_id: str = DEFAULT_DATASET,
    write_disposition: str = "WRITE_TRUNCATE",
) -> bigquery.LoadJob:
    """
    Load data into a BigQuery table.

    Creates the dataset and table if they don't exist.

    Args:
        client: Authenticated BigQuery client
        table_id: Target table name
        rows: List of row dictionaries to load
        schema: Table schema definition
        dataset_id: Dataset name (default: raw_data)
        write_disposition: WRITE_TRUNCATE, WRITE_APPEND, or WRITE_EMPTY

    Returns:
        Completed LoadJob
    """
    project_id = os.environ.get("GCP_PROJECT_ID")
    table_ref = f"{project_id}.{dataset_id}.{table_id}"

    # Ensure dataset exists
    ensure_dataset_exists(client, dataset_id)

    # Configure and run load job
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition=getattr(bigquery.WriteDisposition, write_disposition),
    )

    logger.info(f"Loading {len(rows)} rows to {table_ref}...")

    job = client.load_table_from_json(rows, table_ref, job_config=job_config)
    job.result()  # Wait for completion

    logger.info(f"Successfully loaded {len(rows)} rows to {table_ref}")
    return job
