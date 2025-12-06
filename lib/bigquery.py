"""
Shared BigQuery utilities for ETL workflows.

Provides common functionality for authenticating to BigQuery and
managing datasets/tables across all sync scripts.
"""

import base64
import json
import logging
import os
import uuid
from typing import Any

from google.cloud import bigquery
from google.cloud.exceptions import NotFound

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


def merge_table(
    client: bigquery.Client,
    table_id: str,
    rows: list[dict[str, Any]],
    schema: list[bigquery.SchemaField],
    primary_key: str,
    dataset_id: str = DEFAULT_DATASET,
) -> None:
    """
    Incrementally merge data into a BigQuery table (upsert).

    Inserts new rows and updates existing rows based on primary key.
    Use this instead of load_table() when you need to preserve historical
    data that may no longer be available from the source API.

    Args:
        client: Authenticated BigQuery client
        table_id: Target table name
        rows: List of row dictionaries to merge
        schema: Table schema definition
        primary_key: Column name to match on (e.g., "id")
        dataset_id: Dataset name (default: raw_data)
    """
    if not rows:
        logger.info("No rows to merge")
        return

    project_id = os.environ.get("GCP_PROJECT_ID")
    table_ref = f"{project_id}.{dataset_id}.{table_id}"
    temp_table_id = f"{table_id}_temp_{uuid.uuid4().hex[:8]}"
    temp_table_ref = f"{project_id}.{dataset_id}.{temp_table_id}"

    # Ensure dataset exists
    ensure_dataset_exists(client, dataset_id)

    # Check if target table exists; if not, just do a regular load
    try:
        client.get_table(table_ref)
    except NotFound:
        logger.info(f"Table {table_ref} does not exist, creating with initial load")
        load_table(client, table_id, rows, schema, dataset_id, "WRITE_TRUNCATE")
        return

    # Load data to temp table
    logger.info(f"Loading {len(rows)} rows to temp table {temp_table_ref}...")
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    job = client.load_table_from_json(rows, temp_table_ref, job_config=job_config)
    job.result()

    try:
        # Build column lists for MERGE statement
        columns = [field.name for field in schema]
        update_cols = [c for c in columns if c != primary_key]

        update_clause = ", ".join([f"T.{c} = S.{c}" for c in update_cols])
        insert_cols = ", ".join(columns)
        insert_vals = ", ".join([f"S.{c}" for c in columns])

        merge_sql = f"""
        MERGE `{table_ref}` T
        USING `{temp_table_ref}` S
        ON T.{primary_key} = S.{primary_key}
        WHEN MATCHED THEN
            UPDATE SET {update_clause}
        WHEN NOT MATCHED THEN
            INSERT ({insert_cols})
            VALUES ({insert_vals})
        """

        logger.info(f"Merging into {table_ref}...")
        query_job = client.query(merge_sql)
        query_job.result()
        logger.info(f"Merged {len(rows)} rows into {table_ref}")
    finally:
        # Always clean up temp table, even on failure
        try:
            client.delete_table(temp_table_ref)
            logger.debug(f"Cleaned up temp table {temp_table_ref}")
        except Exception:
            logger.warning(f"Failed to clean up temp table {temp_table_ref}")
