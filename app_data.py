"""
Shared data loading functions for Streamlit app.

This module provides cached BigQuery client and data loading functions
that can be shared across all pages.
"""

import os

import streamlit as st
from dotenv import load_dotenv
from google.cloud import bigquery

load_dotenv()


@st.cache_resource
def get_client():
    """Create BigQuery client from service account file."""
    return bigquery.Client.from_service_account_json(
        os.environ["GCP_SA_KEY_FILE"],
        project=os.environ["GCP_PROJECT_ID"],
    )


@st.cache_data(ttl=300)  # Cache for 5 minutes
def load_issues():
    """Load issues from BigQuery."""
    client = get_client()
    query = """
    SELECT
        identifier,
        title,
        state,
        estimate,
        assignee_name,
        cycle_name,
        cycle_starts_at,
        labels,
        project_name,
        days_since_created,
        created_at,
        updated_at,
        parent_identifier,
        parent_title,
        is_parent,
        is_child,
        child_count
    FROM linear.fct_issues
    ORDER BY updated_at DESC
    """
    return client.query(query).to_dataframe()


@st.cache_data(ttl=300)  # Cache for 5 minutes
def load_pull_requests():
    """Load pull requests from BigQuery."""
    client = get_client()
    query = """
    SELECT *
    FROM linear.fct_pull_requests
    ORDER BY created_at DESC
    """
    return client.query(query).to_dataframe()


@st.cache_data(ttl=300)  # Cache for 5 minutes
def load_oura_daily():
    """Load daily Oura wellness data from BigQuery."""
    client = get_client()
    query = """
    SELECT *
    FROM linear.fct_oura_daily
    ORDER BY day DESC
    """
    return client.query(query).to_dataframe()


@st.cache_data(ttl=300)  # Cache for 5 minutes
def load_reviewer_activity():
    """Load reviewer activity metrics from BigQuery."""
    client = get_client()
    query = """
    SELECT *
    FROM github.fct_reviewer_activity
    ORDER BY pr_created_at DESC
    """
    return client.query(query).to_dataframe()
