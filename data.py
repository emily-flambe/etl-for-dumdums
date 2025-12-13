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
        cycle_ends_at,
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
    FROM github.fct_pull_requests
    ORDER BY created_at DESC
    """
    return client.query(query).to_dataframe()


@st.cache_data(ttl=300)  # Cache for 5 minutes
def load_oura_daily():
    """Load daily Oura wellness data from BigQuery."""
    client = get_client()
    query = """
    SELECT *
    FROM oura.fct_oura_daily
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


@st.cache_data(ttl=300)  # Cache for 5 minutes
def load_hn_weekly_stats():
    """Load Hacker News weekly statistics from BigQuery."""
    client = get_client()
    query = """
    SELECT *
    FROM hacker_news.fct_hn_weekly_stats
    ORDER BY week DESC
    """
    return client.query(query).to_dataframe()


@st.cache_data(ttl=300)  # Cache for 5 minutes
def load_hn_domain_stats():
    """Load Hacker News domain statistics from BigQuery."""
    client = get_client()
    query = """
    SELECT *
    FROM hacker_news.fct_hn_domain_stats
    ORDER BY week DESC, story_count DESC
    """
    return client.query(query).to_dataframe()


@st.cache_data(ttl=300)  # Cache for 5 minutes
def load_hn_keyword_trends():
    """Load Hacker News keyword trends from BigQuery."""
    client = get_client()
    query = """
    SELECT *
    FROM hacker_news.fct_hn_keyword_trends
    ORDER BY week DESC, mention_count DESC
    """
    return client.query(query).to_dataframe()


@st.cache_data(ttl=300)  # Cache for 5 minutes
def load_keyword_trends():
    """Load Google Trends keyword interest data from BigQuery."""
    client = get_client()
    query = """
    SELECT *
    FROM trends.fct_keyword_trends
    ORDER BY date DESC
    """
    return client.query(query).to_dataframe()


@st.cache_data(ttl=300)  # Cache for 5 minutes
def load_hn_keyword_sentiment():
    """Load Hacker News keyword sentiment trends from BigQuery."""
    client = get_client()
    query = """
    SELECT *
    FROM hacker_news.fct_hn_keyword_sentiment
    ORDER BY day DESC, comment_count DESC
    """
    return client.query(query).to_dataframe()


@st.cache_data(ttl=300)  # Cache for 5 minutes
def load_fda_recalls_by_state():
    """Load FDA food recalls aggregated by state from BigQuery."""
    client = get_client()
    query = """
    SELECT *
    FROM fda_food.fct_fda_recalls_by_state
    ORDER BY total_recalls DESC
    """
    return client.query(query).to_dataframe()


@st.cache_data(ttl=300)  # Cache for 5 minutes
def load_fda_recalls_raw():
    """Load raw FDA food recalls data from BigQuery."""
    client = get_client()
    query = """
    SELECT *
    FROM fda_food.stg_fda__recalls
    ORDER BY recall_initiation_date DESC
    """
    return client.query(query).to_dataframe()


@st.cache_data(ttl=300)  # Cache for 5 minutes
def load_fda_recalls_by_topic():
    """Load FDA food recalls aggregated by topic from BigQuery."""
    client = get_client()
    query = """
    SELECT *
    FROM fda_food.fct_fda_recalls_by_topic
    ORDER BY recall_count DESC
    """
    return client.query(query).to_dataframe()


@st.cache_data(ttl=300)  # Cache for 5 minutes
def load_fda_recall_topics():
    """Load FDA food recalls with topic tags from BigQuery."""
    client = get_client()
    query = """
    SELECT *
    FROM fda_food.int_fda__recall_topics
    ORDER BY recall_initiation_date DESC
    """
    return client.query(query).to_dataframe()
