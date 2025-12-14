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


@st.cache_data(ttl=300)  # Cache for 5 minutes
def load_iowa_liquor_monthly():
    """Load Iowa liquor sales by month and category from BigQuery."""
    client = get_client()
    query = """
    SELECT *
    FROM iowa_liquor.fct_sales_monthly
    ORDER BY sale_month DESC, total_sales DESC
    """
    return client.query(query).to_dataframe()


@st.cache_data(ttl=300)  # Cache for 5 minutes
def load_iowa_liquor_by_county():
    """Load Iowa liquor sales by county from BigQuery."""
    client = get_client()
    query = """
    SELECT *
    FROM iowa_liquor.fct_sales_by_county
    ORDER BY total_sales DESC
    """
    return client.query(query).to_dataframe()


@st.cache_data(ttl=300)  # Cache for 5 minutes
def load_iowa_liquor_vendors():
    """Load top Iowa liquor vendors from BigQuery."""
    client = get_client()
    query = """
    SELECT *
    FROM iowa_liquor.fct_top_vendors
    ORDER BY total_sales DESC
    """
    return client.query(query).to_dataframe()


@st.cache_data(ttl=300)  # Cache for 5 minutes
def load_fda_events_by_reaction():
    """Load FDA food adverse events aggregated by reaction category."""
    client = get_client()
    query = """
    SELECT *
    FROM fda_food.fct_fda_events_by_reaction
    ORDER BY event_count DESC
    """
    return client.query(query).to_dataframe()


@st.cache_data(ttl=300)  # Cache for 5 minutes
def load_fda_events_by_product():
    """Load FDA food adverse events aggregated by product industry."""
    client = get_client()
    query = """
    SELECT *
    FROM fda_food.fct_fda_events_by_product
    ORDER BY event_count DESC
    """
    return client.query(query).to_dataframe()


@st.cache_data(ttl=300)  # Cache for 5 minutes
def load_fda_events_monthly():
    """Load FDA food adverse events monthly trends."""
    client = get_client()
    query = """
    SELECT *
    FROM fda_food.fct_fda_events_monthly
    ORDER BY month DESC
    """
    return client.query(query).to_dataframe()


@st.cache_data(ttl=300)  # Cache for 5 minutes
def load_fda_event_reactions():
    """Load FDA food events with reaction categorization."""
    client = get_client()
    query = """
    SELECT *
    FROM fda_food.int_fda__food_event_reactions
    ORDER BY event_date DESC
    """
    return client.query(query).to_dataframe()


@st.cache_data(ttl=300)  # Cache for 5 minutes
def load_fda_events_monthly_by_industry():
    """Load FDA food adverse events monthly trends by product industry."""
    client = get_client()
    query = """
    SELECT
        event_month_start as month,
        EXTRACT(YEAR FROM event_month_start) as year,
        industry_name,
        COUNT(DISTINCT report_number) as event_count,
        COUNTIF(has_gastrointestinal) as gastrointestinal_count,
        COUNTIF(has_allergic) as allergic_count,
        COUNTIF(has_respiratory) as respiratory_count,
        COUNT(DISTINCT CASE WHEN REGEXP_CONTAINS(outcomes, r'Hospitalization') THEN report_number END) as hospitalization_count
    FROM fda_food.int_fda__food_event_reactions
    WHERE industry_name IS NOT NULL
      AND event_month_start IS NOT NULL
    GROUP BY event_month_start, industry_name
    ORDER BY event_month_start DESC, event_count DESC
    """
    return client.query(query).to_dataframe()


@st.cache_data(ttl=300)  # Cache for 5 minutes
def load_fda_events_by_gender():
    """Load FDA food adverse events aggregated by gender."""
    client = get_client()
    query = """
    SELECT *
    FROM fda_food.fct_fda_events_by_gender
    ORDER BY event_count DESC
    """
    return client.query(query).to_dataframe()


@st.cache_data(ttl=300)  # Cache for 5 minutes
def load_fda_events_monthly_by_gender():
    """Load FDA food adverse events monthly trends by gender."""
    client = get_client()
    query = """
    SELECT
        event_month_start as month,
        EXTRACT(YEAR FROM event_month_start) as year,
        CASE
            WHEN UPPER(gender) IN ('F', 'FEMALE') THEN 'Female'
            WHEN UPPER(gender) IN ('M', 'MALE') THEN 'Male'
            WHEN gender IS NULL OR TRIM(gender) = '' THEN 'Not Reported'
            ELSE 'Other'
        END as gender,
        COUNT(DISTINCT report_number) as event_count,
        COUNTIF(has_gastrointestinal) as gastrointestinal_count,
        COUNTIF(has_allergic) as allergic_count,
        COUNTIF(has_respiratory) as respiratory_count,
        COUNTIF(has_cardiovascular) as cardiovascular_count,
        COUNTIF(has_neurological) as neurological_count,
        COUNTIF(has_systemic) as systemic_count,
        COUNT(DISTINCT CASE WHEN REGEXP_CONTAINS(outcomes, r'Hospitalization') THEN report_number END) as hospitalization_count
    FROM fda_food.int_fda__food_event_reactions
    WHERE event_month_start IS NOT NULL
      AND UPPER(product_role) = 'SUSPECT'
    GROUP BY event_month_start, gender
    ORDER BY event_month_start DESC, event_count DESC
    """
    return client.query(query).to_dataframe()
