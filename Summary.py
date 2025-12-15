"""
Analytics Dashboard - Summary

Multi-page Streamlit app for exploring data from Linear, GitHub, Oura, and more.

Run with: make app
"""

import os

import streamlit as st

from data import (
    load_issues,
    load_pull_requests,
    load_oura_daily,
    load_hn_weekly_stats,
    load_keyword_trends,
    load_hn_keyword_sentiment,
    load_fda_recalls_raw,
    load_iowa_liquor_monthly,
    load_fda_events_monthly,
    load_stock_prices,
    load_sector_performance,
)

# Check deployment mode
DEPLOYMENT_MODE = os.environ.get("DEPLOYMENT_MODE", "local")
IS_PUBLIC = DEPLOYMENT_MODE == "public"

st.title("Summary")

st.markdown("Use the sidebar to navigate between pages.")

# Data freshness section
st.subheader("Data Sources")

# Linear (private - hide in public mode)
if not IS_PUBLIC:
    st.markdown("#### Linear")
    try:
        issues = load_issues()
        latest_update = issues["updated_at"].max()
        done_states = ["Done", "Done Pending Deployment"]
        open_count = len(issues[~issues["state"].isin(done_states)])
        col1, col2, col3 = st.columns(3)
        col1.metric("Last Sync", latest_update.strftime("%Y-%m-%d %H:%M") if latest_update else "N/A")
        col2.metric("Total Issues", len(issues))
        col3.metric("Open Issues", open_count)
    except Exception as e:
        st.warning(f"Could not load Linear data: {e}")

    st.divider()

# GitHub (private - hide in public mode)
if not IS_PUBLIC:
    st.markdown("#### GitHub")
    try:
        prs = load_pull_requests()
        latest_update = prs["updated_at"].max()
        open_count = len(prs[prs["state"] == "open"])
        col1, col2, col3 = st.columns(3)
        col1.metric("Last Sync", latest_update.strftime("%Y-%m-%d %H:%M") if latest_update else "N/A")
        col2.metric("Total PRs", len(prs))
        col3.metric("Open PRs", open_count)
    except Exception as e:
        st.warning(f"Could not load GitHub data: {e}")

    st.divider()

# Oura (public)
st.markdown("#### Oura")
try:
    oura = load_oura_daily()
    latest_day = oura["day"].max()
    avg_wellness = oura["combined_wellness_score"].mean()
    col1, col2, col3 = st.columns(3)
    col1.metric("Latest Day", str(latest_day) if latest_day else "N/A")
    col2.metric("Total Days", len(oura))
    col3.metric("Avg Wellness", f"{avg_wellness:.0f}" if avg_wellness else "N/A")
except Exception as e:
    st.warning(f"Could not load Oura data: {e}")

st.divider()

# Hacker News (public)
st.markdown("#### Hacker News")
try:
    hn = load_hn_weekly_stats()
    latest_week = hn["week"].max()
    total_stories = hn["story_count"].sum()
    avg_score = hn["avg_score"].mean()
    col1, col2, col3 = st.columns(3)
    col1.metric("Latest Week", str(latest_week)[:10] if latest_week else "N/A")
    col2.metric("Total Stories", f"{total_stories:,}")
    col3.metric("Avg Score", f"{avg_score:.1f}" if avg_score else "N/A")
except Exception as e:
    st.warning(f"Could not load Hacker News data: {e}")

st.divider()

# HN Sentiment (public)
st.markdown("#### HN Sentiment")
try:
    hn_sentiment = load_hn_keyword_sentiment()
    latest_day = hn_sentiment["day"].max()
    total_comments = hn_sentiment["comment_count"].sum()
    keywords_tracked = hn_sentiment["keyword"].nunique()
    col1, col2, col3 = st.columns(3)
    col1.metric("Latest Day", str(latest_day)[:10] if latest_day else "N/A")
    col2.metric("Comments Analyzed", f"{total_comments:,}")
    col3.metric("Keywords Tracked", keywords_tracked)
except Exception as e:
    st.warning(f"Could not load HN Sentiment data: {e}")

st.divider()

# Google Trends (public)
st.markdown("#### Google Trends")
try:
    trends = load_keyword_trends()
    latest_date = trends["date"].max()
    keywords = trends["keyword"].nunique()
    avg_interest = trends[trends["recency_rank"] == 1]["interest"].mean()
    col1, col2, col3 = st.columns(3)
    col1.metric("Latest Date", str(latest_date)[:10] if latest_date else "N/A")
    col2.metric("Keywords Tracked", keywords)
    col3.metric("Avg Interest", f"{avg_interest:.0f}" if avg_interest else "N/A")
except Exception as e:
    st.warning(f"Could not load Google Trends data: {e}")

st.divider()

# FDA Food Recalls (public)
st.markdown("#### FDA Food Recalls")
try:
    recalls = load_fda_recalls_raw()
    latest_date = recalls["recall_initiation_date"].max()
    total_recalls = len(recalls)
    class_i_count = len(recalls[recalls["classification"] == "Class I"])
    col1, col2, col3 = st.columns(3)
    col1.metric("Latest Recall", str(latest_date)[:10] if latest_date else "N/A")
    col2.metric("Total Recalls", f"{total_recalls:,}")
    col3.metric("Class I (Serious)", f"{class_i_count:,}")
except Exception as e:
    st.warning(f"Could not load FDA Food Recalls data: {e}")

st.divider()

# Iowa Liquor Sales (public)
st.markdown("#### Iowa Liquor Sales")
try:
    liquor = load_iowa_liquor_monthly()
    latest_month = liquor["sale_month"].max()
    total_sales = liquor["total_sales"].sum()
    total_bottles = liquor["total_bottles"].sum()
    col1, col2, col3 = st.columns(3)
    col1.metric("Latest Month", str(latest_month)[:10] if latest_month else "N/A")
    col2.metric("Total Sales", f"${total_sales/1e6:.1f}M")
    col3.metric("Bottles Sold", f"{total_bottles/1e6:.1f}M")
except Exception as e:
    st.warning(f"Could not load Iowa Liquor Sales data: {e}")

st.divider()

# FDA Food Events (public)
st.markdown("#### FDA Food Events")
try:
    events = load_fda_events_monthly()
    latest_month = events["month"].max()
    total_events = events["event_count"].sum()
    total_hospitalizations = events["hospitalization_count"].sum()
    col1, col2, col3 = st.columns(3)
    col1.metric("Latest Month", str(latest_month)[:10] if latest_month else "N/A")
    col2.metric("Total Events", f"{total_events:,}")
    col3.metric("Hospitalizations", f"{total_hospitalizations:,}")
except Exception as e:
    st.warning(f"Could not load FDA Food Events data: {e}")

st.divider()

# Stock Prices (public)
st.markdown("#### Stock Prices")
try:
    stocks = load_stock_prices()
    sectors = load_sector_performance()
    latest_date = stocks["trade_date"].max()
    tickers_count = stocks["ticker"].nunique()
    gainers = len(sectors[sectors["avg_daily_change_pct"] > 0]) if not sectors.empty else 0
    col1, col2, col3 = st.columns(3)
    col1.metric("Latest Date", str(latest_date)[:10] if latest_date else "N/A")
    col2.metric("Tickers Tracked", tickers_count)
    col3.metric("Sectors Up", f"{gainers}/5")
except Exception as e:
    st.warning(f"Could not load Stock Prices data: {e}")
