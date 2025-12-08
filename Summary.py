"""
Analytics Dashboard - Summary

Multi-page Streamlit app for exploring data from Linear, GitHub, Oura, and more.

Run with: make app
"""

import streamlit as st

from data import (
    load_issues,
    load_pull_requests,
    load_oura_daily,
    load_hn_weekly_stats,
    load_keyword_trends,
)

st.set_page_config(page_title="Summary", layout="wide")
st.title("Summary")

st.markdown("Use the sidebar to navigate between pages.")

# Data freshness section
st.subheader("Data Sources")

# Linear
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

# GitHub
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

# Oura
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

# Hacker News
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

# Google Trends
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
