"""
Analytics Dashboard - Home

Multi-page Streamlit app for exploring data from Linear, GitHub, and Oura.

Run with: make app
"""

import streamlit as st

from app_data import load_issues, load_pull_requests, load_oura_daily

st.set_page_config(page_title="Home", layout="wide")
st.title("Home")

st.markdown("Use the sidebar to navigate between pages.")

# Data freshness section
st.subheader("Data Sources")

col1, col2, col3 = st.columns(3)

with col1:
    st.markdown("### Linear")
    try:
        issues = load_issues()
        latest_update = issues["updated_at"].max()
        done_states = ["Done", "Done Pending Deployment"]
        open_count = len(issues[~issues["state"].isin(done_states)])
        st.metric("Last Sync", latest_update.strftime("%Y-%m-%d %H:%M") if latest_update else "N/A")
        st.metric("Total Issues", len(issues))
        st.metric("Open Issues", open_count)
    except Exception as e:
        st.warning(f"Could not load Linear data: {e}")

with col2:
    st.markdown("### GitHub")
    try:
        prs = load_pull_requests()
        latest_update = prs["updated_at"].max()
        open_count = len(prs[prs["state"] == "open"])
        st.metric("Last Sync", latest_update.strftime("%Y-%m-%d %H:%M") if latest_update else "N/A")
        st.metric("Total PRs", len(prs))
        st.metric("Open PRs", open_count)
    except Exception as e:
        st.warning(f"Could not load GitHub data: {e}")

with col3:
    st.markdown("### Oura")
    try:
        oura = load_oura_daily()
        latest_day = oura["day"].max()
        avg_wellness = oura["combined_wellness_score"].mean()
        st.metric("Latest Day", str(latest_day) if latest_day else "N/A")
        st.metric("Total Days", len(oura))
        st.metric("Avg Wellness", f"{avg_wellness:.0f}" if avg_wellness else "N/A")
    except Exception as e:
        st.warning(f"Could not load Oura data: {e}")
