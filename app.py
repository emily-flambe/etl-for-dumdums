"""
DDX Analytics Dashboard - Home

Multi-page Streamlit app for exploring Linear and GitHub data.

Run with: make app
"""

import streamlit as st

from app_data import load_issues, load_pull_requests

st.set_page_config(page_title="DDX Analytics", layout="wide")
st.title("DDX Analytics")

st.markdown("""
Welcome to the DDX Analytics dashboard. Use the sidebar to navigate between pages.

## Available Pages

- **Linear Issues** - Track issues, points, and cycle progress
- **GitHub PRs** - Monitor pull requests and code review activity (coming soon)
""")

# Quick stats
st.subheader("Quick Stats")

col1, col2 = st.columns(2)

with col1:
    st.markdown("### Linear")
    try:
        issues = load_issues()
        done_states = ["Done", "Done Pending Deployment"]
        st.metric("Total Issues", len(issues))
        st.metric("Open Issues", len(issues[~issues["state"].isin(done_states)]))
        st.metric("Total Points", f"{issues['estimate'].sum():.0f}")
    except Exception as e:
        st.warning(f"Could not load Linear data: {e}")

with col2:
    st.markdown("### GitHub")
    try:
        prs = load_pull_requests()
        st.metric("Total PRs", len(prs))
        st.metric("Open PRs", len(prs[prs["state"] == "open"]))
        st.metric("Merged PRs", len(prs[prs["merged_at"].notna()]))
    except Exception as e:
        st.warning(f"Could not load GitHub data: {e}")
