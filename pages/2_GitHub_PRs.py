"""
GitHub Pull Requests dashboard.
"""

import streamlit as st

from app_data import load_pull_requests

st.set_page_config(page_title="GitHub PRs", layout="wide")
st.title("GitHub Pull Requests")

try:
    df = load_pull_requests()

    # Metrics
    st.subheader("Overview")
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total PRs", len(df))
    col2.metric("Open", len(df[df["state"] == "open"]))
    col3.metric("Merged", len(df[df["merged_at"].notna()]))
    col4.metric("Closed (not merged)", len(df[(df["state"] == "closed") & (df["merged_at"].isna())]))

    # Data table
    st.subheader("Pull Requests")
    st.dataframe(
        df,
        use_container_width=True,
        hide_index=True,
    )

except Exception as e:
    st.error(f"Could not load GitHub data: {e}")
    st.info("Make sure you have run `make sync-github` to sync GitHub data.")
