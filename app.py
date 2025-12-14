"""
Analytics Dashboard - Main Entry Point

Multi-page Streamlit app with environment-based page visibility.

Set DEPLOYMENT_MODE=public for public deployment (hides PII-containing pages).
Set DEPLOYMENT_MODE=local (default) to show all pages.

Run with: make app
"""

import os

import streamlit as st
from dotenv import load_dotenv

load_dotenv()

# Deployment mode: "local" (all pages) or "public" (public-safe pages only)
DEPLOYMENT_MODE = os.environ.get("DEPLOYMENT_MODE", "local")
IS_PUBLIC = DEPLOYMENT_MODE == "public"

# Define page groups
SUMMARY_PAGE = st.Page("Summary.py", title="Summary", default=True)

# Pages with public or personal data (safe for public deployment)
PUBLIC_PAGES = [
    st.Page("pages/3_Oura_Wellness.py", title="Oura Wellness", icon=":material/favorite:"),
    st.Page("pages/4_Hacker_News.py", title="Hacker News", icon=":material/newspaper:"),
    st.Page("pages/5_HN_Sentiment.py", title="HN Sentiment", icon=":material/sentiment_satisfied:"),
    st.Page("pages/6_Google_Trends.py", title="Google Trends", icon=":material/trending_up:"),
    st.Page("pages/7_FDA_Food_Recalls.py", title="FDA Food Recalls", icon=":material/warning:"),
    st.Page("pages/8_Iowa_Liquor_Sales.py", title="Iowa Liquor Sales", icon=":material/liquor:"),
    st.Page("pages/9_FDA_Food_Events.py", title="FDA Food Events", icon=":material/restaurant:"),
]

# Build navigation based on deployment mode
if IS_PUBLIC:
    # Public deployment: only show public-safe pages (private page files not in container)
    nav_config = {
        "Overview": [SUMMARY_PAGE],
        "Public Data": PUBLIC_PAGES,
    }
else:
    # Local deployment: show all pages including private ones
    PRIVATE_PAGES = [
        st.Page("pages/1_Linear_Issues.py", title="Linear Issues", icon=":material/task:"),
        st.Page("pages/2_GitHub_PRs.py", title="GitHub PRs", icon=":material/code:"),
    ]
    nav_config = {
        "Overview": [SUMMARY_PAGE],
        "Work": PRIVATE_PAGES,
        "Public Data": PUBLIC_PAGES,
    }

# Configure app and run navigation
st.set_page_config(
    page_title="Analytics Dashboard",
    page_icon=":material/analytics:",
    layout="wide",
)

pg = st.navigation(nav_config)
pg.run()
