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
    st.Page("pages/3b_Oura_Investigation.py", title="Oura Investigation", icon=":material/science:"),
    st.Page("pages/4_Hacker_News.py", title="Hacker News", icon=":material/newspaper:"),
    st.Page("pages/5_HN_Sentiment.py", title="HN Sentiment", icon=":material/sentiment_satisfied:"),
    st.Page("pages/6_Google_Trends.py", title="Google Trends", icon=":material/trending_up:"),
    st.Page("pages/7_FDA_Food_Recalls.py", title="FDA Food Recalls", icon=":material/warning:"),
    st.Page("pages/8_Iowa_Liquor_Sales.py", title="Iowa Liquor Sales", icon=":material/liquor:"),
    st.Page("pages/9_FDA_Food_Events.py", title="FDA Food Events", icon=":material/restaurant:"),
    st.Page("pages/10_Stock_Prices.py", title="Stock Prices", icon=":material/candlestick_chart:"),
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

# GitHub link in sidebar
GITHUB_SVG = """
<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="currentColor">
<path d="M12 0c-6.626 0-12 5.373-12 12 0 5.302 3.438 9.8 8.207 11.387.599.111.793-.261.793-.577v-2.234c-3.338.726-4.033-1.416-4.033-1.416-.546-1.387-1.333-1.756-1.333-1.756-1.089-.745.083-.729.083-.729 1.205.084 1.839 1.237 1.839 1.237 1.07 1.834 2.807 1.304 3.492.997.107-.775.418-1.305.762-1.604-2.665-.305-5.467-1.334-5.467-5.931 0-1.311.469-2.381 1.236-3.221-.124-.303-.535-1.524.117-3.176 0 0 1.008-.322 3.301 1.23.957-.266 1.983-.399 3.003-.404 1.02.005 2.047.138 3.006.404 2.291-1.552 3.297-1.23 3.297-1.23.653 1.653.242 2.874.118 3.176.77.84 1.235 1.911 1.235 3.221 0 4.609-2.807 5.624-5.479 5.921.43.372.823 1.102.823 2.222v3.293c0 .319.192.694.801.576 4.765-1.589 8.199-6.086 8.199-11.386 0-6.627-5.373-12-12-12z"/>
</svg>
"""

st.sidebar.markdown(
    f"""
    <a href="https://github.com/emily-flambe/etl-for-dumdums" target="_blank" style="text-decoration: none; color: inherit; display: inline-flex; align-items: center; gap: 6px; opacity: 0.7; font-size: 13px;">
        {GITHUB_SVG} View source
    </a>
    """,
    unsafe_allow_html=True,
)

pg = st.navigation(nav_config)
pg.run()
