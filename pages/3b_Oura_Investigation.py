"""
Oura Data Investigation - Embedded marimo notebook.

This page embeds a static export of a marimo notebook that analyzes
4+ years of Oura wellness data with 10 statistical hypothesis tests.

To regenerate the notebook: make notebook-oura-export
"""

import os
from pathlib import Path

import streamlit as st

st.set_page_config(layout="wide")

st.title("Oura Data Investigation")
st.markdown("""
**5 time-based patterns explored across 4+ years of personal wellness data.**

This is an embedded [marimo](https://marimo.io) notebook exploring patterns in sleep,
recovery, and activity. To edit interactively, run `make notebook-oura`.
""")

# Load the exported HTML
notebook_path = Path(__file__).parent.parent / "notebooks" / "oura_investigation.html"

if notebook_path.exists():
    html_content = notebook_path.read_text()

    # Embed the notebook using an iframe with the HTML as a data URI
    # This is cleaner than st.components.v1.html for full-page content
    import base64

    # Encode HTML to base64 for data URI
    html_bytes = html_content.encode("utf-8")
    b64_html = base64.b64encode(html_bytes).decode("utf-8")

    # Create iframe with data URI
    iframe_html = f"""
    <iframe
        src="data:text/html;base64,{b64_html}"
        width="100%"
        height="2000"
        style="border: 1px solid #ddd; border-radius: 8px;"
    ></iframe>
    """

    st.components.v1.html(iframe_html, height=2050, scrolling=True)

    # Show last updated time
    mtime = notebook_path.stat().st_mtime
    from datetime import datetime
    updated = datetime.fromtimestamp(mtime).strftime("%Y-%m-%d %H:%M")
    st.caption(f"Notebook last exported: {updated}")
else:
    st.error(f"Notebook not found at {notebook_path}")
    st.info("Run `make notebook-oura-export` to generate the HTML export.")
