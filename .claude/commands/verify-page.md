---
description: Verify a Streamlit page renders correctly with Playwright
allowed-tools: Bash, Read, mcp__plugin_playwright_playwright__browser_navigate, mcp__plugin_playwright_playwright__browser_snapshot, mcp__plugin_playwright_playwright__browser_take_screenshot, mcp__plugin_playwright_playwright__browser_wait_for
---

Verify the Streamlit page renders correctly: $ARGUMENTS

Steps:
1. Check if Streamlit is running on localhost:8501 (try to connect)
2. If not running, tell user to run `make app` in another terminal
3. Navigate to the specified page (e.g., /Oura_Wellness, /Hacker_News)
4. Wait for the page to load (wait for data to appear)
5. Take a screenshot and save to /tmp/
6. Capture accessibility snapshot to check for errors
7. Scroll down and take additional screenshots if needed
8. Report:
   - Whether charts display actual data (not empty axes)
   - Any error messages or warnings
   - Layout issues (overlapping elements, cut-off labels)
   - Interactive elements that appear broken

Page URL patterns:
- Summary: /
- Linear Issues: /Linear_Issues
- GitHub PRs: /GitHub_PRs
- Oura Wellness: /Oura_Wellness
- Hacker News: /Hacker_News
- HN Sentiment: /HN_Sentiment
