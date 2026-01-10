---
description: Run ETL sync for a specific source
allowed-tools: Bash, Read
---

Run the ETL sync for the specified source: $ARGUMENTS

Steps:
1. If no source specified, list available sources from Makefile (sync-* targets)
2. Run `make sync-<source>` for incremental sync
3. If user says "full", add `FULL=1` flag
4. Show the output and summarize records synced
5. Optionally run `make dbt-<source>` to transform the data

Available sources: linear, github, oura, hacker-news, trends, fda-food-recalls, fda-food-events, iowa-liquor, stocks
