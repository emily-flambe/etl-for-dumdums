# Load .env file if it exists
ifneq (,$(wildcard .env))
    include .env
    export
endif

# Optional flags
FULL ?=

.PHONY: help run run-linear run-github run-oura run-hacker-news run-trends run-fda-food \
        sync sync-linear sync-github sync-oura sync-hacker-news sync-trends sync-fda-food \
        dbt dbt-linear dbt-github dbt-oura dbt-hacker-news dbt-trends dbt-fda-food \
        dbt-run dbt-run-linear dbt-run-github dbt-run-oura dbt-run-hacker-news dbt-run-trends dbt-run-fda-food \
        dbt-test dbt-test-linear dbt-test-github dbt-test-oura dbt-test-hacker-news dbt-test-trends dbt-test-fda-food \
        dbt-compile dbt-debug dbt-deps dbt-clean dbt-docs dbt-docs-serve dbt-seed dbt-snapshot \
        app test

# Default target
help:
	@echo "Pipeline (sync + dbt, add FULL=1 for full sync):"
	@echo "  make run                  - Full pipeline (all sources)"
	@echo "  make run-linear           - Linear pipeline"
	@echo "  make run-github           - GitHub pipeline"
	@echo "  make run-oura             - Oura pipeline"
	@echo "  make run-hacker-news      - Hacker News pipeline"
	@echo "  make run-trends           - Google Trends pipeline"
	@echo "  make run-fda-food         - FDA Food Recalls pipeline"
	@echo ""
	@echo "Syncs (add FULL=1 for full sync instead of incremental):"
	@echo "  make sync                 - All sources"
	@echo "  make sync-linear          - Linear (7 day lookback)"
	@echo "  make sync-github          - GitHub (30 day lookback)"
	@echo "  make sync-oura            - Oura (7 day lookback)"
	@echo "  make sync-hacker-news     - Hacker News (30 day lookback)"
	@echo "  make sync-trends          - Google Trends (3 month lookback)"
	@echo "  make sync-fda-food        - FDA Food Recalls (from 2025-01-01)"
	@echo "  make sync-linear FULL=1   - Linear full sync"
	@echo "  make sync-hacker-news FULL=1 - Hacker News full sync (5 years)"
	@echo "  make sync-fda-food FULL=1 - FDA Food Recalls full sync (from 2012)"
	@echo ""
	@echo "dbt (append -linear, -github, or -oura to filter by source):"
	@echo "  make dbt                  - Build + test all models"
	@echo "  make dbt-linear           - Build + test Linear models"
	@echo "  make dbt-run              - Run all models (no tests)"
	@echo "  make dbt-run-linear       - Run Linear models"
	@echo "  make dbt-test             - Test all models"
	@echo "  make dbt-test-github      - Test GitHub models"
	@echo "  make dbt-compile          - Compile (no execution)"
	@echo "  make dbt-docs             - Generate docs"
	@echo ""
	@echo "App:"
	@echo "  make app                  - Run Streamlit dashboard"
	@echo ""
	@echo "Tests:"
	@echo "  make test                 - Run all Python tests"

# ---------- Syncs ----------

sync-linear:
	uv run python scripts/sync_linear.py $(if $(FULL),--full,)

sync-github:
	uv run python scripts/sync_github.py $(if $(FULL),--full,)

sync-oura:
	uv run python scripts/sync_oura.py $(if $(FULL),--full,)

sync-hacker-news:
	uv run python scripts/sync_hacker_news.py $(if $(FULL),--full,)

# Backfill historical HN sentiment (DAYS=7, WORKERS=10 by default)
backfill-hn-sentiment:
	uv run python scripts/backfill_hn_sentiment.py --days $(or $(DAYS),7) --workers $(or $(WORKERS),10)

sync-trends:
	uv run python scripts/sync_trends.py

sync-fda-food:
	uv run python scripts/sync_fda_food.py $(if $(FULL),--full,)

sync: sync-linear sync-github sync-oura sync-hacker-news sync-trends sync-fda-food

# ---------- dbt ----------

dbt:
	cd dbt && uv run dbt build --profiles-dir .

dbt-linear:
	cd dbt && uv run dbt build --profiles-dir . --select tag:linear

dbt-github:
	cd dbt && uv run dbt build --profiles-dir . --select tag:github

dbt-oura:
	cd dbt && uv run dbt build --profiles-dir . --select tag:oura

dbt-hacker-news:
	cd dbt && uv run dbt build --profiles-dir . --select tag:hacker_news

dbt-trends:
	cd dbt && uv run dbt build --profiles-dir . --select tag:trends

dbt-fda-food:
	cd dbt && uv run dbt build --profiles-dir . --select tag:fda_food

dbt-run:
	cd dbt && uv run dbt run --profiles-dir .

dbt-run-linear:
	cd dbt && uv run dbt run --profiles-dir . --select tag:linear

dbt-run-github:
	cd dbt && uv run dbt run --profiles-dir . --select tag:github

dbt-run-oura:
	cd dbt && uv run dbt run --profiles-dir . --select tag:oura

dbt-run-hacker-news:
	cd dbt && uv run dbt run --profiles-dir . --select tag:hacker_news

dbt-run-trends:
	cd dbt && uv run dbt run --profiles-dir . --select tag:trends

dbt-run-fda-food:
	cd dbt && uv run dbt run --profiles-dir . --select tag:fda_food

dbt-test:
	cd dbt && uv run dbt test --profiles-dir .

dbt-test-linear:
	cd dbt && uv run dbt test --profiles-dir . --select tag:linear

dbt-test-github:
	cd dbt && uv run dbt test --profiles-dir . --select tag:github

dbt-test-oura:
	cd dbt && uv run dbt test --profiles-dir . --select tag:oura

dbt-test-hacker-news:
	cd dbt && uv run dbt test --profiles-dir . --select tag:hacker_news

dbt-test-trends:
	cd dbt && uv run dbt test --profiles-dir . --select tag:trends

dbt-test-fda-food:
	cd dbt && uv run dbt test --profiles-dir . --select tag:fda_food

dbt-compile:
	cd dbt && uv run dbt compile --profiles-dir .

dbt-debug:
	cd dbt && uv run dbt debug --profiles-dir .

dbt-deps:
	cd dbt && uv run dbt deps --profiles-dir .

dbt-seed:
	cd dbt && uv run dbt seed --profiles-dir .

dbt-snapshot:
	cd dbt && uv run dbt snapshot --profiles-dir .

dbt-docs:
	cd dbt && uv run dbt docs generate --profiles-dir .

dbt-docs-serve:
	cd dbt && uv run dbt docs serve --profiles-dir .

dbt-clean:
	cd dbt && uv run dbt clean --profiles-dir .

# ---------- Full pipeline ----------

run: sync dbt

run-linear: sync-linear dbt-linear

run-github: sync-github dbt-github

run-oura: sync-oura dbt-oura

run-hacker-news: sync-hacker-news dbt-hacker-news

run-trends: sync-trends dbt-trends

run-fda-food: sync-fda-food dbt-fda-food

# ---------- Streamlit app ----------

app:
	uv run streamlit run Summary.py

# ---------- Tests ----------

test:
	uv run pytest tests/ -v
