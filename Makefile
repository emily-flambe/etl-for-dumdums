# Load .env file if it exists
ifneq (,$(wildcard .env))
    include .env
    export
endif

# Optional flags
FULL ?=

.PHONY: help run run-linear run-github run-oura run-hacker-news run-trends run-fda-food-recalls run-fda-food-events run-iowa-liquor run-stocks \
        sync sync-linear sync-github sync-oura sync-hacker-news sync-trends sync-fda-food-recalls sync-fda-food-events sync-iowa-liquor sync-stocks \
        dbt dbt-linear dbt-github dbt-oura dbt-hacker-news dbt-trends dbt-fda-food dbt-iowa-liquor dbt-stocks \
        dbt-run dbt-run-linear dbt-run-github dbt-run-oura dbt-run-hacker-news dbt-run-trends dbt-run-fda-food dbt-run-iowa-liquor dbt-run-stocks \
        dbt-test dbt-test-linear dbt-test-github dbt-test-oura dbt-test-hacker-news dbt-test-trends dbt-test-fda-food dbt-test-iowa-liquor dbt-test-stocks \
        dbt-compile dbt-debug dbt-deps dbt-clean dbt-docs dbt-docs-serve dbt-seed dbt-snapshot \
        app app-public deploy test notebook notebook-oura notebook-export

# Default target
help:
	@echo "Pipeline (sync + dbt, add FULL=1 for full sync):"
	@echo "  make run                  - Full pipeline (all sources)"
	@echo "  make run-linear           - Linear pipeline"
	@echo "  make run-github           - GitHub pipeline"
	@echo "  make run-oura             - Oura pipeline"
	@echo "  make run-hacker-news      - Hacker News pipeline"
	@echo "  make run-trends           - Google Trends pipeline"
	@echo "  make run-fda-food-recalls - FDA Food Recalls pipeline"
	@echo "  make run-fda-food-events  - FDA Food Adverse Events pipeline"
	@echo "  make run-iowa-liquor      - Iowa Liquor Sales pipeline"
	@echo "  make run-stocks           - Stock Prices pipeline"
	@echo ""
	@echo "Syncs (add FULL=1 for full sync instead of incremental):"
	@echo "  make sync                 - All sources"
	@echo "  make sync-linear          - Linear (7 day lookback)"
	@echo "  make sync-github          - GitHub (30 day lookback)"
	@echo "  make sync-oura            - Oura (7 day lookback)"
	@echo "  make sync-hacker-news     - Hacker News (30 day lookback)"
	@echo "  make sync-trends          - Google Trends (3 month lookback)"
	@echo "  make sync-fda-food-recalls - FDA Food Recalls (from 2025-01-01)"
	@echo "  make sync-fda-food-events  - FDA Food Adverse Events (90 day lookback)"
	@echo "  make sync-iowa-liquor     - Iowa Liquor Sales (90 day lookback)"
	@echo "  make sync-stocks          - Stock Prices (30 day lookback)"
	@echo "  make sync-linear FULL=1   - Linear full sync"
	@echo "  make sync-hacker-news FULL=1 - Hacker News full sync (5 years)"
	@echo "  make sync-fda-food-recalls FULL=1 - FDA Food Recalls full sync (from 2012)"
	@echo "  make sync-fda-food-events FULL=1  - FDA Food Adverse Events full sync (10 years)"
	@echo "  make sync-iowa-liquor FULL=1 - Iowa Liquor Sales full sync (13 years)"
	@echo "  make sync-stocks FULL=1   - Stock Prices full sync (5 years)"
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
	@echo "  make app                  - Run Streamlit dashboard (all pages)"
	@echo "  make app-public           - Run in public mode (hides PII pages)"
	@echo "  make deploy               - Deploy to Google Cloud Run"
	@echo ""
	@echo "Notebooks (marimo):"
	@echo "  make notebook-oura        - Edit Oura investigation notebook"
	@echo "  make notebook-oura-run    - Run as interactive app"
	@echo "  make notebook-oura-export - Export to HTML"
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

sync-fda-food-recalls:
	uv run python scripts/sync_fda_food.py $(if $(FULL),--full,)

sync-fda-food-events:
	uv run python scripts/sync_fda_food_events.py $(if $(FULL),--full,)

sync-iowa-liquor:
	uv run python scripts/sync_iowa_liquor.py $(if $(FULL),--full,)

sync-stocks:
	uv run python scripts/sync_stocks.py $(if $(FULL),--full,)

sync: sync-linear sync-github sync-oura sync-hacker-news sync-trends sync-fda-food-recalls sync-fda-food-events sync-iowa-liquor sync-stocks

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

dbt-iowa-liquor:
	cd dbt && uv run dbt build --profiles-dir . --select tag:iowa_liquor

dbt-stocks:
	cd dbt && uv run dbt build --profiles-dir . --select tag:stocks

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

dbt-run-iowa-liquor:
	cd dbt && uv run dbt run --profiles-dir . --select tag:iowa_liquor

dbt-run-stocks:
	cd dbt && uv run dbt run --profiles-dir . --select tag:stocks

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

dbt-test-iowa-liquor:
	cd dbt && uv run dbt test --profiles-dir . --select tag:iowa_liquor

dbt-test-stocks:
	cd dbt && uv run dbt test --profiles-dir . --select tag:stocks

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

run-fda-food-recalls: sync-fda-food-recalls dbt-fda-food

run-fda-food-events: sync-fda-food-events dbt-fda-food

run-iowa-liquor: sync-iowa-liquor dbt-iowa-liquor

run-stocks: sync-stocks dbt-stocks

# ---------- Streamlit app ----------

app:
	uv run streamlit run app.py

app-public:
	DEPLOYMENT_MODE=public uv run streamlit run app.py

deploy:
	./deploy.sh

# ---------- Notebooks (marimo) ----------

notebook-oura:
	uv run marimo edit notebooks/oura_investigation.py

notebook-oura-run:
	uv run marimo run notebooks/oura_investigation.py

notebook-oura-export:
	uv run marimo export html notebooks/oura_investigation.py -o notebooks/oura_investigation.html

# ---------- Tests ----------

test:
	uv run pytest tests/ -v
