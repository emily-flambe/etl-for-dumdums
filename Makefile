# Load .env file if it exists
ifneq (,$(wildcard .env))
    include .env
    export
endif

.PHONY: help run sync sync-linear \
        dbt dbt-run dbt-test dbt-compile dbt-debug dbt-deps dbt-clean \
        dbt-docs dbt-docs-serve dbt-seed dbt-snapshot \
        app

# Default target
help:
	@echo "Pipeline:"
	@echo "  make run            - Full pipeline (all syncs + dbt build)"
	@echo "  make sync           - Run all data syncs"
	@echo "  make sync-linear    - Fetch data from Linear"
	@echo ""
	@echo "dbt:"
	@echo "  make dbt            - Build models + run tests"
	@echo "  make dbt-run        - Run models only"
	@echo "  make dbt-test       - Run tests only"
	@echo "  make dbt-compile    - Compile models (no execution)"
	@echo "  make dbt-debug      - Test connection and config"
	@echo "  make dbt-deps       - Install dbt packages"
	@echo "  make dbt-seed       - Load seed data"
	@echo "  make dbt-snapshot   - Run snapshots"
	@echo "  make dbt-docs       - Generate documentation"
	@echo "  make dbt-docs-serve - Serve docs locally"
	@echo "  make dbt-clean      - Clean artifacts"
	@echo ""
	@echo "App:"
	@echo "  make app            - Run Streamlit dashboard"

# ---------- Syncs ----------

sync-linear:
	uv run python scripts/sync_linear.py

# Add future syncs here:
# sync-github:
# 	uv run python scripts/sync_github.py

sync: sync-linear

# ---------- dbt ----------

dbt:
	cd dbt && uv run dbt build --profiles-dir .

dbt-run:
	cd dbt && uv run dbt run --profiles-dir .

dbt-test:
	cd dbt && uv run dbt test --profiles-dir .

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

# ---------- Streamlit app ----------

app:
	uv run streamlit run app.py
