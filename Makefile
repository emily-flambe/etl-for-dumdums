# Load .env file if it exists
ifneq (,$(wildcard .env))
    include .env
    export
endif

# Optional SOURCE filter (linear, github, or empty for all)
SOURCE ?=

.PHONY: help run sync sync-linear sync-github \
        dbt dbt-run dbt-test dbt-compile dbt-debug dbt-deps dbt-clean \
        dbt-docs dbt-docs-serve dbt-seed dbt-snapshot \
        app

# Default target
help:
	@echo "Pipeline:"
	@echo "  make run                  - Full pipeline (all syncs + dbt build)"
	@echo "  make sync                 - Run all data syncs"
	@echo "  make sync SOURCE=linear   - Sync only Linear"
	@echo "  make sync SOURCE=github   - Sync only GitHub"
	@echo ""
	@echo "dbt:"
	@echo "  make dbt                  - Build models + run tests"
	@echo "  make dbt-run              - Run models only"
	@echo "  make dbt-run SOURCE=github - Run only GitHub models"
	@echo "  make dbt-test             - Run tests only"
	@echo "  make dbt-compile          - Compile models (no execution)"
	@echo "  make dbt-debug            - Test connection and config"
	@echo "  make dbt-deps             - Install dbt packages"
	@echo "  make dbt-seed             - Load seed data"
	@echo "  make dbt-snapshot         - Run snapshots"
	@echo "  make dbt-docs             - Generate documentation"
	@echo "  make dbt-docs-serve       - Serve docs locally"
	@echo "  make dbt-clean            - Clean artifacts"
	@echo ""
	@echo "App:"
	@echo "  make app                  - Run Streamlit dashboard"

# ---------- Syncs ----------

sync-linear:
	uv run python scripts/sync_linear.py

sync-linear-full:
	uv run python scripts/sync_linear.py --full

sync-github:
	uv run python scripts/sync_github.py

# Conditional sync based on SOURCE
sync:
ifeq ($(SOURCE),linear)
	$(MAKE) sync-linear
else ifeq ($(SOURCE),github)
	$(MAKE) sync-github
else
	$(MAKE) sync-linear
	$(MAKE) sync-github
endif

# ---------- dbt ----------

# Build dbt select argument based on SOURCE
ifdef SOURCE
    DBT_SELECT := --select staging.$(SOURCE)+ marts.core
else
    DBT_SELECT :=
endif

dbt:
	cd dbt && uv run dbt build --profiles-dir . $(DBT_SELECT)

dbt-run:
	cd dbt && uv run dbt run --profiles-dir . $(DBT_SELECT)

dbt-test:
	cd dbt && uv run dbt test --profiles-dir . $(DBT_SELECT)

dbt-compile:
	cd dbt && uv run dbt compile --profiles-dir . $(DBT_SELECT)

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
