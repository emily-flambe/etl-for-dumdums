---
description: Scaffold a new data source
allowed-tools: Bash, Read, Write, Edit, Glob, Grep
---

Scaffold a new data source: $ARGUMENTS

The source name should be provided as the argument (e.g., "reddit" or "spotify").

Steps:
1. Parse source name from arguments
2. Create `sources/<name>.py` with:
   - Classes extending `Source` from `lib/source.py`
   - Define `dataset_id`, `table_id` (with `raw_` prefix), `primary_key`, `schema`
   - Implement `fetch()` and `transform()` stub methods
   - Reference existing sources like `sources/linear.py` for patterns

3. Create `scripts/sync_<name>.py` entry point:
   - Import source classes
   - Add argparse for `--full` flag
   - Call `run_sync()` for each source

4. Add Makefile targets:
   - `sync-<name>`: Run the sync script
   - `dbt-<name>`: Run dbt with tag filter
   - `run-<name>`: sync + dbt

5. Create dbt staging models in `dbt/models/staging/<name>/`:
   - `_<name>__sources.yml`: Define BigQuery source
   - `_<name>__models.yml`: Model documentation
   - `stg_<name>__<entity>.sql`: Staging view

6. Create placeholder mart in `dbt/models/marts/<name>/`:
   - `_<name>__models.yml`
   - `fct_<name>_<entity>.sql`

7. Report what was created and next steps (API credentials, GitHub workflow, Streamlit page)
