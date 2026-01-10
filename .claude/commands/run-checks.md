---
description: Run all verification checks before committing
allowed-tools: Bash, Read
---

Run all verification checks for the project.

Steps:
1. Run ruff linter:
   ```bash
   cd /Users/emilycogsdill/Documents/GitHub/etl-for-dumdums-claude-config && uv run ruff check .
   ```

2. Run ruff formatter check:
   ```bash
   cd /Users/emilycogsdill/Documents/GitHub/etl-for-dumdums-claude-config && uv run ruff format --check .
   ```

3. Run pyright type checker:
   ```bash
   cd /Users/emilycogsdill/Documents/GitHub/etl-for-dumdums-claude-config && uv run pyright
   ```

4. Run pytest:
   ```bash
   cd /Users/emilycogsdill/Documents/GitHub/etl-for-dumdums-claude-config && uv run pytest tests/ -v
   ```

5. Run dbt compile (validates SQL without executing):
   ```bash
   cd /Users/emilycogsdill/Documents/GitHub/etl-for-dumdums-claude-config && make dbt-compile
   ```

6. Summarize results:
   - List any lint errors
   - List any type errors
   - List any test failures
   - List any dbt compilation errors
   - Indicate if all checks passed
