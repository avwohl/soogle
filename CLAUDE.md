# Soogle

Smalltalk code search engine. Scrapes Smalltalk source archives, processes them through an LLM review pipeline, and serves a search UI.

## Python dependency management

Uses [uv](https://docs.astral.sh/uv/) for dependency management.

- `uv sync` — install/update all dependencies into `.venv`
- `uv run python -m scrape <command>` — run the scraper
- `uv run python web/manage.py runserver` — run the Django web app
- `uv run python tests/test_monticello.py` — run tests
- `uv lock` — update the lockfile
- `pyproject.toml` replaces the old `requirements.txt`

`daily.bash` and `weekly.bash` call `.venv/bin/python` directly (not `uv run`) so each phase has no uv/network overhead; `PYTHON_BIN` overrides it. The preflight runs `uv sync` automatically if `.venv` is missing or the required modules can't be imported.

## Git hooks

Uses [prek](https://github.com/j178/prek) (Rust, drop-in pre-commit replacement) for git hooks. Config lives in `.pre-commit-config.yaml` (read natively by prek). `mise install` auto-installs the hooks via prek's `postinstall`. Run `prek run` (or `mise run hooks`) to run all hooks manually.

## Scraper scheduling

- `daily.bash` runs free scrapers + all processing. It calls `.venv/bin/python -m scrape custom all`, which dispatches to every entry in `CUSTOM_SCRAPERS` (in `scrape/custom.py`). Adding a new scraper class to that dict is enough to get it into the daily run — no bash changes needed.
- `weekly.bash` runs user submissions, paid-API discovery (SerpAPI), YouTube, then calls `daily.bash`.
- When adding a new scraper: register the site in the `sites` table, add the class to the appropriate dispatch dict (`SCRAPERS` in `web.py` or `CUSTOM_SCRAPERS` in `custom.py`), add its CLI choice to `__main__.py`, and add the domain to `_KNOWN_DOMAINS` in `web.py` so discovery skips it. If the scraper uses a paid API or should only run weekly, add an explicit call in `weekly.bash` before the `daily.bash` exec.
