# Soogle

Smalltalk code search engine. Scrapes Smalltalk source archives, processes them through an LLM review pipeline, and serves a search UI.

## Scraper scheduling

- `daily.bash` runs free scrapers + all processing. It calls `python -m scrape custom all`, which dispatches to every entry in `CUSTOM_SCRAPERS` (in `scrape/custom.py`). Adding a new scraper class to that dict is enough to get it into the daily run — no bash changes needed.
- `weekly.bash` runs user submissions, paid-API discovery (SerpAPI), YouTube, then calls `daily.bash`.
- When adding a new scraper: register the site in the `sites` table, add the class to the appropriate dispatch dict (`SCRAPERS` in `web.py` or `CUSTOM_SCRAPERS` in `custom.py`), add its CLI choice to `__main__.py`, and add the domain to `_KNOWN_DOMAINS` in `web.py` so discovery skips it. If the scraper uses a paid API or should only run weekly, add an explicit call in `weekly.bash` before the `daily.bash` exec.
