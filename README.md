# Soogle

Search engine for Smalltalk source code and videos.

Soogle indexes Smalltalk packages from repositories across multiple dialects (Pharo, Squeak, Cuis, GemStone, VisualWorks, GNU Smalltalk, Dolphin, and more). It also indexes Smalltalk videos — conference talks, tutorials, and screencasts. Everything is searchable through a clean web interface at [soogle.org](https://soogle.org).

## Features

- Full-text search across Smalltalk packages
- Filter by dialect, source site, or category
- Browse indexed sources and recently added packages
- Video index with search, dialect filtering, and sort by views/date
- LLM-powered quality review to filter false positives (C#/.NET repos, PLC code, "small talk" videos)
- Auto-detection of Smalltalk dialect and package categories
- SEO sitemaps for package and video pages
- Submit new Smalltalk sites for indexing

## Data sources

Packages are scraped from:

- **GitHub** — `language:Smalltalk` search with date segmentation to work around the 1,000-result API limit
- **SqueakSource** — Seaside-based project listings
- **SmalltalkHub** — Static project archive
- **Rosetta Code** — MediaWiki API, Smalltalk code blocks
- **VS Knowledge Base** — Visual Smalltalk source code library
- **SqueakMap** — Package registry
- **Lukas Renggli's archive** — Monticello `.mcz` files
- **SourceForge** — Smalltalk projects directory
- **Launchpad** — Smalltalk branches via REST API
- **Web discovery** — SerpAPI web search finds new sources automatically

Videos are scraped from:

- **YouTube search** — SerpAPI queries for Smalltalk-related videos
- **Known playlists** — Pharo MOOC (English and French)

Trusted video channels (esugboard, Cincom Smalltalk, etc.) are always accepted. Videos about conversation "small talk" or GemStone jewelry are filtered out.

## Data flow

The pipeline has four phases that move data from raw scrapes to what users see on the site.

### 1. Scrape — collect raw metadata

Each scraper fetches metadata from its source and writes rows into the `scrape_raw` staging table. Every row gets a SHA-256 checksum so unchanged entries are skipped on future runs. The GitHub scraper handles rate limits automatically (30 req/min for search, sleeps and retries on 403).

### 2. Process — normalize into packages

`uv run python -m scrape process` reads pending rows from `scrape_raw` and for each one:

- Detects the Smalltalk **dialect** from GitHub topics and name/description keywords (pharo, squeak, cuis, etc.) with a confidence score
- **Auto-categorizes** into one or more of 19 categories (web, database, testing, ui/graphics, etc.) via keyword matching
- Applies **quality gates** — skips repos with no description, no stars, and unknown dialect; rejects names matching known non-Smalltalk patterns (Arduino, TensorFlow, Unity, etc.)
- Does an **atomic upsert** — inserts/updates the package and its categories in a single transaction so data is always consistent

### 3. LLM review — filter false positives

`uv run python -m scrape llm-review` sends batches of packages to Claude for quality review. The LLM catches false positives that regex alone misses:

- C# / .NET projects (GitHub's linguist confuses `.cs` changesets with Smalltalk)
- IEC 61131-3 Structured Text / PLC code (`.st` extension overlap)
- ML/NLP research using StringTemplate `.st` files
- Unity game projects

Packages marked "block" are added to a blocklist and deleted. Packages marked "keep" are stamped with the model name that reviewed them.

`uv run python -m scrape video-review` does the same for videos — blocks conversation-skills videos, design-pattern talks that only mention Smalltalk in passing, GemStone jewelry content, and spam.

Both commands support a **model tier system** (haiku < sonnet < opus). The `--scope upgrade` flag re-reviews items that were previously reviewed by a lower-tier model, so you can upgrade quality without reprocessing everything.

### 4. Serve — Django web frontend

The Django app reads directly from the database (MySQL or SQLite):

- **Home page** — package count, video count, dialect breakdown, recently added packages
- **Search** — full-text search with filters for dialect, source site, and category; sort by relevance, stars, update date, or name
- **Package detail** — description, README excerpt, metadata (license, stars, forks, topics, dates), categories
- **Videos** — searchable gallery with dialect filter, sort by views or date
- **Sources** — lists all indexed sites with package counts
- **Submit** — users can submit new Smalltalk URLs for indexing

### 5. MCP server

A standalone MCP server (`mcp_server/`) exposes the same search service to AI
agents. It reads the same database directly — no HTTP, no Django dependency.
The `mcp` SDK lives in its own uv dependency group, so `uv sync` alone doesn't
pull it in; install with `uv sync --group mcp`. Tools:

- `search_packages` — search with dialect/site/category filters and sorting
- `get_package` — package detail with categories, classes, and method list
- `get_method` — a single method's source code
- `list_sources` — active sites with package counts
- `search_videos` — video search with dialect filter and sorting

Run it locally over stdio (`mise run mcp`) and register it in your MCP client
(e.g. Claude Code, opencode):

```json
{
  "mcpServers": {
    "soogle": {
      "command": "uv",
      "args": ["run", "--group", "mcp", "python", "-m", "mcp_server"],
      "cwd": "/path/to/soogle"
    }
  }
}
```

Or run it in a container:

```bash
docker compose up -d --build   # serves the site and /mcp on :8000
```

The compose mounts `./data` as the SQLite database directory. For MySQL,
override the `SOOGLE_DB_*` environment variables instead. The `Dockerfile`
builds the Django app under uvicorn (ASGI), which serves both the site and
the `/mcp` endpoint.

### Serving /mcp from Django

Django can also serve the MCP endpoint itself at `/mcp`, by wrapping the MCP
Starlette app in the ASGI application (`web/soogle_web/asgi.py`). This
requires running Django under an ASGI server (uvicorn/gunicorn) instead of
mod_wsgi, and the `mcp` uv group:

```bash
uv run --group mcp uvicorn soogle_web.asgi:application --port 8000
```

The MCP app's lifespan (its session manager) is run from Django's own
lifespan handler, since uvicorn only runs the top-level app's lifespan.

## Daily and weekly updates

### daily.bash

Runs the free scrapers and all processing. Intended for daily cron:

1. `github --incremental` — fetch repos updated in the last 30 days
2. `web all` — scrape SqueakSource, SmalltalkHub, Rosetta Code, VSKB
3. `custom all` — scrape SqueakMap, SourceForge, Launchpad, Lukas Renggli
4. `process` — move `scrape_raw` rows into `packages`
5. `analyze` — LLM assessment of newly discovered domains (if ANTHROPIC_API_KEY set)
6. `llm-review` — quality review new packages (if ANTHROPIC_API_KEY set)
7. `video-review` — quality review new videos (if ANTHROPIC_API_KEY set)
8. `status` — print pipeline stats

### weekly.bash

Runs paid-API scrapers (SerpAPI free tier: ~100 searches/month), then calls `daily.bash`:

1. `discover serpapi` — web search for new Smalltalk code sources
2. `youtube` — search YouTube for Smalltalk videos + scrape known playlists
3. Runs `daily.bash` (all free scrapers + processing)

## Project structure

```
soogle/
  mise.toml               Dev tools (uv, ruff, prek) + task runner
  daily.bash              Daily cron script (free scrapers + processing)
  weekly.bash             Weekly cron script (paid APIs + daily.bash)
   pyproject.toml        dependencies (requests, pymysql, beautifulsoup4, anthropic, django)
  db/schema.sql           Full MySQL schema and seed data
   db/schema.sqlite.sql    SQLite schema and seed data
  scrape/
    __main__.py           CLI entry point (python -m scrape <command>)
    config.py             DB connection, API keys, rate limits
    db.py                 Database helpers (blocklist, dedup, transactions)
    models.py             LLM model tier system (haiku < sonnet < opus)
    github.py             GitHub scraper with date segmentation
    web.py                Web scrapers (SqueakSource, SmalltalkHub, Rosetta, VSKB, discovery)
    custom.py             Custom scrapers (SqueakMap, Lukas Renggli, SourceForge, Launchpad)
    youtube.py            YouTube video scraper
    processor.py          scrape_raw -> packages processing pipeline
    llm_review.py         LLM quality review for packages and videos
    analyze.py            LLM domain analysis for discovered sites
  web/
    manage.py
    soogle_web/           Django project settings, URLs, WSGI
    search/
      models.py           Django ORM models (read-only mappings)
      views.py            View handlers (search, detail, videos, sources, SEO)
      urls.py             URL routing
      templates/search/   HTML templates (base, index, results, detail, videos, etc.)
  mcp_server/             MCP server (stdio) exposing the same search service
  www/                    Static files (CSS, images)
```

## CLI reference

```
uv run python -m scrape github [--incremental | --since YYYY-MM-DD]
uv run python -m scrape web <source>                    # squeaksource | smalltalkhub | rosettacode | vskb | all
uv run python -m scrape custom <source>                 # squeakmap | lukas_renggli | sourceforge | launchpad | all
uv run python -m scrape youtube [--playlists-only]
uv run python -m scrape discover <engine>               # brave | serpapi | bing | ddg
uv run python -m scrape process [--limit N]
uv run python -m scrape analyze [--limit N] [--show] [--min-score 50]
uv run python -m scrape llm-review [--model M] [--scope S] [--limit N] [--fetch-only] [--review-only]
uv run python -m scrape video-review [--model M] [--scope S] [--limit N]
uv run python -m scrape block <external_id> [--site github] [--reason '...']
uv run python -m scrape status
```

## Requirements

- [mise](https://mise.jdx.dev) — dev tools (uv, ruff, prek) and task runner
- Python 3.10+
- MySQL / MariaDB **or** SQLite (see below)
- `mise install` (installs uv, ruff, prek + git hooks)

### Database: MySQL or SQLite

The engine is auto-detected: if `SOOGLE_DB_PASS` is set, the pipeline and web
app use MySQL; otherwise they use a local SQLite file — no server, no
password.  Force one explicitly with `SOOGLE_DB_ENGINE=mysql|sqlite`.

For SQLite, apply the schema once:

```bash
sqlite3 soogle.db < db/schema.sqlite.sql
```

The scrapers' SQL is translated to SQLite at the cursor (`scrape/db.py`), so
the same code runs against both backends.

Environment variables:

- `SOOGLE_DB_ENGINE` — `mysql` or `sqlite` (optional; auto-detected)
- `SOOGLE_DB_PATH` — SQLite database file (default `soogle.db`)
- `SOOGLE_DB_PASS` — MySQL password; its presence selects MySQL
- `GITHUB_TOKEN` — GitHub API token (required for github scraper)
- `SERPAPI_KEY` — SerpAPI key (required for weekly.bash: discovery + youtube)
- `ANTHROPIC_API_KEY` — Anthropic API key (required for LLM review, analyze)

## Running

```bash
# Install dependencies and git hooks
mise install
mise run install        # uv sync

# Run the daily pipeline
./daily.bash

# Run the weekly pipeline (requires SERPAPI_KEY)
./weekly.bash

# Run individual commands
uv run python -m scrape github --incremental
uv run python -m scrape process
uv run python -m scrape llm-review --model claude-haiku-4-5-20251001 --scope unreviewed

# Run the web server
mise run server         # uv run python web/manage.py runserver

# Run the MCP server (stdio)
mise run mcp            # uv run --group mcp python -m mcp_server

# Tests and lint
mise run test
mise run lint
mise run hooks          # prek run (all git hooks)
```

## Related

Other repos in this collection:

- [smalltalk80-2026](https://github.com/avwohl/smalltalk80-2026) — C++17 virtual machine for Smalltalk-80 on macOS, Mac Catalyst, Linux, and Windows. It boots the 1983 Xerox virtual image to the desktop.
- [iospharo](https://github.com/avwohl/iospharo) — Virtual machine for Pharo Smalltalk on iOS and macOS. The C++ interpreter runs Pharo 13 and Pharo 14 images without a just-in-time compiler, and it uses low-bit oop encoding to work with ASLR.
- [validate_smalltalk_image](https://github.com/avwohl/validate_smalltalk_image) — Standalone validator and export tool for Spur-format Smalltalk images. It checks the heap, and it writes SHA-256 manifests and reference graphs.
- [pharo-headless-test](https://github.com/avwohl/pharo-headless-test) — Headless test runner for Pharo Smalltalk with a fake GUI. It clicks menus, takes screenshots, and runs the SUnit suite without a display.
- [claude-skills](https://github.com/avwohl/claude-skills) — Collection of open source skills for Claude Code. Each skill is a Markdown file in `.claude/skills/` that holds reusable knowledge and algorithms.

## License

GPL-3.0 — see [LICENSE](LICENSE) for details.
