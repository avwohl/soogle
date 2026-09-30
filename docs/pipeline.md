# Data Pipeline

The pipeline scrapes Smalltalk packages and videos, processes the rows into packages, reviews the packages with an LLM, and serves the result on the web site.

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

`python -m scrape process` reads pending rows from `scrape_raw` and for each one:

- Detects the Smalltalk **dialect** from GitHub topics and name/description keywords (pharo, squeak, cuis, etc.) with a confidence score
- **Auto-categorizes** into one or more of 19 categories (web, database, testing, ui/graphics, etc.) via keyword matching
- Applies **quality gates** — skips repos with no description, no stars, and unknown dialect; rejects names matching known non-Smalltalk patterns (Arduino, TensorFlow, Unity, etc.)
- Does an **atomic upsert** — inserts/updates the package and its categories in a single transaction so data is always consistent

### 3. LLM review — filter false positives

`python -m scrape llm-review` sends batches of packages to Claude for quality review. The LLM catches false positives that regex alone misses:

- C# / .NET projects (GitHub's linguist confuses `.cs` changesets with Smalltalk)
- IEC 61131-3 Structured Text / PLC code (`.st` extension overlap)
- ML/NLP research using StringTemplate `.st` files
- Unity game projects

Packages marked "block" are added to a blocklist and deleted. Packages marked "keep" are stamped with the model name that reviewed them.

`python -m scrape video-review` does the same for videos — blocks conversation-skills videos, design-pattern talks that only mention Smalltalk in passing, GemStone jewelry content, and spam.

Both commands support a **model tier system** (haiku < sonnet < opus). The `--scope upgrade` flag re-reviews items that were previously reviewed by a lower-tier model, so you can upgrade quality without reprocessing everything.

### 4. Serve — Django web frontend

The Django app reads directly from the MySQL database:

- **Home page** — package count, video count, dialect breakdown, recently added packages
- **Search** — full-text search with filters for dialect, source site, and category; sort by relevance, stars, update date, or name
- **Package detail** — description, README excerpt, metadata (license, stars, forks, topics, dates), categories
- **Videos** — searchable gallery with dialect filter, sort by views or date
- **Sources** — lists all indexed sites with package counts
- **Submit** — users can submit new Smalltalk URLs for indexing

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
