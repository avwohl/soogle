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
## Requirements

- Python 3.10+
- MySQL / MariaDB
- `pip install -r requirements.txt` (requests, pymysql, beautifulsoup4)

Environment variables:

- `SOOGLE_DB_PASS` — MySQL password
- `GH_TOKEN` — GitHub API token (required for github scraper)
- `SERPAPI_KEY` — SerpAPI key (required for weekly.bash: discovery + youtube)
- `ANTHROPIC_API_KEY` — Anthropic API key (required for LLM review, analyze)

## Running

```bash
# Run the daily pipeline
./daily.bash

# Run the weekly pipeline (requires SERPAPI_KEY)
./weekly.bash

# Run individual commands
python -m scrape github --incremental
python -m scrape process
python -m scrape llm-review --model claude-haiku-4-5-20251001 --scope unreviewed

# Run the web server
cd web
python manage.py runserver
```

## Documentation

- [Data pipeline](docs/pipeline.md) - data sources, the four pipeline phases, and what `daily.bash` and `weekly.bash` run.
- [Project structure and CLI reference](docs/reference.md) - source layout and every `python -m scrape` command.
- [Feasibility study](docs/feasibility.md) - the original 2026 study of Smalltalk code sources.

## Related

Other repos in this collection:

- [smalltalk80-2026](https://github.com/avwohl/smalltalk80-2026) — C++17 virtual machine for Smalltalk-80 on macOS, Mac Catalyst, Linux, and Windows. It boots the 1983 Xerox virtual image to the desktop.
- [iospharo](https://github.com/avwohl/iospharo) — Virtual machine for Pharo Smalltalk on iOS and macOS. The C++ interpreter runs Pharo 13 and Pharo 14 images without a just-in-time compiler, and it uses low-bit oop encoding to work with ASLR.
- [validate_smalltalk_image](https://github.com/avwohl/validate_smalltalk_image) — Standalone validator and export tool for Spur-format Smalltalk images. It checks the heap, and it writes SHA-256 manifests and reference graphs.
- [pharo-headless-test](https://github.com/avwohl/pharo-headless-test) — Headless test runner for Pharo Smalltalk with a fake GUI. It clicks menus, takes screenshots, and runs the SUnit suite without a display.
- [claude-skills](https://github.com/avwohl/claude-skills) — Collection of open source skills for Claude Code. Each skill is a Markdown file in `.claude/skills/` that holds reusable knowledge and algorithms.

## License

GPL-3.0 — see [LICENSE](LICENSE) for details.
