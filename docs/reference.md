# Project Structure and CLI Reference

## Project structure

```
soogle/
  daily.bash              Daily cron script (free scrapers + processing)
  weekly.bash             Weekly cron script (paid APIs + daily.bash)
  requirements.txt        requests, pymysql, beautifulsoup4
  db/schema.sql           Full database schema and seed data
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
  www/                    Static files (CSS, images)
```

## CLI reference

```
python -m scrape github [--incremental | --since YYYY-MM-DD]
python -m scrape web <source>                    # squeaksource | smalltalkhub | rosettacode | vskb | all
python -m scrape custom <source>                 # squeakmap | lukas_renggli | sourceforge | launchpad | all
python -m scrape youtube [--playlists-only]
python -m scrape discover <engine>               # brave | serpapi | bing | ddg
python -m scrape process [--limit N]
python -m scrape analyze [--limit N] [--show] [--min-score 50]
python -m scrape llm-review [--model M] [--scope S] [--limit N] [--fetch-only] [--review-only]
python -m scrape video-review [--model M] [--scope S] [--limit N]
python -m scrape block <external_id> [--site github] [--reason '...']
python -m scrape status
```
