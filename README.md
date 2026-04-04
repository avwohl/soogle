# Soogle

Search engine for Smalltalk source code.

Soogle indexes packages, classes, and methods from Smalltalk repositories across multiple dialects (Pharo, Squeak, GemStone, and more) and makes them searchable through a clean web interface.

## Features

- Full-text search across Smalltalk packages, classes, and methods
- Filter by dialect, source site, or package type
- Browse indexed sources and recently added packages
- Smalltalk video index (Pharo MOOC, conference talks, tutorials)
- Submit new Smalltalk sites for indexing

## Architecture

- **scrape/** — Scrapers that pull package metadata from GitHub, SmalltalkHub, and other sources into a MySQL database
- **web/** — Django app serving the search UI
- **db/** — Database schema and migrations

## Requirements

- Python 3.10+
- MySQL / MariaDB
- Dependencies: `pip install -r requirements.txt`

## Running

```bash
# Scrape sources
python -m scrape

# Run the web server
cd web
python manage.py runserver
```

## License

GPL-3.0 — see [LICENSE](LICENSE) for details.
