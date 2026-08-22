"""Configuration for Soogle scrapers."""

import os
from pathlib import Path

def _load_env_file(path):
    """Read KEY=VALUE lines from .env into the environment.

    Deliberately not python-dotenv. That package is installed only in a
    user-local site-packages here, which the mod_wsgi daemon - running as
    www-data - cannot read, so `from dotenv import load_dotenv` raised
    ImportError, the except swallowed it, and .env was never loaded at all.
    Nobody noticed while the password had a hardcoded default to fall back
    on. A dozen lines of parsing removes both the dependency and the trap.

    An existing environment variable wins, so an explicit export still
    overrides the file.
    """
    try:
        text = path.read_text()
    except OSError:
        return
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[len("export "):]
        key, sep, value = line.partition("=")
        if not sep:
            continue
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
            value = value[1:-1]
        os.environ.setdefault(key.strip(), value)


# Load .env from the repo root.
_load_env_file(Path(__file__).resolve().parent.parent / ".env")

# MySQL
DB_HOST = os.environ.get("SOOGLE_DB_HOST", "127.0.0.1")
DB_PORT = int(os.environ.get("SOOGLE_DB_PORT", "3306"))
DB_USER = os.environ.get("SOOGLE_DB_USER", "root")
# No default: the database password belongs in .env (gitignored), not in
# the source. A missing value fails here rather than surfacing later as an
# "Access denied for user" traceback from deep inside a scrape.
DB_PASS = os.environ.get("SOOGLE_DB_PASS")
if not DB_PASS:
    raise RuntimeError(
        "SOOGLE_DB_PASS is not set. Add it to the .env file in the repo root."
    )
DB_NAME = os.environ.get("SOOGLE_DB_NAME", "soogle")

# GitHub
GITHUB_TOKEN = os.environ.get("GH_TOKEN", "")
GITHUB_API = "https://api.github.com"
GITHUB_SEARCH_PER_PAGE = 100
GITHUB_SEARCH_PAUSE = 2.5          # seconds between search requests (30/min limit)
GITHUB_API_PAUSE = 0.8             # seconds between general API requests

# Web scraping
REQUEST_TIMEOUT = 30
USER_AGENT = "Soogle/0.2 (Smalltalk code search engine)"

# Web search backends for discovery (tried in order: first configured one wins)
# Brave Search API  (free tier: 2,000 queries/month)
BRAVE_API_KEY = os.environ.get("BRAVE_API_KEY", "")
BRAVE_RESULTS_PER_QUERY = 20

# SerpAPI  (wraps Google results; free tier: 100 searches/month)
SERPAPI_KEY = os.environ.get("SERPAPI_KEY", "")
SERPAPI_RESULTS_PER_QUERY = 20

# Bing Web Search API  (free tier: 1,000 calls/month)
BING_API_KEY = os.environ.get("BING_API_KEY", "")
BING_RESULTS_PER_QUERY = 20

# LLM analysis (for site structure assessment)
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
ANALYZE_MODEL = "claude-sonnet-4-6"

# Processing
PROCESS_BATCH_SIZE = 100
ACTIVE_THRESHOLD_DAYS = 365        # repos pushed within this many days are "active"

# User submissions / email notifications
HCAPTCHA_SITEKEY = os.environ.get("HCAPTCHA_SITEKEY", "")
HCAPTCHA_SECRET = os.environ.get("HCAPTCHA_SECRET", "")
SUBMISSION_EMAIL_TO = os.environ.get("SUBMISSION_EMAIL_TO", "")
SUBMISSION_EMAIL_FROM = os.environ.get("SUBMISSION_EMAIL_FROM", "noreply@soogle.org")
EMAIL_HOST = os.environ.get("EMAIL_HOST", "localhost")
EMAIL_PORT = int(os.environ.get("EMAIL_PORT", "25"))
