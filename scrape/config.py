"""Configuration for Soogle scrapers."""

import os
from pathlib import Path

# Load .env from the repo root if present.  Safe no-op if python-dotenv
# isn't installed or .env doesn't exist.
try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).resolve().parent.parent / ".env")
except ImportError:
    pass

# MySQL
DB_HOST = os.environ.get("SOOGLE_DB_HOST", "127.0.0.1")
DB_PORT = int(os.environ.get("SOOGLE_DB_PORT", "3306"))
DB_USER = os.environ.get("SOOGLE_DB_USER", "root")
DB_PASS = os.environ.get("SOOGLE_DB_PASS", "[elided]")
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
