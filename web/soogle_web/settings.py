"""Django settings for soogle_web project."""

import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent

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


# Load .env from the repo root (one level above BASE_DIR / web/).
_load_env_file(BASE_DIR.parent / ".env")

def _required(name):
    """A setting with no safe default - the password belongs in .env."""
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(
            f"{name} is not set. Add it to the .env file in the repo root; "
            "mod_wsgi does not inherit the shell environment."
        )
    return value


SECRET_KEY = os.environ.get(
    "DJANGO_SECRET_KEY",
    "uAAsFWLtsEb0IaP6p995yqb1n1kZGo8ApXZtMRL5hHXyoNLvqWzgsq017H2fNWqtkUc",
)

DEBUG = os.environ.get("DJANGO_DEBUG", "0") == "1"

ALLOWED_HOSTS = ["soogle.org", "www.soogle.org", "localhost", "127.0.0.1"]

INSTALLED_APPS = [
    "django.contrib.staticfiles",
    "search",
]

MIDDLEWARE = [
    "django.middleware.security.SecurityMiddleware",
    "django.middleware.common.CommonMiddleware",
    "django.middleware.csrf.CsrfViewMiddleware",
]

ROOT_URLCONF = "soogle_web.urls"

TEMPLATES = [
    {
        "BACKEND": "django.template.backends.django.DjangoTemplates",
        "DIRS": [],
        "APP_DIRS": True,
        "OPTIONS": {
            "context_processors": [
                "django.template.context_processors.request",
                "search.context_processors.site_url",
            ],
        },
    },
]

WSGI_APPLICATION = "soogle_web.wsgi.application"

DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.mysql",
        "NAME": os.environ.get("SOOGLE_DB_NAME", "soogle"),
        "USER": os.environ.get("SOOGLE_DB_USER", "root"),
        "PASSWORD": _required("SOOGLE_DB_PASS"),
        "HOST": os.environ.get("SOOGLE_DB_HOST", "127.0.0.1"),
        "PORT": os.environ.get("SOOGLE_DB_PORT", "3306"),
        "OPTIONS": {"charset": "utf8mb4"},
    }
}

LANGUAGE_CODE = "en-us"
TIME_ZONE = "UTC"
USE_I18N = False
USE_TZ = True

STATIC_URL = "/static/"
STATIC_ROOT = Path(__file__).resolve().parent.parent.parent / "www" / "static"

DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"

# --- hCaptcha (used on /submit/ form) ---
HCAPTCHA_SITEKEY = os.environ.get("HCAPTCHA_SITEKEY", "")
HCAPTCHA_SECRET = os.environ.get("HCAPTCHA_SECRET", "")

# --- Email (notifications when a user submits a URL) ---
EMAIL_BACKEND = os.environ.get(
    "EMAIL_BACKEND", "django.core.mail.backends.smtp.EmailBackend"
)
EMAIL_HOST = os.environ.get("EMAIL_HOST", "localhost")
EMAIL_PORT = int(os.environ.get("EMAIL_PORT", "25"))
EMAIL_HOST_USER = os.environ.get("EMAIL_HOST_USER", "")
EMAIL_HOST_PASSWORD = os.environ.get("EMAIL_HOST_PASSWORD", "")
EMAIL_USE_TLS = os.environ.get("EMAIL_USE_TLS", "0") == "1"
DEFAULT_FROM_EMAIL = os.environ.get("SUBMISSION_EMAIL_FROM", "noreply@soogle.org")
SUBMISSION_EMAIL_TO = os.environ.get("SUBMISSION_EMAIL_TO", "")
