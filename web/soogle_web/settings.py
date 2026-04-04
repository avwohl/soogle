"""Django settings for soogle_web project."""

import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent

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
        "PASSWORD": os.environ.get("SOOGLE_DB_PASS", "xrain"),
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
