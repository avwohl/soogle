"""Database helpers for Soogle scrapers.

Uses SQLAlchemy Core so the same statements run on MySQL (default) and
SQLite.  Set SOOGLE_DB_ENGINE=sqlite to use a local SQLite file
(SOOGLE_DB_PATH, default soogle.db) instead of a MySQL server.  Table
metadata lives in scrape/schema.py; the dialect-specific upsert helpers
below are the only place that knows about MySQL vs SQLite.
"""

import hashlib
import json
from contextlib import contextmanager
from pathlib import Path

from sqlalchemy import create_engine, event, func, select
from sqlalchemy.dialects import mysql as mysql_dialect
from sqlalchemy.dialects import sqlite as sqlite_dialect
from sqlalchemy.exc import IntegrityError  # noqa: F401  (re-exported for callers)
from sqlalchemy import inspect

from . import config
from .schema import blocklist, packages, scrape_jobs, scrape_raw, sites

_BLOCKLIST = None

if config.DB_ENGINE == "sqlite":
    engine = create_engine(f"sqlite:///{config.DB_PATH}")

    @event.listens_for(engine, "connect")
    def _enable_foreign_keys(dbapi_conn, connection_record):
        dbapi_conn.execute("PRAGMA foreign_keys = ON")

    # A fresh SQLite file has no tables; apply the schema (tables + seed data)
    # once so a new checkout or CI run works without a manual step.
    if not inspect(engine).get_table_names():
        schema = Path(__file__).resolve().parent.parent / "db" / "schema.sqlite.sql"
        with engine.raw_connection() as conn:
            conn.executescript(schema.read_text())
else:
    engine = create_engine(
        f"mysql+pymysql://{config.DB_USER}:{config.DB_PASS}"
        f"@{config.DB_HOST}:{config.DB_PORT}/{config.DB_NAME}?charset=utf8mb4"
    )


def upsert(table, values, conflict_cols):
    """Build an INSERT ... ON DUPLICATE KEY UPDATE (MySQL) / ON CONFLICT
    DO UPDATE (SQLite) statement.  All non-conflict columns in *values*
    are updated on conflict."""
    if engine.dialect.name == "sqlite":
        stmt = sqlite_dialect.insert(table).values(**values)
        return stmt.on_conflict_do_update(
            index_elements=conflict_cols,
            set_={c: stmt.excluded[c] for c in values if c in table.c},
        )
    stmt = mysql_dialect.insert(table).values(**values)
    return stmt.on_duplicate_key_update(
        {c: stmt.inserted[c] for c in values if c in table.c}
    )


def insert_ignore(table, values, conflict_cols):
    """Build an INSERT IGNORE (MySQL) / ON CONFLICT DO NOTHING (SQLite)
    statement."""
    if engine.dialect.name == "sqlite":
        stmt = sqlite_dialect.insert(table).values(**values)
        return stmt.on_conflict_do_nothing(index_elements=conflict_cols)
    return mysql_dialect.insert(table).values(**values).prefix_with("IGNORE")


<<<<<<< HEAD
=======
# Sites are seeded here, not in the schema files: new scrapers add sites
# after a DB already exists (the release DB is restored, not recreated), so
# the seed must be re-applied idempotently on every startup.
_SITES = [
    ("github", "GitHub", "https://github.com", "git_host", "github_api"),
    ("smalltalkhub", "SmalltalkHub", "http://smalltalkhub.com", "archive", "http_crawl"),
    ("squeaksource", "SqueakSource", "http://squeaksource.com", "archive", "http_crawl"),
    ("squeaksource3", "SqueakSource3", "http://ss3.gemstone.com", "archive", "http_crawl"),
    ("squeakmap", "SqueakMap", "http://map.squeak.org", "catalog", "http_crawl"),
    ("gitlab", "GitLab", "https://gitlab.com", "git_host", "gitlab_api"),
    ("sourceforge", "SourceForge", "https://sourceforge.net", "archive", "http_crawl"),
    ("rosettacode", "Rosetta Code", "https://rosettacode.org", "web", "http_crawl"),
    ("vskb", "VS Knowledge Base", "https://vs-kb.archiv.apis.de", "archive", "http_crawl"),
    ("web_discovered", "Web Discovered", "", "web", "discovery"),
    ("lukas_renggli", "Lukas Renggli", "https://source.lukas-renggli.ch", "archive", "http_crawl"),
    ("launchpad", "Launchpad", "https://code.launchpad.net", "git_host", "http_crawl"),
    ("squeaktrunk", "Squeak Trunk", "https://source.squeak.org", "archive", "http_crawl"),
]

if "sites" in inspect(engine).get_table_names():
    with engine.begin() as conn:
        for name, display, url, stype, method in _SITES:
            conn.execute(insert_ignore(sites, {
                "name": name,
                "display_name": display,
                "base_url": url,
                "site_type": stype,
                "scrape_method": method,
            }, ["name"]))

# Schema drift: columns added after a DB was created (the release DB is
# restored, not recreated). Guard on the column's absence.
if "packages" in inspect(engine).get_table_names():
    cols = {c["name"] for c in inspect(engine).get_columns("packages")}
    if "llm_review" not in cols:
        with engine.begin() as conn:
            conn.exec_driver_sql("ALTER TABLE packages ADD COLUMN llm_review VARCHAR(100) NULL")


@contextmanager
def connection():
    with engine.connect() as conn:
        yield conn


@contextmanager
def transaction(conn):
    """Context manager that commits on success, rolls back on exception."""
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise


def load_blocklist(conn):
    """Load the set of blocked (site_name, external_id) pairs from the blocklist table."""
    global _BLOCKLIST
    if _BLOCKLIST is not None:
        return _BLOCKLIST
    rows = conn.execute(select(blocklist.c.site_name, blocklist.c.external_id)).mappings().fetchall()
    _BLOCKLIST = {(r["site_name"], r["external_id"]) for r in rows}
    return _BLOCKLIST


def is_blocked(conn, site_name, external_id):
    """Check if an external_id is on the blocklist."""
    return (site_name, external_id) in load_blocklist(conn)


def get_site_id(conn, site_name):
    row = conn.execute(
        select(sites.c.id).where(sites.c.name == site_name)
    ).mappings().fetchone()
    if not row:
        raise ValueError(f"Unknown site: {site_name}")
    return row["id"]


def get_site_name(conn, site_id):
    row = conn.execute(
        select(sites.c.name).where(sites.c.id == site_id)
    ).mappings().fetchone()
    return row["name"] if row else None


def create_scrape_job(conn, site_id, job_type="full_crawl"):
    result = conn.execute(
        scrape_jobs.insert().values(
            site_id=site_id, job_type=job_type, status="running",
            started_at=func.now(),
        )
    )
    conn.commit()
    return result.inserted_primary_key[0]


def finish_scrape_job(conn, job_id, items_found, items_processed, items_failed, error=None):
    status = "failed" if error else "completed"
    conn.execute(
        scrape_jobs.update()
        .where(scrape_jobs.c.id == job_id)
        .values(
            status=status, completed_at=func.now(),
            items_found=items_found, items_processed=items_processed,
            items_failed=items_failed, error_message=error,
        )
    )
    conn.commit()


def compute_checksum(data):
    """SHA-256 of the JSON-serialized data, for change detection."""
    raw = json.dumps(data, sort_keys=True, default=str)
    return hashlib.sha256(raw.encode()).hexdigest()


def insert_scrape_raw(conn, job_id, site_id, external_id, raw_metadata):
    """Insert a raw scraped record.  Returns the row id, or None if an
    identical checksum for this source was already processed."""
    checksum = compute_checksum(raw_metadata)

    row = conn.execute(
        select(scrape_raw.c.id)
        .select_from(scrape_raw.join(packages, packages.c.id == scrape_raw.c.package_id))
        .where(
            scrape_raw.c.site_id == site_id,
            scrape_raw.c.external_id == external_id,
            packages.c.scrape_checksum == checksum,
            scrape_raw.c.status == "processed",
        )
        .limit(1)
    ).fetchone()
    if row:
        return None  # unchanged, skip

    result = conn.execute(
        scrape_raw.insert().values(
            scrape_job_id=job_id, site_id=site_id, external_id=external_id,
            raw_metadata=json.dumps(raw_metadata, default=str),
            raw_checksum=checksum,
        )
    )
    conn.commit()
    return result.inserted_primary_key[0]


def fetch_pending_raw(conn, limit=100):
    """Fetch a batch of pending scrape_raw rows for processing."""
    rows = conn.execute(
        select(
            scrape_raw.c.id, scrape_raw.c.scrape_job_id, scrape_raw.c.site_id,
            scrape_raw.c.external_id, scrape_raw.c.raw_metadata,
            scrape_raw.c.raw_checksum,
        )
        .where(scrape_raw.c.status == "pending")
        .order_by(scrape_raw.c.id)
        .limit(limit)
    ).mappings().fetchall()
    if rows:
        ids = [r["id"] for r in rows]
        conn.execute(
            scrape_raw.update()
            .where(scrape_raw.c.id.in_(ids))
            .values(status="processing")
        )
        conn.commit()
    return rows