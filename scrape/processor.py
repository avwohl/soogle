"""Processor: moves data from scrape_raw into packages and related tables.

Reads pending/processing rows from scrape_raw, parses the raw metadata,
detects dialect, auto-categorizes, and atomically upserts into:
    packages, package_classes, package_methods, package_categories

Usage:
    python -m scrape process [--limit 100]
"""

import re
import json
import logging
from datetime import datetime, timedelta
from . import config, db

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Dialect detection heuristics
# ---------------------------------------------------------------------------
DIALECT_TOPIC_MAP = {
    "pharo": "pharo",
    "squeak": "squeak",
    "cuis": "cuis",
    "cuis-smalltalk": "cuis",
    "gnu-smalltalk": "gnu_smalltalk",
    "gst": "gnu_smalltalk",
    "gemstone": "gemstone",
    "gemstone-s": "gemstone",
    "gsdevkit": "gemstone",
    "topaz": "gemstone",
    "visualworks": "visualworks",
    "vw": "visualworks",
    "cincom": "visualworks",
    "dolphin": "dolphin",
    "dolphin-smalltalk": "dolphin",
    "va-smalltalk": "va_smalltalk",
    "vast": "va_smalltalk",
    "envy": "va_smalltalk",
    "smalltalk-80": "st80",
    "st80": "st80",
    "smalltalk/x": "st80",
}


def detect_dialect(meta):
    """Detect Smalltalk dialect from metadata.  Returns (dialect, confidence)."""
    scores = {}

    # Check topics
    topics = meta.get("topics") or []
    if isinstance(topics, str):
        try:
            topics = json.loads(topics)
        except (json.JSONDecodeError, TypeError):
            topics = []

    for topic in topics:
        t = topic.lower().strip()
        if t in DIALECT_TOPIC_MAP:
            dialect = DIALECT_TOPIC_MAP[t]
            scores[dialect] = scores.get(dialect, 0) + 40

    # Check name and description for dialect keywords
    text = " ".join([
        meta.get("name", ""),
        meta.get("description", "") or "",
        meta.get("qualified_name", "") or "",
    ]).lower()

    dialect_keywords = {
        "pharo": ["pharo", "baseline", "metacello"],
        "squeak": ["squeak", "etoys", "morphic"],
        "cuis": ["cuis"],
        "gnu_smalltalk": ["gnu smalltalk", "gnu-smalltalk", "gst"],
        "gemstone": ["gemstone", "gsdevkit", "topaz", "seaside/gemstone"],
        "visualworks": ["visualworks", "cincom", "parcel"],
        "dolphin": ["dolphin"],
        "va_smalltalk": ["va smalltalk", "vast", "instantiations"],
    }
    for dialect, keywords in dialect_keywords.items():
        for kw in keywords:
            if kw in text:
                scores[dialect] = scores.get(dialect, 0) + 25

    if not scores:
        return "unknown", 0

    best = max(scores, key=scores.get)
    confidence = min(scores[best], 95)
    return best, confidence


# ---------------------------------------------------------------------------
# Auto-categorization
# ---------------------------------------------------------------------------
CATEGORY_RULES = {
    "web": ["seaside", "zinc", "teapot", "rest", "http server", "web framework"],
    "database": ["glorp", "magma", "omnibase", "sqlite", "sql", "database", "postgres", "mysql", "mongo"],
    "ui_graphics": ["morphic", "spec", "roassal", "bloc", "graphics", "widget", "gui", "ui framework"],
    "testing": ["sunit", "test", "mocketry", "mutation testing", "coverage"],
    "ide_dev_tools": ["refactoring", "browser", "linter", "debugger", "inspector", "dev tool"],
    "networking": ["socket", "smtp", "dns", "ssh", "websocket", "tcp", "udp", "network"],
    "scientific": ["polymath", "statistics", "machine learning", "data science"],
    "games": ["game", "retro", "engine", "etoys", "arcade"],
    "education": ["tutorial", "example", "learning", "howto", "course", "lesson", "teach"],
    "serialization": ["json", "xml", "ston", "csv", "messagepack", "yaml", "parser", "serializ"],
    "cloud_infra": ["aws", "docker", "ci/cd", "cloud", "kubernetes", "deploy"],
    "system_os": ["ffi", "file system", "process", "os", "system call", "native"],
    "math": ["linear algebra", "cryptograph", "numeric", "math", "matrix", "biginteger"],
    "multimedia": ["sound", "image process", "animation", "audio", "video", "midi"],
    "language_extensions": ["trait", "pragma", "compiler", "extension", "metalink", "reflecti"],
    "concurrency": ["actor", "promise", "parallel", "concurrent", "async", "future"],
    "iot_hardware": ["gpio", "embedded", "sensor", "raspberry", "arduino", "iot"],
    "packaging_vcs": ["metacello", "monticello", "iceberg", "tonel", "git", "package manager"],
}


def auto_categorize(meta):
    """Return list of (category_name, confidence) tuples."""
    text = " ".join([
        meta.get("name", ""),
        meta.get("description", "") or "",
        " ".join(meta.get("topics") or []) if isinstance(meta.get("topics"), list) else "",
    ]).lower()

    matches = []
    for cat, keywords in CATEGORY_RULES.items():
        score = 0
        for kw in keywords:
            if kw in text:
                score += 30
        if score > 0:
            matches.append((cat, min(score, 90)))

    return matches


# ---------------------------------------------------------------------------
# Process one scrape_raw row into packages
# ---------------------------------------------------------------------------
def _parse_github(meta):
    """Normalize GitHub API response into our package fields."""
    return {
        "name": meta.get("name", ""),
        "qualified_name": meta.get("full_name", ""),
        "description": (meta.get("description") or "")[:5000],
        "url": meta.get("html_url", ""),
        "clone_url": meta.get("clone_url", ""),
        "stars": meta.get("stargazers_count", 0) or 0,
        "forks": meta.get("forks_count", 0) or 0,
        "size_kb": meta.get("size", 0) or 0,
        "license": (meta.get("license") or {}).get("spdx_id") if isinstance(meta.get("license"), dict) else None,
        "is_fork": bool(meta.get("fork")),
        "is_archived": bool(meta.get("archived")),
        "default_branch": meta.get("default_branch", ""),
        "topics": json.dumps(meta.get("topics", [])),
        "source_created_at": meta.get("created_at"),
        "source_updated_at": meta.get("updated_at"),
        "source_pushed_at": meta.get("pushed_at"),
    }


def _parse_web(meta):
    """Normalize web-scraped metadata into our package fields."""
    return {
        "name": meta.get("name", ""),
        "qualified_name": meta.get("qualified_name") or meta.get("name", ""),
        "description": (meta.get("description") or "")[:5000],
        "url": meta.get("url", ""),
        "clone_url": None,
        "stars": 0,
        "forks": 0,
        "size_kb": 0,
        "license": None,
        "is_fork": False,
        "is_archived": False,
        "default_branch": None,
        "topics": json.dumps([]),
        "source_created_at": None,
        "source_updated_at": None,
        "source_pushed_at": None,
    }


def _parse_timestamp(val):
    """Parse a timestamp string into MySQL-compatible format, or None."""
    if not val:
        return None
    if isinstance(val, datetime):
        return val.strftime("%Y-%m-%d %H:%M:%S")
    # GitHub format: 2024-01-15T10:30:00Z
    for fmt in ("%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(val, fmt).strftime("%Y-%m-%d %H:%M:%S")
        except (ValueError, TypeError):
            continue
    return None


def _is_active(pushed_at):
    """Is the package considered active based on last push date?"""
    if not pushed_at:
        return False
    ts = _parse_timestamp(pushed_at)
    if not ts:
        return False
    pushed = datetime.strptime(ts, "%Y-%m-%d %H:%M:%S")
    return (datetime.utcnow() - pushed).days <= config.ACTIVE_THRESHOLD_DAYS


def process_one(conn, raw_row):
    """Process a single scrape_raw row.  Atomic: all-or-nothing."""
    raw_id = raw_row["id"]
    site_id = raw_row["site_id"]
    external_id = raw_row["external_id"]
    checksum = raw_row["raw_checksum"]

    meta = raw_row["raw_metadata"]
    if isinstance(meta, str):
        meta = json.loads(meta)

    # Determine parser based on site
    site_name = db.get_site_name(conn, site_id)
    if site_name == "github":
        pkg = _parse_github(meta)
    else:
        pkg = _parse_web(meta)

    # Dialect detection
    dialect, dialect_confidence = detect_dialect(meta)

    # Auto-categorize
    categories = auto_categorize(meta)

    # Determine if active
    active = _is_active(pkg.get("source_pushed_at"))

    # Atomic upsert
    with db.transaction(conn):
        cur = conn.cursor()

        # Upsert package
        cur.execute("""
            INSERT INTO packages (
                name, qualified_name, description,
                dialect, dialect_confidence, file_format,
                site_id, external_id, url, clone_url,
                stars, forks, size_kb, license,
                is_fork, is_archived, default_branch, topics,
                source_created_at, source_updated_at, source_pushed_at,
                is_active, last_scraped_at, scrape_checksum
            ) VALUES (
                %s, %s, %s,
                %s, %s, 'unknown',
                %s, %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s,
                %s, NOW(), %s
            )
            ON DUPLICATE KEY UPDATE
                name = VALUES(name),
                qualified_name = VALUES(qualified_name),
                description = VALUES(description),
                dialect = VALUES(dialect),
                dialect_confidence = VALUES(dialect_confidence),
                url = VALUES(url),
                clone_url = VALUES(clone_url),
                stars = VALUES(stars),
                forks = VALUES(forks),
                size_kb = VALUES(size_kb),
                license = VALUES(license),
                is_fork = VALUES(is_fork),
                is_archived = VALUES(is_archived),
                default_branch = VALUES(default_branch),
                topics = VALUES(topics),
                source_created_at = VALUES(source_created_at),
                source_updated_at = VALUES(source_updated_at),
                source_pushed_at = VALUES(source_pushed_at),
                is_active = VALUES(is_active),
                last_scraped_at = NOW(),
                scrape_checksum = VALUES(scrape_checksum)
        """, (
            pkg["name"], pkg["qualified_name"], pkg["description"],
            dialect, dialect_confidence,
            site_id, external_id, pkg["url"], pkg["clone_url"],
            pkg["stars"], pkg["forks"], pkg["size_kb"], pkg["license"],
            pkg["is_fork"], pkg["is_archived"], pkg["default_branch"], pkg["topics"],
            _parse_timestamp(pkg["source_created_at"]),
            _parse_timestamp(pkg["source_updated_at"]),
            _parse_timestamp(pkg["source_pushed_at"]),
            active, checksum,
        ))

        # Get the package id (works for both insert and update)
        cur.execute(
            "SELECT id FROM packages WHERE site_id = %s AND external_id = %s",
            (site_id, external_id),
        )
        package_id = cur.fetchone()["id"]

        # Clear and re-insert related data
        cur.execute("DELETE FROM package_methods WHERE package_id = %s", (package_id,))
        cur.execute("DELETE FROM package_classes WHERE package_id = %s", (package_id,))
        cur.execute("DELETE FROM package_categories WHERE package_id = %s", (package_id,))

        # Insert categories
        for cat_name, confidence in categories:
            cur.execute("SELECT id FROM categories WHERE name = %s", (cat_name,))
            cat_row = cur.fetchone()
            if cat_row:
                cur.execute(
                    "INSERT INTO package_categories (package_id, category_id, confidence) "
                    "VALUES (%s, %s, %s)",
                    (package_id, cat_row["id"], confidence),
                )

        # Mark scrape_raw as processed
        cur.execute(
            "UPDATE scrape_raw SET status='processed', processed_at=NOW(), package_id=%s "
            "WHERE id = %s",
            (package_id, raw_id),
        )

        cur.close()
        return package_id


def process_batch(conn, limit=None):
    """Process a batch of pending scrape_raw rows."""
    if limit is None:
        limit = config.PROCESS_BATCH_SIZE

    rows = db.fetch_pending_raw(conn, limit)
    if not rows:
        log.info("No pending rows to process")
        return {"processed": 0, "errors": 0}

    processed = 0
    errors = 0
    for row in rows:
        try:
            pkg_id = process_one(conn, row)
            processed += 1
            log.debug("Processed raw %d -> package %d", row["id"], pkg_id)
        except Exception as e:
            errors += 1
            log.error("Failed to process raw id %d (%s): %s", row["id"], row["external_id"], e)
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE scrape_raw SET status='failed', error_message=%s WHERE id=%s",
                        (str(e)[:5000], row["id"]),
                    )
                    conn.commit()
            except Exception:
                pass

    log.info("Batch done: processed=%d errors=%d", processed, errors)
    return {"processed": processed, "errors": errors}


def process_all(conn):
    """Process all pending rows in batches."""
    total_processed = 0
    total_errors = 0
    while True:
        result = process_batch(conn)
        total_processed += result["processed"]
        total_errors += result["errors"]
        if result["processed"] == 0 and result["errors"] == 0:
            break
    log.info("All done: processed=%d errors=%d", total_processed, total_errors)
    return {"processed": total_processed, "errors": total_errors}
