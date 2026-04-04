"""Database helpers for Soogle scrapers."""

import json
import hashlib
import pymysql
from contextlib import contextmanager
from . import config


def connect():
    return pymysql.connect(
        host=config.DB_HOST,
        port=config.DB_PORT,
        user=config.DB_USER,
        password=config.DB_PASS,
        database=config.DB_NAME,
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=False,
    )


@contextmanager
def connection():
    conn = connect()
    try:
        yield conn
    finally:
        conn.close()


@contextmanager
def transaction(conn):
    """Context manager that commits on success, rolls back on exception."""
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise


def get_site_id(conn, site_name):
    with conn.cursor() as cur:
        cur.execute("SELECT id FROM sites WHERE name = %s", (site_name,))
        row = cur.fetchone()
        if not row:
            raise ValueError(f"Unknown site: {site_name}")
        return row["id"]


def create_scrape_job(conn, site_id, job_type="full_crawl"):
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO scrape_jobs (site_id, job_type, status, started_at) "
            "VALUES (%s, %s, 'running', NOW())",
            (site_id, job_type),
        )
        conn.commit()
        return cur.lastrowid


def finish_scrape_job(conn, job_id, items_found, items_processed, items_failed, error=None):
    status = "failed" if error else "completed"
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE scrape_jobs SET status=%s, completed_at=NOW(), "
            "items_found=%s, items_processed=%s, items_failed=%s, error_message=%s "
            "WHERE id=%s",
            (status, items_found, items_processed, items_failed, error, job_id),
        )
        conn.commit()


def compute_checksum(data):
    """SHA-256 of the JSON-serialized data, for change detection."""
    raw = json.dumps(data, sort_keys=True, default=str)
    return hashlib.sha256(raw.encode()).hexdigest()


def insert_scrape_raw(conn, job_id, site_id, external_id, raw_metadata):
    """Insert a raw scraped record.  Returns the row id."""
    checksum = compute_checksum(raw_metadata)

    # Skip if we already have an identical checksum for this source
    with conn.cursor() as cur:
        cur.execute(
            "SELECT sr.id FROM scrape_raw sr "
            "JOIN packages p ON p.id = sr.package_id "
            "WHERE sr.site_id = %s AND sr.external_id = %s "
            "AND p.scrape_checksum = %s AND sr.status = 'processed' "
            "LIMIT 1",
            (site_id, external_id, checksum),
        )
        if cur.fetchone():
            return None  # unchanged, skip

        cur.execute(
            "INSERT INTO scrape_raw "
            "(scrape_job_id, site_id, external_id, raw_metadata, raw_checksum) "
            "VALUES (%s, %s, %s, %s, %s)",
            (job_id, site_id, external_id, json.dumps(raw_metadata, default=str), checksum),
        )
        conn.commit()
        return cur.lastrowid


def fetch_pending_raw(conn, limit=100):
    """Fetch a batch of pending scrape_raw rows for processing."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT id, scrape_job_id, site_id, external_id, raw_metadata, raw_checksum "
            "FROM scrape_raw WHERE status = 'pending' "
            "ORDER BY id LIMIT %s",
            (limit,),
        )
        rows = cur.fetchall()
        if rows:
            ids = [r["id"] for r in rows]
            placeholders = ",".join(["%s"] * len(ids))
            cur.execute(
                f"UPDATE scrape_raw SET status='processing' WHERE id IN ({placeholders})",
                ids,
            )
            conn.commit()
        return rows


def get_site_name(conn, site_id):
    with conn.cursor() as cur:
        cur.execute("SELECT name FROM sites WHERE id = %s", (site_id,))
        row = cur.fetchone()
        return row["name"] if row else None
