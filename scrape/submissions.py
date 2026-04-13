"""Process user-submitted URLs from the site_submissions table.

Pulls pending rows from `site_submissions`, runs sanity checks, then feeds
each URL through the same extraction logic the discovery scraper uses.
Saved rows land in `scrape_raw` and flow through the existing
process -> llm-review pipeline, where videos get rerouted to the videos
table automatically.

Usage:
    python -m scrape submissions [--limit N]
"""

import logging
from urllib.parse import urlparse

from . import db
from .web import DiscoveryScraper

log = logging.getLogger(__name__)


def _fetch_pending(conn, limit):
    sql = ("SELECT id, url, comment FROM site_submissions "
           "WHERE status = 'pending' ORDER BY id")
    args = ()
    if limit:
        sql += " LIMIT %s"
        args = (limit,)
    with conn.cursor() as cur:
        cur.execute(sql, args)
        return cur.fetchall()


def _mark(conn, sub_id, status):
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE site_submissions SET status = %s WHERE id = %s",
            (status, sub_id),
        )
        conn.commit()


def _sanity_check(url):
    """Return (ok, reason).  Lightweight pre-flight before fetching."""
    if not url or len(url) > 2000:
        return False, "url empty or too long"
    parsed = urlparse(url)
    if parsed.scheme not in ("http", "https"):
        return False, f"unsupported scheme: {parsed.scheme!r}"
    if not parsed.hostname:
        return False, "no hostname"
    return True, ""


def _already_known(conn, url):
    """True if this exact URL is already in packages or scrape_raw."""
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM packages WHERE url = %s LIMIT 1", (url,))
        if cur.fetchone():
            return True
        cur.execute(
            "SELECT 1 FROM scrape_raw WHERE external_id = %s LIMIT 1",
            (url,),
        )
        return bool(cur.fetchone())


def process_submissions(conn, limit=None):
    """Process pending submissions.  Returns counts dict."""
    pending = _fetch_pending(conn, limit)
    if not pending:
        log.info("No pending submissions.")
        return {"pending": 0, "added": 0, "rejected": 0, "skipped": 0, "errors": 0}

    log.info("Processing %d pending submission(s)", len(pending))

    scraper = DiscoveryScraper(conn)
    site_id = scraper.site_id
    job_id = db.create_scrape_job(conn, site_id, "submissions")

    added = rejected = skipped = errors = 0

    try:
        for row in pending:
            sub_id = row["id"]
            url = (row["url"] or "").strip().split("#")[0]
            log.info("Submission %d: %s", sub_id, url)

            ok, reason = _sanity_check(url)
            if not ok:
                log.warning("  reject: %s", reason)
                _mark(conn, sub_id, "rejected")
                rejected += 1
                continue

            if _already_known(conn, url):
                log.info("  skip: already known")
                _mark(conn, sub_id, "added")
                skipped += 1
                continue

            try:
                meta, _children = scraper._extract_from_page(url)
            except Exception as e:
                log.error("  extract failed: %s", e)
                errors += 1
                # Leave as pending so a future run can retry.
                continue

            if meta is None:
                # Page reachable but no Smalltalk content / no useful signal.
                # Save a stub so a human can still see it from scrape_raw and
                # so the LLM review stage gets a chance at it.
                meta = {
                    "name": url,
                    "url": url,
                    "source": "user_submitted",
                    "description": (row["comment"] or "")[:2000],
                    "code_blocks": [],
                    "code_block_count": 0,
                    "file_links": [],
                    "file_link_count": 0,
                    "user_comment": row["comment"] or "",
                }
            else:
                meta["source"] = "user_submitted"
                if row["comment"]:
                    meta["user_comment"] = row["comment"]

            try:
                row_id = db.insert_scrape_raw(conn, job_id, site_id, url, meta)
                if row_id:
                    log.info("  saved scrape_raw id=%d", row_id)
                else:
                    log.info("  duplicate, not re-saved")
                _mark(conn, sub_id, "added")
                added += 1
            except Exception as e:
                log.error("  save failed: %s", e)
                errors += 1
    finally:
        db.finish_scrape_job(conn, job_id, len(pending), added, errors)

    log.info("Submissions: added=%d rejected=%d skipped=%d errors=%d",
             added, rejected, skipped, errors)
    return {"pending": len(pending), "added": added, "rejected": rejected,
            "skipped": skipped, "errors": errors}
