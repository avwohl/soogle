"""Process user-submitted URLs from the site_submissions table.

Pulls pending rows from `site_submissions`, runs sanity checks, then feeds
each URL through the same extraction logic the discovery scraper uses.
Saved rows land in `scrape_raw` and flow through the existing
process -> llm-review pipeline, where videos get rerouted to the videos
table automatically.

When a submitted URL looks like a multi-project site (many internal links
or file downloads), an email is sent suggesting a dedicated scraper be
written for it.

Usage:
    python -m scrape submissions [--limit N]
"""

import logging
import smtplib
from email.message import EmailMessage
from urllib.parse import urlparse

from . import config, db
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


# Minimum number of internal links or file links to flag a submission as a
# potential multi-project site that warrants a dedicated scraper.
_RICH_SITE_THRESHOLD = 10


def _looks_like_rich_site(meta, children):
    """Return True if extraction results suggest a multi-project site."""
    if meta is None:
        return len(children) >= _RICH_SITE_THRESHOLD
    file_count = meta.get("file_link_count", 0)
    code_count = meta.get("code_block_count", 0)
    return (len(children) >= _RICH_SITE_THRESHOLD
            or file_count >= _RICH_SITE_THRESHOLD
            or (len(children) + file_count + code_count) >= _RICH_SITE_THRESHOLD)


def _notify_rich_site(url, comment, child_count, file_count):
    """Email admin that a submitted URL looks like it deserves a scraper."""
    to_addr = config.SUBMISSION_EMAIL_TO
    if not to_addr:
        log.info("  SUBMISSION_EMAIL_TO not set, skipping rich-site notification")
        return

    hostname = urlparse(url).hostname or url
    body = (
        f"A user-submitted URL looks like a multi-project site that may\n"
        f"warrant a dedicated scraper.\n\n"
        f"URL:             {url}\n"
        f"Hostname:        {hostname}\n"
        f"Internal links:  {child_count}\n"
        f"File links:      {file_count}\n"
        f"User comment:    {comment or '(none)'}\n\n"
        f"Consider writing a custom scraper and adding it to\n"
        f"scrape/custom.py + CUSTOM_SCRAPERS.\n"
    )
    msg = EmailMessage()
    msg["Subject"] = f"[soogle] Submitted URL may need a scraper: {hostname}"
    msg["From"] = config.SUBMISSION_EMAIL_FROM
    msg["To"] = to_addr
    msg.set_content(body)

    try:
        with smtplib.SMTP(config.EMAIL_HOST, config.EMAIL_PORT) as smtp:
            smtp.send_message(msg)
        log.info("  sent rich-site notification for %s", hostname)
    except Exception as e:
        log.warning("  rich-site notification failed: %s", e)


def process_submissions(conn, limit=None):
    """Process pending submissions.  Returns counts dict."""
    pending = _fetch_pending(conn, limit)
    if not pending:
        log.info("No pending submissions.")
        return {"pending": 0, "added": 0, "rejected": 0, "skipped": 0, "errors": 0}

    log.info("Processing %d pending submission(s)", len(pending))

    scraper = DiscoveryScraper(conn)
    site_id = scraper.site_id
    job_id = db.create_scrape_job(conn, site_id, "discovery")

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
                meta, children = scraper._extract_from_page(url)
            except Exception as e:
                log.error("  extract failed: %s", e)
                errors += 1
                # Leave as pending so a future run can retry.
                continue

            # Check if this looks like a multi-project site that should
            # have its own dedicated scraper instead of a one-off entry.
            file_count = (meta or {}).get("file_link_count", 0)
            if _looks_like_rich_site(meta, children):
                log.info("  rich site detected (%d children, %d files) — notifying",
                         len(children), file_count)
                _notify_rich_site(url, row["comment"], len(children), file_count)

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
