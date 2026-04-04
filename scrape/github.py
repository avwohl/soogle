"""GitHub scraper for Soogle.

Enumerates all Smalltalk repos via the GitHub Search API, segmenting by
date range to work around the 1,000-result-per-query limit.  Writes raw
API responses into scrape_raw for later processing.

Usage:
    python -m scrape github [--incremental]
"""

import time
import logging
import requests
from datetime import datetime, timedelta
from . import config, db

log = logging.getLogger(__name__)


class GitHubScraper:
    def __init__(self, conn):
        self.conn = conn
        self.session = requests.Session()
        self.session.headers.update({
            "Accept": "application/vnd.github+json",
            "User-Agent": config.USER_AGENT,
            "X-GitHub-Api-Version": "2022-11-28",
        })
        if config.GITHUB_TOKEN:
            self.session.headers["Authorization"] = f"Bearer {config.GITHUB_TOKEN}"
        else:
            log.warning("No GITHUB_TOKEN set -- rate limits will be very restrictive (60 req/hr)")

        self.site_id = db.get_site_id(conn, "github")

    # ----- low-level API helpers -----

    def _get(self, url, params=None):
        """GET with rate-limit awareness."""
        resp = self.session.get(url, params=params, timeout=config.REQUEST_TIMEOUT)

        # Handle rate limiting
        remaining = int(resp.headers.get("X-RateLimit-Remaining", 999))
        if resp.status_code == 403 and remaining == 0:
            reset_at = int(resp.headers.get("X-RateLimit-Reset", 0))
            wait = max(reset_at - time.time(), 1) + 1
            log.warning("Rate limited, sleeping %.0fs", wait)
            time.sleep(wait)
            return self._get(url, params)

        if resp.status_code == 422:
            # GitHub returns 422 for queries with >1000 results or bad queries
            log.warning("422 from GitHub: %s", resp.json().get("message", ""))
            return None

        resp.raise_for_status()
        return resp.json()

    def _search_repos(self, query, page=1):
        """One page of /search/repositories."""
        time.sleep(config.GITHUB_SEARCH_PAUSE)
        params = {
            "q": query,
            "per_page": config.GITHUB_SEARCH_PER_PAGE,
            "page": page,
            "sort": "updated",
            "order": "asc",
        }
        return self._get(f"{config.GITHUB_API}/search/repositories", params)

    def _get_repo(self, full_name):
        """Full repo details via /repos/:owner/:repo."""
        time.sleep(config.GITHUB_API_PAUSE)
        return self._get(f"{config.GITHUB_API}/repos/{full_name}")

    # ----- date segmentation -----

    def _date_segments(self, start_date, end_date, days_per_segment=90):
        """Generate date range segments for search queries."""
        segments = []
        cursor = start_date
        while cursor < end_date:
            seg_end = min(cursor + timedelta(days=days_per_segment), end_date)
            segments.append((cursor, seg_end))
            cursor = seg_end + timedelta(days=1)
        return segments

    def _search_segment(self, date_from, date_to):
        """Search one date segment.  If >1000 results, split in half and recurse."""
        q = f"language:Smalltalk created:{date_from:%Y-%m-%d}..{date_to:%Y-%m-%d}"
        result = self._search_repos(q, page=1)
        if result is None:
            return []

        total = result.get("total_count", 0)
        log.info("Segment %s..%s: %d repos", date_from.date(), date_to.date(), total)

        if total > 1000:
            # Split and recurse
            if (date_to - date_from).days <= 1:
                log.warning("Single day %s has >1000 repos, can only fetch first 1000", date_from.date())
            else:
                mid = date_from + (date_to - date_from) / 2
                left = self._search_segment(date_from, mid)
                right = self._search_segment(mid + timedelta(days=1), date_to)
                return left + right

        # Paginate through all results
        repos = list(result.get("items", []))
        page = 2
        while len(repos) < total and page <= 10:
            result = self._search_repos(q, page=page)
            if result is None:
                break
            items = result.get("items", [])
            if not items:
                break
            repos.extend(items)
            page += 1

        return repos

    # ----- main entry point -----

    def run(self, incremental=False):
        """Enumerate all Smalltalk repos on GitHub, write to scrape_raw.

        If incremental, only fetch repos updated in the last 30 days.
        """
        job_id = db.create_scrape_job(
            self.conn, self.site_id,
            "incremental" if incremental else "full_crawl",
        )
        log.info("Started scrape job %d (incremental=%s)", job_id, incremental)

        found = 0
        saved = 0
        errors = 0

        try:
            if incremental:
                start = datetime.utcnow() - timedelta(days=30)
            else:
                start = datetime(2008, 1, 1)  # GitHub launched 2008
            end = datetime.utcnow()

            segments = self._date_segments(start, end)
            log.info("Searching %d date segments", len(segments))

            for seg_from, seg_to in segments:
                try:
                    repos = self._search_segment(seg_from, seg_to)
                except Exception as e:
                    log.error("Segment %s..%s failed: %s", seg_from.date(), seg_to.date(), e)
                    errors += 1
                    continue

                found += len(repos)

                for repo in repos:
                    full_name = repo.get("full_name", "")
                    try:
                        # Fetch full repo details (search results omit some fields)
                        detail = self._get_repo(full_name)
                        if detail is None:
                            errors += 1
                            continue

                        row_id = db.insert_scrape_raw(
                            self.conn, job_id, self.site_id, full_name, detail,
                        )
                        if row_id:
                            saved += 1
                            log.debug("Saved %s (raw id %d)", full_name, row_id)
                        else:
                            log.debug("Skipped %s (unchanged)", full_name)
                    except Exception as e:
                        log.error("Failed to save %s: %s", full_name, e)
                        errors += 1

                log.info(
                    "Progress: found=%d saved=%d errors=%d",
                    found, saved, errors,
                )

        except Exception as e:
            log.exception("GitHub scrape failed")
            db.finish_scrape_job(self.conn, job_id, found, saved, errors, str(e))
            raise

        db.finish_scrape_job(self.conn, job_id, found, saved, errors)
        log.info("Done: found=%d saved=%d errors=%d", found, saved, errors)
        return {"found": found, "saved": saved, "errors": errors}
