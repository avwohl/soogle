"""YouTube video scraper for Smalltalk content.

Uses SerpAPI's YouTube engine to search for Smalltalk videos,
and scrapes known playlists (e.g. Pharo MOOC).

Usage:
    python -m scrape youtube [--playlists-only]
"""

import re
import time
import json
import logging
import pymysql
from . import config, db

log = logging.getLogger(__name__)

# Search queries to find Smalltalk videos on YouTube
_VIDEO_QUERIES = [
    "smalltalk programming tutorial",
    "smalltalk programming language",
    "pharo smalltalk tutorial",
    "pharo smalltalk",
    "squeak smalltalk",
    "smalltalk object oriented programming",
    "smalltalk IDE demo",
    "seaside smalltalk web",
    "gemstone smalltalk",
    "gnu smalltalk",
    "visualworks smalltalk",
    "cuis smalltalk",
    "smalltalk design patterns",
    "smalltalk live coding",
    "ESUG smalltalk",
    "smalltalk conference talk",
]

# Known playlists to scrape completely
_PLAYLISTS = [
    # Pharo MOOC - English
    "PL2okA_2qDJ-kCHVcNXdO5wsUZJCY31zwf",
    # Pharo MOOC - French
    "PL2okA_2qDJ-k83Kxu_d8EPzMXtvCrReRn",
]

# Map keywords in title/description to dialect
_DIALECT_PATTERNS = [
    (re.compile(r"\bpharo\b", re.I), "pharo"),
    (re.compile(r"\bsqueak\b", re.I), "squeak"),
    (re.compile(r"\bcuis\b", re.I), "cuis"),
    (re.compile(r"\bgnu.?smalltalk\b", re.I), "gnu_smalltalk"),
    (re.compile(r"\bgemstone\b", re.I), "gemstone"),
    (re.compile(r"\bvisualworks\b", re.I), "visualworks"),
    (re.compile(r"\bdolphin\b", re.I), "dolphin"),
    (re.compile(r"\bva.?smalltalk\b|vast\b", re.I), "va_smalltalk"),
]


# Channels known to be all-Smalltalk content (always accept)
_TRUSTED_CHANNELS = {
    "esugboard", "Cincom Smalltalk", "jarober", "Smalltalk Renaissance",
    "FAST - Fundación Argentina de Smalltalk", "James Foster",
    "UK Smalltalk User Group", "Smalltalk Renaissance — 50 Years of Smalltalk",
    "Ken Dickey", "Cuisme", "gandysmedicineshow", "Docendo Disco ",
    "HuwsTube", "Lawson English", "Eiichiro Ito", "bwbadger", "redbear8174",
    "Inria Learning Lab", "TkTorah", "Kirill Nick Melnikov",
}

# Keywords that confirm a video is about Smalltalk-the-language
_ST_KEYWORDS = re.compile(
    r"\bsmalltalk\b|\bpharo\b|\bsqueak\b|\bcuis\b|\bgemstone\b"
    r"|\bvisualworks\b|\bseaside\b|\bgnu.?smalltalk\b|\bva.?smalltalk\b"
    r"|\bdolphin.?smalltalk\b|\bsmalltalk[-/]?80\b",
    re.IGNORECASE,
)


# Title patterns that indicate chitchat / conversation skills, not programming
_CHITCHAT_TITLE = re.compile(
    r"\bsmall\s+talk\b(?!.*(?:programming|language|code|pharo|squeak))",
    re.IGNORECASE,
)


def _is_relevant(title, description, channel_name, source):
    """Return True if the video is about Smalltalk the programming language."""
    if source == "mooc_pharo":
        return True
    if channel_name in _TRUSTED_CHANNELS:
        return True
    # Reject "small talk" (conversation) videos even if description has
    # #smalltalk hashtag — the title is a stronger signal
    if _CHITCHAT_TITLE.search(title):
        return False
    text = f"{title} {description}"
    return bool(_ST_KEYWORDS.search(text))


def _detect_dialect(title, description=""):
    """Guess the Smalltalk dialect from video title/description."""
    text = f"{title} {description}"
    for pattern, dialect in _DIALECT_PATTERNS:
        if pattern.search(text):
            return dialect
    return "unknown"


def _parse_duration(text):
    """Parse duration strings like '12:34' or '1:02:15' to seconds."""
    if not text:
        return None
    parts = text.strip().split(":")
    try:
        parts = [int(p) for p in parts]
    except (ValueError, TypeError):
        return None
    if len(parts) == 2:
        return parts[0] * 60 + parts[1]
    if len(parts) == 3:
        return parts[0] * 3600 + parts[1] * 60 + parts[2]
    return None


def _parse_views(text):
    """Parse view count strings like '1,234 views' or '1.2K views'."""
    if not text:
        return 0
    text = text.lower().replace(",", "").replace("views", "").strip()
    try:
        if "k" in text:
            return int(float(text.replace("k", "")) * 1000)
        if "m" in text:
            return int(float(text.replace("m", "")) * 1_000_000)
        return int(text)
    except (ValueError, TypeError):
        return 0


class YouTubeScraper:
    """Scrapes YouTube for Smalltalk videos via SerpAPI."""

    def __init__(self, conn):
        self.conn = conn
        import requests
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": config.USER_AGENT})

    def _serpapi_youtube_search(self, query, max_pages=5):
        """Search YouTube via SerpAPI.  Returns list of video dicts."""
        all_results = []
        params = {
            "api_key": config.SERPAPI_KEY,
            "engine": "youtube",
            "search_query": query,
        }
        for page in range(max_pages):
            try:
                resp = self.session.get(
                    "https://serpapi.com/search",
                    params=params,
                    timeout=config.REQUEST_TIMEOUT,
                )
                resp.raise_for_status()
                data = resp.json()
                results = data.get("video_results", [])
                all_results.extend(results)
                if not results:
                    break
                # SerpAPI pagination via serpapi_pagination
                pagination = data.get("serpapi_pagination", {})
                next_url = pagination.get("next")
                if not next_url:
                    break
                # For next page, use the next_page_token
                next_token = pagination.get("next_page_token")
                if next_token:
                    params["sp"] = next_token
                else:
                    break
                time.sleep(1)
            except Exception as e:
                log.warning("SerpAPI YouTube search failed for %r (page %d): %s",
                            query, page, e)
                break
        return all_results

    def _serpapi_playlist(self, playlist_id):
        """Fetch all videos from a YouTube playlist via SerpAPI YouTube search."""
        all_results = []
        params = {
            "api_key": config.SERPAPI_KEY,
            "engine": "youtube",
            "search_query": f"https://www.youtube.com/playlist?list={playlist_id}",
        }
        for page in range(20):  # playlists can be long
            try:
                resp = self.session.get(
                    "https://serpapi.com/search",
                    params=params,
                    timeout=config.REQUEST_TIMEOUT,
                )
                resp.raise_for_status()
                data = resp.json()
                # Playlist videos may appear under playlist_results or video_results
                results = []
                for pr in data.get("playlist_results", []):
                    videos = pr.get("videos", [])
                    results.extend(videos)
                results.extend(data.get("video_results", []))
                all_results.extend(results)
                if not results:
                    break
                pagination = data.get("serpapi_pagination", {})
                next_token = pagination.get("next_page_token")
                if next_token:
                    params["sp"] = next_token
                else:
                    break
                time.sleep(1)
            except Exception as e:
                log.warning("SerpAPI playlist fetch failed for %s (page %d): %s",
                            playlist_id, page, e)
                break
        return all_results

    def _save_video(self, video_id, title, url, description="",
                    channel_name="", channel_url="", thumbnail_url="",
                    duration_seconds=None, published_at=None,
                    view_count=0, dialect="unknown", source="youtube"):
        """Insert or update a video row. Returns True if new."""
        if db.is_blocked(self.conn, "youtube", video_id):
            return False
        with self.conn.cursor() as cur:
            try:
                cur.execute(
                    "INSERT INTO videos "
                    "(video_id, title, url, description, channel_name, channel_url, "
                    "thumbnail_url, duration_seconds, published_at, view_count, "
                    "dialect, source) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) "
                    "ON DUPLICATE KEY UPDATE "
                    "title=VALUES(title), description=VALUES(description), "
                    "view_count=VALUES(view_count), thumbnail_url=VALUES(thumbnail_url)",
                    (video_id, title[:500], url, (description or "")[:5000],
                     channel_name or "", channel_url or "", thumbnail_url or "",
                     duration_seconds, published_at, view_count,
                     dialect, source),
                )
                self.conn.commit()
                return cur.rowcount == 1  # 1 = insert, 2 = update
            except pymysql.err.IntegrityError:
                self.conn.rollback()
                return False

    def _process_search_result(self, r, source="youtube"):
        """Extract fields from a SerpAPI YouTube search result and save."""
        vid = r.get("id", {})
        if isinstance(vid, dict):
            video_id = vid.get("videoId", "")
        else:
            video_id = str(vid)
        if not video_id:
            # Try extracting from link
            link = r.get("link", "")
            m = re.search(r"[?&]v=([a-zA-Z0-9_-]{11})", link)
            if m:
                video_id = m.group(1)
            else:
                return False

        title = r.get("title", "")
        if not title:
            return False

        url = r.get("link", f"https://www.youtube.com/watch?v={video_id}")
        description = r.get("description", "") or r.get("snippet", "")
        channel = r.get("channel", {})
        if isinstance(channel, dict):
            channel_name = channel.get("name", "")
            channel_url = channel.get("link", "")
        else:
            channel_name = str(channel) if channel else ""
            channel_url = ""

        # Skip videos not about Smalltalk the programming language
        if not _is_relevant(title, description, channel_name, source):
            return False
        thumbnail = r.get("thumbnail", {})
        if isinstance(thumbnail, dict):
            thumbnail_url = thumbnail.get("static", "") or thumbnail.get("rich", "")
        elif isinstance(thumbnail, str):
            thumbnail_url = thumbnail
        else:
            thumbnail_url = ""
        # Thumbnails from video info
        thumbnails = r.get("thumbnails", [])
        if not thumbnail_url and thumbnails:
            thumbnail_url = thumbnails[-1].get("url", "") if isinstance(thumbnails[-1], dict) else ""

        duration_text = r.get("length", "") or r.get("duration", "")
        duration_seconds = _parse_duration(duration_text)

        views_text = r.get("views", "") or r.get("view_count", "")
        if isinstance(views_text, int):
            view_count = views_text
        else:
            view_count = _parse_views(str(views_text))

        published = r.get("published_date", None) or r.get("date", None)
        dialect = _detect_dialect(title, description)

        return self._save_video(
            video_id=video_id,
            title=title,
            url=url,
            description=description,
            channel_name=channel_name,
            channel_url=channel_url,
            thumbnail_url=thumbnail_url,
            duration_seconds=duration_seconds,
            published_at=None,  # SerpAPI gives relative dates, not timestamps
            view_count=view_count,
            dialect=dialect,
            source=source,
        )

    def run(self, playlists_only=False):
        """Run the YouTube scraper."""
        if not config.SERPAPI_KEY:
            raise RuntimeError(
                "YouTube scraper requires SERPAPI_KEY. Export it: export SERPAPI_KEY=your-key"
            )

        found = saved = errors = 0

        # 1. Scrape known playlists
        for playlist_id in _PLAYLISTS:
            log.info("Scraping playlist %s", playlist_id)
            try:
                results = self._serpapi_playlist(playlist_id)
                log.info("  playlist %s: %d videos", playlist_id, len(results))
                found += len(results)
                for r in results:
                    try:
                        if self._process_search_result(r, source="mooc_pharo"):
                            saved += 1
                    except Exception as e:
                        log.error("  playlist video failed: %s", e)
                        errors += 1
            except Exception as e:
                log.error("Playlist %s failed: %s", playlist_id, e)
                errors += 1

        if playlists_only:
            log.info("YouTube done (playlists only): found=%d saved=%d errors=%d",
                     found, saved, errors)
            return {"found": found, "saved": saved, "errors": errors}

        # 2. Search queries
        seen_ids = set()
        for qi, query in enumerate(_VIDEO_QUERIES, 1):
            log.info("YouTube search [%d/%d]: %s", qi, len(_VIDEO_QUERIES), query)
            try:
                results = self._serpapi_youtube_search(query, max_pages=3)
                new = 0
                for r in results:
                    try:
                        if self._process_search_result(r):
                            saved += 1
                            new += 1
                        found += 1
                    except Exception as e:
                        log.error("  video save failed: %s", e)
                        errors += 1
                log.info("  results: %d found, %d new", len(results), new)
                time.sleep(2)
            except Exception as e:
                log.error("YouTube search %r failed: %s", query, e)
                errors += 1

        log.info("YouTube done: found=%d saved=%d errors=%d", found, saved, errors)
        return {"found": found, "saved": saved, "errors": errors}
