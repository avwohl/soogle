"""LLM-based quality review of packages and videos.

Fetches README excerpts from GitHub, then asks an LLM to judge whether
each package is genuinely Smalltalk-related.  Non-Smalltalk packages are
moved to the blocklist.

Also reviews videos for relevance — removes "small talk" conversation
videos, generic OOP content, gemstone jewelry, spam, etc.

The package and video review queues can cross-route: a "package" that is
actually a YouTube video is moved to the videos table, and a "video"
that is actually a code package is moved to the packages table.

Usage:
    python -m scrape llm-review [--limit N] [--fetch-only] [--review-only] [--since-id N] [--since-date DATE]
    python -m scrape video-review [--limit N] [--model MODEL] [--since-id N] [--since-date DATE]
"""

import base64
import json
import logging
import re
import time
from typing import Any, Literal
from urllib.parse import parse_qs, urlparse

import anthropic
import openai
import requests
from pydantic import BaseModel
from sqlalchemy import or_, select
from sqlalchemy.engine import Connection, RowMapping
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_random_exponential
from tqdm import tqdm

from . import config, db
from .models import is_upgrade
from .schema import (
    blocklist,
    package_categories,
    package_classes,
    package_methods,
    packages,
    scrape_raw,
    sites,
    videos,
)

log = logging.getLogger(__name__)

BATCH_SIZE = 20  # packages per LLM call


class Verdict(BaseModel):
    id: int
    verdict: Literal["keep", "block", "video"]
    reason: str | None = None


class VerdictBatch(BaseModel):
    results: list[Verdict]


class VideoVerdict(BaseModel):
    id: int
    verdict: Literal["keep", "block", "package"]
    reason: str | None = None


class VideoVerdictBatch(BaseModel):
    results: list[VideoVerdict]


# Matches an 11-character YouTube video id in any common URL form.
_YOUTUBE_ID_RE = re.compile(r"([A-Za-z0-9_-]{11})")


def _is_youtube_id(s: str) -> bool:
    return bool(_YOUTUBE_ID_RE.fullmatch(s))


def _extract_youtube_id(url: str | None) -> str | None:
    """Return the 11-char YouTube video id from a URL, or None.

    Handles youtube.com/watch?v=, youtu.be/, /embed/, /v/, /shorts/.
    """
    if not url:
        return None
    try:
        parsed = urlparse(url)
    except Exception:
        return None
    host = (parsed.hostname or "").lower()
    path = parsed.path or ""
    if host in ("youtu.be",):
        candidate = path.lstrip("/").split("/", 1)[0]
        if _is_youtube_id(candidate):
            return candidate
    if "youtube.com" in host or "youtube-nocookie.com" in host:
        if path.startswith(("/embed/", "/v/", "/shorts/")):
            candidate = path.split("/", 2)[2].split("/", 1)[0]
            if _is_youtube_id(candidate):
                return candidate
        qs = parse_qs(parsed.query)
        v = qs.get("v", [""])[0]
        if _is_youtube_id(v):
            return v
    return None

SYSTEM_PROMPT = """\
You are a classifier for a Smalltalk code search engine called Soogle.
Your job is to decide whether each entry is a genuine Smalltalk *code
package* (a downloadable code repo, source archive, Monticello/Tonel
package, .mcz/.st bundle, or similar) for one of the Smalltalk dialects
(Pharo, Squeak, Cuis, GemStone, VisualWorks, GNU Smalltalk, Dolphin,
VA Smalltalk, Newspeak, etc.).

Block anything that is not itself shippable code, even if it is Smalltalk-
related.  In particular, BLOCK these (do not keep them just because the
URL or text mentions Smalltalk):
- Documentation, tutorials, book chapters, FAQ pages
- Discussion forums, Google Groups, mailing list archives
- Web directories, link lists, link farms (e.g. dmoz/odp/cetus)
- Internet Archive book/magazine/document downloads
- Blog posts, news articles, museum write-ups, history pieces
- Video pages, video announcements (LinkedIn / Facebook / Twitter posts
  about a video), playlists
- Generic download pages, vendor "developer resources" pages
- Docker images, vendor product brochures, fix-pack readmes
- Conference / event pages
- C# / .NET projects (GitHub linguist confuses .cs changeset files)
- IEC 61131-3 Structured Text / PLC projects (.st extension overlap)
- NLP/ML research using StringTemplate .st files
- Unity game projects
- Random repos with a tiny .cs or .st file

KEEP only entries that are themselves a Smalltalk package, repo, or
source bundle that someone could load into an image.  When in doubt,
block — Soogle indexes code, not commentary about code.

If the entry's URL is a watchable YouTube video (youtube.com/watch,
youtu.be/, /shorts/, /embed/), return verdict "video" so we can move it
to the video review queue.  Do NOT use "video" for blog posts, LinkedIn
or Facebook announcements, playlists, or pages that merely link to a
video — those should be blocked.

For each entry, respond with a JSON array of objects:
[{"id": 123, "verdict": "keep"},
 {"id": 456, "verdict": "block", "reason": "C# .NET project"},
 {"id": 789, "verdict": "video", "reason": "YouTube tutorial"}]

Verdicts: "keep" (a real Smalltalk code package), "block" (not a code
package), or "video" (actually a YouTube watch URL — route to video
queue).  Only add "reason" for "block" or "video" verdicts.  Be concise.
"""


def _github_session() -> requests.Session:
    session = requests.Session()
    session.headers.update({
        "Accept": "application/vnd.github+json",
        "User-Agent": config.USER_AGENT,
        "X-GitHub-Api-Version": "2022-11-28",
    })
    if config.GITHUB_TOKEN:
        session.headers["Authorization"] = f"Bearer {config.GITHUB_TOKEN}"
    return session


def _build_client() -> Any:
    """Return an instructor-wrapped LLM client.

    Uses the OpenAI SDK pointed at any OpenAI-compatible endpoint when
    OPENAI_BASE_URL is set, otherwise the Anthropic SDK.
    """
    import instructor

    if config.OPENAI_BASE_URL:
        from openai import OpenAI
        return instructor.from_openai(
            OpenAI(api_key=config.OPENAI_API_KEY, base_url=config.OPENAI_BASE_URL)
        )
    import anthropic
    return instructor.from_anthropic(
        anthropic.Anthropic(api_key=config.ANTHROPIC_API_KEY)
    )


def fetch_readmes(conn: Connection, limit: int | None = None) -> int:
    """Fetch README excerpts from GitHub for packages missing them."""
    stmt = (
        select(packages.c.id, packages.c.external_id)
        .select_from(packages.join(sites, sites.c.id == packages.c.site_id))
        .where(sites.c.name == "github", packages.c.readme_excerpt.is_(None))
        .order_by(packages.c.stars.desc(), packages.c.id)
    )
    if limit:
        stmt = stmt.limit(limit)
    rows = conn.execute(stmt).mappings().fetchall()
    if not rows:
        log.info("No packages need README fetching")
        return 0

    log.info("Fetching READMEs for %d packages", len(rows))
    session = _github_session()
    fetched = 0
    errors = 0

    for row in tqdm(rows, desc="readmes", unit="pkg"):
        pkg_id = row["id"]
        full_name = row["external_id"]
        try:
            time.sleep(config.GITHUB_API_PAUSE)
            resp = session.get(
                f"{config.GITHUB_API}/repos/{full_name}/readme",
                timeout=config.REQUEST_TIMEOUT,
            )

            # Rate limit handling
            remaining = int(resp.headers.get("X-RateLimit-Remaining", 999))
            if resp.status_code == 403 and remaining == 0:
                reset_at = int(resp.headers.get("X-RateLimit-Reset", 0))
                wait = max(reset_at - time.time(), 1) + 1
                log.warning("Rate limited, sleeping %.0fs", wait)
                time.sleep(wait)
                resp = session.get(
                    f"{config.GITHUB_API}/repos/{full_name}/readme",
                    timeout=config.REQUEST_TIMEOUT,
                )

            if resp.status_code == 404:
                # No README — store empty string so we don't retry
                conn.execute(
                    packages.update()
                    .where(packages.c.id == pkg_id)
                    .values(readme_excerpt="")
                )
                conn.commit()
                fetched += 1
                continue

            if resp.status_code != 200:
                errors += 1
                continue

            data = resp.json()
            content_b64 = data.get("content", "")
            try:
                content = base64.b64decode(content_b64).decode("utf-8", errors="replace")
            except Exception:
                content = ""

            # Store first 10KB
            excerpt = content[:10000]
            conn.execute(
                packages.update()
                .where(packages.c.id == pkg_id)
                .values(readme_excerpt=excerpt)
            )
            conn.commit()
            fetched += 1

        except Exception as e:
            log.warning("Error fetching README for %s: %s", full_name, e)
            errors += 1

    log.info("README fetch done: fetched=%d errors=%d", fetched, errors)
    return fetched


# Transient API errors worth retrying (both SDKs share the same hierarchy).
# Deterministic errors (BadRequestError, auth, validation) are not retried.
_TRANSIENT = (
    anthropic.APIConnectionError, anthropic.APITimeoutError,
    anthropic.RateLimitError, anthropic.InternalServerError,
    openai.APIConnectionError, openai.APITimeoutError,
    openai.RateLimitError, openai.InternalServerError,
)


@retry(
    stop=stop_after_attempt(3),
    wait=wait_random_exponential(multiplier=1, max=30),
    retry=retry_if_exception_type(_TRANSIENT),
    reraise=True,
)
def _call_llm(client: Any, items: list[dict], model: str,
              prompt: str, response_model: type[BaseModel],
              max_tokens: int) -> list[dict]:
    """Send a batch of items to the LLM for classification."""
    user_msg = json.dumps(items, indent=None)
    resp = client.create(
        model=model,
        max_tokens=max_tokens,
        messages=[
            {"role": "system", "content": prompt},
            {"role": "user", "content": user_msg},
        ],
        response_model=response_model,
    )
    return [v.model_dump(exclude_none=True) for v in resp.results]


def _package_items(packages: list[RowMapping]) -> list[dict]:
    items = []
    for p in packages:
        item = {
            "id": p["id"],
            "name": p["name"],
            "qualified_name": p["qualified_name"],
            "description": p["description"] or "",
            "stars": p["stars"],
            "dialect": p["dialect"],
            "topics": p["topics"] or "[]",
        }
        if p.get("url"):
            item["url"] = p["url"]
        readme = (p.get("readme_excerpt") or "")[:2000]
        if readme:
            item["readme_start"] = readme
        items.append(item)
    return items


def _delete_package(conn: Connection, pkg_id: int) -> None:
    """Remove a package and its dependent rows, detaching scrape_raw."""
    conn.execute(
        scrape_raw.update()
        .where(scrape_raw.c.package_id == pkg_id)
        .values(package_id=None)
    )
    conn.execute(package_methods.delete().where(package_methods.c.package_id == pkg_id))
    conn.execute(package_classes.delete().where(package_classes.c.package_id == pkg_id))
    conn.execute(package_categories.delete().where(package_categories.c.package_id == pkg_id))
    conn.execute(packages.delete().where(packages.c.id == pkg_id))


def review_packages(conn: Connection, limit: int | None = None,
                    model: str = config.OPENAI_MODEL,
                    scope: Literal["all", "upgrade", "unreviewed"] = "unreviewed",
                    since_id: int | None = None,
                    since_date: str | None = None) -> dict[str, int]:
    """LLM-review packages.

    scope: "unreviewed" — only NULL llm_review
           "upgrade"    — unreviewed + reviewed by a lower-tier model
           "all"        — every package
    since_id:   only review packages with id >= this value
    since_date: only review packages with created_at >= this value (str)
    """
    stmt = select(
        packages.c.id, packages.c.name, packages.c.qualified_name,
        packages.c.description, packages.c.stars, packages.c.dialect,
        packages.c.topics, packages.c.readme_excerpt, packages.c.url,
        packages.c.external_id, sites.c.name.label("site_name"),
        packages.c.llm_review,
    ).select_from(packages.join(sites, sites.c.id == packages.c.site_id))

    conditions = []
    if scope == "upgrade":
        conditions.append(or_(packages.c.llm_review.is_(None), packages.c.llm_review != model))
    elif scope != "all":  # unreviewed (also re-pick rows stranded as '<model>:error')
        conditions.append(or_(packages.c.llm_review.is_(None), packages.c.llm_review.like("%:error")))

    if since_id is not None:
        conditions.append(packages.c.id >= since_id)
    if since_date is not None:
        conditions.append(packages.c.created_at >= since_date)

    if conditions:
        stmt = stmt.where(*conditions)
    stmt = stmt.order_by(packages.c.stars.desc(), packages.c.id)
    if limit:
        stmt = stmt.limit(limit)
    rows = conn.execute(stmt).mappings().fetchall()

    # For upgrade scope, filter to rows actually reviewed by a lower tier
    if scope == "upgrade":
        rows = [r for r in rows
                if r["llm_review"] is None or is_upgrade(r["llm_review"], model)]
    if not rows:
        log.info("No packages need LLM review")
        return {"reviewed": 0, "blocked": 0, "kept": 0, "errors": 0}

    # Build the client up front so a missing 'anthropic' module or bad
    # credentials fail loudly here, instead of being caught per-batch and
    # silently marking every package as ':error' (which never gets retried).
    client = _build_client()

    log.info("LLM reviewing %d packages with %s", len(rows), model)

    reviewed = 0
    blocked = 0
    kept = 0
    errors = 0

    # Process in batches
    routed = 0
    for i in tqdm(range(0, len(rows), BATCH_SIZE), desc="llm-review", unit="batch"):
        batch = rows[i:i + BATCH_SIZE]
        try:
            results = _call_llm(client, _package_items(batch), model,
                                SYSTEM_PROMPT, VerdictBatch, 32768)
            for item in results:
                pkg_id = item["id"]
                verdict = item["verdict"]
                pkg_row = next((r for r in batch if r["id"] == pkg_id), None)
                site_name = pkg_row["site_name"] if pkg_row else "github"
                # Prefer external_id (URL for web_discovered) so blocklist
                # actually matches what scrapers see; fall back to title.
                ext_id = (pkg_row["external_id"] if pkg_row and pkg_row.get("external_id")
                          else (pkg_row["qualified_name"] if pkg_row else str(pkg_id)))

                if verdict == "video":
                    reason = item.get("reason", "actually a video")
                    pkg_url = pkg_row["url"] if pkg_row else ""
                    video_id = _extract_youtube_id(pkg_url)
                    if video_id and pkg_row:
                        try:
                            conn.execute(
                                db.insert_ignore(
                                    videos,
                                    {
                                        "video_id": video_id,
                                        "title": (pkg_row["name"] or "")[:500],
                                        "url": pkg_url,
                                        "description": (pkg_row["description"] or "")[:5000],
                                        "dialect": pkg_row["dialect"] or "unknown",
                                        "source": "package_review_routed",
                                    },
                                    ["video_id"],
                                )
                            )
                        except Exception as e:
                            log.warning("Could not insert routed video %s: %s",
                                        video_id, e)
                        _delete_package(conn, pkg_id)
                        routed += 1
                        log.info("ROUTED to videos: %s (video_id=%s) — %s",
                                 (pkg_row["name"] or ext_id)[:80], video_id, reason)
                        continue
                    # Could not extract a video_id — fall through to block.
                    log.info("Video verdict but no extractable id for %s; blocking",
                             ext_id)
                    verdict = "block"
                    if "reason" not in item:
                        item["reason"] = f"video reference (no extractable id): {reason}"

                if verdict == "block":
                    reason = item.get("reason", "LLM flagged as non-Smalltalk")
                    # Add to blocklist
                    conn.execute(
                        db.insert_ignore(
                            blocklist,
                            {
                                "external_id": ext_id,
                                "site_name": site_name,
                                "reason": f"LLM: {reason}"[:500],
                            },
                            ["external_id", "site_name"],
                        )
                    )
                    # Delete the package
                    _delete_package(conn, pkg_id)
                    blocked += 1
                    log.info("BLOCKED: %s — %s", ext_id, reason)
                else:
                    conn.execute(
                        packages.update()
                        .where(packages.c.id == pkg_id)
                        .values(llm_review=model)
                    )
                    kept += 1
            conn.commit()
            reviewed += len(batch)

        except Exception as e:
            log.error("LLM batch error at offset %d: %s", i, e)
            errors += 1
            # Mark batch as reviewed with error so we don't retry endlessly
            for row in batch:
                conn.execute(
                    packages.update()
                    .where(packages.c.id == row["id"])
                    .values(llm_review=f"{model}:error")
                )
            conn.commit()

    log.info("LLM review done: reviewed=%d kept=%d blocked=%d routed=%d errors=%d",
             reviewed, kept, blocked, routed, errors)
    return {"reviewed": reviewed, "blocked": blocked, "kept": kept,
            "routed": routed, "errors": errors}


# ---------------------------------------------------------------------------
# Video review
# ---------------------------------------------------------------------------

VIDEO_BATCH_SIZE = 30

VIDEO_SYSTEM_PROMPT = """\
You are a classifier for a Smalltalk code search engine called Soogle.
Your job is to decide whether each video is genuinely about Smalltalk
the programming language (Pharo, Squeak, Cuis, GemStone/S, VisualWorks,
GNU Smalltalk, Dolphin, VA Smalltalk, Amber, Newspeak, etc.) or is a
false positive.

Common false positives:
- "Small talk" social/business conversation skills, meeting etiquette,
  English lessons about making small talk
- General OOP lectures that only *mention* Smalltalk as a historical
  footnote — the video must actually teach or demonstrate Smalltalk
- Design pattern talks in other languages (C++, Java) that cite
  Smalltalk as the origin but never show Smalltalk code
- GemStone *jewelry* / gemstone valuation — not GemStone/S the database
- Apps or products named "SmallTalk" (chat apps, language-learning apps)
- JavaScript MVC framework videos that reference Smalltalk MVC only
  as historical context
- Spam / SEO keyword-stuffed videos, pirated book download link-farms
- Videos about other languages (Clojure, Self, etc.) that are merely
  "inspired by" Smalltalk but don't cover Smalltalk itself

Keep videos where Smalltalk is a major focus: tutorials, conference
talks (ESUG, Pharo Days, etc.), demos, live coding, IDE walkthroughs,
historical deep-dives that substantially feature Smalltalk.

Sometimes the video queue receives an entry that is actually a code
package or repo (the URL is a github.com / gitlab.com / sourceforge
project page, not a watchable video).  When that happens, return verdict
"package" so we can move it to the package review queue.  Only use
"package" when the URL itself is clearly a code-hosting page.

For each video, respond with a JSON array of objects:
[{"id": 123, "verdict": "keep"},
 {"id": 456, "verdict": "block", "reason": "business small talk lesson"},
 {"id": 789, "verdict": "package", "reason": "github repo, not a video"}]

Verdicts: "keep", "block", or "package".
Only add "reason" for "block" or "package" verdicts.
"""


def _video_items(videos: list[RowMapping]) -> list[dict]:
    items = []
    for v in videos:
        item = {
            "id": v["id"],
            "title": v["title"],
            "channel_name": v["channel_name"] or "",
            "dialect": v["dialect"],
            "source": v["source"],
        }
        if v.get("url"):
            item["url"] = v["url"]
        desc = (v.get("description") or "")[:1000]
        if desc:
            item["description"] = desc
        items.append(item)
    return items


def review_videos(conn: Connection, limit: int | None = None,
                  model: str = config.OPENAI_MODEL,
                  scope: Literal["all", "upgrade", "unreviewed"] = "unreviewed",
                  since_id: int | None = None,
                  since_date: str | None = None) -> dict[str, int]:
    """LLM-review videos.

    scope: "unreviewed" — only NULL llm_review
           "upgrade"    — unreviewed + reviewed by a lower-tier model
           "all"        — every video
    since_id:   only review videos with id >= this value
    since_date: only review videos with created_at >= this value (str)

    Blocked videos are added to the blocklist and deleted.
    """
    stmt = select(
        videos.c.id, videos.c.video_id, videos.c.title, videos.c.description,
        videos.c.url, videos.c.channel_name, videos.c.dialect, videos.c.source,
        videos.c.llm_review,
    )

    conditions = []
    if scope == "upgrade":
        conditions.append(or_(videos.c.llm_review.is_(None), videos.c.llm_review != model))
    elif scope != "all":  # unreviewed (also re-pick rows stranded as '<model>:error')
        conditions.append(or_(videos.c.llm_review.is_(None), videos.c.llm_review.like("%:error")))

    if since_id is not None:
        conditions.append(videos.c.id >= since_id)
    if since_date is not None:
        conditions.append(videos.c.created_at >= since_date)

    if conditions:
        stmt = stmt.where(*conditions)
    stmt = stmt.order_by(videos.c.id)
    if limit:
        stmt = stmt.limit(limit)
    rows = conn.execute(stmt).mappings().fetchall()

    if scope == "upgrade":
        rows = [r for r in rows
                if r["llm_review"] is None or is_upgrade(r["llm_review"], model)]
    if not rows:
        log.info("No videos need LLM review")
        return {"reviewed": 0, "blocked": 0, "kept": 0, "errors": 0}

    # Build the client up front so a missing 'anthropic' module or bad
    # credentials fail loudly here, instead of being caught per-batch and
    # silently marking every video as ':error' (which never gets retried).
    client = _build_client()

    log.info("LLM reviewing %d videos with %s", len(rows), model)

    reviewed = 0
    blocked = 0
    kept = 0
    routed = 0
    errors = 0

    # Lazily create a routing scrape job + resolve site id when first needed
    routing_job_id = None
    web_site_id = None

    def _ensure_routing_job():
        nonlocal routing_job_id, web_site_id
        if routing_job_id is None:
            web_site_id = db.get_site_id(conn, "web_discovered")
            routing_job_id = db.create_scrape_job(
                conn, web_site_id, job_type="video_review_routed",
            )
        return routing_job_id, web_site_id

    for i in tqdm(range(0, len(rows), VIDEO_BATCH_SIZE), desc="video-review", unit="batch"):
        batch = rows[i:i + VIDEO_BATCH_SIZE]
        try:
            results = _call_llm(client, _video_items(batch), model,
                                VIDEO_SYSTEM_PROMPT, VideoVerdictBatch, 32768)
            for item in results:
                vid_id = item["id"]
                verdict = item["verdict"]
                vid_row = next((r for r in batch if r["id"] == vid_id), None)

                if verdict == "package":
                    reason = item.get("reason", "actually a code package")
                    vid_url = vid_row["url"] if vid_row else ""
                    if vid_row and vid_url:
                        meta = {
                            "name": (vid_row["title"] or "")[:500],
                            "url": vid_url,
                            "source": "video_review_routed",
                            "description": (vid_row["description"] or "")[:5000],
                            "code_blocks": [],
                            "code_block_count": 0,
                            "file_links": [],
                            "file_link_count": 0,
                        }
                        try:
                            job_id, site_id_for_routing = _ensure_routing_job()
                            db.insert_scrape_raw(
                                conn, job_id, site_id_for_routing,
                                vid_url[:500], meta,
                            )
                        except Exception as e:
                            log.warning("Could not route video %s to packages: %s",
                                        vid_id, e)
                        conn.execute(videos.delete().where(videos.c.id == vid_id))
                        routed += 1
                        title = vid_row["title"] if vid_row else "?"
                        log.info("ROUTED to packages: %s — %s",
                                 title[:80], reason)
                        continue
                    log.info("Package verdict but no URL for video id %s; blocking",
                             vid_id)
                    verdict = "block"

                if verdict == "block":
                    reason = item.get("reason", "LLM flagged as not Smalltalk")
                    video_id = vid_row["video_id"] if vid_row else str(vid_id)
                    # Add to blocklist so it doesn't come back on re-scrape
                    conn.execute(
                        db.insert_ignore(
                            blocklist,
                            {
                                "external_id": video_id,
                                "site_name": "youtube",
                                "reason": f"LLM: {reason}"[:500],
                            },
                            ["external_id", "site_name"],
                        )
                    )
                    conn.execute(videos.delete().where(videos.c.id == vid_id))
                    blocked += 1
                    title = vid_row["title"] if vid_row else "?"
                    log.info("BLOCKED video: %s — %s", title[:80], reason)
                else:
                    conn.execute(
                        videos.update()
                        .where(videos.c.id == vid_id)
                        .values(llm_review=model)
                    )
                    kept += 1
            conn.commit()
            reviewed += len(batch)

        except Exception as e:
            log.error("Video LLM batch error at offset %d: %s", i, e)
            errors += 1
            for row in batch:
                conn.execute(
                    videos.update()
                    .where(videos.c.id == row["id"])
                    .values(llm_review=f"{model}:error")
                )
            conn.commit()

    log.info("Video review done: reviewed=%d kept=%d blocked=%d routed=%d errors=%d",
             reviewed, kept, blocked, routed, errors)
    return {"reviewed": reviewed, "blocked": blocked, "kept": kept,
            "routed": routed, "errors": errors}
