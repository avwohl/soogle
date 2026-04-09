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

import re
import time
import logging
import base64
import requests
import json
from urllib.parse import urlparse, parse_qs
from . import config, db
from .models import model_tier, is_upgrade

log = logging.getLogger(__name__)

BATCH_SIZE = 20  # packages per LLM call


# Matches an 11-character YouTube video id in any common URL form.
_YOUTUBE_ID_RE = re.compile(r"([A-Za-z0-9_-]{11})")


def _extract_youtube_id(url):
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
        if _YOUTUBE_ID_RE.fullmatch(candidate):
            return candidate
    if "youtube.com" in host or "youtube-nocookie.com" in host:
        if path.startswith(("/embed/", "/v/", "/shorts/")):
            candidate = path.split("/", 2)[2].split("/", 1)[0]
            if _YOUTUBE_ID_RE.fullmatch(candidate):
                return candidate
        qs = parse_qs(parsed.query)
        v = qs.get("v", [""])[0]
        if _YOUTUBE_ID_RE.fullmatch(v):
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


def _github_session():
    session = requests.Session()
    session.headers.update({
        "Accept": "application/vnd.github+json",
        "User-Agent": config.USER_AGENT,
        "X-GitHub-Api-Version": "2022-11-28",
    })
    if config.GITHUB_TOKEN:
        session.headers["Authorization"] = f"Bearer {config.GITHUB_TOKEN}"
    return session


def fetch_readmes(conn, limit=None):
    """Fetch README excerpts from GitHub for packages missing them."""
    cur = conn.cursor()
    sql = """
        SELECT p.id, p.external_id
        FROM packages p JOIN sites s ON p.site_id = s.id
        WHERE s.name = 'github' AND p.readme_excerpt IS NULL
        ORDER BY p.stars DESC, p.id
    """
    if limit:
        sql += f" LIMIT {int(limit)}"
    cur.execute(sql)
    rows = cur.fetchall()
    if not rows:
        log.info("No packages need README fetching")
        return 0

    log.info("Fetching READMEs for %d packages", len(rows))
    session = _github_session()
    fetched = 0
    errors = 0

    for row in rows:
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
                cur.execute(
                    "UPDATE packages SET readme_excerpt = '' WHERE id = %s",
                    (pkg_id,),
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
            cur.execute(
                "UPDATE packages SET readme_excerpt = %s WHERE id = %s",
                (excerpt, pkg_id),
            )
            conn.commit()
            fetched += 1

            if fetched % 100 == 0:
                log.info("Fetched %d/%d READMEs (%d errors)", fetched, len(rows), errors)

        except Exception as e:
            log.warning("Error fetching README for %s: %s", full_name, e)
            errors += 1

    log.info("README fetch done: fetched=%d errors=%d", fetched, errors)
    cur.close()
    return fetched


def _call_llm(packages, model):
    """Send a batch of packages to the LLM for classification."""
    import anthropic
    client = anthropic.Anthropic(api_key=config.ANTHROPIC_API_KEY)

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

    user_msg = json.dumps(items, indent=None)
    resp = client.messages.create(
        model=model,
        max_tokens=2048,
        system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content": user_msg}],
    )
    text = resp.content[0].text.strip()
    # Strip markdown code fences
    if text.startswith("```"):
        text = text.split("\n", 1)[1].rsplit("```", 1)[0].strip()
    # Find the JSON array even if surrounded by prose
    start = text.find("[")
    end = text.rfind("]")
    if start >= 0 and end > start:
        text = text[start:end + 1]
    return json.loads(text)


def review_packages(conn, limit=None, model="claude-haiku-4-5-20251001",
                    scope="unreviewed", since_id=None, since_date=None):
    """LLM-review packages.

    scope: "unreviewed" — only NULL llm_review
           "upgrade"    — unreviewed + reviewed by a lower-tier model
           "all"        — every package
    since_id:   only review packages with id >= this value
    since_date: only review packages with created_at >= this value (str)
    """
    cur = conn.cursor()
    params = []
    cols = """p.id, p.name, p.qualified_name, p.description, p.stars,
                   p.dialect, p.topics, p.readme_excerpt, p.url, p.external_id,
                   s.name as site_name, p.llm_review"""
    base = f"SELECT {cols}\n            FROM packages p JOIN sites s ON p.site_id = s.id"

    conditions = []
    if scope == "upgrade":
        conditions.append("(p.llm_review IS NULL OR p.llm_review != %s)")
        params.append(model)
    elif scope != "all":  # unreviewed
        conditions.append("p.llm_review IS NULL")

    if since_id is not None:
        conditions.append("p.id >= %s")
        params.append(since_id)
    if since_date is not None:
        conditions.append("p.created_at >= %s")
        params.append(since_date)

    sql = base
    if conditions:
        sql += "\n            WHERE " + " AND ".join(conditions)
    sql += "\n            ORDER BY p.stars DESC, p.id"
    if limit:
        sql += f" LIMIT {int(limit)}"
    cur.execute(sql, params)
    rows = cur.fetchall()

    # For upgrade scope, filter to rows actually reviewed by a lower tier
    if scope == "upgrade":
        rows = [r for r in rows
                if r["llm_review"] is None or is_upgrade(r["llm_review"], model)]
    if not rows:
        log.info("No packages need LLM review")
        return {"reviewed": 0, "blocked": 0, "kept": 0, "errors": 0}

    log.info("LLM reviewing %d packages with %s", len(rows), model)

    reviewed = 0
    blocked = 0
    kept = 0
    errors = 0

    # Process in batches
    routed = 0
    for i in range(0, len(rows), BATCH_SIZE):
        batch = rows[i:i + BATCH_SIZE]
        try:
            results = _call_llm(batch, model)
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
                            cur.execute(
                                "INSERT IGNORE INTO videos "
                                "(video_id, title, url, description, dialect, source) "
                                "VALUES (%s, %s, %s, %s, %s, %s)",
                                (video_id,
                                 (pkg_row["name"] or "")[:500],
                                 pkg_url,
                                 (pkg_row["description"] or "")[:5000],
                                 pkg_row["dialect"] or "unknown",
                                 "package_review_routed"),
                            )
                        except Exception as e:
                            log.warning("Could not insert routed video %s: %s",
                                        video_id, e)
                        cur.execute("UPDATE scrape_raw SET package_id=NULL WHERE package_id=%s", (pkg_id,))
                        cur.execute("DELETE FROM package_methods WHERE package_id=%s", (pkg_id,))
                        cur.execute("DELETE FROM package_classes WHERE package_id=%s", (pkg_id,))
                        cur.execute("DELETE FROM package_categories WHERE package_id=%s", (pkg_id,))
                        cur.execute("DELETE FROM packages WHERE id=%s", (pkg_id,))
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
                    cur.execute(
                        "INSERT IGNORE INTO blocklist (external_id, site_name, reason) "
                        "VALUES (%s, %s, %s)",
                        (ext_id, site_name, f"LLM: {reason}"[:500]),
                    )
                    # Delete the package
                    cur.execute("UPDATE scrape_raw SET package_id=NULL WHERE package_id=%s", (pkg_id,))
                    cur.execute("DELETE FROM package_methods WHERE package_id=%s", (pkg_id,))
                    cur.execute("DELETE FROM package_classes WHERE package_id=%s", (pkg_id,))
                    cur.execute("DELETE FROM package_categories WHERE package_id=%s", (pkg_id,))
                    cur.execute("DELETE FROM packages WHERE id=%s", (pkg_id,))
                    blocked += 1
                    log.info("BLOCKED: %s — %s", ext_id, reason)
                else:
                    cur.execute(
                        "UPDATE packages SET llm_review = %s WHERE id = %s",
                        (model, pkg_id),
                    )
                    kept += 1
            conn.commit()
            reviewed += len(batch)

            if reviewed % 100 == 0:
                log.info("Progress: reviewed=%d kept=%d blocked=%d routed=%d",
                         reviewed, kept, blocked, routed)

        except Exception as e:
            log.error("LLM batch error at offset %d: %s", i, e)
            errors += 1
            # Mark batch as reviewed with error so we don't retry endlessly
            for row in batch:
                cur.execute(
                    "UPDATE packages SET llm_review = %s WHERE id = %s",
                    (f"{model}:error", row["id"]),
                )
            conn.commit()

    log.info("LLM review done: reviewed=%d kept=%d blocked=%d routed=%d errors=%d",
             reviewed, kept, blocked, routed, errors)
    cur.close()
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


def _call_video_llm(videos, model):
    """Send a batch of videos to the LLM for classification."""
    import anthropic
    client = anthropic.Anthropic(api_key=config.ANTHROPIC_API_KEY)

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

    user_msg = json.dumps(items, indent=None)
    resp = client.messages.create(
        model=model,
        max_tokens=4096,
        system=VIDEO_SYSTEM_PROMPT,
        messages=[{"role": "user", "content": user_msg}],
    )
    text = resp.content[0].text.strip()
    if text.startswith("```"):
        text = text.split("\n", 1)[1].rsplit("```", 1)[0].strip()
    start = text.find("[")
    end = text.rfind("]")
    if start >= 0 and end > start:
        text = text[start:end + 1]
    return json.loads(text)


def review_videos(conn, limit=None, model="claude-haiku-4-5-20251001",
                  scope="unreviewed", since_id=None, since_date=None):
    """LLM-review videos.

    scope: "unreviewed" — only NULL llm_review
           "upgrade"    — unreviewed + reviewed by a lower-tier model
           "all"        — every video
    since_id:   only review videos with id >= this value
    since_date: only review videos with created_at >= this value (str)

    Blocked videos are added to the blocklist and deleted.
    """
    cur = conn.cursor()
    params = []
    cols = """id, video_id, title, description, url, channel_name,
                   dialect, source, llm_review"""
    base = f"SELECT {cols}\n            FROM videos"

    conditions = []
    if scope == "upgrade":
        conditions.append("(llm_review IS NULL OR llm_review != %s)")
        params.append(model)
    elif scope != "all":  # unreviewed
        conditions.append("llm_review IS NULL")

    if since_id is not None:
        conditions.append("id >= %s")
        params.append(since_id)
    if since_date is not None:
        conditions.append("created_at >= %s")
        params.append(since_date)

    sql = base
    if conditions:
        sql += "\n            WHERE " + " AND ".join(conditions)
    sql += "\n            ORDER BY id"
    if limit:
        sql += f" LIMIT {int(limit)}"
    cur.execute(sql, params)
    rows = cur.fetchall()

    if scope == "upgrade":
        rows = [r for r in rows
                if r["llm_review"] is None or is_upgrade(r["llm_review"], model)]
    if not rows:
        log.info("No videos need LLM review")
        return {"reviewed": 0, "blocked": 0, "kept": 0, "errors": 0}

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

    for i in range(0, len(rows), VIDEO_BATCH_SIZE):
        batch = rows[i:i + VIDEO_BATCH_SIZE]
        try:
            results = _call_video_llm(batch, model)
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
                        cur.execute("DELETE FROM videos WHERE id = %s", (vid_id,))
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
                    cur.execute(
                        "INSERT IGNORE INTO blocklist (external_id, site_name, reason) "
                        "VALUES (%s, %s, %s)",
                        (video_id, "youtube", f"LLM: {reason}"[:500]),
                    )
                    cur.execute("DELETE FROM videos WHERE id = %s", (vid_id,))
                    blocked += 1
                    title = vid_row["title"] if vid_row else "?"
                    log.info("BLOCKED video: %s — %s", title[:80], reason)
                else:
                    cur.execute(
                        "UPDATE videos SET llm_review = %s WHERE id = %s",
                        (model, vid_id),
                    )
                    kept += 1
            conn.commit()
            reviewed += len(batch)

            if reviewed % 100 == 0:
                log.info("Video progress: reviewed=%d kept=%d blocked=%d routed=%d",
                         reviewed, kept, blocked, routed)

        except Exception as e:
            log.error("Video LLM batch error at offset %d: %s", i, e)
            errors += 1
            for row in batch:
                cur.execute(
                    "UPDATE videos SET llm_review = %s WHERE id = %s",
                    (f"{model}:error", row["id"]),
                )
            conn.commit()

    log.info("Video review done: reviewed=%d kept=%d blocked=%d routed=%d errors=%d",
             reviewed, kept, blocked, routed, errors)
    cur.close()
    return {"reviewed": reviewed, "blocked": blocked, "kept": kept,
            "routed": routed, "errors": errors}
