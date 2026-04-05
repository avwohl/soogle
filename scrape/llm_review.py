"""LLM-based quality review of packages and videos.

Fetches README excerpts from GitHub, then asks an LLM to judge whether
each package is genuinely Smalltalk-related.  Non-Smalltalk packages are
moved to the blocklist.

Also reviews videos for relevance — removes "small talk" conversation
videos, generic OOP content, gemstone jewelry, spam, etc.

Usage:
    python -m scrape llm-review [--limit N] [--fetch-only] [--review-only]
    python -m scrape video-review [--limit N] [--model MODEL]
"""

import time
import logging
import base64
import requests
import json
from . import config, db
from .models import model_tier, is_upgrade

log = logging.getLogger(__name__)

BATCH_SIZE = 20  # packages per LLM call

SYSTEM_PROMPT = """\
You are a classifier for a Smalltalk code search engine called Soogle.
Your job is to decide whether each package is genuinely related to Smalltalk
(the programming language family: Pharo, Squeak, Cuis, GemStone, VisualWorks,
GNU Smalltalk, Dolphin, VA Smalltalk, Newspeak, etc.) or is a false positive
that was mis-indexed.

Common false positives:
- C# / .NET projects (GitHub linguist confuses .cs changeset files)
- IEC 61131-3 Structured Text / PLC projects (.st extension overlap)
- NLP/ML research using StringTemplate .st files
- Unity game projects
- Random repos with a tiny .cs or .st file

For each package, respond with a JSON array of objects:
[{"id": 123, "verdict": "keep"}, {"id": 456, "verdict": "block", "reason": "C# .NET project"}]

Verdicts: "keep" (genuine Smalltalk) or "block" (not Smalltalk).
Only add "reason" for "block" verdicts. Be concise.
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
    # Extract JSON from response (may be wrapped in markdown code block)
    if text.startswith("```"):
        text = text.split("\n", 1)[1].rsplit("```", 1)[0].strip()
    return json.loads(text)


def review_packages(conn, limit=None, model="claude-haiku-4-5-20251001",
                    scope="unreviewed"):
    """LLM-review packages.

    scope: "unreviewed" — only NULL llm_review
           "upgrade"    — unreviewed + reviewed by a lower-tier model
           "all"        — every package
    """
    cur = conn.cursor()
    if scope == "all":
        sql = """
            SELECT p.id, p.name, p.qualified_name, p.description, p.stars,
                   p.dialect, p.topics, p.readme_excerpt, s.name as site_name,
                   p.llm_review
            FROM packages p JOIN sites s ON p.site_id = s.id
            ORDER BY p.stars DESC, p.id
        """
    elif scope == "upgrade":
        sql = """
            SELECT p.id, p.name, p.qualified_name, p.description, p.stars,
                   p.dialect, p.topics, p.readme_excerpt, s.name as site_name,
                   p.llm_review
            FROM packages p JOIN sites s ON p.site_id = s.id
            WHERE p.llm_review IS NULL OR p.llm_review != %s
            ORDER BY p.stars DESC, p.id
        """
    else:  # unreviewed
        sql = """
            SELECT p.id, p.name, p.qualified_name, p.description, p.stars,
                   p.dialect, p.topics, p.readme_excerpt, s.name as site_name,
                   p.llm_review
            FROM packages p JOIN sites s ON p.site_id = s.id
            WHERE p.llm_review IS NULL
            ORDER BY p.stars DESC, p.id
        """
    if limit:
        sql += f" LIMIT {int(limit)}"
    if scope == "upgrade":
        cur.execute(sql, (model,))
    else:
        cur.execute(sql)
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
    for i in range(0, len(rows), BATCH_SIZE):
        batch = rows[i:i + BATCH_SIZE]
        try:
            results = _call_llm(batch, model)
            for item in results:
                pkg_id = item["id"]
                verdict = item["verdict"]
                if verdict == "block":
                    reason = item.get("reason", "LLM flagged as non-Smalltalk")
                    # Find site_name for this package
                    pkg_row = next((r for r in batch if r["id"] == pkg_id), None)
                    site_name = pkg_row["site_name"] if pkg_row else "github"
                    ext_id = pkg_row["qualified_name"] if pkg_row else str(pkg_id)
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
                log.info("Progress: reviewed=%d kept=%d blocked=%d", reviewed, kept, blocked)

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

    log.info("LLM review done: reviewed=%d kept=%d blocked=%d errors=%d",
             reviewed, kept, blocked, errors)
    cur.close()
    return {"reviewed": reviewed, "blocked": blocked, "kept": kept, "errors": errors}


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

For each video, respond with a JSON array of objects:
[{"id": 123, "verdict": "keep"}, {"id": 456, "verdict": "block", "reason": "business small talk lesson"}]

Verdicts: "keep" or "block". Only add "reason" for "block" verdicts.
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
    return json.loads(text)


def review_videos(conn, limit=None, model="claude-haiku-4-5-20251001",
                  scope="unreviewed"):
    """LLM-review videos.

    scope: "unreviewed" — only NULL llm_review
           "upgrade"    — unreviewed + reviewed by a lower-tier model
           "all"        — every video

    Blocked videos are added to the blocklist and deleted.
    """
    cur = conn.cursor()
    if scope == "all":
        sql = """
            SELECT id, video_id, title, description, channel_name,
                   dialect, source, llm_review
            FROM videos ORDER BY id
        """
    elif scope == "upgrade":
        sql = """
            SELECT id, video_id, title, description, channel_name,
                   dialect, source, llm_review
            FROM videos
            WHERE llm_review IS NULL OR llm_review != %s
            ORDER BY id
        """
    else:  # unreviewed
        sql = """
            SELECT id, video_id, title, description, channel_name,
                   dialect, source, llm_review
            FROM videos
            WHERE llm_review IS NULL
            ORDER BY id
        """
    if limit:
        sql += f" LIMIT {int(limit)}"
    if scope == "upgrade":
        cur.execute(sql, (model,))
    else:
        cur.execute(sql)
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
    errors = 0

    for i in range(0, len(rows), VIDEO_BATCH_SIZE):
        batch = rows[i:i + VIDEO_BATCH_SIZE]
        try:
            results = _call_video_llm(batch, model)
            for item in results:
                vid_id = item["id"]
                verdict = item["verdict"]
                if verdict == "block":
                    reason = item.get("reason", "LLM flagged as not Smalltalk")
                    vid_row = next((r for r in batch if r["id"] == vid_id), None)
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
                log.info("Video progress: reviewed=%d kept=%d blocked=%d",
                         reviewed, kept, blocked)

        except Exception as e:
            log.error("Video LLM batch error at offset %d: %s", i, e)
            errors += 1
            for row in batch:
                cur.execute(
                    "UPDATE videos SET llm_review = %s WHERE id = %s",
                    (f"{model}:error", row["id"]),
                )
            conn.commit()

    log.info("Video review done: reviewed=%d kept=%d blocked=%d errors=%d",
             reviewed, kept, blocked, errors)
    cur.close()
    return {"reviewed": reviewed, "blocked": blocked, "kept": kept, "errors": errors}
