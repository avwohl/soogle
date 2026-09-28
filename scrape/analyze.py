"""Analyze discovered domains for structured data potential.

After the discovery scraper finds pages across many domains, this module
groups them by domain, probes each site (root page, sitemap, robots.txt),
and asks an LLM whether the site has structured data worth a dedicated
scraper.

Usage:
    python -m scrape analyze [--limit N] [--min-urls 2]
"""

import json
import logging
import time
from typing import Any
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup
from pydantic import BaseModel
from sqlalchemy import func, select
from tqdm import tqdm

from . import config, db
from .schema import scrape_raw, site_analyses

log = logging.getLogger(__name__)

# Domains we already have dedicated scrapers for, or that are aggregators
# (not actual code hosts). Skip these to avoid wasting LLM calls.
_SKIP_DOMAINS = {
    # Already scraped by dedicated scrapers
    "github.com", "www.github.com", "gist.github.com",
    "gitlab.com", "www.gitlab.com",
    "smalltalkhub.com", "www.smalltalkhub.com",
    "squeaksource.com", "www.squeaksource.com",
    "ss3.gemtalksystems.com",  # SqueakSource3
    "squeaksource3.com", "www.squeaksource3.com",
    # Aggregators / social / not code sources
    "news.ycombinator.com",
    "www.reddit.com", "reddit.com",
    "stackoverflow.com", "www.stackoverflow.com",
    "twitter.com", "x.com",
    "www.youtube.com", "youtube.com",
    "www.amazon.com", "amazon.com",
    "www.google.com", "google.com",
    "www.linkedin.com", "linkedin.com",
    "www.facebook.com", "facebook.com",
    "web.archive.org",
    # Aggregators / directories that link to code but don't host it
    "www.libhunt.com", "awesomeopensource.com",
    "openhub.net",
    # Clearly not Smalltalk code sources
    "finance.yahoo.com", "markets.financialcontent.com",
    "www.onthisday.com", "www.google-dorking.com",
    "store.steampowered.com", "ssojet.com",
    "www.caingram.info", "www.city-data.com",
    "www.websiteplanet.com", "lowcodeplatforms.org",
    # Slide/doc hosting (no scrapable code)
    "www.slideshare.net", "www.scribd.com",
}

_SYSTEM_PROMPT = """\
You are analyzing a website to determine if it contains structured Smalltalk \
source code that could be programmatically scraped more thoroughly than a \
generic web crawler.

You will receive:
- The domain name
- A common URL prefix if the Smalltalk content lives under a subtree (e.g. /wiki/smalltalk/)
- Sample URLs we already found on this domain
- The subtree root page HTML if applicable
- The domain root page HTML (truncated)
- Contents of sitemap.xml and/or robots.txt if available

IMPORTANT: The Smalltalk content may only exist under a subtree of the site, \
not the whole domain. A university wiki might have Smalltalk code only under \
/courses/cs101/. Focus your analysis on the relevant section, not the whole site.

Assess whether this site has:
1. A project/package listing page (browsable index of code)
2. An API (REST, JSON, XML feeds, RSS)
3. A sitemap or structured URL scheme
4. Pagination that a crawler could follow
5. Downloadable archives (.st, .mcz, .zip, .changes files)
6. Any other structure that would let us enumerate ALL Smalltalk content \
rather than just the pages a search engine happened to index

Respond with JSON only, no markdown fences:
{
  "structured_score": <0-100, how confident you are this site has exploitable structure>,
  "has_sitemap": <true/false>,
  "features": [<list of structured features found, e.g. "project listing page", "JSON API", "file index">],
  "recommended_approach": "<one paragraph: how a dedicated scraper should work, or 'not worth it' if generic scraping is fine>",
  "key_urls": [<any URLs that look like entry points for structured scraping>]
}"""


def _fetch_quietly(session, url, timeout=15):
    """Fetch a URL, return (text, status_code) or (None, None) on failure."""
    try:
        resp = session.get(url, timeout=timeout)
        return resp.text[:50000], resp.status_code
    except Exception:
        return None, None


def _common_url_prefix(urls):
    """Find the common path prefix across a list of URLs on the same domain.

    Returns the longest shared path prefix (directory level), or "/" if
    the URLs only share the domain.  For example:
        https://example.com/smalltalk/tools/foo
        https://example.com/smalltalk/libs/bar
    -> "/smalltalk/"
    """
    if not urls:
        return "/"
    paths = [urlparse(u).path for u in urls]
    # Split into segments, find common prefix
    split = [p.strip("/").split("/") for p in paths]
    prefix_parts = []
    for parts in zip(*split):
        if len(set(parts)) == 1:
            prefix_parts.append(parts[0])
        else:
            break
    # Filter out empty segments (from root-path-only URLs)
    prefix_parts = [p for p in prefix_parts if p]
    if prefix_parts:
        return "/" + "/".join(prefix_parts) + "/"
    return "/"


def _get_discovered_domains(conn, min_urls=2):
    """Group web_discovered scrape_raw rows by domain, return domains with info.

    Tracks all URLs per domain so we can compute the common subtree prefix.
    """
    site_id = db.get_site_id(conn, "web_discovered")
    rows = conn.execute(
        select(scrape_raw.c.external_id, scrape_raw.c.raw_metadata)
        .where(
            scrape_raw.c.site_id == site_id,
            scrape_raw.c.status.in_(["pending", "processed"]),
        )
    ).mappings().fetchall()

    domains = {}
    for row in rows:
        url = row["external_id"]
        host = urlparse(url).hostname
        if not host:
            continue
        if host not in domains:
            domains[host] = {"all_urls": [], "urls": [], "count": 0}
        domains[host]["all_urls"].append(url)
        domains[host]["count"] += 1
        if len(domains[host]["urls"]) < 5:
            domains[host]["urls"].append(url)

    # Compute common prefix per domain
    for info in domains.values():
        info["prefix"] = _common_url_prefix(info["all_urls"])
        del info["all_urls"]  # don't carry the full list forward

    # Filter to domains with enough hits and not already analyzed
    already = {r["domain"] for r in conn.execute(select(site_analyses.c.domain)).mappings().fetchall()}

    return {
        d: info for d, info in domains.items()
        if info["count"] >= min_urls and d not in already and d not in _SKIP_DOMAINS
    }


def _probe_site(session, domain, prefix="/"):
    """Fetch root page, subtree page, sitemap.xml, and robots.txt for a domain.

    If prefix is not "/", we fetch both the domain root and the subtree root,
    since the Smalltalk content may live under a subtree like /smalltalk/.
    """
    base = f"https://{domain}"

    root_html, root_status = _fetch_quietly(session, base)
    if root_status is None or root_status >= 400:
        base = f"http://{domain}"
        root_html, root_status = _fetch_quietly(session, base)

    # If there's a common subtree, fetch that page too
    subtree_html = None
    subtree_url = None
    if prefix != "/":
        subtree_url = base + prefix
        subtree_html, subtree_status = _fetch_quietly(session, subtree_url)
        if subtree_status is None or subtree_status >= 400:
            subtree_html = None

    sitemap_text, sitemap_status = _fetch_quietly(session, f"{base}/sitemap.xml")
    if sitemap_status != 200:
        sitemap_text = None

    robots_text, robots_status = _fetch_quietly(session, f"{base}/robots.txt")
    if robots_status != 200:
        robots_text = None

    root_title = None
    if root_html:
        soup = BeautifulSoup(root_html, "html.parser")
        t = soup.find("title")
        if t:
            root_title = t.get_text(strip=True)[:500]

    return {
        "base_url": base,
        "prefix": prefix,
        "subtree_url": subtree_url,
        "root_html": root_html,
        "root_title": root_title,
        "subtree_html": subtree_html,
        "sitemap": sitemap_text,
        "robots": robots_text,
    }


class SiteAnalysis(BaseModel):
    structured_score: int
    has_sitemap: bool
    features: list[str]
    recommended_approach: str
    key_urls: list[str]


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


def _ask_llm(client, domain, sample_urls, probe):
    """Send site info to the LLM and get a structured assessment."""
    user_parts = [f"Domain: {domain}"]

    prefix = probe.get("prefix", "/")
    if prefix != "/":
        user_parts.append(f"Common URL prefix (subtree where Smalltalk content lives): {prefix}")
        user_parts.append(
            "NOTE: The Smalltalk content is concentrated under this subtree, "
            "not spread across the whole site. Focus your analysis on this area."
        )
    user_parts.append("")

    user_parts.append("Sample URLs we found via web search:")
    for u in sample_urls:
        user_parts.append(f"  - {u}")
    user_parts.append("")

    if probe.get("subtree_html"):
        user_parts.append(f"=== Subtree root page: {probe['subtree_url']} (truncated) ===")
        user_parts.append(probe["subtree_html"][:15000])
        user_parts.append("")

    if probe["root_html"]:
        user_parts.append("=== Domain root page HTML (truncated) ===")
        user_parts.append(probe["root_html"][:15000])
        user_parts.append("")

    if probe["sitemap"]:
        user_parts.append("=== sitemap.xml ===")
        user_parts.append(probe["sitemap"][:10000])
        user_parts.append("")

    if probe["robots"]:
        user_parts.append("=== robots.txt ===")
        user_parts.append(probe["robots"][:5000])
        user_parts.append("")

    model = config.OPENAI_MODEL if config.OPENAI_BASE_URL else config.ANALYZE_MODEL
    try:
        resp = client.create(
            model=model,
            max_tokens=32768,
            messages=[
                {"role": "system", "content": _SYSTEM_PROMPT},
                {"role": "user", "content": "\n".join(user_parts)},
            ],
            response_model=SiteAnalysis,
        )
    except Exception as e:
        log.warning("LLM analysis failed for %s: %s", domain, e)
        return None
    return resp.model_dump()


def _save_analysis(conn, domain, urls_found, sample_urls, root_title, result):
    """Save LLM analysis to site_analyses table."""
    conn.execute(
        db.upsert(
            site_analyses,
            {
                "domain": domain,
                "urls_found": urls_found,
                "sample_urls": json.dumps(sample_urls),
                "root_page_title": root_title,
                "has_sitemap": result.get("has_sitemap", False),
                "structured_score": result.get("structured_score", 0),
                "recommendation": json.dumps(result, indent=2),
                "llm_model": config.OPENAI_MODEL if config.OPENAI_BASE_URL else config.ANALYZE_MODEL,
                "analyzed_at": func.now(),
            },
            ["domain"],
        )
    )
    conn.commit()


def analyze_domains(conn, limit=None, min_urls=2):
    """Analyze discovered domains for structured scraping potential."""
    if not config.ANTHROPIC_API_KEY and not config.OPENAI_BASE_URL:
        raise RuntimeError(
            "analyze requires ANTHROPIC_API_KEY or OPENAI_BASE_URL to be set."
        )

    domains = _get_discovered_domains(conn, min_urls=min_urls)
    if not domains:
        log.info("No new domains to analyze")
        return {"analyzed": 0, "promising": 0, "errors": 0}

    # Sort by URL count descending — most-seen domains first
    sorted_domains = sorted(domains.items(), key=lambda x: -x[1]["count"])
    if limit:
        sorted_domains = sorted_domains[:limit]

    log.info("Analyzing %d discovered domains", len(sorted_domains))

    # Build the client up front so a missing SDK module or bad credentials
    # fail loudly here instead of being swallowed per-domain.
    client = _build_client()

    session = requests.Session()
    session.headers.update({"User-Agent": config.USER_AGENT})

    analyzed = 0
    promising = 0
    errors = 0

    for domain, info in tqdm(sorted_domains, desc="analyze", unit="domain"):
        prefix = info.get("prefix", "/")
        prefix_msg = f" prefix={prefix}" if prefix != "/" else ""
        log.info("Analyzing %s (%d URLs found%s) ...", domain, info["count"], prefix_msg)
        try:
            probe = _probe_site(session, domain, prefix=prefix)
            time.sleep(1)

            result = _ask_llm(client, domain, info["urls"], probe)
            if result is None:
                log.warning("  skipped (LLM parse failure)")
                continue

            score = result.get("structured_score", 0)
            _save_analysis(
                conn, domain, info["count"], info["urls"],
                probe["root_title"], result,
            )
            analyzed += 1

            if score >= 50:
                promising += 1
                log.info("  score=%d  PROMISING  %s",
                         score, result.get("recommended_approach", "")[:120])
            else:
                log.info("  score=%d  (not worth a dedicated scraper)", score)

        except Exception as e:
            log.error("  failed: %s", e)
            errors += 1

    log.info("Analysis done: analyzed=%d promising=%d errors=%d (score>=50)",
             analyzed, promising, errors)
    return {"analyzed": analyzed, "promising": promising, "errors": errors}


def show_results(conn, min_score=0):
    """Print analysis results."""
    rows = conn.execute(
        select(
            site_analyses.c.domain, site_analyses.c.urls_found,
            site_analyses.c.root_page_title, site_analyses.c.has_sitemap,
            site_analyses.c.structured_score, site_analyses.c.recommendation,
            site_analyses.c.analyzed_at,
        )
        .where(site_analyses.c.structured_score >= min_score)
        .order_by(site_analyses.c.structured_score.desc())
    ).mappings().fetchall()

    if not rows:
        print("No analyzed domains" + (f" with score >= {min_score}" if min_score else ""))
        return

    for r in rows:
        print(f"\n{'='*60}")
        print(f"{r['domain']}\tscore={r['structured_score']}\turls_found={r['urls_found']}")
        if r['root_page_title']:
            print(f"  title: {r['root_page_title']}")
        if r['has_sitemap']:
            print("  has sitemap.xml")

        rec = r["recommendation"]
        if rec:
            try:
                data = json.loads(rec)
                if data.get("features"):
                    print(f"  features: {', '.join(data['features'])}")
                if data.get("recommended_approach"):
                    print(f"  approach: {data['recommended_approach']}")
                if data.get("key_urls"):
                    for u in data["key_urls"]:
                        print(f"  key URL: {u}")
            except (json.JSONDecodeError, TypeError):
                print(f"  {rec[:300]}")
