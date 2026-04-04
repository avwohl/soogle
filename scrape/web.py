"""Web scrapers for non-GitHub Smalltalk sources.

Covers:
    - SqueakSource      (enumerate projects, scrape metadata)
    - SmalltalkHub       (archived projects listing)
    - Rosetta Code       (MediaWiki API for Smalltalk examples)
    - VS-KB              (Visual Smalltalk Knowledge Base code library)
    - discover           (web-search discovery of new Smalltalk code sources)

Each scraper writes raw data into scrape_raw for later processing.

Usage:
    python -m scrape web squeaksource
    python -m scrape web smalltalkhub
    python -m scrape web rosettacode
    python -m scrape web vskb
    python -m scrape web discover
    python -m scrape web all
"""

import re
import time
import json
import logging
import warnings
import requests
from bs4 import BeautifulSoup, XMLParsedAsHTMLWarning

warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)
from urllib.parse import urljoin, urlparse
from . import config, db

log = logging.getLogger(__name__)


class BaseScraper:
    def __init__(self, conn, site_name):
        self.conn = conn
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": config.USER_AGENT})
        self.site_id = db.get_site_id(conn, site_name)
        self.site_name = site_name

    def get(self, url, **kwargs):
        kwargs.setdefault("timeout", config.REQUEST_TIMEOUT)
        time.sleep(1)  # polite crawling
        resp = self.session.get(url, **kwargs)
        resp.raise_for_status()
        return resp

    def soup(self, url):
        resp = self.get(url)
        ct = resp.headers.get("content-type", "")
        if "xml" in ct:
            return BeautifulSoup(resp.text, "xml")
        return BeautifulSoup(resp.text, "html.parser")


# ---------------------------------------------------------------------------
# SqueakSource
# ---------------------------------------------------------------------------
class SqueakSourceScraper(BaseScraper):
    """Scrapes SqueakSource project listings.

    SqueakSource uses Seaside (session-based URLs).  We navigate the
    paginated project listing, follow each project's session link to
    its detail page, and extract the stable slug from the
    MCHttpRepository URL shown there.
    """

    BASE_URL = "http://www.squeaksource.com"

    def __init__(self, conn):
        super().__init__(conn, "squeaksource")

    def _navigate_to_projects(self):
        """Start a session and navigate to the Projects listing page."""
        soup = self.soup(self.BASE_URL)
        for link in soup.find_all("a"):
            if link.get_text(strip=True) == "Projects":
                return self.soup(urljoin(self.BASE_URL, link["href"]))
        return None

    def _parse_listing_page(self, soup):
        """Extract project session hrefs and the next-page URL from a listing."""
        import re as _re
        table = soup.find("table")
        if not table:
            return [], None

        project_hrefs = []
        for row in table.find_all("tr", recursive=False):
            cls = row.get("class", [])
            if "oddRow" not in cls and "evenRow" not in cls:
                continue
            cells = row.find_all("td", recursive=False)
            if not cells:
                continue
            link = cells[0].find("a")
            if link and link.get("href"):
                project_hrefs.append(urljoin(self.BASE_URL, link["href"]))

        # Find the ">>" (next) pagination link
        next_url = None
        batch = soup.find("div", id="batch")
        if batch:
            for a in batch.find_all("a"):
                if a.get("title") == "next" or a.get_text(strip=True) == ">>":
                    next_url = urljoin(self.BASE_URL, a["href"])
                    break

        return project_hrefs, next_url

    def _scrape_project_detail(self, detail_url):
        """Follow a session link to a project detail page, return metadata."""
        import re as _re
        soup = self.soup(detail_url)
        text = soup.get_text()

        # Extract slug from MCHttpRepository URL
        m = _re.search(
            r"location:\s*'http://www\.squeaksource\.com/([^']+)'", text,
        )
        if not m:
            return None
        slug = m.group(1)

        meta = {
            "name": slug,
            "url": f"{self.BASE_URL}/{slug}",
            "source": "squeaksource",
        }

        # Description (first <p> in the main content)
        desc_el = soup.find("p")
        if desc_el:
            meta["description"] = desc_el.get_text(strip=True)[:2000]

        # Tags
        tag_links = soup.select("a[href*='tag']")
        tags = [a.get_text(strip=True) for a in tag_links if a.get_text(strip=True)]
        if tags:
            meta["tags"] = tags

        # Stats from the page text (no space after colon in Seaside output)
        for pattern, key in [
            (r"Total Versions:(\d+)", "version_count"),
            (r"Total Downloads:(\d+)", "download_count"),
            (r"Total Releases:(\d+)", "release_count"),
            (r"Registered:(.+?)Total", "registered"),
        ]:
            sm = _re.search(pattern, text)
            if sm:
                meta[key] = sm.group(1).strip()

        return slug, meta

    def run(self):
        job_id = db.create_scrape_job(self.conn, self.site_id, "full_crawl")
        log.info("SqueakSource scrape job %d started", job_id)

        found = saved = errors = 0
        try:
            listing = self._navigate_to_projects()
            if listing is None:
                raise RuntimeError("Could not navigate to SqueakSource projects page")

            page_num = 0
            while True:
                page_num += 1
                project_hrefs, next_url = self._parse_listing_page(listing)
                if not project_hrefs:
                    break

                found += len(project_hrefs)
                log.info("SqueakSource page %d: %d projects (total found=%d)",
                         page_num, len(project_hrefs), found)

                for href in project_hrefs:
                    try:
                        result = self._scrape_project_detail(href)
                        if result is None:
                            continue
                        slug, meta = result
                        row_id = db.insert_scrape_raw(
                            self.conn, job_id, self.site_id, slug, meta,
                        )
                        if row_id:
                            saved += 1
                    except Exception as e:
                        log.error("SqueakSource project failed: %s", e)
                        errors += 1

                if not next_url:
                    break
                listing = self.soup(next_url)

        except Exception as e:
            log.exception("SqueakSource scrape failed")
            db.finish_scrape_job(self.conn, job_id, found, saved, errors, str(e))
            raise

        db.finish_scrape_job(self.conn, job_id, found, saved, errors)
        log.info("SqueakSource done: found=%d saved=%d errors=%d", found, saved, errors)
        return {"found": found, "saved": saved, "errors": errors}


# ---------------------------------------------------------------------------
# SmalltalkHub
# ---------------------------------------------------------------------------
class SmalltalkHubScraper(BaseScraper):
    """Scrapes SmalltalkHub project listings.

    SmalltalkHub is now a static archive.  All project data lives in a
    single projects.json file served from the root.
    """

    BASE_URL = "http://smalltalkhub.com"
    PROJECTS_URL = "http://smalltalkhub.com/projects.json"

    def __init__(self, conn):
        super().__init__(conn, "smalltalkhub")

    def _load_projects(self):
        """Fetch the static projects.json archive."""
        resp = self.get(self.PROJECTS_URL)
        text = resp.content.decode("utf-8-sig")  # BOM-aware
        import json
        data = json.loads(text)
        if isinstance(data, list):
            return data
        if isinstance(data, dict):
            return data.get("data", data.get("projects", []))
        return []

    def run(self):
        job_id = db.create_scrape_job(self.conn, self.site_id, "full_crawl")
        log.info("SmalltalkHub scrape job %d started", job_id)

        found = saved = errors = 0
        try:
            projects = self._load_projects()
            found = len(projects)
            log.info("Found %d projects in SmalltalkHub archive", found)

            for i, proj in enumerate(projects):
                try:
                    owner = proj.get("owner", "") if isinstance(proj, dict) else ""
                    name = proj.get("name", "") if isinstance(proj, dict) else str(proj)
                    if not name:
                        continue

                    meta = {
                        "name": name,
                        "team": owner,
                        "qualified_name": f"{owner}/{name}" if owner else name,
                        "url": f"{self.BASE_URL}/{owner}/{name}" if owner else f"{self.BASE_URL}/{name}",
                        "source": "smalltalkhub",
                    }
                    if isinstance(proj, dict):
                        for key in ("tags", "contributors", "created"):
                            if key in proj:
                                meta[key] = proj[key]

                    ext_id = meta["qualified_name"]
                    row_id = db.insert_scrape_raw(
                        self.conn, job_id, self.site_id, ext_id, meta,
                    )
                    if row_id:
                        saved += 1
                    if (i + 1) % 500 == 0:
                        log.info("SmalltalkHub progress: %d/%d saved=%d", i + 1, found, saved)
                except Exception as e:
                    log.error("SmalltalkHub project %s failed: %s", proj, e)
                    errors += 1

        except Exception as e:
            log.exception("SmalltalkHub scrape failed")
            db.finish_scrape_job(self.conn, job_id, found, saved, errors, str(e))
            raise

        db.finish_scrape_job(self.conn, job_id, found, saved, errors)
        log.info("SmalltalkHub done: found=%d saved=%d errors=%d", found, saved, errors)
        return {"found": found, "saved": saved, "errors": errors}


# ---------------------------------------------------------------------------
# Rosetta Code
# ---------------------------------------------------------------------------
class RosettaCodeScraper(BaseScraper):
    """Scrapes Smalltalk examples from Rosetta Code via MediaWiki API.

    Rosetta Code uses a MediaWiki backend.  Smalltalk pages are in the
    category "Smalltalk" (and sub-categories per dialect).  Each task page
    has code blocks for multiple languages.
    """

    API_URL = "https://rosettacode.org/w/api.php"

    def __init__(self, conn):
        super().__init__(conn, "rosettacode")

    def _category_members(self, category, cmtype="page"):
        """Enumerate all pages in a MediaWiki category, handling continuation."""
        pages = []
        params = {
            "action": "query",
            "list": "categorymembers",
            "cmtitle": f"Category:{category}",
            "cmtype": cmtype,
            "cmlimit": 500,
            "format": "json",
        }
        while True:
            data = self.get(self.API_URL, params=params).json()
            members = data.get("query", {}).get("categorymembers", [])
            pages.extend(members)
            cont = data.get("continue")
            if not cont:
                break
            params.update(cont)
        return pages

    def _get_page_wikitext(self, title):
        """Fetch raw wikitext for a page."""
        params = {
            "action": "query",
            "titles": title,
            "prop": "revisions",
            "rvprop": "content",
            "rvslots": "main",
            "format": "json",
        }
        data = self.get(self.API_URL, params=params).json()
        pages = data.get("query", {}).get("pages", {})
        for page in pages.values():
            revs = page.get("revisions", [])
            if revs:
                return revs[0].get("slots", {}).get("main", {}).get("*", "")
        return ""

    def _extract_smalltalk_code(self, wikitext):
        """Extract Smalltalk code blocks from wikitext.

        Looks for patterns like:
            =={{header|Smalltalk}}==
            <lang smalltalk>...code...</lang>
        or:
            <syntaxhighlight lang="smalltalk">...code...</syntaxhighlight>
        """
        import re
        blocks = []

        # <lang smalltalk>...</lang>  (older Rosetta Code format)
        for m in re.finditer(
            r'<lang\s+["\']?smalltalk["\']?\s*>(.*?)</lang>',
            wikitext, re.DOTALL | re.IGNORECASE,
        ):
            blocks.append(m.group(1).strip())

        # <syntaxhighlight lang="smalltalk">...</syntaxhighlight>  (newer format)
        for m in re.finditer(
            r'<syntaxhighlight\s+lang\s*=\s*["\']?smalltalk["\']?\s*>(.*?)</syntaxhighlight>',
            wikitext, re.DOTALL | re.IGNORECASE,
        ):
            blocks.append(m.group(1).strip())

        return blocks

    def run(self):
        job_id = db.create_scrape_job(self.conn, self.site_id, "full_crawl")
        log.info("Rosetta Code scrape job %d started", job_id)

        found = saved = errors = 0
        try:
            # Get pages in the Smalltalk category
            pages = self._category_members("Smalltalk")
            found = len(pages)
            log.info("Found %d Rosetta Code pages with Smalltalk", found)

            for i, page in enumerate(pages):
                title = page.get("title", "")
                if not title:
                    continue
                try:
                    wikitext = self._get_page_wikitext(title)
                    code_blocks = self._extract_smalltalk_code(wikitext)
                    if not code_blocks:
                        continue

                    meta = {
                        "name": title,
                        "url": f"https://rosettacode.org/wiki/{title.replace(' ', '_')}",
                        "source": "rosettacode",
                        "code_blocks": code_blocks,
                        "code_block_count": len(code_blocks),
                    }

                    row_id = db.insert_scrape_raw(
                        self.conn, job_id, self.site_id, title, meta,
                    )
                    if row_id:
                        saved += 1

                    if (i + 1) % 50 == 0:
                        log.info("Rosetta Code progress: %d/%d saved=%d", i + 1, found, saved)
                except Exception as e:
                    log.error("Rosetta Code page %s failed: %s", title, e)
                    errors += 1

        except Exception as e:
            log.exception("Rosetta Code scrape failed")
            db.finish_scrape_job(self.conn, job_id, found, saved, errors, str(e))
            raise

        db.finish_scrape_job(self.conn, job_id, found, saved, errors)
        log.info("Rosetta Code done: found=%d saved=%d errors=%d", found, saved, errors)
        return {"found": found, "saved": saved, "errors": errors}


# ---------------------------------------------------------------------------
# VS Knowledge Base (Visual Smalltalk)
# ---------------------------------------------------------------------------
class VSKBScraper(BaseScraper):
    """Scrapes the Visual Smalltalk Knowledge Base source code library.

    A small archive of VS-Smalltalk code: inline .st/.cls files on
    sub-pages, and .zip downloads.  Everything is on a single page
    with links to sub-pages and files.
    """

    BASE_URL = "https://vs-kb.archiv.apis.de"
    LIBRARY_URL = "https://vs-kb.archiv.apis.de/source-code-library-for-vs-smalltalk/"

    def __init__(self, conn):
        super().__init__(conn, "vskb")

    def _scrape_library(self):
        """Scrape the source code library index page for items."""
        soup = self.soup(self.LIBRARY_URL)
        items = []

        for link in soup.find_all("a", href=True):
            href = link["href"]
            text = link.get_text(strip=True)
            if not text:
                continue

            # Sub-pages with inline code (within the source-code-library section)
            if href.startswith(self.LIBRARY_URL) and href != self.LIBRARY_URL:
                items.append({"type": "page", "url": href, "name": text})

            # Direct file downloads (.st, .cls, .zip, .mcz)
            elif re.search(r"\.(st|cls|zip|mcz|gz)$", href, re.IGNORECASE):
                items.append({"type": "file", "url": urljoin(self.LIBRARY_URL, href), "name": text})

        return items

    def _scrape_code_page(self, url, name):
        """Fetch a sub-page and extract inline Smalltalk code."""
        soup = self.soup(url)

        # Look for code in <pre>, <code>, or main content
        code_blocks = []
        for tag in soup.find_all(["pre", "code"]):
            text = tag.get_text(strip=True)
            if len(text) > 50:  # skip tiny fragments
                code_blocks.append(text)

        # If no pre/code blocks, try the main article content
        if not code_blocks:
            article = soup.find("article") or soup.find("div", class_="entry-content")
            if article:
                text = article.get_text(strip=True)
                if len(text) > 100:
                    code_blocks.append(text)

        desc_el = soup.find("meta", attrs={"name": "description"})
        description = desc_el["content"] if desc_el and desc_el.get("content") else ""

        return {
            "name": name,
            "url": url,
            "source": "vskb",
            "description": description[:2000],
            "code_blocks": code_blocks,
            "code_block_count": len(code_blocks),
        }

    def run(self):
        job_id = db.create_scrape_job(self.conn, self.site_id, "full_crawl")
        log.info("VS-KB scrape job %d started", job_id)

        found = saved = errors = 0
        try:
            items = self._scrape_library()
            found = len(items)
            log.info("Found %d items in VS-KB source code library", found)

            for item in items:
                try:
                    if item["type"] == "page":
                        meta = self._scrape_code_page(item["url"], item["name"])
                    else:
                        meta = {
                            "name": item["name"],
                            "url": item["url"],
                            "source": "vskb",
                            "type": "download",
                            "file_url": item["url"],
                        }

                    ext_id = item["url"]
                    row_id = db.insert_scrape_raw(
                        self.conn, job_id, self.site_id, ext_id, meta,
                    )
                    if row_id:
                        saved += 1
                except Exception as e:
                    log.error("VS-KB item %s failed: %s", item.get("name"), e)
                    errors += 1

        except Exception as e:
            log.exception("VS-KB scrape failed")
            db.finish_scrape_job(self.conn, job_id, found, saved, errors, str(e))
            raise

        db.finish_scrape_job(self.conn, job_id, found, saved, errors)
        log.info("VS-KB done: found=%d saved=%d errors=%d", found, saved, errors)
        return {"found": found, "saved": saved, "errors": errors}


# ---------------------------------------------------------------------------
# Web Search Discovery
# ---------------------------------------------------------------------------

# Search queries designed to surface Smalltalk code archives and collections
_DISCOVERY_QUERIES = [
    # General
    '+smalltalk source code',
    # Specific ecosystems
    '+smalltalk monticello repository',
# disable doesnt find anything
#    '+smalltalk seaside framework source',
    '+smalltalk metacello baseline',
    '+smalltalk pharo catalog packages list',
    '+smalltalk squeak swiki source code',
    # File types
    'filetype:st +smalltalk',
# disable doesnt find anything
#    'filetype:mcz +smalltalk',
    # Archives and collections
    '+smalltalk code archive ftp',
    '+smalltalk goodies library freeware',
    # Video content
    '+smalltalk +video tutorial',
    '+smalltalk +video programming',
]

# Domains we already have dedicated scrapers for (skip during discovery)
_KNOWN_DOMAINS = {
    "github.com", "gitlab.com",
    "squeaksource.com", "www.squeaksource.com",
    "smalltalkhub.com", "www.smalltalkhub.com",
    "rosettacode.org", "www.rosettacode.org",
    "ss3.gemstone.com",
    "stackoverflow.com", "www.stackoverflow.com",
    "reddit.com", "www.reddit.com",
    "en.wikipedia.org", "ja.wikipedia.org",
    "youtube.com", "www.youtube.com",
    "amazon.com", "www.amazon.com",
    # File archives and wikis — not package sources
    "ftp.squeak.org",
    "wiki.squeak.org",
    "handwiki.org",
    "en.scratch-wiki.info",
    "www.wikihow.com",
    # Spam/irrelevant sites that mention "smalltalk" incidentally
    "www.websiteplanet.com",
    "www.libhunt.com",
    "awesomeopensource.com",
    "news.ycombinator.com",
    "www.e-booksdirectory.com",
    "finance.yahoo.com",
    "store.steampowered.com",
    "www.google-dorking.com",
}

# Exclude these from search queries so they don't waste result slots
_SITE_EXCLUSIONS = " ".join(
    f"-site:{d}" for d in [
        "github.com", "gitlab.com", "squeaksource.com",
        "smalltalkhub.com", "rosettacode.org", "ss3.gemstone.com",
        "stackoverflow.com", "reddit.com", "wikipedia.org",
        "youtube.com", "amazon.com",
    ]
)

# File extensions that indicate Smalltalk code
_ST_FILE_EXTENSIONS = re.compile(
    r"\.(st|cls|mcz|ston|cs|sources|changes)$", re.IGNORECASE,
)

# Patterns suggesting Smalltalk code in page text
_ST_CODE_INDICATORS = re.compile(
    r"(?:"
    r"subclass:\s*#|"              # class definition
    r">>|"                         # method selector
    r"\bself\b.*\bmessage\b|"     # message send patterns
    r"Transcript\s+show:|"        # common Smalltalk pattern
    r"\bOrderedCollection\b|"     # core class
    r"ifTrue:\s*\[|"              # conditional
    r"do:\s*\[|"                  # iteration
    r"MCHttpRepository|"          # Monticello
    r"Smalltalk\s+at:"            # global access
    r")",
    re.IGNORECASE,
)


class DiscoveryScraper(BaseScraper):
    """Discovers new Smalltalk code sources via web search.

    Uses Google Custom Search API when GOOGLE_API_KEY and GOOGLE_CSE_ID
    are configured, otherwise falls back to DuckDuckGo HTML scraping.

    For each search result, visits the page and extracts:
      - inline Smalltalk code blocks
      - links to Smalltalk files (.st, .mcz, etc.)
      - follows internal links one level deep looking for more code

    Pages are saved if they contain code OR link to Smalltalk files
    OR even just mention Smalltalk prominently (for manual review).
    """

    def __init__(self, conn):
        super().__init__(conn, "web_discovered")

    # -- Search backends ---------------------------------------------------
    # Tried in order by _search(): first configured backend wins.
    # DuckDuckGo is always available as the final fallback.

    def _search_brave(self, query):
        """Search via Brave Search API with pagination.  Returns list of URLs."""
        all_urls = []
        per_page = config.BRAVE_RESULTS_PER_QUERY
        for page in range(10):  # Brave offset max is 9
            try:
                resp = self.session.get(
                    "https://api.search.brave.com/res/v1/web/search",
                    headers={"X-Subscription-Token": config.BRAVE_API_KEY,
                             "Accept": "application/json"},
                    params={"q": query, "count": per_page, "offset": page},
                    timeout=config.REQUEST_TIMEOUT,
                )
                resp.raise_for_status()
                data = resp.json()
                results = data.get("web", {}).get("results", [])
                urls = [r["url"] for r in results if r.get("url")]
                all_urls.extend(urls)
                if not results:
                    break
                if page < 9:
                    time.sleep(1)  # rate-limit courtesy
            except Exception as e:
                log.warning("Brave search failed for %r (page %d): %s", query, page, e)
                break
        return all_urls

    def _search_serpapi(self, query):
        """Search via SerpAPI (Bing) with pagination.  Returns list of URLs."""
        all_urls = []
        per_page = config.SERPAPI_RESULTS_PER_QUERY
        first = 1
        for page in range(10):  # cap at 10 pages
            try:
                resp = self.session.get(
                    "https://serpapi.com/search",
                    params={
                        "api_key": config.SERPAPI_KEY,
                        "engine": "bing",
                        "q": query,
                        "count": per_page,
                        "first": first,
                    },
                    timeout=config.REQUEST_TIMEOUT,
                )
                resp.raise_for_status()
                results = resp.json().get("organic_results", [])
                urls = [r["link"] for r in results if r.get("link")]
                all_urls.extend(urls)
                if not results:
                    break
                first += len(results)
                if page < 9:
                    time.sleep(1)
            except Exception as e:
                log.warning("SerpAPI search failed for %r (page %d): %s", query, page, e)
                break
        return all_urls

    def _search_bing(self, query):
        """Search via Bing Web Search API.  Returns list of URLs."""
        try:
            resp = self.session.get(
                "https://api.bing.microsoft.com/v7.0/search",
                headers={"Ocp-Apim-Subscription-Key": config.BING_API_KEY},
                params={"q": query, "count": config.BING_RESULTS_PER_QUERY},
                timeout=config.REQUEST_TIMEOUT,
            )
            resp.raise_for_status()
            results = resp.json().get("webPages", {}).get("value", [])
            return [r["url"] for r in results if r.get("url")]
        except Exception as e:
            log.warning("Bing search failed for %r: %s", query, e)
            return []

    def _search_ddg(self, query):
        """Fallback: search via DuckDuckGo HTML scraping.  Returns list of URLs."""
        from urllib.parse import parse_qs, urlparse as _urlparse
        try:
            resp = self.session.get(
                "https://html.duckduckgo.com/html/",
                params={"q": query},
                timeout=config.REQUEST_TIMEOUT,
            )
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, "html.parser")
            urls = []
            for a in soup.select("a.result__a"):
                href = a.get("href", "")
                if "uddg=" in href:
                    qs = parse_qs(_urlparse(href).query)
                    real = qs.get("uddg", [""])[0]
                    if real:
                        href = real
                if href.startswith("http"):
                    urls.append(href)
            return urls
        except Exception as e:
            log.warning("DuckDuckGo search failed for %r: %s", query, e)
            return []

    _ENGINES = {
        "brave": "_search_brave",
        "serpapi": "_search_serpapi",
        "bing": "_search_bing",
        "ddg": "_search_ddg",
    }

    def _search(self, query):
        """Run a web search using the engine set by run()."""
        return getattr(self, self._search_method)(query + " " + _SITE_EXCLUSIONS)

    # -- Page analysis -----------------------------------------------------

    def _is_known_domain(self, url):
        """Return True if the URL belongs to a site we already scrape."""
        host = urlparse(url).hostname or ""
        return host in _KNOWN_DOMAINS

    def _extract_from_page(self, url):
        """Fetch a page and extract Smalltalk-related content.

        Returns (meta_dict, internal_links) or (None, []).
        meta_dict is saved to scrape_raw; internal_links are followed
        for deeper crawling.
        """
        try:
            resp = self.get(url)
        except Exception:
            return None, []

        ct = resp.headers.get("content-type", "")
        # If it's a direct .st file download, grab the content
        if _ST_FILE_EXTENSIONS.search(url) and "html" not in ct:
            return {
                "name": url.rsplit("/", 1)[-1],
                "url": url,
                "source": "web_discovered",
                "description": "",
                "code_blocks": [resp.text[:50000]],
                "code_block_count": 1,
                "file_links": [],
                "file_link_count": 0,
            }, []

        soup = BeautifulSoup(resp.text, "html.parser")
        page_text = soup.get_text(" ", strip=True)

        # Collect inline code blocks
        code_blocks = []
        for tag in soup.find_all(["pre", "code", "syntaxhighlight"]):
            block = tag.get_text(strip=True)
            if len(block) > 40 and _ST_CODE_INDICATORS.search(block):
                code_blocks.append(block[:10000])

        # Check if the whole page looks like Smalltalk source
        if not code_blocks and _ST_CODE_INDICATORS.search(page_text):
            body = soup.find("body")
            if body:
                raw = body.get_text(strip=True)
                if len(raw) > 100:
                    code_blocks.append(raw[:10000])

        # Collect links to Smalltalk files
        file_links = []
        internal_links = []
        base_domain = urlparse(url).hostname
        for a in soup.find_all("a", href=True):
            href = a["href"]
            full = urljoin(url, href)
            if _ST_FILE_EXTENSIONS.search(href):
                file_links.append(full)
            # Collect same-domain links for crawl depth
            link_host = urlparse(full).hostname
            if link_host == base_domain and full != url and not full.endswith("#"):
                internal_links.append(full)

        # Determine if page is worth saving:
        #   - has code blocks
        #   - has file links
        #   - mentions "smalltalk" prominently (for manual review)
        has_st_mention = "smalltalk" in page_text.lower()

        if not code_blocks and not file_links and not has_st_mention:
            return None, internal_links

        title = soup.find("title")
        description = ""
        desc_meta = soup.find("meta", attrs={"name": "description"})
        if desc_meta and desc_meta.get("content"):
            description = desc_meta["content"][:2000]

        meta = {
            "name": title.get_text(strip=True)[:200] if title else url,
            "url": url,
            "source": "web_discovered",
            "description": description,
            "code_blocks": code_blocks,
            "code_block_count": len(code_blocks),
            "file_links": file_links,
            "file_link_count": len(file_links),
        }
        return meta, internal_links

    # -- Main run ----------------------------------------------------------

    # API key required for each engine (None = no key needed)
    _ENGINE_KEYS = {
        "brave": ("BRAVE_API_KEY", lambda: config.BRAVE_API_KEY),
        "serpapi": ("SERPAPI_KEY", lambda: config.SERPAPI_KEY),
        "bing": ("BING_API_KEY", lambda: config.BING_API_KEY),
        "ddg": None,
    }

    def run(self, engine="ddg", video_only=False):
        method = self._ENGINES.get(engine)
        if not method:
            raise ValueError(f"Unknown search engine: {engine}  (choices: {', '.join(self._ENGINES)})")

        key_info = self._ENGINE_KEYS.get(engine)
        if key_info is not None:
            env_name, get_value = key_info
            if not get_value():
                raise RuntimeError(
                    f"{engine} requires {env_name} to be set.  "
                    f"Export it:  export {env_name}=your-key-here"
                )

        self._search_method = method
        job_id = db.create_scrape_job(self.conn, self.site_id, "discovery")
        log.info("Web discovery job %d started (search: %s, %d queries)",
                 job_id, engine, len(_DISCOVERY_QUERIES))

        found = saved = errors = 0
        seen_urls = set()

        def _extract_url(url):
            """Visit a URL, return (meta, children) or (None, [])."""
            url = url.split("#")[0]  # strip fragment
            if url in seen_urls or self._is_known_domain(url):
                return None, []
            seen_urls.add(url)
            try:
                return self._extract_from_page(url)
            except Exception as e:
                log.error("Discovery page %s failed: %s", url, e)
                return None, []

        def _process_top_url(url):
            """Process a search-result URL: extract it and its children as one entry."""
            nonlocal found, saved, errors
            url = url.split("#")[0]
            if url in seen_urls or self._is_known_domain(url):
                return
            found += 1
            meta, children = _extract_url(url)

            # Follow internal links one level deep; merge their content
            # into the parent rather than saving separate entries
            child_pages = 0
            for child in children[:5]:
                child = child.split("#")[0]
                if child in seen_urls or self._is_known_domain(child):
                    continue
                child_meta, _ = _extract_url(child)
                if child_meta is not None:
                    child_pages += 1
                    if meta is None:
                        meta = child_meta
                        meta["url"] = url  # credit the parent URL
                    else:
                        # Merge child code/links into parent
                        meta["code_blocks"] = (meta.get("code_blocks") or []) + (child_meta.get("code_blocks") or [])
                        meta["file_links"] = (meta.get("file_links") or []) + (child_meta.get("file_links") or [])

            if meta is not None:
                meta["code_block_count"] = len(meta.get("code_blocks") or [])
                meta["file_link_count"] = len(meta.get("file_links") or [])
                if child_pages:
                    meta["child_pages"] = child_pages
                try:
                    row_id = db.insert_scrape_raw(
                        self.conn, job_id, self.site_id, url, meta,
                    )
                    if row_id:
                        saved += 1
                except Exception as e:
                    log.error("Discovery save %s failed: %s", url, e)
                    errors += 1

        try:
            queries = _DISCOVERY_QUERIES
            if video_only:
                queries = [q for q in queries if "video" in q.lower()]
                log.info("Video-only mode: %d of %d queries match",
                         len(queries), len(_DISCOVERY_QUERIES))

            for qi, query in enumerate(queries, 1):
                log.info("Discovery search [%d/%d]: %s",
                         qi, len(queries), query)
                time.sleep(2)
                urls = self._search(query)
                total = len(urls)
                known = sum(1 for u in urls if u in seen_urls or self._is_known_domain(u))
                new = total - known
                log.info("  results: %d total, %d duplicate/known, %d new",
                         total, known, new)

                for ui, url in enumerate(urls, 1):
                    _process_top_url(url)

                    if ui % 10 == 0:
                        log.info("  progress: %d/%d URLs, saved=%d",
                                 ui, len(urls), saved)

        except Exception as e:
            log.exception("Web discovery failed")
            db.finish_scrape_job(self.conn, job_id, found, saved, errors, str(e))
            raise

        db.finish_scrape_job(self.conn, job_id, found, saved, errors)
        log.info("Discovery done: visited=%d saved=%d errors=%d", found, saved, errors)
        return {"found": found, "saved": saved, "errors": errors}


# ---------------------------------------------------------------------------
# Dispatch
# ---------------------------------------------------------------------------
SCRAPERS = {
    "squeaksource": SqueakSourceScraper,
    "smalltalkhub": SmalltalkHubScraper,
    "rosettacode": RosettaCodeScraper,
    "vskb": VSKBScraper,
}


def run_web_scraper(conn, name):
    """Run a named web scraper."""
    if name == "all":
        results = {}
        for n, cls in SCRAPERS.items():
            log.info("--- Running %s scraper ---", n)
            results[n] = cls(conn).run()
        return results

    cls = SCRAPERS.get(name)
    if not cls:
        raise ValueError(f"Unknown web scraper: {name}  (choices: {', '.join(SCRAPERS)} or all)")
    return cls(conn).run()
