"""Custom scrapers for sites identified by the analyze step.

Covers:
    - SqueakMap         (package registry at map.squeak.org)
    - Debian Archive    (Smalltalk file mirror at cdimage.debian.org)
    - Lukas Renggli     (Monticello repos at source.lukas-renggli.ch)
    - SourceForge       (Smalltalk projects directory)
    - Squeak Wiki       (Swiki at wiki.squeak.org, pages by integer ID)
    - FTP Squeak        (file archive at ftp.squeak.org)
    - Launchpad         (Smalltalk branches via REST API)

Each scraper writes raw data into scrape_raw for later processing.

Usage:
    python -m scrape custom <name>
    python -m scrape custom all
"""

import re
import time
import json
import logging
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

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
        time.sleep(1)
        resp = self.session.get(url, **kwargs)
        resp.raise_for_status()
        return resp

    def soup(self, url):
        resp = self.get(url)
        return BeautifulSoup(resp.text, "html.parser")

    def get_json(self, url, **kwargs):
        kwargs.setdefault("timeout", config.REQUEST_TIMEOUT)
        time.sleep(1)
        resp = self.session.get(url, **kwargs)
        resp.raise_for_status()
        return resp.json()


# ---------------------------------------------------------------------------
# SqueakMap  (map.squeak.org)
# ---------------------------------------------------------------------------
class SqueakMapScraper(BaseScraper):
    """Scrapes the SqueakMap package registry.

    Fetches /packagesbyname for a full list, then each /package/<uuid>
    for metadata (description, author, versions, categories).
    """

    BASE = "http://map.squeak.org"

    def __init__(self, conn):
        super().__init__(conn, "squeakmap")

    def _get_package_uuids(self):
        """Parse /packagesbyname and extract (uuid, name) pairs."""
        soup = self.soup(f"{self.BASE}/packagesbyname")
        packages = []
        for a in soup.find_all("a", href=True):
            href = a["href"]
            # Links look like /package/<uuid>
            m = re.match(r"/package/([0-9a-f-]{36})", href)
            if m:
                packages.append((m.group(1), a.get_text(strip=True)))
        return packages

    def _scrape_package(self, uuid, name):
        """Fetch a package detail page and extract metadata."""
        url = f"{self.BASE}/package/{uuid}"
        soup = self.soup(url)
        text = soup.get_text(" ", strip=True)

        meta = {
            "name": name,
            "qualified_name": f"squeakmap/{name}",
            "url": url,
            "uuid": uuid,
            "source": "squeakmap",
            "description": "",
        }

        # Extract description - usually in the main content area
        # Look for description text after the package name heading
        content = soup.find("div", class_="content") or soup.find("body")
        if content:
            paragraphs = content.find_all("p")
            desc_parts = []
            for p in paragraphs:
                t = p.get_text(strip=True)
                if t and len(t) > 20:
                    desc_parts.append(t)
            if desc_parts:
                meta["description"] = " ".join(desc_parts)[:2000]

        # Extract author/maintainer
        for label in ["Author", "Maintainer", "Owner"]:
            m = re.search(rf"{label}:\s*(.+?)(?:\n|$)", text)
            if m:
                meta["author"] = m.group(1).strip()[:200]
                break

        # Extract categories
        categories = []
        for a in soup.find_all("a", href=True):
            if "/category/" in a["href"]:
                categories.append(a.get_text(strip=True))
        if categories:
            meta["topics"] = categories

        # Extract version/release info
        versions = []
        for a in soup.find_all("a", href=True):
            if "/version/" in a["href"] or ".mcz" in a["href"]:
                versions.append(a.get_text(strip=True))
        if versions:
            meta["versions"] = versions[:20]  # cap at 20

        return meta

    def run(self):
        job_id = db.create_scrape_job(self.conn, self.site_id, "full_crawl")
        log.info("SqueakMap scrape job %d started", job_id)
        found = saved = errors = 0

        try:
            packages = self._get_package_uuids()
            found = len(packages)
            log.info("SqueakMap: found %d packages", found)

            for i, (uuid, name) in enumerate(packages):
                try:
                    meta = self._scrape_package(uuid, name)
                    row_id = db.insert_scrape_raw(
                        self.conn, job_id, self.site_id, uuid, meta,
                    )
                    if row_id:
                        saved += 1
                    if (i + 1) % 50 == 0:
                        log.info("SqueakMap progress: %d/%d saved=%d", i + 1, found, saved)
                except Exception as e:
                    log.error("SqueakMap package %s (%s) failed: %s", name, uuid, e)
                    errors += 1

        except Exception as e:
            log.exception("SqueakMap scrape failed")
            db.finish_scrape_job(self.conn, job_id, found, saved, errors, str(e))
            raise

        db.finish_scrape_job(self.conn, job_id, found, saved, errors)
        log.info("SqueakMap done: found=%d saved=%d errors=%d", found, saved, errors)
        return {"found": found, "saved": saved, "errors": errors}


# ---------------------------------------------------------------------------
# Debian Smalltalk Archive  (cdimage.debian.org)
# ---------------------------------------------------------------------------
class DebianArchiveScraper(BaseScraper):
    """Recursively crawls the Debian Smalltalk file mirror.

    Apache autoindex directory at:
    /mirror/archive/ftp.sunet.se/pub/lang/smalltalk/
    """

    ROOT = "https://cdimage.debian.org/mirror/archive/ftp.sunet.se/pub/lang/smalltalk/"

    # File extensions worth recording
    _CODE_EXT = re.compile(
        r"\.(st|cs|cls|mcz|changes|sources|ston|im|image|gz|tar|zip)$",
        re.IGNORECASE,
    )

    def __init__(self, conn):
        super().__init__(conn, "debianarchive")

    def _list_directory(self, url):
        """Parse an Apache autoindex page, return (dirs, files)."""
        soup = self.soup(url)
        dirs, files = [], []
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if href.startswith("?") or href.startswith("/"):
                continue  # skip sort links and parent
            full = urljoin(url, href)
            if href.endswith("/"):
                dirs.append(full)
            elif self._CODE_EXT.search(href):
                files.append({"url": full, "name": href})
        return dirs, files

    def _crawl_recursive(self, url, job_id, depth=0, max_depth=10):
        """Recursively crawl directories, saving file entries."""
        saved = errors = 0
        try:
            dirs, files = self._list_directory(url)
        except Exception as e:
            log.error("Failed to list %s: %s", url, e)
            return 0, 0, 1

        found = len(files)
        for f in files:
            try:
                # Determine the relative path from ROOT
                rel_path = f["url"].replace(self.ROOT, "")
                meta = {
                    "name": f["name"],
                    "qualified_name": rel_path,
                    "url": f["url"],
                    "source": "debianarchive",
                    "description": f"Smalltalk file from Debian archive: {rel_path}",
                    "type": "file",
                }
                row_id = db.insert_scrape_raw(
                    self.conn, job_id, self.site_id, f["url"], meta,
                )
                if row_id:
                    saved += 1
            except Exception as e:
                log.error("Failed to save %s: %s", f["url"], e)
                errors += 1

        if depth < max_depth:
            for d in dirs:
                s, e_count, _ = self._crawl_recursive(d, job_id, depth + 1, max_depth)
                saved += s
                errors += e_count

        return saved, errors, found

    def run(self):
        job_id = db.create_scrape_job(self.conn, self.site_id, "full_crawl")
        log.info("Debian Archive scrape job %d started", job_id)

        try:
            saved, errors, found = self._crawl_recursive(self.ROOT, job_id)
        except Exception as e:
            log.exception("Debian Archive scrape failed")
            db.finish_scrape_job(self.conn, job_id, 0, 0, 0, str(e))
            raise

        db.finish_scrape_job(self.conn, job_id, found, saved, errors)
        log.info("Debian Archive done: found=%d saved=%d errors=%d", found, saved, errors)
        return {"found": found, "saved": saved, "errors": errors}


# ---------------------------------------------------------------------------
# Lukas Renggli Source  (source.lukas-renggli.ch)
# ---------------------------------------------------------------------------
class LukasRenggliScraper(BaseScraper):
    """Scrapes Monticello repos at source.lukas-renggli.ch.

    Root page lists project directories (seaside/, magritte/, etc.).
    Each is a Monticello HTTP repository with .mcz files.
    """

    BASE = "https://source.lukas-renggli.ch"

    def __init__(self, conn):
        super().__init__(conn, "lukas_renggli")

    def _list_projects(self):
        """Get project directory names from the root page."""
        soup = self.soup(self.BASE + "/")
        projects = []
        for a in soup.find_all("a", href=True):
            href = a["href"]
            # Project dirs end with / and are relative
            if href.endswith("/") and not href.startswith("?") and not href.startswith("/") and not href.startswith("http"):
                name = href.rstrip("/")
                if name and name != "..":
                    projects.append(name)
        return projects

    def _list_mcz_files(self, project):
        """List .mcz files in a project directory."""
        url = f"{self.BASE}/{project}/"
        try:
            soup = self.soup(url)
        except Exception:
            return []
        files = []
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if href.endswith(".mcz"):
                files.append(href)
        return files

    def run(self):
        job_id = db.create_scrape_job(self.conn, self.site_id, "full_crawl")
        log.info("Lukas Renggli scrape job %d started", job_id)
        found = saved = errors = 0

        try:
            projects = self._list_projects()
            log.info("Lukas Renggli: found %d project directories", len(projects))

            for project in projects:
                mcz_files = self._list_mcz_files(project)
                found += len(mcz_files)
                log.info("  %s: %d .mcz files", project, len(mcz_files))

                for mcz in mcz_files:
                    try:
                        # Parse package name from mcz filename
                        # Format: PackageName-author.N.mcz
                        pkg_name = re.sub(r"-[^-]+\.\d+\.mcz$", "", mcz)
                        url = f"{self.BASE}/{project}/{mcz}"
                        meta = {
                            "name": pkg_name,
                            "qualified_name": f"{project}/{pkg_name}",
                            "url": url,
                            "source": "lukas_renggli",
                            "description": f"Monticello package from {project} repository",
                            "project": project,
                            "filename": mcz,
                        }
                        ext_id = f"{project}/{mcz}"
                        row_id = db.insert_scrape_raw(
                            self.conn, job_id, self.site_id, ext_id, meta,
                        )
                        if row_id:
                            saved += 1
                    except Exception as e:
                        log.error("Failed %s/%s: %s", project, mcz, e)
                        errors += 1

        except Exception as e:
            log.exception("Lukas Renggli scrape failed")
            db.finish_scrape_job(self.conn, job_id, found, saved, errors, str(e))
            raise

        db.finish_scrape_job(self.conn, job_id, found, saved, errors)
        log.info("Lukas Renggli done: found=%d saved=%d errors=%d", found, saved, errors)
        return {"found": found, "saved": saved, "errors": errors}


# ---------------------------------------------------------------------------
# SourceForge  (sourceforge.net)
# ---------------------------------------------------------------------------
class SourceForgeScraper(BaseScraper):
    """Scrapes SourceForge's Smalltalk project directory.

    Paginates through /directory/smalltalk/ to collect project slugs,
    then fetches each project's /files/ page for downloadable content.
    """

    DIR_URL = "https://sourceforge.net/directory/smalltalk/"

    def __init__(self, conn):
        super().__init__(conn, "sourceforge")

    def _get_directory_page(self, page=1):
        """Fetch one page of the Smalltalk directory, return project slugs."""
        url = self.DIR_URL if page == 1 else f"{self.DIR_URL}?page={page}"
        try:
            soup = self.soup(url)
        except requests.exceptions.HTTPError as e:
            if e.response is not None and e.response.status_code == 404:
                return [], False
            raise
        slugs = []
        for a in soup.find_all("a", href=True):
            m = re.match(r"/projects/([a-z0-9_-]+)/?$", a["href"], re.IGNORECASE)
            if m:
                slug = m.group(1)
                if slug not in slugs:
                    slugs.append(slug)
        # Check if there's a link to the next page specifically
        next_page = page + 1
        has_next = bool(soup.find("a", href=re.compile(rf"page={next_page}\b")))
        return slugs, has_next

    def _scrape_project(self, slug):
        """Fetch a project's main page for metadata."""
        url = f"https://sourceforge.net/projects/{slug}/"
        soup = self.soup(url)

        title_el = soup.find("h1")
        desc_el = soup.find("div", class_="description") or soup.find("meta", attrs={"name": "description"})

        title = title_el.get_text(strip=True) if title_el else slug
        description = ""
        if desc_el:
            if desc_el.name == "meta":
                description = desc_el.get("content", "")
            else:
                description = desc_el.get_text(strip=True)

        meta = {
            "name": title,
            "qualified_name": f"sourceforge/{slug}",
            "url": url,
            "source": "sourceforge",
            "description": description[:2000],
            "slug": slug,
        }

        # Try to get stats
        for span in soup.find_all("span"):
            text = span.get_text(strip=True)
            if "download" in text.lower() and re.search(r"\d", text):
                meta["downloads_text"] = text

        return meta

    def run(self):
        job_id = db.create_scrape_job(self.conn, self.site_id, "full_crawl")
        log.info("SourceForge scrape job %d started", job_id)
        found = saved = errors = 0

        try:
            # Paginate through the directory
            all_slugs = []
            page = 1
            while True:
                slugs, has_next = self._get_directory_page(page)
                if not slugs:
                    break
                all_slugs.extend(slugs)
                log.info("SourceForge directory page %d: %d projects (total %d)",
                         page, len(slugs), len(all_slugs))
                if not has_next:
                    break
                page += 1

            found = len(all_slugs)
            log.info("SourceForge: found %d Smalltalk projects", found)

            for i, slug in enumerate(all_slugs):
                try:
                    meta = self._scrape_project(slug)
                    row_id = db.insert_scrape_raw(
                        self.conn, job_id, self.site_id, slug, meta,
                    )
                    if row_id:
                        saved += 1
                    if (i + 1) % 20 == 0:
                        log.info("SourceForge progress: %d/%d saved=%d", i + 1, found, saved)
                except Exception as e:
                    log.error("SourceForge project %s failed: %s", slug, e)
                    errors += 1

        except Exception as e:
            log.exception("SourceForge scrape failed")
            db.finish_scrape_job(self.conn, job_id, found, saved, errors, str(e))
            raise

        db.finish_scrape_job(self.conn, job_id, found, saved, errors)
        log.info("SourceForge done: found=%d saved=%d errors=%d", found, saved, errors)
        return {"found": found, "saved": saved, "errors": errors}


# ---------------------------------------------------------------------------
# Squeak Wiki  (wiki.squeak.org)
# ---------------------------------------------------------------------------
class SqueakWikiScraper(BaseScraper):
    """Scrapes the Squeak Swiki by iterating page IDs.

    The Swiki has ~6674 pages at /squeak/{id}. We iterate through all
    IDs and save pages that contain Smalltalk code.
    """

    BASE = "http://wiki.squeak.org"
    MAX_PAGE_ID = 7000

    _ST_CODE_PATTERN = re.compile(
        r"(?:"
        r"subclass:\s*#|"
        r">>|"
        r"ifTrue:\s*\[|"
        r"do:\s*\[|"
        r"Transcript\s+show:|"
        r"\bOrderedCollection\b|"
        r"MCHttpRepository|"
        r"Smalltalk\s+at:"
        r")",
        re.IGNORECASE,
    )

    def __init__(self, conn):
        super().__init__(conn, "squeakwiki")

    def _scrape_page(self, page_id):
        """Fetch a Swiki page, return metadata or None if 404 / no content."""
        url = f"{self.BASE}/squeak/{page_id}"
        try:
            resp = self.session.get(url, timeout=config.REQUEST_TIMEOUT)
        except Exception:
            return None
        if resp.status_code == 404:
            return None
        if resp.status_code != 200:
            return None

        soup = BeautifulSoup(resp.text, "html.parser")
        title_el = soup.find("title")
        title = title_el.get_text(strip=True) if title_el else f"Page {page_id}"

        # Extract code blocks
        code_blocks = []
        for tag in soup.find_all(["pre", "code"]):
            text = tag.get_text(strip=True)
            if len(text) > 30 and self._ST_CODE_PATTERN.search(text):
                code_blocks.append(text[:10000])

        # Get page text for description
        body = soup.find("body")
        page_text = body.get_text(" ", strip=True)[:5000] if body else ""

        # Save all pages (they're all Smalltalk-related on this wiki),
        # but flag those with actual code
        meta = {
            "name": title,
            "qualified_name": f"squeakwiki/{page_id}",
            "url": url,
            "source": "squeakwiki",
            "description": page_text[:500],
            "page_id": page_id,
            "code_blocks": code_blocks,
            "code_block_count": len(code_blocks),
            "has_code": len(code_blocks) > 0,
        }
        return meta

    def run(self):
        job_id = db.create_scrape_job(self.conn, self.site_id, "full_crawl")
        log.info("Squeak Wiki scrape job %d started (scanning IDs 1..%d)", job_id, self.MAX_PAGE_ID)
        found = saved = errors = 0

        try:
            for page_id in range(1, self.MAX_PAGE_ID + 1):
                try:
                    time.sleep(1)  # respect crawl-delay
                    meta = self._scrape_page(page_id)
                    if meta is None:
                        continue
                    found += 1

                    row_id = db.insert_scrape_raw(
                        self.conn, job_id, self.site_id,
                        str(page_id), meta,
                    )
                    if row_id:
                        saved += 1

                except Exception as e:
                    log.error("Squeak Wiki page %d failed: %s", page_id, e)
                    errors += 1

                if page_id % 200 == 0:
                    log.info("Squeak Wiki progress: scanned %d/%d found=%d saved=%d",
                             page_id, self.MAX_PAGE_ID, found, saved)

        except Exception as e:
            log.exception("Squeak Wiki scrape failed")
            db.finish_scrape_job(self.conn, job_id, found, saved, errors, str(e))
            raise

        db.finish_scrape_job(self.conn, job_id, found, saved, errors)
        log.info("Squeak Wiki done: found=%d saved=%d errors=%d", found, saved, errors)
        return {"found": found, "saved": saved, "errors": errors}


# ---------------------------------------------------------------------------
# FTP Squeak  (ftp.squeak.org)
# ---------------------------------------------------------------------------
class FtpSqueakScraper(BaseScraper):
    """Recursively crawls ftp.squeak.org for Smalltalk files.

    This is an nginx directory listing. We crawl the entire tree
    looking for .st, .cs, .changes, .sources, .image, .tar.gz files.
    """

    BASE = "http://ftp.squeak.org"

    _CODE_EXT = re.compile(
        r"\.(st|cs|cls|mcz|changes|sources|ston|im|image|gz|tar|zip|pdf)$",
        re.IGNORECASE,
    )

    def __init__(self, conn):
        super().__init__(conn, "ftpsqueak")

    def _list_directory(self, url):
        """Parse an nginx autoindex page."""
        soup = self.soup(url)
        dirs, files = [], []
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if href in ("../", ".."):
                continue
            if href.startswith("?") or href.startswith("/"):
                continue
            full = urljoin(url, href)
            if href.endswith("/"):
                dirs.append(full)
            elif self._CODE_EXT.search(href):
                files.append({"url": full, "name": href})
        return dirs, files

    def _crawl_recursive(self, url, job_id, depth=0, max_depth=10):
        saved = errors = found = 0
        try:
            dirs, files = self._list_directory(url)
        except Exception as e:
            log.error("Failed to list %s: %s", url, e)
            return 0, 0, 1

        found = len(files)
        rel_base = url.replace(self.BASE, "")

        for f in files:
            try:
                rel_path = f["url"].replace(self.BASE + "/", "")
                meta = {
                    "name": f["name"],
                    "qualified_name": rel_path,
                    "url": f["url"],
                    "source": "ftpsqueak",
                    "description": f"File from ftp.squeak.org: {rel_path}",
                    "type": "file",
                    "directory": rel_base,
                }
                row_id = db.insert_scrape_raw(
                    self.conn, job_id, self.site_id, f["url"], meta,
                )
                if row_id:
                    saved += 1
            except Exception as e:
                log.error("Failed to save %s: %s", f["url"], e)
                errors += 1

        if depth < max_depth:
            for d in dirs:
                s, e_count, f_count = self._crawl_recursive(d, job_id, depth + 1, max_depth)
                saved += s
                errors += e_count
                found += f_count

        return saved, errors, found

    def run(self):
        job_id = db.create_scrape_job(self.conn, self.site_id, "full_crawl")
        log.info("FTP Squeak scrape job %d started", job_id)

        try:
            saved, errors, found = self._crawl_recursive(self.BASE + "/", job_id)
        except Exception as e:
            log.exception("FTP Squeak scrape failed")
            db.finish_scrape_job(self.conn, job_id, 0, 0, 0, str(e))
            raise

        db.finish_scrape_job(self.conn, job_id, found, saved, errors)
        log.info("FTP Squeak done: found=%d saved=%d errors=%d", found, saved, errors)
        return {"found": found, "saved": saved, "errors": errors}


# ---------------------------------------------------------------------------
# Launchpad  (code.launchpad.net)
# ---------------------------------------------------------------------------
class LaunchpadScraper(BaseScraper):
    """Scrapes Smalltalk projects from Launchpad via REST API.

    Uses the Launchpad API to find Smalltalk-related projects and
    enumerate their branches.
    """

    API = "https://api.launchpad.net/1.0"

    # Known Smalltalk projects on Launchpad
    _PROJECTS = [
        "gnu-smalltalk",
        "squeak",
        "pharo",
        "cuis",
        "opensmalltalk-vm",
    ]

    # Search terms for discovering more projects
    _SEARCH_TERMS = [
        "smalltalk",
        "squeak",
        "pharo",
    ]

    def __init__(self, conn):
        super().__init__(conn, "launchpad")

    def _get_api(self, url, params=None):
        """Fetch from Launchpad REST API."""
        headers = {"Accept": "application/json"}
        return self.get_json(url, headers=headers, params=params)

    def _search_projects(self):
        """Search for Smalltalk-related projects."""
        all_projects = set()

        # Add known projects
        for name in self._PROJECTS:
            all_projects.add(name)

        # Search for more
        for term in self._SEARCH_TERMS:
            try:
                data = self._get_api(
                    f"{self.API}/projects",
                    params={"ws.op": "search", "text": term, "ws.size": 75},
                )
                for entry in data.get("entries", []):
                    name = entry.get("name", "")
                    if name:
                        all_projects.add(name)
            except Exception as e:
                log.warning("Launchpad search for %r failed: %s", term, e)

        return list(all_projects)

    def _get_project_branches(self, project_name):
        """Get all branches for a project."""
        branches = []
        url = f"{self.API}/{project_name}"

        try:
            project = self._get_api(url)
        except Exception as e:
            log.warning("Could not fetch project %s: %s", project_name, e)
            return [], None

        # Try to get branches
        branches_url = project.get("branches_collection_link")
        if not branches_url:
            return [], project

        try:
            while branches_url:
                data = self._get_api(branches_url)
                for entry in data.get("entries", []):
                    branches.append(entry)
                branches_url = data.get("next_collection_link")
        except Exception as e:
            log.warning("Could not fetch branches for %s: %s", project_name, e)

        return branches, project

    def run(self):
        job_id = db.create_scrape_job(self.conn, self.site_id, "full_crawl")
        log.info("Launchpad scrape job %d started", job_id)
        found = saved = errors = 0

        try:
            projects = self._search_projects()
            log.info("Launchpad: found %d projects to check", len(projects))

            for project_name in projects:
                branches, project_meta = self._get_project_branches(project_name)
                if project_meta is None:
                    continue

                # Save the project itself
                display_name = project_meta.get("display_name", project_name)
                description = project_meta.get("summary", "") or project_meta.get("description", "")

                if branches:
                    found += len(branches)
                    for branch in branches:
                        try:
                            branch_name = branch.get("unique_name", "")
                            meta = {
                                "name": branch.get("name", branch_name),
                                "qualified_name": branch_name,
                                "url": branch.get("web_link", ""),
                                "source": "launchpad",
                                "description": f"{display_name}: {description}"[:2000],
                                "project": project_name,
                                "bzr_identity": branch.get("bzr_identity", ""),
                                "lifecycle_status": branch.get("lifecycle_status", ""),
                                "last_modified": branch.get("date_last_modified", ""),
                            }
                            ext_id = branch_name or branch.get("web_link", "")
                            row_id = db.insert_scrape_raw(
                                self.conn, job_id, self.site_id, ext_id, meta,
                            )
                            if row_id:
                                saved += 1
                        except Exception as e:
                            log.error("Launchpad branch %s failed: %s",
                                      branch.get("unique_name", "?"), e)
                            errors += 1
                else:
                    # Save project even if no branches found
                    found += 1
                    try:
                        meta = {
                            "name": display_name,
                            "qualified_name": f"launchpad/{project_name}",
                            "url": project_meta.get("web_link", f"https://code.launchpad.net/{project_name}"),
                            "source": "launchpad",
                            "description": description[:2000],
                            "project": project_name,
                        }
                        row_id = db.insert_scrape_raw(
                            self.conn, job_id, self.site_id, project_name, meta,
                        )
                        if row_id:
                            saved += 1
                    except Exception as e:
                        log.error("Launchpad project %s failed: %s", project_name, e)
                        errors += 1

                log.info("Launchpad %s: %d branches", project_name, len(branches))

        except Exception as e:
            log.exception("Launchpad scrape failed")
            db.finish_scrape_job(self.conn, job_id, found, saved, errors, str(e))
            raise

        db.finish_scrape_job(self.conn, job_id, found, saved, errors)
        log.info("Launchpad done: found=%d saved=%d errors=%d", found, saved, errors)
        return {"found": found, "saved": saved, "errors": errors}


# ---------------------------------------------------------------------------
# Dispatch
# ---------------------------------------------------------------------------
CUSTOM_SCRAPERS = {
    "squeakmap": SqueakMapScraper,
    "debianarchive": DebianArchiveScraper,
    "lukas_renggli": LukasRenggliScraper,
    "sourceforge": SourceForgeScraper,
    "squeakwiki": SqueakWikiScraper,
    "ftpsqueak": FtpSqueakScraper,
    "launchpad": LaunchpadScraper,
}


def run_custom_scraper(conn, name):
    """Run a named custom scraper."""
    if name == "all":
        results = {}
        for n, cls in CUSTOM_SCRAPERS.items():
            log.info("--- Running %s scraper ---", n)
            try:
                results[n] = cls(conn).run()
            except Exception as e:
                log.error("Scraper %s failed: %s", n, e)
                results[n] = {"found": 0, "saved": 0, "errors": 1}
        return results

    cls = CUSTOM_SCRAPERS.get(name)
    if not cls:
        raise ValueError(
            f"Unknown custom scraper: {name}  "
            f"(choices: {', '.join(CUSTOM_SCRAPERS)} or all)"
        )
    return cls(conn).run()
