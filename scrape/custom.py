"""Custom scrapers for sites identified by the analyze step.

Covers:
    - SqueakMap         (package registry at map.squeak.org)
    - Lukas Renggli     (Monticello repos at source.lukas-renggli.ch)
    - SourceForge       (Smalltalk projects directory)
    - Launchpad         (Smalltalk branches via REST API)
    - Squeak Trunk      (source.squeak.org — Squeak core Monticello repos)

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
# Squeak Trunk  (source.squeak.org)
# ---------------------------------------------------------------------------
class SqueakTrunkScraper(BaseScraper):
    """Scrapes Monticello projects at source.squeak.org.

    source.squeak.org is a SqueakSource3 (Seaside) instance hosting the
    official Squeak trunk, release branches, VM Maker, FFI, and related
    projects.  The scraper navigates the Seaside session to discover all
    projects, extracts each project's stable slug, then fetches the
    static file listing at /{slug} to enumerate .mcz packages.
    """

    BASE_URL = "https://source.squeak.org"

    def __init__(self, conn):
        super().__init__(conn, "squeaktrunk")

    # -- Seaside navigation ------------------------------------------------

    def _navigate_to_projects(self):
        """Start a session and navigate to the Projects listing page."""
        soup = self.soup(self.BASE_URL + "/")
        for link in soup.find_all("a"):
            if link.get_text(strip=True) == "Projects":
                return self.soup(self.BASE_URL + link["href"])
        return None

    def _parse_project_table(self, soup):
        """Extract project session hrefs from the Projects table."""
        table = soup.find("table")
        if not table:
            return []

        hrefs = []
        for row in table.find_all("tr", recursive=False):
            cls = row.get("class", [])
            if "oddRow" not in cls and "evenRow" not in cls:
                continue
            cells = row.find_all("td", recursive=False)
            if not cells:
                continue
            link = cells[0].find("a")
            if link and link.get("href"):
                hrefs.append(self.BASE_URL + link["href"])
        return hrefs

    def _extract_project_slug(self, detail_url):
        """Follow a session link to a project detail page, return (slug, meta).

        The detail page contains an MCHttpRepository location like:
            location: 'http://source.squeak.org/trunk'
        """
        soup = self.soup(detail_url)
        text = soup.get_text()

        m = re.search(
            r"location:\s*'http://source\.squeak\.org/([^']+)'", text,
        )
        if not m:
            return None
        slug = m.group(1)

        meta = {
            "name": slug,
            "url": f"{self.BASE_URL}/{slug}",
            "source": "squeaktrunk",
        }

        # Description (first <p> in the main content)
        desc_el = soup.find("p")
        if desc_el:
            meta["description"] = desc_el.get_text(strip=True)[:2000]

        # Stats from the page text
        for pattern, key in [
            (r"Total Versions:(\d+)", "version_count"),
            (r"Total Downloads:(\d+)", "download_count"),
        ]:
            sm = re.search(pattern, text)
            if sm:
                meta[key] = sm.group(1).strip()

        return slug, meta

    # -- File listing ------------------------------------------------------

    def _list_mcz_files(self, slug):
        """Fetch the static listing at /{slug} and extract .mcz filenames."""
        url = f"{self.BASE_URL}/{slug}"
        try:
            soup = self.soup(url)
        except Exception:
            return []
        files = []
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if href.endswith(".mcz") or href.endswith(".mcm"):
                files.append(href)
        return files

    # -- Main run ----------------------------------------------------------

    def run(self):
        job_id = db.create_scrape_job(self.conn, self.site_id, "full_crawl")
        log.info("SqueakTrunk scrape job %d started", job_id)

        found = saved = errors = 0
        try:
            listing = self._navigate_to_projects()
            if listing is None:
                raise RuntimeError("Could not navigate to source.squeak.org Projects page")

            project_hrefs = self._parse_project_table(listing)
            log.info("SqueakTrunk: %d projects in listing", len(project_hrefs))

            for href in project_hrefs:
                try:
                    result = self._extract_project_slug(href)
                    if result is None:
                        continue
                    slug, project_meta = result
                    log.info("  project %s (slug=%s)", project_meta["name"], slug)

                    mcz_files = self._list_mcz_files(slug)
                    found += len(mcz_files)
                    log.info("    %d .mcz/.mcm files", len(mcz_files))

                    for mcz in mcz_files:
                        try:
                            # Parse package name: PackageName-author.N.mcz
                            pkg_name = re.sub(r"-[^-]+\.\d+\.(mcz|mcm)$", "", mcz)
                            meta = {
                                "name": pkg_name,
                                "qualified_name": f"{slug}/{pkg_name}",
                                "url": f"{self.BASE_URL}/{slug}/{mcz}",
                                "source": "squeaktrunk",
                                "description": project_meta.get("description", ""),
                                "project": slug,
                                "filename": mcz,
                            }
                            ext_id = f"{slug}/{mcz}"
                            row_id = db.insert_scrape_raw(
                                self.conn, job_id, self.site_id, ext_id, meta,
                            )
                            if row_id:
                                saved += 1
                        except Exception as e:
                            log.error("SqueakTrunk file %s/%s failed: %s", slug, mcz, e)
                            errors += 1
                except Exception as e:
                    log.error("SqueakTrunk project failed: %s", e)
                    errors += 1

        except Exception as e:
            log.exception("SqueakTrunk scrape failed")
            db.finish_scrape_job(self.conn, job_id, found, saved, errors, str(e))
            raise

        db.finish_scrape_job(self.conn, job_id, found, saved, errors)
        log.info("SqueakTrunk done: found=%d saved=%d errors=%d", found, saved, errors)
        return {"found": found, "saved": saved, "errors": errors}


# ---------------------------------------------------------------------------
# Dispatch
# ---------------------------------------------------------------------------
CUSTOM_SCRAPERS = {
    "squeakmap": SqueakMapScraper,
    "lukas_renggli": LukasRenggliScraper,
    "sourceforge": SourceForgeScraper,
    "launchpad": LaunchpadScraper,
    "squeaktrunk": SqueakTrunkScraper,
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
