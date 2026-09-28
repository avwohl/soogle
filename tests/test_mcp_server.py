#!/usr/bin/env python3
"""MCP server tools query the same database the Django web app serves.

Run:  python3 tests/test_mcp_server.py
"""

import os
import sys
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

tmpdir = tempfile.mkdtemp()
os.environ["SOOGLE_DB_ENGINE"] = "sqlite"
os.environ["SOOGLE_DB_PATH"] = os.path.join(tmpdir, "test.db")

from sqlalchemy import select

from mcp_server import get_method, get_package, list_sources, search_packages, search_videos
from scrape.db import engine
from scrape.schema import package_classes, package_methods, packages, sites

results = []


def check(condition, label):
    results.append(bool(condition))
    print(f"{'PASS' if condition else 'FAIL'}: {label}")


with engine.begin() as conn:
    site_id = conn.execute(select(sites.c.id).where(sites.c.name == "github")).scalar_one()
    pkg_id = conn.execute(packages.insert().values(
        name="Seaside", qualified_name="Seaside/Seaside", description="A web framework for Pharo",
        dialect="pharo", dialect_confidence=90, file_format="tonel", site_id=site_id,
        external_id="Seaside/Seaside", stars=1200, forks=300, is_active=True,
    )).inserted_primary_key[0]
    class_id = conn.execute(package_classes.insert().values(
        package_id=pkg_id, class_name="WAComponent", superclass_name="Object", category="Web",
    )).inserted_primary_key[0]
    conn.execute(package_methods.insert().values(
        package_id=pkg_id, class_id=class_id, selector="renderOn:",
        protocol="rendering", source_code="renderOn: html\n\thtml text: 'hi'",
    ))

github = next(s for s in list_sources() if s["name"] == "github")
check(github["package_count"] == 1, "list_sources reports the seeded package count")

hits = search_packages(q="web framework", limit=10)
check(len(hits) == 1 and hits[0]["name"] == "Seaside" and hits[0]["dialect"] == "pharo",
      "search_packages finds the package by description text")
check(search_packages(dialect="squeak") == [], "dialect filter excludes non-matching packages")
check(search_packages(sort="stars")[0]["stars"] == 1200, "sort by stars works")

detail = get_package(pkg_id)
check(detail["package"]["name"] == "Seaside", "get_package returns the package")
check([c["class_name"] for c in detail["classes"]] == ["WAComponent"], "get_package lists classes")
check(detail["methods"][0]["selector"] == "renderOn:", "get_package lists methods without source")
check("source_code" not in detail["methods"][0], "method source is not dumped in get_package")

method = get_method(pkg_id, "WAComponent", "renderOn:")
check(method and "html text: 'hi'" in method["source_code"], "get_method returns the source code")
check(get_method(pkg_id, "WAComponent", "nope") is None, "get_method returns None for a missing selector")

check(search_videos() == [], "search_videos runs on an empty table")

print("\n" + "=" * 60)
failed = results.count(False)
print(f"Results: {results.count(True)} passed, {failed} failed")
sys.exit(1 if failed else 0)