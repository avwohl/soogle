#!/usr/bin/env python3
"""SQLite backend end-to-end: scrape_raw -> process -> packages.

Exercises the SQLAlchemy Core layer in scrape/db.py against a real SQLite
file: schema load, scrape job, scrape_raw insert, processing into packages,
the upsert (update not duplicate), INSERT IGNORE, and NOW().

No MySQL env vars are set here, so the engine auto-detects to SQLite.

Run:  python3 tests/test_sqlite_pipeline.py
"""

import os
import sys
import tempfile

os.environ["SOOGLE_DB_PATH"] = os.path.join(tempfile.mkdtemp(), "test.db")

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import sqlite3

from sqlalchemy import func, select

from scrape import config, db
from scrape.processor import process_all
from scrape.schema import blocklist, packages, scrape_jobs

results = []


def check(condition, label):
    results.append(bool(condition))
    print(f"{'PASS' if condition else 'FAIL'}: {label}")


check(config.DB_ENGINE == "sqlite", "engine auto-detects to sqlite without MySQL vars")
check(config.DB_PATH == os.environ["SOOGLE_DB_PATH"], "DB_PATH read from env")

# The schema is applied automatically by scrape/db.py on an empty DB.

with db.connection() as conn:
    site_id = db.get_site_id(conn, "github")
    job_id = db.create_scrape_job(conn, site_id, "incremental")

    meta = {
        "name": "pharo-project",
        "full_name": "pharo-project/pharo",
        "description": "Pharo Smalltalk",
        "html_url": "https://github.com/pharo-project/pharo",
        "clone_url": "https://github.com/pharo-project/pharo.git",
        "stargazers_count": 100,
        "forks_count": 10,
        "size": 5000,
        "license": {"spdx_id": "MIT"},
        "fork": False,
        "archived": False,
        "default_branch": "master",
        "topics": ["pharo", "smalltalk"],
        "created_at": "2024-01-15T10:30:00Z",
        "updated_at": "2024-01-15T10:30:00Z",
        "pushed_at": "2024-01-15T10:30:00Z",
    }
    row_id = db.insert_scrape_raw(conn, job_id, site_id, "pharo-project/pharo", meta)
    check(row_id is not None, "insert_scrape_raw returns a row id")

    result = process_all(conn)
    check(result["processed"] == 1 and result["errors"] == 0,
          "process_all processes the row")

    pkg = conn.execute(
        select(packages.c.name, packages.c.dialect, packages.c.stars)
    ).mappings().fetchone()
    check(pkg and pkg["name"] == "pharo-project" and pkg["dialect"] == "pharo",
          "package upserted with detected dialect")

    # Re-scrape with changed stars -> upsert updates, not a duplicate
    meta["stargazers_count"] = 200
    db.insert_scrape_raw(conn, job_id, site_id, "pharo-project/pharo", meta)
    process_all(conn)
    check(conn.execute(select(func.count()).select_from(packages)).scalar() == 1,
          "re-process updates, does not duplicate")
    check(conn.execute(select(packages.c.stars)).scalar() == 200,
          "upsert updates changed fields")

    # INSERT IGNORE (blocklist)
    conn.execute(
        db.insert_ignore(
            blocklist,
            {"external_id": "pharo-project/pharo", "site_name": "github", "reason": "test"},
            ["external_id", "site_name"],
        )
    )
    conn.commit()
    conn.execute(
        db.insert_ignore(
            blocklist,
            {"external_id": "pharo-project/pharo", "site_name": "github", "reason": "test"},
            ["external_id", "site_name"],
        )
    )
    conn.commit()
    check(conn.execute(select(func.count()).select_from(blocklist)).scalar() == 1,
          "INSERT IGNORE dedupes on conflict")

    # NOW() (scrape_jobs timestamps)
    started = conn.execute(
        select(scrape_jobs.c.started_at).where(scrape_jobs.c.id == job_id)
    ).scalar()
    check(started is not None, "NOW() translated to a timestamp")

print(f"\n{sum(results)}/{len(results)} checks passed")
sys.exit(0 if all(results) else 1)