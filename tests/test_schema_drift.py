#!/usr/bin/env python3
"""Catch schema drift: scrape/schema.py (SQLAlchemy metadata) vs db/schema.sqlite.sql.

The ORM reads/writes columns from schema.py; the DDL creates the actual
tables. A column added to one and not the other fails at runtime (e.g.
"no such column: packages.llm_review"). This test builds a fresh DB from
the DDL and asserts every metadata column exists.

Run:  uv run pytest tests/test_schema_drift.py
"""

import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from scrape.schema import metadata

DDL = Path(__file__).resolve().parent.parent / "db" / "schema.sqlite.sql"


def test_metadata_matches_ddl(tmp_path):
    conn = sqlite3.connect(tmp_path / "drift.db")
    conn.executescript(DDL.read_text())

    for table in metadata.tables.values():
        db_cols = {r[1] for r in conn.execute(f"PRAGMA table_info({table.name})")}
        missing = set(table.c.keys()) - db_cols
        assert not missing, (
            f"{table.name}: columns in schema.py missing from schema.sqlite.sql: "
            f"{sorted(missing)}"
        )