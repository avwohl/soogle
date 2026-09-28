#!/usr/bin/env python3
"""Round-trip: sqlite -> parquet dump script -> read back.

Run:  uv run --group dump pytest tests/test_dump_parquet.py
"""

import sqlite3
import subprocess
import sys
from pathlib import Path

import pandas as pd

DUMP_SCRIPT = Path(__file__).resolve().parent.parent / "scripts" / "dump_parquet.py"


def test_roundtrip(tmp_path):
    db = tmp_path / "test.db"
    out = tmp_path / "out"

    conn = sqlite3.connect(db)
    conn.execute("CREATE TABLE t (id INTEGER, name TEXT)")
    conn.executemany("INSERT INTO t VALUES (?, ?)", [(1, "a"), (2, None)])
    conn.execute("CREATE TABLE empty (x INTEGER)")
    conn.commit()
    conn.close()

    subprocess.run([sys.executable, str(DUMP_SCRIPT), str(db), str(out)], check=True)

    df = pd.read_parquet(out / "t.parquet")
    assert list(df.columns) == ["id", "name"]
    assert len(df) == 2
    assert df["name"].isna().sum() == 1

    df_empty = pd.read_parquet(out / "empty.parquet")
    assert len(df_empty) == 0