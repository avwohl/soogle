#!/usr/bin/env python
"""Dump every table in the SQLite DB to one parquet file per table.

Usage: python scripts/dump_parquet.py [DB_PATH] [OUT_DIR]
"""

import sys
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, inspect


def main() -> None:
    db_path = sys.argv[1] if len(sys.argv) > 1 else "soogle.db"
    out_dir = Path(sys.argv[2] if len(sys.argv) > 2 else ".")
    out_dir.mkdir(parents=True, exist_ok=True)

    engine = create_engine(f"sqlite:///{db_path}")
    for table in inspect(engine).get_table_names():
        df = pd.read_sql(f'SELECT * FROM "{table}"', engine)
        df.to_parquet(out_dir / f"{table}.parquet", index=False)
        print(f"{table}: {len(df)} rows")


if __name__ == "__main__":
    main()