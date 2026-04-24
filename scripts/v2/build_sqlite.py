#!/usr/bin/env python3
"""Mirror the v2 DuckDB to a SQLite sibling.

Same approach as v1's scripts/build_sqlite.py: generate DDL with explicit
FK + UNIQUE constraints via sqlite3, bulk-copy rows via DuckDB's sqlite_scanner
extension, then verify row-count parity + FK integrity.

Discovers the schema + FK graph directly from the source DuckDB — no hardcoded
table list — so any v2 additions ship automatically.

Usage:
    python3 scripts/v2/build_sqlite.py                        # mirror .v2 → .sqlite
    python3 scripts/v2/build_sqlite.py --source data/x.duckdb --output data/y.sqlite
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
import time
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_DUCK = ROOT / "data" / "nflverse.duckdb.v2"
DEFAULT_SQLITE = ROOT / "data" / "nflverse.sqlite.v2"

TYPE_MAP = {
    "VARCHAR": "TEXT", "TEXT": "TEXT",
    "BIGINT": "INTEGER", "INTEGER": "INTEGER", "HUGEINT": "INTEGER",
    "SMALLINT": "INTEGER", "TINYINT": "INTEGER",
    "DOUBLE": "REAL", "FLOAT": "REAL", "REAL": "REAL",
    "BOOLEAN": "INTEGER",
    "DATE": "TEXT", "TIMESTAMP": "TEXT", "TIME": "TEXT",
    "TIMESTAMP WITH TIME ZONE": "TEXT",
    "BLOB": "BLOB",
}


def sqlite_type_for(duckdb_type: str) -> str:
    base = duckdb_type.split("(")[0].strip().upper()
    return TYPE_MAP.get(base, "TEXT")


def load_order_from_fks(duck: duckdb.DuckDBPyConnection) -> list[str]:
    """Topological order: parents before children, derived from FK metadata."""
    tables = [r[0] for r in duck.execute(
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_schema='main' AND table_type='BASE TABLE' ORDER BY table_name"
    ).fetchall()]
    edges = duck.execute("""
        SELECT table_name, referenced_table
        FROM duckdb_constraints()
        WHERE constraint_type='FOREIGN KEY'
    """).fetchall()
    deps: dict[str, set] = {t: set() for t in tables}
    for child, parent in edges:
        if parent != child:
            deps[child].add(parent)
    # Kahn
    order = []
    remaining = set(tables)
    while remaining:
        ready = sorted([t for t in remaining if not (deps[t] & remaining)])
        if not ready:
            raise RuntimeError(f"FK cycle among: {remaining}")
        order.extend(ready)
        remaining -= set(ready)
    return order


def build_table_ddl(duck: duckdb.DuckDBPyConnection, table_name: str) -> str:
    cols = duck.execute(
        "SELECT column_name, data_type FROM information_schema.columns "
        "WHERE table_schema='main' AND table_name=? ORDER BY ordinal_position",
        [table_name],
    ).fetchall()
    col_defs = [f'"{name}" {sqlite_type_for(dtype)}' for name, dtype in cols]

    uniques = duck.execute(
        "SELECT constraint_column_names[1] FROM duckdb_constraints() "
        "WHERE table_name=? AND constraint_type IN ('PRIMARY KEY','UNIQUE') "
        "AND constraint_column_names[1] IS NOT NULL",
        [table_name],
    ).fetchall()
    constraint_clauses = [f'UNIQUE("{u[0]}")' for u in uniques]

    fks = duck.execute(
        "SELECT constraint_column_names[1], referenced_table, referenced_column_names[1] "
        "FROM duckdb_constraints() "
        "WHERE table_name=? AND constraint_type='FOREIGN KEY' "
        "ORDER BY constraint_column_names[1]",
        [table_name],
    ).fetchall()
    constraint_clauses += [
        f'FOREIGN KEY("{c}") REFERENCES "{rt}"("{rc}")' for c, rt, rc in fks
    ]

    body = ",\n  ".join(col_defs + constraint_clauses)
    return f'CREATE TABLE "{table_name}" (\n  {body}\n)'


def build_view_as_table_ddl(duck: duckdb.DuckDBPyConnection, view_name: str) -> str:
    cols = duck.execute(
        "SELECT column_name, data_type FROM information_schema.columns "
        "WHERE table_schema='main' AND table_name=? ORDER BY ordinal_position",
        [view_name],
    ).fetchall()
    if not cols:
        raise RuntimeError(f"view {view_name!r} has no column metadata")
    col_defs = [f'"{name}" {sqlite_type_for(dtype)}' for name, dtype in cols]
    body = ",\n  ".join(col_defs)
    return f'CREATE TABLE "{view_name}" (\n  {body}\n)'


def views_in_db(duck: duckdb.DuckDBPyConnection) -> list[str]:
    return [r[0] for r in duck.execute(
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_schema='main' AND table_type='VIEW' ORDER BY table_name"
    ).fetchall()]


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--source", type=str, default=str(DEFAULT_DUCK))
    parser.add_argument("--output", type=str, default=str(DEFAULT_SQLITE))
    parser.add_argument("--no-vacuum", action="store_true")
    args = parser.parse_args()

    src = Path(args.source)
    dst = Path(args.output)
    if not src.exists():
        raise SystemExit(f"ERROR: source DB not found: {src}")

    t0 = time.time()
    print(f"Source: {src}")
    print(f"Target: {dst}")

    for suffix in ("", "-wal", "-shm"):
        p = Path(str(dst) + suffix)
        if p.exists():
            p.unlink()

    duck = duckdb.connect(str(src), read_only=True)
    order = load_order_from_fks(duck)
    views = views_in_db(duck)
    print(f"Tables: {len(order)}  Views: {len(views)}")

    lite = sqlite3.connect(str(dst))
    lite.execute("PRAGMA journal_mode = WAL")
    lite.execute("PRAGMA synchronous = NORMAL")
    lite.execute("PRAGMA foreign_keys = ON")

    print("\nCreating schema:")
    for t in order:
        lite.execute(build_table_ddl(duck, t))
        print(f"  + {t}")
    for v in views:
        lite.execute(build_view_as_table_ddl(duck, v))
        print(f"  + {v} (materialized as table)")
    lite.commit()
    lite.close()
    duck.close()

    print("\nCopying data:")
    duck = duckdb.connect(":memory:")
    duck.execute("INSTALL sqlite")
    duck.execute("LOAD sqlite")
    duck.execute(f"ATTACH '{src}' AS s (READ_ONLY)")
    duck.execute(f"ATTACH '{dst}' AS d (TYPE SQLITE)")
    for t in order + views:
        n = duck.execute(f'SELECT COUNT(*) FROM s.main."{t}"').fetchone()[0]
        duck.execute(f'INSERT INTO d."{t}" SELECT * FROM s.main."{t}"')
        print(f"  {t:<24} {n:>12,} rows")
    duck.execute("DETACH d")
    duck.execute("DETACH s")
    duck.close()

    print("\nVerifying:")
    lite = sqlite3.connect(str(dst))
    lite.execute("PRAGMA foreign_keys = ON")
    duck = duckdb.connect(str(src), read_only=True)
    mism = 0
    for t in order + views:
        a = duck.execute(f'SELECT COUNT(*) FROM "{t}"').fetchone()[0]
        b = lite.execute(f'SELECT COUNT(*) FROM "{t}"').fetchone()[0]
        ok = a == b
        mism += 0 if ok else 1
        print(f"  {t:<24} duck {a:>12,}  sqlite {b:>12,}  [{'OK' if ok else 'FAIL'}]")
    duck.close()
    if mism:
        raise SystemExit(f"{mism} tables mismatched")

    fk_count = 0
    for (t,) in lite.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
    ).fetchall():
        fk_count += len(lite.execute(f'PRAGMA foreign_key_list("{t}")').fetchall())
    print(f"\n  FKs declared: {fk_count}")

    # Orphan sweep
    bad = 0
    for (t,) in lite.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
    ).fetchall():
        for row in lite.execute(f'PRAGMA foreign_key_list("{t}")').fetchall():
            ref_table, from_col, to_col = row[2], row[3], row[4]
            cnt = lite.execute(
                f'SELECT COUNT(*) FROM "{t}" c WHERE c."{from_col}" IS NOT NULL '
                f'AND NOT EXISTS (SELECT 1 FROM "{ref_table}" p WHERE p."{to_col}" = c."{from_col}")'
            ).fetchone()[0]
            if cnt > 0:
                bad += 1
                print(f"  ORPHAN: {t}.{from_col} → {ref_table}.{to_col}: {cnt}")
    print(f"  Orphans: {bad}")
    print(f"  integrity_check: {lite.execute('PRAGMA integrity_check').fetchone()[0]}")

    if not args.no_vacuum:
        print("\nVACUUM...")
        lite.execute("VACUUM")
    lite.close()

    size = dst.stat().st_size / (1024 * 1024)
    print(f"\nDone: {dst.name} {size:,.0f} MB in {time.time()-t0:.0f}s")


if __name__ == "__main__":
    main()
