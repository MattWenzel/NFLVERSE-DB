#!/usr/bin/env python3
"""Build a SQLite sibling of data/nflverse.duckdb with identical data + FKs.

The canonical build is DuckDB (scripts/build_db.py). This script reads the
built DuckDB and emits a matching SQLite file at data/nflverse.sqlite. Same
tables, same row counts, same UNIQUE/FK declarations, same indexes, plus a
materialized v_depth_charts table (SQLite doesn't get the live view — see
docs/INGESTION.md §12 for why).

Run order:
    python3 scripts/build_db.py --all            # build DuckDB
    python3 scripts/build_db.py --pbp --no-backup
    python3 scripts/build_sqlite.py              # mirror to SQLite

Usage:
    python3 scripts/build_sqlite.py
    python3 scripts/build_sqlite.py --output data/custom.sqlite
    python3 scripts/build_sqlite.py --no-vacuum  # skip VACUUM at the end
"""

import argparse
import sqlite3
import time
from pathlib import Path

import duckdb

from config import DB_PATH, SQLITE_DB_PATH


# DuckDB type -> SQLite type. SQLite has 5 storage classes (INTEGER, REAL,
# TEXT, BLOB, NULL); anything else maps to TEXT. We only see 6 distinct
# DuckDB types in the current schema (VARCHAR / DOUBLE / INTEGER / BIGINT /
# BOOLEAN / DATE) so the lookup is finite.
TYPE_MAP = {
    "VARCHAR": "TEXT",
    "TEXT": "TEXT",
    "BIGINT": "INTEGER",
    "INTEGER": "INTEGER",
    "HUGEINT": "INTEGER",
    "SMALLINT": "INTEGER",
    "TINYINT": "INTEGER",
    "DOUBLE": "REAL",
    "FLOAT": "REAL",
    "REAL": "REAL",
    "BOOLEAN": "INTEGER",
    "DATE": "TEXT",
    "TIMESTAMP": "TEXT",
    "TIME": "TEXT",
    "TIMESTAMP WITH TIME ZONE": "TEXT",
    "BLOB": "BLOB",
}

# Load order: parents before children. Must match build_db.py TABLE_CONFIGS
# so any FK validation DuckDB applied also makes sense here.
TABLE_ORDER = [
    "players", "player_ids", "games",
    "combine", "draft_picks", "snap_counts", "pfr_advanced",
    "depth_charts", "depth_charts_2025", "ngs_stats", "qbr",
    "game_stats", "season_stats", "play_by_play",
]

# Views that get materialized as tables in SQLite (SQLite doesn't grok
# DuckDB's view SQL verbatim, and static materialization is fine for our
# append-only rebuild workflow).
VIEW_MATERIALIZATIONS = ["v_depth_charts"]


def sqlite_type_for(duckdb_type: str) -> str:
    """Map a DuckDB column type string to a SQLite storage class name."""
    base = duckdb_type.split("(")[0].strip().upper()
    return TYPE_MAP.get(base, "TEXT")


def build_table_ddl(duck: duckdb.DuckDBPyConnection, table_name: str) -> str:
    """Generate SQLite CREATE TABLE for a DuckDB table, including UNIQUE + FK."""
    cols = duck.execute(
        """
        SELECT column_name, data_type FROM information_schema.columns
        WHERE table_schema = 'main' AND table_name = ?
        ORDER BY ordinal_position
        """,
        [table_name],
    ).fetchall()
    col_defs = [f'"{name}" {sqlite_type_for(dtype)}' for name, dtype in cols]

    uniques = duck.execute(
        """
        SELECT constraint_column_names[1] FROM duckdb_constraints()
        WHERE table_name = ?
          AND constraint_type IN ('PRIMARY KEY', 'UNIQUE')
          AND constraint_column_names[1] IS NOT NULL
        """,
        [table_name],
    ).fetchall()
    constraint_clauses = [f'UNIQUE("{u[0]}")' for u in uniques]

    fks = duck.execute(
        """
        SELECT constraint_column_names[1], referenced_table, referenced_column_names[1]
        FROM duckdb_constraints()
        WHERE table_name = ? AND constraint_type = 'FOREIGN KEY'
        ORDER BY constraint_column_names[1]
        """,
        [table_name],
    ).fetchall()
    constraint_clauses += [
        f'FOREIGN KEY("{c}") REFERENCES "{rt}"("{rc}")' for c, rt, rc in fks
    ]

    body = ",\n  ".join(col_defs + constraint_clauses)
    return f'CREATE TABLE "{table_name}" (\n  {body}\n)'


def build_view_as_table_ddl(duck: duckdb.DuckDBPyConnection, view_name: str) -> str:
    """A view's column schema is the same metadata shape as a table's, so we
    can reuse the type-mapping loop. Just emit a plain `CREATE TABLE`."""
    cols = duck.execute(
        """
        SELECT column_name, data_type FROM information_schema.columns
        WHERE table_schema = 'main' AND table_name = ?
        ORDER BY ordinal_position
        """,
        [view_name],
    ).fetchall()
    if not cols:
        raise RuntimeError(f"view {view_name!r} has no column metadata")
    col_defs = [f'"{name}" {sqlite_type_for(dtype)}' for name, dtype in cols]
    body = ",\n  ".join(col_defs)
    return f'CREATE TABLE "{view_name}" (\n  {body}\n)'


def main():
    parser = argparse.ArgumentParser(
        description="Build a SQLite sibling of data/nflverse.duckdb."
    )
    parser.add_argument(
        "--output", type=str,
        help=f"Output path (default: {SQLITE_DB_PATH})",
    )
    parser.add_argument(
        "--no-vacuum", action="store_true",
        help="Skip VACUUM at the end (saves ~30s, costs some reclaimable space)",
    )
    args = parser.parse_args()

    sqlite_path = Path(args.output) if args.output else Path(SQLITE_DB_PATH)
    if not Path(DB_PATH).exists():
        raise SystemExit(
            f"ERROR: {DB_PATH} not found. Build the DuckDB first with "
            f"`python3 scripts/build_db.py --all && python3 scripts/build_db.py --pbp`."
        )

    start = time.time()
    print(f"Source: {DB_PATH}")
    print(f"Target: {sqlite_path}")

    # Clean slate. VACUUM won't reclaim space from a partially-populated file
    # that a prior failed run may have left.
    if sqlite_path.exists():
        print(f"  Removing existing {sqlite_path.name}")
        sqlite_path.unlink()
    for suffix in ("-wal", "-shm"):
        stray = Path(str(sqlite_path) + suffix)
        if stray.exists():
            stray.unlink()

    # -------- Phase 1: schema via sqlite3 (authoritative DDL) --------
    duck = duckdb.connect(str(DB_PATH), read_only=True)
    lite = sqlite3.connect(str(sqlite_path))
    lite.execute("PRAGMA journal_mode = WAL")
    lite.execute("PRAGMA synchronous = NORMAL")
    lite.execute("PRAGMA foreign_keys = ON")  # build-time enforcement

    print("\nCreating schema:")
    for t in TABLE_ORDER:
        lite.execute(build_table_ddl(duck, t))
        print(f"  + {t}")
    for v in VIEW_MATERIALIZATIONS:
        lite.execute(build_view_as_table_ddl(duck, v))
        print(f"  + {v} (as table)")
    lite.commit()
    lite.close()
    duck.close()

    # -------- Phase 2: bulk data transfer via DuckDB's sqlite extension --------
    print("\nCopying data:")
    duck = duckdb.connect(":memory:")
    duck.execute("INSTALL sqlite")
    duck.execute("LOAD sqlite")
    duck.execute(f"ATTACH '{DB_PATH}' AS src (READ_ONLY)")
    duck.execute(f"ATTACH '{sqlite_path}' AS dst (TYPE SQLITE)")

    for t in TABLE_ORDER + VIEW_MATERIALIZATIONS:
        n = duck.execute(f'SELECT COUNT(*) FROM src.main."{t}"').fetchone()[0]
        duck.execute(f'INSERT INTO dst."{t}" SELECT * FROM src.main."{t}"')
        print(f"  {t:<22} {n:>12,} rows")

    duck.execute("DETACH dst")
    duck.execute("DETACH src")
    duck.close()

    # -------- Phase 3: indexes + verification --------
    lite = sqlite3.connect(str(sqlite_path))
    lite.execute("PRAGMA foreign_keys = ON")

    print("\nCreating indexes:")
    indexes = [
        ("idx_game_stats_player_season",
         'CREATE INDEX "idx_game_stats_player_season" ON "game_stats"("player_gsis_id", "season")'),
        ("idx_season_stats_player_season",
         'CREATE INDEX "idx_season_stats_player_season" ON "season_stats"("player_gsis_id", "season")'),
        ("idx_players_player_gsis_id",
         'CREATE INDEX "idx_players_player_gsis_id" ON "players"("player_gsis_id")'),
    ]
    for name, sql in indexes:
        lite.execute(sql)
        print(f"  + {name}")
    lite.commit()

    # Row-count parity check.
    print("\nRow-count parity:")
    duck = duckdb.connect(str(DB_PATH), read_only=True)
    mismatches = 0
    for t in TABLE_ORDER + VIEW_MATERIALIZATIONS:
        a = duck.execute(f'SELECT COUNT(*) FROM "{t}"').fetchone()[0]
        b = lite.execute(f'SELECT COUNT(*) FROM "{t}"').fetchone()[0]
        ok = a == b
        if not ok:
            mismatches += 1
        print(f"  {t:<22} duckdb {a:>12,}  sqlite {b:>12,}  [{'OK' if ok else 'FAIL'}]")
    duck.close()
    if mismatches:
        raise SystemExit(f"{mismatches} table(s) have row-count mismatches.")

    # FK metadata count.
    fk_count = 0
    for (t,) in lite.execute(
        "SELECT name FROM sqlite_master WHERE type = 'table' "
        "AND name NOT LIKE 'sqlite_%'"
    ).fetchall():
        fk_count += len(lite.execute(f'PRAGMA foreign_key_list("{t}")').fetchall())
    print(f"\n  FKs declared in SQLite: {fk_count}")

    # Orphan sweep (with FK enforcement on).
    bad = 0
    for (t,) in lite.execute(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'"
    ).fetchall():
        for row in lite.execute(f'PRAGMA foreign_key_list("{t}")').fetchall():
            # row columns: id, seq, table, from, to, on_update, on_delete, match
            ref_table = row[2]
            from_col = row[3]
            to_col = row[4]
            cnt = lite.execute(
                f'SELECT COUNT(*) FROM "{t}" c '
                f'WHERE c."{from_col}" IS NOT NULL '
                f'AND NOT EXISTS (SELECT 1 FROM "{ref_table}" p WHERE p."{to_col}" = c."{from_col}")'
            ).fetchone()[0]
            if cnt > 0:
                bad += 1
                print(f"  ORPHAN: {t}.{from_col} -> {ref_table}.{to_col}: {cnt}")
    print(f"  Orphans on declared FKs: {bad}")

    # PRAGMA integrity_check — should report 'ok'.
    result = lite.execute("PRAGMA integrity_check").fetchone()[0]
    print(f"  integrity_check: {result}")

    if not args.no_vacuum:
        print("\nRunning VACUUM...")
        lite.execute("VACUUM")

    lite.close()

    size_mb = sqlite_path.stat().st_size / (1024 * 1024)
    elapsed = time.time() - start
    print(f"\nBuilt {sqlite_path.name}: {size_mb:,.0f} MB in {elapsed:.1f}s")
    print(f"\nConsumers: remember `PRAGMA foreign_keys = ON` per connection "
          f"if you want FK enforcement (SQLite default is OFF).")


if __name__ == "__main__":
    main()
