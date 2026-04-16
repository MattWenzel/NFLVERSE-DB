#!/usr/bin/env python3
"""Shared pipeline for building/updating nflverse databases.

Provides the `TableConfig` class, year-partition and full-replace update modes,
schema-drift handling, backups, indexing, and the shared `run()` entry point
used by both `build_db.py` (local parquet source) and `build_db_nflreadpy.py`
(nflreadpy network source).
"""

import argparse
import shutil
import sqlite3
from datetime import datetime
from pathlib import Path

from config import DB_PATH, PBP_DB_PATH


# ---------------------------------------------------------------------------
# Table config
# ---------------------------------------------------------------------------

class TableConfig:
    """How to fetch and update a single table."""

    def __init__(self, name, *, db="main", update_mode="year_partition",
                 fetch_fn=None, dedup_cols=None, drop_na_col=None):
        self.name = name
        self.db = db  # "main" or "pbp"
        self.update_mode = update_mode  # "year_partition" or "full_replace"
        self.fetch_fn = fetch_fn
        self.dedup_cols = dedup_cols  # subset columns for drop_duplicates
        self.drop_na_col = drop_na_col  # column to dropna on (e.g. "player_id")


# First year of available data for each year-partitioned table.
# Shared between build_db.py and build_db_nflreadpy.py so they stay in sync.
YEAR_RANGE_START = {
    "game_stats": 1999,
    "season_stats": 1999,
    "games": 1999,
    "snap_counts": 2012,
    "depth_charts": 2001,
    "play_by_play": 1999,
}

# depth_charts schema changed in 2025 — pre-2025 lives in `depth_charts`,
# 2025+ lives in `depth_charts_2025` (handled as full_replace).
DEPTH_CHARTS_LEGACY_END = 2025  # exclusive upper bound for the old table


def default_years_for(table_name):
    """Full year range for a year-partitioned table."""
    start = YEAR_RANGE_START.get(table_name, 1999)
    if table_name == "depth_charts":
        return list(range(start, DEPTH_CHARTS_LEGACY_END))
    return list(range(start, datetime.now().year + 1))


# ---------------------------------------------------------------------------
# Backup
# ---------------------------------------------------------------------------

def create_backup(db_path):
    """Create a rolling .bak copy of a database file."""
    if not db_path.exists():
        return
    bak_path = db_path.with_suffix(".db.bak")
    print(f"  Backing up {db_path.name} -> {bak_path.name}")
    shutil.copy2(db_path, bak_path)


# ---------------------------------------------------------------------------
# Schema helpers
# ---------------------------------------------------------------------------

def _table_exists(conn, table_name):
    cur = conn.cursor()
    cur.execute(
        "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?",
        (table_name,),
    )
    return cur.fetchone()[0] > 0


def _add_missing_columns(conn, table_name, df):
    """Add any columns present in df but missing from the table.

    Handles schema drift between years (e.g., game_id added in later seasons).
    """
    cur = conn.cursor()
    cur.execute(f"PRAGMA table_info([{table_name}])")
    existing_cols = {row[1] for row in cur.fetchall()}
    new_cols = [c for c in df.columns if c not in existing_cols]
    for col in new_cols:
        dtype = df[col].dtype
        if dtype.kind == "i":
            col_type = "INTEGER"
        elif dtype.kind == "f":
            col_type = "REAL"
        else:
            col_type = "TEXT"
        conn.execute(f"ALTER TABLE [{table_name}] ADD COLUMN [{col}] {col_type}")
    if new_cols:
        conn.commit()
        print(f"(added {len(new_cols)} cols: {', '.join(new_cols)}) ", end="", flush=True)


# ---------------------------------------------------------------------------
# Update modes
# ---------------------------------------------------------------------------

def update_year_partition(conn, config, years, dry_run=False):
    """Delete + re-insert data for specific years."""
    for year in years:
        print(f"  {config.name} [{year}]: ", end="", flush=True)

        if dry_run:
            try:
                cur = conn.cursor()
                cur.execute(
                    f"SELECT COUNT(*) FROM [{config.name}] WHERE season = ?", (year,)
                )
                existing = cur.fetchone()[0]
                print(f"would replace {existing:,} existing rows")
            except sqlite3.OperationalError:
                print("table does not exist (would create)")
            continue

        try:
            df = config.fetch_fn([year])
        except Exception as e:
            print(f"FETCH ERROR: {e}")
            continue

        if df is None or len(df) == 0:
            print("no data returned")
            continue

        if config.drop_na_col and config.drop_na_col in df.columns:
            df = df.dropna(subset=[config.drop_na_col])

        if config.dedup_cols:
            dedup_available = [c for c in config.dedup_cols if c in df.columns]
            if dedup_available:
                df = df.drop_duplicates(subset=dedup_available, keep="first")

        if not _table_exists(conn, config.name):
            try:
                df.to_sql(config.name, conn, if_exists="replace", index=False)
                conn.commit()
                print(f"{len(df):,} rows inserted (table created)")
            except Exception as e:
                conn.rollback()
                print(f"ERROR creating table: {e}")
            continue

        _add_missing_columns(conn, config.name, df)

        try:
            conn.execute(f"DELETE FROM [{config.name}] WHERE season = ?", (year,))
            conn.commit()
            df.to_sql(config.name, conn, if_exists="append", index=False)
            conn.commit()
            print(f"{len(df):,} rows inserted")
        except Exception as e:
            conn.rollback()
            print(f"ERROR (rolled back): {e}")


def update_full_replace(conn, config, dry_run=False):
    """Replace the entire table with fresh data."""
    print(f"  {config.name}: ", end="", flush=True)

    if dry_run:
        try:
            cur = conn.cursor()
            cur.execute(f"SELECT COUNT(*) FROM [{config.name}]")
            existing = cur.fetchone()[0]
            print(f"would replace {existing:,} existing rows")
        except sqlite3.OperationalError:
            print("table does not exist (would create)")
        return

    try:
        df = config.fetch_fn()
    except Exception as e:
        print(f"FETCH ERROR: {e}")
        return

    if df is None or len(df) == 0:
        print("no data returned — skipping (kept existing)")
        return

    if config.drop_na_col and config.drop_na_col in df.columns:
        df = df.dropna(subset=[config.drop_na_col])

    if config.dedup_cols:
        dedup_available = [c for c in config.dedup_cols if c in df.columns]
        if dedup_available:
            df = df.drop_duplicates(subset=dedup_available, keep="first")

    df.to_sql(config.name, conn, if_exists="replace", index=False)
    print(f"{len(df):,} rows")


# ---------------------------------------------------------------------------
# Post-update steps
# ---------------------------------------------------------------------------

def backfill_season_stats_team(conn, years=None, dry_run=False):
    """Backfill season_stats.recent_team from game_stats (most common team per player-season)."""
    print("  Backfilling season_stats.recent_team from game_stats...", end=" ", flush=True)

    if dry_run:
        print("would backfill")
        return

    year_clause = ""
    params = ()
    if years:
        placeholders = ",".join("?" for _ in years)
        year_clause = f"AND season_stats.season IN ({placeholders})"
        params = tuple(years)

    try:
        conn.execute(f"""
            UPDATE season_stats
            SET recent_team = (
                SELECT g.team
                FROM game_stats g
                WHERE g.player_id = season_stats.player_id
                  AND g.season = season_stats.season
                GROUP BY g.team
                ORDER BY COUNT(*) DESC
                LIMIT 1
            )
            WHERE EXISTS (
                SELECT 1 FROM game_stats g2
                WHERE g2.player_id = season_stats.player_id
                  AND g2.season = season_stats.season
            )
            {year_clause}
        """, params)
        conn.commit()
        print("done")
    except Exception as e:
        print(f"ERROR: {e}")


def create_indexes(conn):
    """Create standard indexes on the main database."""
    print("  Creating indexes...", end=" ", flush=True)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_game_stats_player_season ON game_stats(player_id, season)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_season_stats_player_season ON season_stats(player_id, season)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_players_gsis_id ON players(gsis_id)")
    conn.commit()
    print("done")


def check_integrity(conn, dry_run=False):
    """Check for orphan records in game_stats/season_stats."""
    if dry_run:
        return

    required = {"game_stats", "season_stats", "players"}
    existing = {
        row[0]
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
    }
    if not required.issubset(existing):
        missing = required - existing
        print(f"  Skipping integrity check (missing tables: {', '.join(sorted(missing))})")
        return

    print("  Checking referential integrity...", end=" ", flush=True)
    cur = conn.cursor()

    cur.execute("""
        SELECT COUNT(*) FROM game_stats g
        WHERE NOT EXISTS (SELECT 1 FROM players p WHERE p.gsis_id = g.player_id)
    """)
    orphan_games = cur.fetchone()[0]

    cur.execute("""
        SELECT COUNT(*) FROM season_stats s
        WHERE NOT EXISTS (SELECT 1 FROM players p WHERE p.gsis_id = s.player_id)
    """)
    orphan_seasons = cur.fetchone()[0]

    if orphan_games or orphan_seasons:
        print(f"WARNING: {orphan_games} orphan game_stats, {orphan_seasons} orphan season_stats")
    else:
        print("ok (0 orphan records)")


# ---------------------------------------------------------------------------
# Shared CLI runner
# ---------------------------------------------------------------------------

def build_arg_parser(description):
    """Return an ArgumentParser with the common flags used by the build scripts."""
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("--tables", nargs="+", help="Specific table(s) to process")
    parser.add_argument("--years", nargs="+", type=int,
                        help="Specific year(s) for year-partitioned tables")
    parser.add_argument("--pbp", action="store_true", help="Include play-by-play DB")
    parser.add_argument("--all", action="store_true",
                        help="Process all tables across all years")
    parser.add_argument("--dry-run", action="store_true",
                        help="Preview changes without modifying")
    parser.add_argument("--no-backup", action="store_true", help="Skip backup step")
    parser.add_argument("--output", type=str, help="Write main DB to a different path")
    parser.add_argument("--output-pbp", type=str, help="Write PBP to a different path")
    return parser


def run(table_configs, args, title="nflverse DB"):
    """Execute the build/update flow against a configs dict.

    `table_configs` maps table name -> TableConfig. Caller provides fetch
    functions via the configs; everything else is shared here.
    """
    start = datetime.now()
    print(f"{title} — {start.strftime('%Y-%m-%d %H:%M')}")
    if args.dry_run:
        print("*** DRY RUN — no changes will be made ***")
    print()

    # Pick tables
    if args.tables:
        table_names = list(args.tables)
    elif args.pbp and not args.all:
        table_names = ["play_by_play"]
    else:
        table_names = [name for name, cfg in table_configs.items() if cfg.db == "main"]

    if args.pbp and "play_by_play" not in table_names and "play_by_play" in table_configs:
        table_names.append("play_by_play")

    for name in table_names:
        if name not in table_configs:
            raise SystemExit(
                f"ERROR: Unknown table '{name}'\n"
                f"Available: {', '.join(sorted(table_configs.keys()))}"
            )

    # Pick years
    if args.years:
        years = sorted(args.years)
    elif args.all:
        years = None  # use default range per table
    else:
        years = [datetime.now().year]

    print(f"Tables: {', '.join(table_names)}")
    print(f"Years: {', '.join(str(y) for y in years) if years else 'all (full range)'}")
    print()

    # Resolve paths
    output_db = Path(args.output) if args.output else DB_PATH
    output_pbp = Path(args.output_pbp) if args.output_pbp else PBP_DB_PATH

    def _resolve_db(cfg):
        return output_pbp if cfg.db == "pbp" else output_db

    dbs_touched = {_resolve_db(table_configs[n]) for n in table_names}

    # Backup
    if not args.no_backup and not args.dry_run and not args.output:
        print("Creating backups:")
        for db_path in dbs_touched:
            create_backup(db_path)
        print()

    for db_path in dbs_touched:
        db_path.parent.mkdir(parents=True, exist_ok=True)

    connections = {p: sqlite3.connect(str(p)) for p in dbs_touched}

    try:
        updated_season_stats = updated_game_stats = False

        for name in table_names:
            cfg = table_configs[name]
            conn = connections[_resolve_db(cfg)]

            print(f"Processing {name}:")
            if cfg.update_mode == "year_partition":
                yr_list = years if years else default_years_for(name)
                update_year_partition(conn, cfg, yr_list, dry_run=args.dry_run)
                if name == "season_stats":
                    updated_season_stats = True
                elif name == "game_stats":
                    updated_game_stats = True
            elif cfg.update_mode == "full_replace":
                update_full_replace(conn, cfg, dry_run=args.dry_run)
            print()

        main_conn = connections.get(output_db)
        if main_conn and updated_season_stats and updated_game_stats:
            backfill_season_stats_team(main_conn, years=years, dry_run=args.dry_run)
            print()

        if main_conn and not args.dry_run and args.all:
            create_indexes(main_conn)
            print()

        if main_conn and not args.dry_run:
            check_integrity(main_conn)
            print()

    finally:
        for conn in connections.values():
            conn.close()

    print(f"Completed in {datetime.now() - start}")
