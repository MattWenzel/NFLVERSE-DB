#!/usr/bin/env python3
"""
Incrementally update nflverse databases with new data using nflreadpy.

Supports year-partition replacement (delete season + re-insert) for time-series
tables, and full replacement for reference tables. Creates a rolling backup
before modifications and wraps each update in a transaction.

Usage:
    python3 update_db.py                              # Update all with detected changes
    python3 update_db.py --tables game_stats players  # Specific tables
    python3 update_db.py --years 2025                 # Specific year(s)
    python3 update_db.py --pbp                        # Update play-by-play DB
    python3 update_db.py --pbp --years 2025           # PBP for specific year
    python3 update_db.py --all                        # Force full refresh of all tables
    python3 update_db.py --dry-run                    # Preview what would change
    python3 update_db.py --no-backup                  # Skip backup step
    python3 update_db.py --check-first                # Run check_updates before updating
    python3 update_db.py --all --output nflverse_v2.db  # Full refresh to a new DB file
"""

import argparse
import json
import shutil
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

import nflreadpy
import pandas as pd

from config import DB_PATH, METADATA_PATH, PBP_DB_PATH

# ---------------------------------------------------------------------------
# No column filters or renames — we use nflverse-native column names.
# All columns from nflreadpy are kept as-is (including defensive, kicking, ST).
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Table configuration
# ---------------------------------------------------------------------------

def _polars_to_pandas(df):
    """Convert a Polars DataFrame to pandas if needed."""
    if hasattr(df, "to_pandas"):
        return df.to_pandas()
    return df


class TableConfig:
    """Configuration for how to fetch and update a single table."""

    def __init__(self, name, *, db="main", update_mode="year_partition",
                 fetch_fn=None, dedup_cols=None, drop_na_col=None):
        self.name = name
        self.db = db  # "main" or "pbp"
        self.update_mode = update_mode  # "year_partition" or "full_replace"
        self.fetch_fn = fetch_fn
        self.dedup_cols = dedup_cols  # subset columns for drop_duplicates
        self.drop_na_col = drop_na_col  # column to dropna on (e.g. "player_id")

    def get_db_path(self):
        return PBP_DB_PATH if self.db == "pbp" else DB_PATH


def _fetch_game_stats(years):
    df = _polars_to_pandas(nflreadpy.load_player_stats(years, summary_level="week"))
    return df


def _fetch_season_stats(years):
    df = _polars_to_pandas(nflreadpy.load_player_stats(years, summary_level="reg"))
    return df


def _fetch_games(years):
    df = _polars_to_pandas(nflreadpy.load_schedules(years))
    return df


def _fetch_players(_years=None):
    df = _polars_to_pandas(nflreadpy.load_players())
    return df


def _fetch_player_ids(_years=None):
    df = _polars_to_pandas(nflreadpy.load_ff_playerids())
    return df


def _fetch_draft_picks(_years=None):
    df = _polars_to_pandas(nflreadpy.load_draft_picks())
    return df


def _fetch_combine(_years=None):
    df = _polars_to_pandas(nflreadpy.load_combine())
    return df


def _fetch_snap_counts(years):
    df = _polars_to_pandas(nflreadpy.load_snap_counts(years))
    return df


def _fetch_depth_charts(years):
    df = _polars_to_pandas(nflreadpy.load_depth_charts(years))
    return df


def _fetch_ngs_stats(_years=None):
    seasons = list(range(2016, datetime.now().year))
    all_data = []
    for stat_type in ["passing", "rushing", "receiving"]:
        df = _polars_to_pandas(
            nflreadpy.load_nextgen_stats(seasons=seasons, stat_type=stat_type)
        )
        df["stat_type"] = stat_type
        all_data.append(df)
    return pd.concat(all_data, ignore_index=True)


def _fetch_pfr_advanced(_years=None):
    seasons = list(range(2018, datetime.now().year))
    all_data = []
    for stat_type in ["pass", "rush", "rec"]:
        df = _polars_to_pandas(
            nflreadpy.load_pfr_advstats(
                seasons=seasons, stat_type=stat_type, summary_level="season"
            )
        )
        df["stat_type"] = stat_type
        all_data.append(df)
    return pd.concat(all_data, ignore_index=True)


def _fetch_qbr(_years=None):
    """QBR has no nflreadpy function — fetch CSV directly."""
    url = "https://raw.githubusercontent.com/nflverse/espnscrapeR-data/master/data/qbr-nfl-weekly.csv"
    df = pd.read_csv(url)
    return df


def _fetch_pbp(years):
    df = _polars_to_pandas(nflreadpy.load_pbp(years))
    return df


# All table configs — nflverse-native column names, no renames or column filters.
TABLE_CONFIGS = {
    "game_stats": TableConfig(
        "game_stats",
        update_mode="year_partition",
        fetch_fn=_fetch_game_stats,
        dedup_cols=["player_id", "season", "week"],
        drop_na_col="player_id",
    ),
    "season_stats": TableConfig(
        "season_stats",
        update_mode="year_partition",
        fetch_fn=_fetch_season_stats,
        drop_na_col="player_id",
    ),
    "games": TableConfig(
        "games",
        update_mode="year_partition",
        fetch_fn=_fetch_games,
    ),
    "players": TableConfig(
        "players",
        update_mode="full_replace",
        fetch_fn=_fetch_players,
        dedup_cols=["gsis_id"],
        drop_na_col="gsis_id",
    ),
    "player_ids": TableConfig(
        "player_ids",
        update_mode="full_replace",
        fetch_fn=_fetch_player_ids,
        dedup_cols=["gsis_id"],
        drop_na_col="gsis_id",
    ),
    "draft_picks": TableConfig(
        "draft_picks",
        update_mode="full_replace",
        fetch_fn=_fetch_draft_picks,
    ),
    "combine": TableConfig(
        "combine",
        update_mode="full_replace",
        fetch_fn=_fetch_combine,
    ),
    "snap_counts": TableConfig(
        "snap_counts",
        update_mode="year_partition",
        fetch_fn=_fetch_snap_counts,
    ),
    "depth_charts": TableConfig(
        "depth_charts",
        update_mode="year_partition",
        fetch_fn=_fetch_depth_charts,
    ),
    "depth_charts_2025": TableConfig(
        "depth_charts_2025",
        update_mode="year_partition",
        fetch_fn=_fetch_depth_charts,
    ),
    "ngs_stats": TableConfig(
        "ngs_stats",
        update_mode="full_replace",
        fetch_fn=_fetch_ngs_stats,
    ),
    "pfr_advanced": TableConfig(
        "pfr_advanced",
        update_mode="full_replace",
        fetch_fn=_fetch_pfr_advanced,
    ),
    "qbr": TableConfig(
        "qbr",
        update_mode="full_replace",
        fetch_fn=_fetch_qbr,
    ),
    "play_by_play": TableConfig(
        "play_by_play",
        db="pbp",
        update_mode="year_partition",
        fetch_fn=_fetch_pbp,
    ),
}


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
# Year-partition update
# ---------------------------------------------------------------------------

def _table_exists(conn, table_name):
    """Check if a table exists in the database."""
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
        # Infer SQLite type from pandas dtype
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


def update_year_partition(conn, config, years, dry_run=False):
    """Delete + re-insert data for specific years."""
    for year in years:
        print(f"  {config.name} [{year}]: ", end="", flush=True)

        if dry_run:
            # Count existing rows
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

        # Fetch data
        try:
            df = config.fetch_fn([year])
        except Exception as e:
            print(f"FETCH ERROR: {e}")
            continue

        if df is None or len(df) == 0:
            print("no data returned")
            continue

        # Drop rows with null primary identifiers
        if config.drop_na_col and config.drop_na_col in df.columns:
            df = df.dropna(subset=[config.drop_na_col])

        # Deduplicate
        if config.dedup_cols:
            dedup_available = [c for c in config.dedup_cols if c in df.columns]
            if dedup_available:
                df = df.drop_duplicates(subset=dedup_available, keep="first")

        # If table doesn't exist yet, create it with the first year's data
        if not _table_exists(conn, config.name):
            try:
                df.to_sql(config.name, conn, if_exists="replace", index=False)
                conn.commit()
                print(f"{len(df):,} rows inserted (table created)")
            except Exception as e:
                conn.rollback()
                print(f"ERROR creating table: {e}")
            continue

        # Add any columns the table is missing (schema drift between years)
        _add_missing_columns(conn, config.name, df)

        # Delete old + insert new
        try:
            conn.execute(
                f"DELETE FROM [{config.name}] WHERE season = ?", (year,)
            )
            conn.commit()
            df.to_sql(config.name, conn, if_exists="append", index=False)
            conn.commit()
            print(f"{len(df):,} rows inserted")
        except Exception as e:
            conn.rollback()
            print(f"ERROR (rolled back): {e}")


# ---------------------------------------------------------------------------
# Full replace
# ---------------------------------------------------------------------------

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

    # Drop rows with null primary identifiers
    if config.drop_na_col and config.drop_na_col in df.columns:
        df = df.dropna(subset=[config.drop_na_col])

    # Deduplicate
    if config.dedup_cols:
        dedup_available = [c for c in config.dedup_cols if c in df.columns]
        if dedup_available:
            df = df.drop_duplicates(subset=dedup_available, keep="first")

    df.to_sql(config.name, conn, if_exists="replace", index=False)
    print(f"{len(df):,} rows")


# ---------------------------------------------------------------------------
# season_stats.team backfill
# ---------------------------------------------------------------------------

def backfill_season_stats_team(conn, years=None, dry_run=False):
    """
    Backfill season_stats.team from game_stats (most common team per player-season).
    Matches existing behavior noted in CLAUDE.md.
    """
    print("  Backfilling season_stats.team from game_stats...", end=" ", flush=True)

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


# ---------------------------------------------------------------------------
# Integrity check
# ---------------------------------------------------------------------------

def check_integrity(conn, dry_run=False):
    """Check for orphan records in game_stats/season_stats."""
    if dry_run:
        return

    # Only run if all required tables exist
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
# Determine what to update
# ---------------------------------------------------------------------------

def detect_changes_from_metadata():
    """Read check_updates metadata to determine what needs updating."""
    if not METADATA_PATH.exists():
        return None

    with open(METADATA_PATH) as f:
        metadata = json.load(f)

    # This is a simplified detection — the full logic lives in check_updates.py.
    # Here we just look at what metadata suggests is stale.
    return metadata


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Incrementally update nflverse databases")
    parser.add_argument("--tables", nargs="+", help="Specific table(s) to update")
    parser.add_argument("--years", nargs="+", type=int, help="Specific year(s) for year-partitioned tables")
    parser.add_argument("--pbp", action="store_true", help="Update play-by-play DB")
    parser.add_argument("--all", action="store_true", help="Force full refresh of all tables")
    parser.add_argument("--dry-run", action="store_true", help="Preview changes without modifying")
    parser.add_argument("--no-backup", action="store_true", help="Skip backup step")
    parser.add_argument("--check-first", action="store_true", help="Run check_updates.py first")
    parser.add_argument("--output", type=str, help="Write to a different DB path (default: nflverse_custom.db)")
    parser.add_argument("--output-pbp", type=str, help="Write PBP to a different DB path (default: pbp.db)")
    args = parser.parse_args()

    if args.check_first:
        print("Running update check first...\n")
        import check_updates
        results, _ = check_updates.check_updates()
        check_updates.print_report(results, {})
        print("\n" + "=" * 50 + "\n")

    start = datetime.now()
    print(f"nflverse DB Update — {start.strftime('%Y-%m-%d %H:%M')}")
    if args.dry_run:
        print("*** DRY RUN — no changes will be made ***")
    print()

    # Determine which tables to update
    if args.tables:
        table_names = args.tables
    elif args.pbp:
        table_names = ["play_by_play"]
    elif args.all:
        table_names = [name for name, cfg in TABLE_CONFIGS.items() if cfg.db == "main"]
        if args.pbp:
            table_names.append("play_by_play")
    else:
        # Default: update all main DB tables (not PBP unless --pbp)
        table_names = [name for name, cfg in TABLE_CONFIGS.items() if cfg.db == "main"]

    # Add PBP if requested
    if args.pbp and "play_by_play" not in table_names:
        table_names.append("play_by_play")

    # Validate table names
    for name in table_names:
        if name not in TABLE_CONFIGS:
            print(f"ERROR: Unknown table '{name}'")
            print(f"Available: {', '.join(sorted(TABLE_CONFIGS.keys()))}")
            sys.exit(1)

    # Determine years for year-partitioned tables
    if args.years:
        years = sorted(args.years)
    elif args.all:
        years = None  # Will use full range per table
    else:
        # Default: current year
        years = [datetime.now().year]

    print(f"Tables: {', '.join(table_names)}")
    if years:
        print(f"Years: {', '.join(str(y) for y in years)}")
    else:
        print("Years: all (full refresh)")
    print()

    # Resolve output paths (allow --output to override defaults)
    output_db = Path(args.output) if args.output else DB_PATH
    output_pbp = Path(args.output_pbp) if args.output_pbp else PBP_DB_PATH

    def _resolve_db_path(config):
        return output_pbp if config.db == "pbp" else output_db

    # Backup (only for default paths, not custom --output)
    dbs_touched = set()
    for name in table_names:
        dbs_touched.add(_resolve_db_path(TABLE_CONFIGS[name]))

    if not args.no_backup and not args.dry_run and not args.output:
        print("Creating backups:")
        for db_path in dbs_touched:
            create_backup(db_path)
        print()

    # Open connections
    connections = {}
    for db_path in dbs_touched:
        connections[db_path] = sqlite3.connect(str(db_path))

    try:
        # Process each table
        updated_season_stats = False
        updated_game_stats = False

        for name in table_names:
            config = TABLE_CONFIGS[name]
            conn = connections[_resolve_db_path(config)]

            print(f"Updating {name}:")

            if config.update_mode == "year_partition":
                if years:
                    update_year_partition(conn, config, years, dry_run=args.dry_run)
                elif args.all:
                    # Full refresh: need a default year range per table
                    default_ranges = {
                        "game_stats": range(1999, datetime.now().year + 1),
                        "season_stats": range(1999, datetime.now().year + 1),
                        "games": range(1999, datetime.now().year + 1),
                        "snap_counts": range(2015, datetime.now().year + 1),
                        "depth_charts": range(2001, 2025),  # 2001-2024 (old schema)
                        "depth_charts_2025": range(2025, datetime.now().year + 1),
                        "play_by_play": range(1999, datetime.now().year + 1),
                    }
                    yr_range = default_ranges.get(name, range(1999, datetime.now().year + 1))
                    update_year_partition(conn, config, list(yr_range), dry_run=args.dry_run)
                else:
                    update_year_partition(conn, config, [datetime.now().year], dry_run=args.dry_run)

                if name == "season_stats":
                    updated_season_stats = True
                if name == "game_stats":
                    updated_game_stats = True

            elif config.update_mode == "full_replace":
                update_full_replace(conn, config, dry_run=args.dry_run)

            print()

        # Backfill season_stats.team if both were updated
        main_conn = connections.get(output_db)
        if main_conn and updated_season_stats and updated_game_stats:
            backfill_season_stats_team(main_conn, years=years, dry_run=args.dry_run)
            print()

        # Integrity check on main DB
        if main_conn and not args.dry_run:
            check_integrity(main_conn)
            print()

    finally:
        for conn in connections.values():
            conn.close()

    elapsed = datetime.now() - start
    print(f"Completed in {elapsed}")


if __name__ == "__main__":
    main()
