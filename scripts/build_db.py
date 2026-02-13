#!/usr/bin/env python3
"""
Build/update nflverse databases from local parquet/CSV files in data/raw/.

Reads data downloaded by download.py and loads it into SQLite databases.
Same update logic as update_db.py (year-partition, full-replace, dedup,
schema drift handling) but reads from local files instead of nflreadpy.

Usage:
    python3 scripts/build_db.py --all                              # Full build from local files
    python3 scripts/build_db.py --tables game_stats players        # Specific tables
    python3 scripts/build_db.py --years 2025                       # Specific year(s)
    python3 scripts/build_db.py --pbp --all                        # Play-by-play
    python3 scripts/build_db.py --pbp --years 2025                 # PBP for one year
    python3 scripts/build_db.py --dry-run                          # Preview only
    python3 scripts/build_db.py --no-backup                        # Skip backup step
    python3 scripts/build_db.py --all --output data/nflverse_v2.db # Full build to specific file
"""

import argparse
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd

from config import DB_PATH, PBP_DB_PATH, RAW_DATA_PATH
from update_db import (
    TableConfig,
    backfill_season_stats_team,
    check_integrity,
    create_backup,
    create_indexes,
    update_full_replace,
    update_year_partition,
)


# ---------------------------------------------------------------------------
# Local file fetch functions — read from data/raw/ parquet/CSV files
# ---------------------------------------------------------------------------

def _read_parquets(folder, pattern, years):
    """Read year-partitioned parquet files, return combined DataFrame."""
    dfs = []
    for year in years:
        path = RAW_DATA_PATH / folder / pattern.format(year=year)
        if not path.exists():
            print(f"SKIP ({path.name} not found) ", end="", flush=True)
            continue
        dfs.append(pd.read_parquet(path))
    if not dfs:
        return pd.DataFrame()
    return pd.concat(dfs, ignore_index=True)


def _fetch_game_stats(years):
    return _read_parquets("stats_player", "stats_player_week_{year}.parquet", years)


def _fetch_season_stats(years):
    return _read_parquets("stats_player", "stats_player_reg_{year}.parquet", years)


def _fetch_games(years):
    path = RAW_DATA_PATH / "schedules" / "games.parquet"
    df = pd.read_parquet(path)
    if years:
        df = df[df["season"].isin(years)]
    return df


def _fetch_players(_years=None):
    return pd.read_parquet(RAW_DATA_PATH / "players" / "players.parquet")


def _fetch_player_ids(_years=None):
    return pd.read_csv(RAW_DATA_PATH / "external" / "db_playerids.csv")


def _fetch_draft_picks(_years=None):
    return pd.read_parquet(RAW_DATA_PATH / "draft_picks" / "draft_picks.parquet")


def _fetch_combine(_years=None):
    return pd.read_parquet(RAW_DATA_PATH / "combine" / "combine.parquet")


def _fetch_snap_counts(years):
    return _read_parquets("snap_counts", "snap_counts_{year}.parquet", years)


def _fetch_depth_charts(years):
    return _read_parquets("depth_charts", "depth_charts_{year}.parquet", years)


def _fetch_depth_charts_2025(_years=None):
    return pd.read_parquet(RAW_DATA_PATH / "depth_charts" / "depth_charts_2025.parquet")


def _fetch_ngs_stats(_years=None):
    all_data = []
    for stat_type in ["passing", "rushing", "receiving"]:
        path = RAW_DATA_PATH / "nextgen_stats" / f"ngs_{stat_type}.parquet"
        if not path.exists():
            print(f"WARNING: {path.name} not found, skipping")
            continue
        df = pd.read_parquet(path)
        df["stat_type"] = stat_type
        all_data.append(df)
    if not all_data:
        return pd.DataFrame()
    return pd.concat(all_data, ignore_index=True)


def _fetch_pfr_advanced(_years=None):
    all_data = []
    for stat_type in ["pass", "rush", "rec"]:
        path = RAW_DATA_PATH / "pfr_advstats" / f"advstats_season_{stat_type}.parquet"
        if not path.exists():
            print(f"WARNING: {path.name} not found, skipping")
            continue
        df = pd.read_parquet(path)
        df["stat_type"] = stat_type
        all_data.append(df)
    if not all_data:
        return pd.DataFrame()
    return pd.concat(all_data, ignore_index=True)


def _fetch_qbr(_years=None):
    return pd.read_csv(RAW_DATA_PATH / "external" / "qbr-nfl-weekly.csv")


def _fetch_pbp(years):
    return _read_parquets("pbp", "play_by_play_{year}.parquet", years)


# ---------------------------------------------------------------------------
# Table configs — same structure as update_db.py, local fetch functions
# ---------------------------------------------------------------------------

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
        update_mode="full_replace",
        fetch_fn=_fetch_depth_charts_2025,
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
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Build nflverse databases from local files")
    parser.add_argument("--tables", nargs="+", help="Specific table(s) to build")
    parser.add_argument("--years", nargs="+", type=int, help="Specific year(s) for year-partitioned tables")
    parser.add_argument("--pbp", action="store_true", help="Include play-by-play DB")
    parser.add_argument("--all", action="store_true", help="Full build (all tables, all years)")
    parser.add_argument("--dry-run", action="store_true", help="Preview changes without modifying")
    parser.add_argument("--no-backup", action="store_true", help="Skip backup step")
    parser.add_argument("--output", type=str, help="Write to a different DB path")
    parser.add_argument("--output-pbp", type=str, help="Write PBP to a different DB path")
    args = parser.parse_args()

    start = datetime.now()
    print(f"nflverse DB Build (local files) — {start.strftime('%Y-%m-%d %H:%M')}")
    print(f"Source: {RAW_DATA_PATH}")
    if args.dry_run:
        print("*** DRY RUN — no changes will be made ***")
    print()

    # Determine which tables to build
    if args.tables:
        table_names = args.tables
    elif args.pbp:
        table_names = ["play_by_play"]
    elif args.all:
        table_names = [name for name, cfg in TABLE_CONFIGS.items() if cfg.db == "main"]
        if args.pbp:
            table_names.append("play_by_play")
    else:
        table_names = [name for name, cfg in TABLE_CONFIGS.items() if cfg.db == "main"]

    if args.pbp and "play_by_play" not in table_names:
        table_names.append("play_by_play")

    # Validate
    for name in table_names:
        if name not in TABLE_CONFIGS:
            print(f"ERROR: Unknown table '{name}'")
            print(f"Available: {', '.join(sorted(TABLE_CONFIGS.keys()))}")
            sys.exit(1)

    # Determine years
    if args.years:
        years = sorted(args.years)
    elif args.all:
        years = None
    else:
        years = [datetime.now().year]

    print(f"Tables: {', '.join(table_names)}")
    if years:
        print(f"Years: {', '.join(str(y) for y in years)}")
    else:
        print("Years: all (full build)")
    print()

    # Resolve output paths
    output_db = Path(args.output) if args.output else DB_PATH
    output_pbp = Path(args.output_pbp) if args.output_pbp else PBP_DB_PATH

    def _resolve_db_path(config):
        return output_pbp if config.db == "pbp" else output_db

    # Backup
    dbs_touched = set()
    for name in table_names:
        dbs_touched.add(_resolve_db_path(TABLE_CONFIGS[name]))

    if not args.no_backup and not args.dry_run and not args.output:
        print("Creating backups:")
        for db_path in dbs_touched:
            create_backup(db_path)
        print()

    # Ensure output directories exist
    for db_path in dbs_touched:
        db_path.parent.mkdir(parents=True, exist_ok=True)

    # Open connections
    connections = {}
    for db_path in dbs_touched:
        connections[db_path] = sqlite3.connect(str(db_path))

    try:
        updated_season_stats = False
        updated_game_stats = False

        for name in table_names:
            config = TABLE_CONFIGS[name]
            conn = connections[_resolve_db_path(config)]

            print(f"Building {name}:")

            if config.update_mode == "year_partition":
                if years:
                    update_year_partition(conn, config, years, dry_run=args.dry_run)
                elif args.all:
                    default_ranges = {
                        "game_stats": range(1999, datetime.now().year + 1),
                        "season_stats": range(1999, datetime.now().year + 1),
                        "games": range(1999, datetime.now().year + 1),
                        "snap_counts": range(2012, datetime.now().year + 1),
                        "depth_charts": range(2001, 2025),
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

        # Backfill season_stats.recent_team
        main_conn = connections.get(output_db)
        if main_conn and updated_season_stats and updated_game_stats:
            backfill_season_stats_team(main_conn, years=years, dry_run=args.dry_run)
            print()

        # Create indexes
        if main_conn and not args.dry_run and args.all:
            create_indexes(main_conn)
            print()

        # Integrity check
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
