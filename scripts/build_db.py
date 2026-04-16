#!/usr/bin/env python3
"""
Build/update nflverse databases from local parquet/CSV files in data/raw/.

Reads data previously downloaded by download.py and loads it into SQLite.
Shares year-partition, full-replace, schema-drift and backup logic with
build_db_nflreadpy.py (the nflreadpy-based fallback) via scripts/pipeline.py.

Usage:
    python3 scripts/build_db.py --all                              # Full build from local files
    python3 scripts/build_db.py --tables game_stats players        # Specific tables
    python3 scripts/build_db.py --years 2025                       # Specific year(s)
    python3 scripts/build_db.py --pbp --all                        # Play-by-play
    python3 scripts/build_db.py --pbp --years 2025                 # PBP for one year
    python3 scripts/build_db.py --dry-run                          # Preview only
    python3 scripts/build_db.py --no-backup                        # Skip backup step
    python3 scripts/build_db.py --all --output data/nflverse.db    # Full build to specific file
"""

import pandas as pd

from config import RAW_DATA_PATH
from pipeline import TableConfig, build_arg_parser, run


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
    df = pd.read_parquet(RAW_DATA_PATH / "schedules" / "games.parquet")
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


TABLE_CONFIGS = {
    "game_stats": TableConfig(
        "game_stats", update_mode="year_partition", fetch_fn=_fetch_game_stats,
        dedup_cols=["player_id", "season", "week"], drop_na_col="player_id",
    ),
    "season_stats": TableConfig(
        "season_stats", update_mode="year_partition", fetch_fn=_fetch_season_stats,
        drop_na_col="player_id",
    ),
    "games": TableConfig(
        "games", update_mode="year_partition", fetch_fn=_fetch_games,
    ),
    "players": TableConfig(
        "players", update_mode="full_replace", fetch_fn=_fetch_players,
        dedup_cols=["gsis_id"], drop_na_col="gsis_id",
    ),
    "player_ids": TableConfig(
        "player_ids", update_mode="full_replace", fetch_fn=_fetch_player_ids,
        dedup_cols=["gsis_id"], drop_na_col="gsis_id",
    ),
    "draft_picks": TableConfig(
        "draft_picks", update_mode="full_replace", fetch_fn=_fetch_draft_picks,
    ),
    "combine": TableConfig(
        "combine", update_mode="full_replace", fetch_fn=_fetch_combine,
    ),
    "snap_counts": TableConfig(
        "snap_counts", update_mode="year_partition", fetch_fn=_fetch_snap_counts,
    ),
    "depth_charts": TableConfig(
        "depth_charts", update_mode="year_partition", fetch_fn=_fetch_depth_charts,
    ),
    "depth_charts_2025": TableConfig(
        "depth_charts_2025", update_mode="full_replace", fetch_fn=_fetch_depth_charts_2025,
    ),
    "ngs_stats": TableConfig(
        "ngs_stats", update_mode="full_replace", fetch_fn=_fetch_ngs_stats,
    ),
    "pfr_advanced": TableConfig(
        "pfr_advanced", update_mode="full_replace", fetch_fn=_fetch_pfr_advanced,
    ),
    "qbr": TableConfig(
        "qbr", update_mode="full_replace", fetch_fn=_fetch_qbr,
    ),
    "play_by_play": TableConfig(
        "play_by_play", db="pbp", update_mode="year_partition", fetch_fn=_fetch_pbp,
    ),
}


def main():
    parser = build_arg_parser("Build nflverse databases from local files in data/raw/")
    args = parser.parse_args()
    run(TABLE_CONFIGS, args, title=f"nflverse DB Build (source: {RAW_DATA_PATH})")


if __name__ == "__main__":
    main()
