#!/usr/bin/env python3
"""
Alternative build path that fetches nflverse data over the network via the
nflreadpy client (https://github.com/nflverse/nflreadpy) instead of reading
local parquet files.

This is a fallback for when the primary `download.py` + `build_db.py` pipeline
breaks — e.g. if nflverse reorganizes a release, renames an asset, or the
hardcoded release URLs in download.py go stale. Under normal circumstances,
prefer the local path; it is deterministic, offline-re-runnable, and does not
depend on nflreadpy's release-mapping logic.

Shares year-partition, full-replace, schema-drift, and backup logic with
`build_db.py` via `scripts/pipeline.py`.

Usage:
    python3 scripts/build_db_nflreadpy.py                              # Current year, all tables
    python3 scripts/build_db_nflreadpy.py --tables game_stats players  # Specific tables
    python3 scripts/build_db_nflreadpy.py --years 2025                 # Specific year(s)
    python3 scripts/build_db_nflreadpy.py --pbp                        # Play-by-play table
    python3 scripts/build_db_nflreadpy.py --pbp --years 2025           # PBP for specific year
    python3 scripts/build_db_nflreadpy.py --all                        # Force full refresh of all tables
    python3 scripts/build_db_nflreadpy.py --dry-run                    # Preview what would change
    python3 scripts/build_db_nflreadpy.py --no-backup                  # Skip backup step
    python3 scripts/build_db_nflreadpy.py --check-first                # Run check_updates before pulling
    python3 scripts/build_db_nflreadpy.py --all --output data/nflverse.duckdb  # Full refresh to a new DB file
"""

import nflreadpy
import pandas as pd

from pipeline import TableConfig, build_arg_parser, run, to_string_id


def _polars_to_pandas(df):
    if hasattr(df, "to_pandas"):
        return df.to_pandas()
    return df


def _fetch_game_stats(years):
    df = _polars_to_pandas(nflreadpy.load_player_stats(years, summary_level="week"))
    return df.rename(columns={"player_id": "player_gsis_id"})


def _fetch_season_stats(years):
    df = _polars_to_pandas(nflreadpy.load_player_stats(years, summary_level="reg"))
    return df.rename(columns={"player_id": "player_gsis_id"})


def _fetch_games(years):
    return _polars_to_pandas(nflreadpy.load_schedules(years))


def _fetch_players(_years=None):
    df = _polars_to_pandas(nflreadpy.load_players())
    return df.rename(columns={
        "gsis_id":  "player_gsis_id",
        "pfr_id":   "player_pfr_id",
        "espn_id":  "player_espn_id",
    })


def _fetch_player_ids(_years=None):
    df = _polars_to_pandas(nflreadpy.load_ff_playerids())
    if "espn_id" in df.columns:
        df["espn_id"] = to_string_id(df["espn_id"])
    return df


def _fetch_draft_picks(_years=None):
    df = _polars_to_pandas(nflreadpy.load_draft_picks())
    return df.rename(columns={
        "gsis_id":       "player_gsis_id",
        "pfr_player_id": "player_pfr_id",
    })


def _fetch_combine(_years=None):
    df = _polars_to_pandas(nflreadpy.load_combine())
    return df.rename(columns={"pfr_id": "player_pfr_id"})


def _fetch_snap_counts(years):
    df = _polars_to_pandas(nflreadpy.load_snap_counts(years))
    return df.rename(columns={"pfr_player_id": "player_pfr_id"})


def _fetch_depth_charts(years):
    df = _polars_to_pandas(nflreadpy.load_depth_charts(years))
    return df.rename(columns={"gsis_id": "player_gsis_id"})


def _fetch_depth_charts_2025(_years=None):
    df = _polars_to_pandas(nflreadpy.load_depth_charts([2025]))
    return df.rename(columns={"gsis_id": "player_gsis_id", "espn_id": "player_espn_id"})


def _fetch_ngs_stats(_years=None):
    # ngs_stats already ships player_gsis_id upstream — no rename needed.
    all_data = []
    for stat_type in ["passing", "rushing", "receiving"]:
        df = _polars_to_pandas(
            nflreadpy.load_nextgen_stats(seasons=True, stat_type=stat_type)
        )
        df["stat_type"] = stat_type
        all_data.append(df)
    return pd.concat(all_data, ignore_index=True)


def _fetch_pfr_advanced(_years=None):
    all_data = []
    for stat_type in ["pass", "rush", "rec"]:
        df = _polars_to_pandas(
            nflreadpy.load_pfr_advstats(
                stat_type=stat_type, summary_level="season"
            )
        )
        df["stat_type"] = stat_type
        all_data.append(df)
    combined = pd.concat(all_data, ignore_index=True)
    return combined.rename(columns={"pfr_id": "player_pfr_id"})


def _fetch_qbr(_years=None):
    """QBR has no nflreadpy function — fetch CSV directly."""
    url = "https://raw.githubusercontent.com/nflverse/espnscrapeR-data/master/data/qbr-nfl-weekly.csv"
    df = pd.read_csv(url)
    if "player_id" in df.columns:
        df["player_id"] = to_string_id(df["player_id"])
    return df.rename(columns={"player_id": "player_espn_id"})


def _fetch_pbp(years):
    return _polars_to_pandas(nflreadpy.load_pbp(years))


TABLE_CONFIGS = {
    "game_stats": TableConfig(
        "game_stats", update_mode="year_partition", fetch_fn=_fetch_game_stats,
        dedup_cols=["player_gsis_id", "season", "week"], drop_na_col="player_gsis_id",
    ),
    "season_stats": TableConfig(
        "season_stats", update_mode="year_partition", fetch_fn=_fetch_season_stats,
        drop_na_col="player_gsis_id",
    ),
    "games": TableConfig(
        "games", update_mode="year_partition", fetch_fn=_fetch_games,
    ),
    "players": TableConfig(
        "players", update_mode="full_replace", fetch_fn=_fetch_players,
        dedup_cols=["player_gsis_id"], drop_na_col="player_gsis_id",
    ),
    "player_ids": TableConfig(
        "player_ids", update_mode="full_replace", fetch_fn=_fetch_player_ids,
        dedup_cols=["gsis_id"], drop_na_col="gsis_id",  # bridge table — not renamed
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
        "play_by_play", update_mode="year_partition", fetch_fn=_fetch_pbp,
    ),
}


def main():
    parser = build_arg_parser(
        "Build nflverse databases via nflreadpy (alternative to download.py + build_db.py)"
    )
    parser.add_argument("--check-first", action="store_true",
                        help="Run check_updates.py before updating")
    args = parser.parse_args()

    if args.check_first:
        print("Running update check first...\n")
        import check_updates
        results, _ = check_updates.check_updates()
        check_updates.print_report(results)
        print("\n" + "=" * 50 + "\n")

    run(TABLE_CONFIGS, args, title="nflverse DB Build (source: nflreadpy)")


if __name__ == "__main__":
    main()
