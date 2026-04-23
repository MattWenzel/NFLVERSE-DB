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

# Re-use the canonical TABLE_CONFIGS from build_db.py so FK metadata, stub_source
# maps, and ordering stay in sync across both build paths. Only the fetch_fns
# differ (they pull from nflreadpy instead of local parquet).
from build_db import TABLE_CONFIGS as _LOCAL_CONFIGS, PBP_PLAYER_ID_COLS
from pipeline import (
    TableConfig,
    build_arg_parser,
    clean_gsis_id_series,
    clean_id_series,
    run,
    to_string_id,
)


def _polars_to_pandas(df):
    if hasattr(df, "to_pandas"):
        return df.to_pandas()
    return df


def _fetch_players(_years=None):
    df = _polars_to_pandas(nflreadpy.load_players())
    df = df.rename(columns={
        "gsis_id":  "player_gsis_id",
        "pfr_id":   "player_pfr_id",
        "espn_id":  "player_espn_id",
    })
    # Loose cleanup for the players source — historical pre-GSIS IDs like
    # 'YOU597411' are real records we want to keep. Child tables still use
    # the strict regex.
    df["player_gsis_id"] = clean_id_series(df["player_gsis_id"])
    df["player_pfr_id"]  = clean_id_series(df["player_pfr_id"])
    df["player_espn_id"] = clean_id_series(df["player_espn_id"])
    return df


def _fetch_player_ids(_years=None):
    df = _polars_to_pandas(nflreadpy.load_ff_playerids())
    if "espn_id" in df.columns:
        df["espn_id"] = to_string_id(df["espn_id"])
    if "gsis_id" in df.columns:
        df["gsis_id"] = clean_gsis_id_series(df["gsis_id"])
    if "pfr_id" in df.columns:
        df["pfr_id"] = clean_id_series(df["pfr_id"])
    if "espn_id" in df.columns:
        df["espn_id"] = clean_id_series(df["espn_id"])
    return df


def _fetch_games(years):
    df = _polars_to_pandas(nflreadpy.load_schedules(years))
    if "game_id" in df.columns:
        df["game_id"] = clean_id_series(df["game_id"])
    return df


def _fetch_combine(_years=None):
    df = _polars_to_pandas(nflreadpy.load_combine())
    df = df.rename(columns={"pfr_id": "player_pfr_id"})
    df["player_pfr_id"] = clean_id_series(df["player_pfr_id"])
    return df


def _fetch_draft_picks(_years=None):
    df = _polars_to_pandas(nflreadpy.load_draft_picks())
    df = df.rename(columns={
        "gsis_id":       "player_gsis_id",
        "pfr_player_id": "player_pfr_id",
    })
    df["player_gsis_id"] = clean_gsis_id_series(df["player_gsis_id"])
    df["player_pfr_id"]  = clean_id_series(df["player_pfr_id"])
    return df


def _fetch_snap_counts(years):
    df = _polars_to_pandas(nflreadpy.load_snap_counts(years))
    df = df.rename(columns={"pfr_player_id": "player_pfr_id"})
    if "player_pfr_id" in df.columns:
        df["player_pfr_id"] = clean_id_series(df["player_pfr_id"])
    return df


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
    combined = combined.rename(columns={"pfr_id": "player_pfr_id"})
    combined["player_pfr_id"] = clean_id_series(combined["player_pfr_id"])
    return combined


def _fetch_depth_charts(years):
    df = _polars_to_pandas(nflreadpy.load_depth_charts(years))
    df = df.rename(columns={"gsis_id": "player_gsis_id"})
    if "player_gsis_id" in df.columns:
        df["player_gsis_id"] = clean_gsis_id_series(df["player_gsis_id"])
    return df


def _fetch_depth_charts_2025(_years=None):
    df = _polars_to_pandas(nflreadpy.load_depth_charts([2025]))
    df = df.rename(columns={"gsis_id": "player_gsis_id", "espn_id": "player_espn_id"})
    df["player_gsis_id"] = clean_gsis_id_series(df["player_gsis_id"])
    df["player_espn_id"] = clean_id_series(df["player_espn_id"])
    return df


def _fetch_ngs_stats(_years=None):
    all_data = []
    for stat_type in ["passing", "rushing", "receiving"]:
        df = _polars_to_pandas(
            nflreadpy.load_nextgen_stats(seasons=True, stat_type=stat_type)
        )
        df["stat_type"] = stat_type
        all_data.append(df)
    combined = pd.concat(all_data, ignore_index=True)
    if "player_gsis_id" in combined.columns:
        combined["player_gsis_id"] = clean_gsis_id_series(combined["player_gsis_id"])
    return combined


def _fetch_qbr(_years=None):
    """QBR has no nflreadpy function — fetch CSV directly."""
    url = "https://raw.githubusercontent.com/nflverse/espnscrapeR-data/master/data/qbr-nfl-weekly.csv"
    df = pd.read_csv(url)
    if "player_id" in df.columns:
        df["player_id"] = to_string_id(df["player_id"])
    df = df.rename(columns={"player_id": "player_espn_id"})
    if "player_espn_id" in df.columns:
        df["player_espn_id"] = clean_id_series(df["player_espn_id"])
    return df


def _fetch_game_stats(years):
    df = _polars_to_pandas(nflreadpy.load_player_stats(years, summary_level="week"))
    df = df.rename(columns={"player_id": "player_gsis_id"})
    if "player_gsis_id" in df.columns:
        df["player_gsis_id"] = clean_gsis_id_series(df["player_gsis_id"])
    if "game_id" in df.columns:
        df["game_id"] = clean_id_series(df["game_id"])
    return df


def _fetch_season_stats(years):
    df = _polars_to_pandas(nflreadpy.load_player_stats(years, summary_level="reg"))
    df = df.rename(columns={"player_id": "player_gsis_id"})
    if "player_gsis_id" in df.columns:
        df["player_gsis_id"] = clean_gsis_id_series(df["player_gsis_id"])
    return df


def _fetch_pbp(years):
    df = _polars_to_pandas(nflreadpy.load_pbp(years))
    if df.empty:
        return df
    for col in PBP_PLAYER_ID_COLS:
        if col in df.columns:
            df[col] = clean_gsis_id_series(df[col])
    if "game_id" in df.columns:
        df["game_id"] = clean_id_series(df["game_id"])
    return df


# Build TABLE_CONFIGS by cloning build_db.py's configs and swapping in the
# nflreadpy fetch functions. Keeps FK metadata, stub_source, ordering identical.
_FETCH_OVERRIDES = {
    "players":           _fetch_players,
    "player_ids":        _fetch_player_ids,
    "games":             _fetch_games,
    "combine":           _fetch_combine,
    "draft_picks":       _fetch_draft_picks,
    "snap_counts":       _fetch_snap_counts,
    "pfr_advanced":      _fetch_pfr_advanced,
    "depth_charts":      _fetch_depth_charts,
    "depth_charts_2025": _fetch_depth_charts_2025,
    "ngs_stats":         _fetch_ngs_stats,
    "qbr":               _fetch_qbr,
    "game_stats":        _fetch_game_stats,
    "season_stats":      _fetch_season_stats,
    "play_by_play":      _fetch_pbp,
}


def _clone_with_fetch(cfg, fetch_fn):
    # nflreadpy has no local parquet glob, so bulk_parquet mode doesn't apply;
    # fall back to year_partition for PBP. Other FK metadata is preserved.
    mode = "year_partition" if cfg.update_mode == "bulk_parquet" else cfg.update_mode
    return TableConfig(
        cfg.name,
        update_mode=mode,
        fetch_fn=fetch_fn,
        dedup_cols=cfg.dedup_cols,
        drop_na_col=cfg.drop_na_col,
        primary_key=cfg.primary_key,
        unique_cols=cfg.unique_cols,
        foreign_keys=cfg.foreign_keys,
        stub_source=cfg.stub_source,
    )


TABLE_CONFIGS = {
    name: _clone_with_fetch(cfg, _FETCH_OVERRIDES[name])
    for name, cfg in _LOCAL_CONFIGS.items()
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
