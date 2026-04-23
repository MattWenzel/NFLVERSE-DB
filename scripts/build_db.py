#!/usr/bin/env python3
"""
Build/update the nflverse DuckDB database from local parquet/CSV files in data/raw/.

Reads data previously downloaded by download.py and loads it into DuckDB.
Shares year-partition, full-replace, schema-drift and backup logic with
build_db_nflreadpy.py (the nflreadpy-based fallback) via scripts/pipeline.py.

Usage:
    python3 scripts/build_db.py --all                                  # Full build from local files
    python3 scripts/build_db.py --tables game_stats players            # Specific tables
    python3 scripts/build_db.py --years 2025                           # Specific year(s)
    python3 scripts/build_db.py --pbp --all                            # Play-by-play
    python3 scripts/build_db.py --pbp --years 2025                     # PBP for one year
    python3 scripts/build_db.py --dry-run                              # Preview only
    python3 scripts/build_db.py --no-backup                            # Skip backup step
    python3 scripts/build_db.py --all --output data/nflverse.duckdb    # Full build to specific file
"""

import pandas as pd

from config import RAW_DATA_PATH
from pipeline import (
    TableConfig,
    build_arg_parser,
    clean_gsis_id_series,
    clean_id_series,
    run,
    to_string_id,
)


# Every play_by_play column that holds a GSIS player reference — carries both
# the `*_player_id` pattern and the bare `passer_id`/`rusher_id`/`receiver_id`
# aliases. Each gets cleanup, a FK, and a stub_source entry.
PBP_PLAYER_ID_COLS = [
    "passer_player_id", "passer_id",
    "rusher_player_id", "rusher_id",
    "receiver_player_id", "receiver_id",
    "sack_player_id",
    "half_sack_1_player_id", "half_sack_2_player_id",
    "interception_player_id", "lateral_interception_player_id",
    "td_player_id",
    "kicker_player_id",
    "punter_player_id",
    "kickoff_returner_player_id", "lateral_kickoff_returner_player_id",
    "punt_returner_player_id", "lateral_punt_returner_player_id",
    "lateral_receiver_player_id", "lateral_rusher_player_id", "lateral_sack_player_id",
    "own_kickoff_recovery_player_id",
    "blocked_player_id",
    "tackle_for_loss_1_player_id", "tackle_for_loss_2_player_id",
    "qb_hit_1_player_id", "qb_hit_2_player_id",
    "forced_fumble_player_1_player_id", "forced_fumble_player_2_player_id",
    "solo_tackle_1_player_id", "solo_tackle_2_player_id",
    "assist_tackle_1_player_id", "assist_tackle_2_player_id",
    "assist_tackle_3_player_id", "assist_tackle_4_player_id",
    "tackle_with_assist_1_player_id", "tackle_with_assist_2_player_id",
    "pass_defense_1_player_id", "pass_defense_2_player_id",
    "fumbled_1_player_id", "fumbled_2_player_id",
    "fumble_recovery_1_player_id", "fumble_recovery_2_player_id",
    "penalty_player_id",
    "safety_player_id",
]

# Map each FK-bearing PBP column to the companion `*_player_name` column used
# for stubbing (when a rarely-seen id isn't in `players` yet, stub from the name
# in the same row).
def _pbp_stub_source():
    out = {}
    for col in PBP_PLAYER_ID_COLS:
        # Strip trailing _id / _player_id to get a stable base, then append _player_name
        if col.endswith("_player_id"):
            base = col[: -len("_player_id")]
            name_col = f"{base}_player_name"
        elif col.endswith("_id"):
            base = col[: -len("_id")]
            name_col = f"{base}_player_name"
        else:
            name_col = None
        if name_col:
            out[col] = {"display_name": name_col}
    return out


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


def _raw_player_ids():
    """Read and clean the dynastyprocess db_playerids.csv bridge table."""
    df = pd.read_csv(RAW_DATA_PATH / "external" / "db_playerids.csv")
    if "espn_id" in df.columns:
        df["espn_id"] = to_string_id(df["espn_id"])
    for col, fn in (("gsis_id", clean_gsis_id_series),
                    ("pfr_id", clean_id_series),
                    ("espn_id", clean_id_series)):
        if col in df.columns:
            df[col] = fn(df[col])
    df = df.dropna(subset=["gsis_id"])
    df = df.drop_duplicates(subset=["gsis_id"], keep="first")
    return df


def _fetch_players(_years=None):
    """Build the players DataFrame with full pandas-level enrichment from the
    player_ids bridge, so DuckDB never has to UPDATE the players table after
    child FKs exist (which DuckDB can't do cleanly).

    Output covers:
    - every row in nflverse's players.parquet (including historical pre-GSIS
      records with non-standard IDs like 'YOU597411' / 'VIT276861' — these
      are real players that just predate the GSIS ID system, so we accept
      any non-junk string, not just the GSIS regex).
    - plus a stub row for every bridge gsis_id missing from the primary source
    - with player_pfr_id / player_espn_id backfilled from the bridge where the
      primary source is NULL and the bridge value isn't already taken by a
      different player (avoids UNIQUE conflicts)
    """
    df = pd.read_parquet(RAW_DATA_PATH / "players" / "players.parquet")
    df = df.rename(columns={
        "gsis_id":  "player_gsis_id",
        "pfr_id":   "player_pfr_id",
        "espn_id":  "player_espn_id",
    })
    # Loose cleanup (junk sentinels only) rather than strict GSIS regex — we
    # want to preserve pre-GSIS-era player records. Child tables use the strict
    # regex because they only ever carry modern-era IDs; any mismatch there is
    # a real data error.
    df["player_gsis_id"] = clean_id_series(df["player_gsis_id"])
    df["player_pfr_id"]  = clean_id_series(df["player_pfr_id"])
    df["player_espn_id"] = clean_id_series(df["player_espn_id"])
    df = df.dropna(subset=["player_gsis_id"]).drop_duplicates("player_gsis_id", keep="first")

    pi = _raw_player_ids()

    # 1. Stub rows for bridge gsis_ids missing from the primary players source.
    known = set(df["player_gsis_id"])
    unknown = pi[~pi["gsis_id"].isin(known)].copy()
    if not unknown.empty:
        name_parts = unknown["name"].fillna("").str.split(n=1, expand=True)
        stubs = pd.DataFrame({
            "player_gsis_id": unknown["gsis_id"].values,
            "player_pfr_id":  unknown["pfr_id"].values  if "pfr_id" in unknown else None,
            "player_espn_id": unknown["espn_id"].values if "espn_id" in unknown else None,
            "display_name":   unknown["name"].values    if "name" in unknown else None,
            "first_name":     (name_parts[0] if 0 in name_parts.columns else pd.Series([None]*len(unknown))).values,
            "last_name":      (name_parts[1] if 1 in name_parts.columns else pd.Series([None]*len(unknown))).values,
            "position":       unknown["position"].values if "position" in unknown else None,
            "latest_team":    unknown["team"].values     if "team" in unknown else None,
        })
        df = pd.concat([df, stubs], ignore_index=True)

    # 2. Backfill NULL player_pfr_id / player_espn_id from the bridge, skipping
    # any bridge value that would collide with a different player already using
    # that ID (DuckDB UNIQUE would reject those; we want honest gaps instead).
    pi_by_gsis = pi.set_index("gsis_id")

    def _backfill(col_in_df, col_in_pi):
        taken = set(df[col_in_df].dropna())
        null_mask = df[col_in_df].isna()
        for idx in df.index[null_mask]:
            gsis = df.at[idx, "player_gsis_id"]
            if gsis in pi_by_gsis.index:
                candidate = pi_by_gsis.at[gsis, col_in_pi]
                if pd.notna(candidate) and candidate not in taken:
                    df.at[idx, col_in_df] = candidate
                    taken.add(candidate)

    if "pfr_id" in pi.columns:
        _backfill("player_pfr_id", "pfr_id")
    if "espn_id" in pi.columns:
        _backfill("player_espn_id", "espn_id")

    return df


def _fetch_player_ids(_years=None):
    # Bridge table; enrichment happens in _fetch_players so this stays minimal.
    return _raw_player_ids()


def _fetch_games(years):
    df = pd.read_parquet(RAW_DATA_PATH / "schedules" / "games.parquet")
    if years:
        df = df[df["season"].isin(years)]
    if "game_id" in df.columns:
        df["game_id"] = clean_id_series(df["game_id"])
    # Force TEXT for time/date columns that can be all-NULL in early years —
    # DuckDB otherwise infers INT on the first partition and chokes later.
    for col in ["gametime", "gameday", "weekday", "time_of_day", "start_time"]:
        if col in df.columns:
            df[col] = df[col].astype("string")
    return df


def _fetch_combine(_years=None):
    df = pd.read_parquet(RAW_DATA_PATH / "combine" / "combine.parquet")
    df = df.rename(columns={"pfr_id": "player_pfr_id"})
    df["player_pfr_id"] = clean_id_series(df["player_pfr_id"])
    return df


def _fetch_draft_picks(_years=None):
    df = pd.read_parquet(RAW_DATA_PATH / "draft_picks" / "draft_picks.parquet")
    df = df.rename(columns={
        "gsis_id":       "player_gsis_id",
        "pfr_player_id": "player_pfr_id",
    })
    df["player_gsis_id"] = clean_gsis_id_series(df["player_gsis_id"])
    df["player_pfr_id"]  = clean_id_series(df["player_pfr_id"])
    return df


def _fetch_snap_counts(years):
    df = _read_parquets("snap_counts", "snap_counts_{year}.parquet", years)
    df = df.rename(columns={"pfr_player_id": "player_pfr_id"})
    if "player_pfr_id" in df.columns:
        df["player_pfr_id"] = clean_id_series(df["player_pfr_id"])
    return df


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
    combined = pd.concat(all_data, ignore_index=True)
    combined = combined.rename(columns={"pfr_id": "player_pfr_id"})
    combined["player_pfr_id"] = clean_id_series(combined["player_pfr_id"])
    return combined


def _fetch_depth_charts(years):
    df = _read_parquets("depth_charts", "depth_charts_{year}.parquet", years)
    df = df.rename(columns={"gsis_id": "player_gsis_id"})
    if "player_gsis_id" in df.columns:
        df["player_gsis_id"] = clean_gsis_id_series(df["player_gsis_id"])
    return df


def _fetch_depth_charts_2025(_years=None):
    df = pd.read_parquet(RAW_DATA_PATH / "depth_charts" / "depth_charts_2025.parquet")
    df = df.rename(columns={"gsis_id": "player_gsis_id", "espn_id": "player_espn_id"})
    df["player_gsis_id"] = clean_gsis_id_series(df["player_gsis_id"])
    df["player_espn_id"] = clean_id_series(df["player_espn_id"])
    return df


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
    combined = pd.concat(all_data, ignore_index=True)
    if "player_gsis_id" in combined.columns:
        combined["player_gsis_id"] = clean_gsis_id_series(combined["player_gsis_id"])
    return combined


def _fetch_qbr(_years=None):
    df = pd.read_csv(RAW_DATA_PATH / "external" / "qbr-nfl-weekly.csv")
    if "player_id" in df.columns:
        df["player_id"] = to_string_id(df["player_id"])
    df = df.rename(columns={"player_id": "player_espn_id"})
    if "player_espn_id" in df.columns:
        df["player_espn_id"] = clean_id_series(df["player_espn_id"])
    return df


def _fetch_game_stats(years):
    df = _read_parquets("stats_player", "stats_player_week_{year}.parquet", years)
    df = df.rename(columns={"player_id": "player_gsis_id"})
    if "player_gsis_id" in df.columns:
        df["player_gsis_id"] = clean_gsis_id_series(df["player_gsis_id"])
    # Always include a game_id column as VARCHAR — 1999-2021 parquets don't
    # carry it, but we need the column present so the FK to `games` can be
    # declared at table-creation time (and populated from 2022 onward).
    if "game_id" not in df.columns:
        df["game_id"] = pd.Series([None] * len(df), dtype="string")
    df["game_id"] = clean_id_series(df["game_id"])
    return df


def _fetch_season_stats(years):
    df = _read_parquets("stats_player", "stats_player_reg_{year}.parquet", years)
    df = df.rename(columns={"player_id": "player_gsis_id"})
    if "player_gsis_id" in df.columns:
        df["player_gsis_id"] = clean_gsis_id_series(df["player_gsis_id"])
    return df


def _fetch_pbp(years):
    df = _read_parquets("pbp", "play_by_play_{year}.parquet", years)
    if df.empty:
        return df
    # Clean every GSIS-bearing role column. game_id is VARCHAR already; just
    # scrub its occasional junk sentinels.
    for col in PBP_PLAYER_ID_COLS:
        if col in df.columns:
            df[col] = clean_gsis_id_series(df[col])
    if "game_id" in df.columns:
        df["game_id"] = clean_id_series(df["game_id"])
    # Force TEXT for datetime/time columns that can be all-NULL in early years
    # (DuckDB infers INT from NULL-only columns and chokes later).
    for col in ["start_time", "time_of_day", "weather", "stadium"]:
        if col in df.columns:
            df[col] = df[col].astype("string")
    return df


# Ordered so FK parents load first: players → player_ids → games → children.
# `run()` iterates this dict's insertion order.
TABLE_CONFIGS = {
    "players": TableConfig(
        "players",
        update_mode="upsert",
        fetch_fn=_fetch_players,
        dedup_cols=["player_gsis_id"],
        drop_na_col="player_gsis_id",
        primary_key="player_gsis_id",
        unique_cols=["player_pfr_id", "player_espn_id"],
    ),
    "player_ids": TableConfig(
        "player_ids",
        update_mode="full_replace",
        fetch_fn=_fetch_player_ids,
        dedup_cols=["gsis_id"],
        drop_na_col="gsis_id",
        # FK works because _fetch_players already enriched the players table
        # at the pandas level to include every bridge gsis_id. No SQL UPDATEs
        # on players happen after this, so DuckDB's UPDATE-as-DELETE-INSERT
        # FK restriction doesn't apply.
        foreign_keys=[("gsis_id", "players", "player_gsis_id")],
    ),
    "games": TableConfig(
        "games",
        update_mode="bulk_parquet",
        fetch_fn=_fetch_games,  # retained for ad-hoc callers; pipeline uses parquet_glob
        parquet_glob=str(RAW_DATA_PATH / "schedules" / "games.parquet"),
        id_cols=["game_id"],
        primary_key="game_id",
    ),
    "combine": TableConfig(
        "combine",
        update_mode="full_replace",
        fetch_fn=_fetch_combine,
        foreign_keys=[("player_pfr_id", "players", "player_pfr_id")],
        stub_source={
            "player_pfr_id": {
                "display_name": "player_name",
                "position": "pos",
                "college_name": "school",
            }
        },
    ),
    "draft_picks": TableConfig(
        "draft_picks",
        update_mode="full_replace",
        fetch_fn=_fetch_draft_picks,
        foreign_keys=[
            ("player_gsis_id", "players", "player_gsis_id"),
            ("player_pfr_id",  "players", "player_pfr_id"),
        ],
        stub_source={
            "player_gsis_id": {
                "display_name": "pfr_player_name",
                "position": "position",
            },
            "player_pfr_id": {
                "display_name": "pfr_player_name",
                "position": "position",
            },
        },
    ),
    "snap_counts": TableConfig(
        "snap_counts",
        update_mode="year_partition",
        fetch_fn=_fetch_snap_counts,
        foreign_keys=[("player_pfr_id", "players", "player_pfr_id")],
        stub_source={
            "player_pfr_id": {
                "display_name": "player",
                "position": "position",
                "latest_team": "team",
            }
        },
    ),
    "pfr_advanced": TableConfig(
        "pfr_advanced",
        update_mode="full_replace",
        fetch_fn=_fetch_pfr_advanced,
        foreign_keys=[("player_pfr_id", "players", "player_pfr_id")],
        stub_source={
            "player_pfr_id": {
                "display_name": "player",
                "position": "pos",
                "latest_team": "team",
            }
        },
    ),
    "depth_charts": TableConfig(
        "depth_charts",
        update_mode="year_partition",
        fetch_fn=_fetch_depth_charts,
        foreign_keys=[("player_gsis_id", "players", "player_gsis_id")],
        stub_source={
            "player_gsis_id": {
                "display_name": "full_name",
                "first_name": "first_name",
                "last_name": "last_name",
                "position": "position",
            }
        },
    ),
    "depth_charts_2025": TableConfig(
        "depth_charts_2025",
        update_mode="full_replace",
        fetch_fn=_fetch_depth_charts_2025,
        foreign_keys=[
            ("player_gsis_id", "players", "player_gsis_id"),
            ("player_espn_id", "players", "player_espn_id"),
        ],
        stub_source={
            "player_gsis_id": {
                "display_name": "player_name",
                "latest_team": "team",
                "position": "pos_abb",
            },
            "player_espn_id": {
                "display_name": "player_name",
                "latest_team": "team",
                "position": "pos_abb",
            },
        },
    ),
    "ngs_stats": TableConfig(
        "ngs_stats",
        update_mode="full_replace",
        fetch_fn=_fetch_ngs_stats,
        foreign_keys=[("player_gsis_id", "players", "player_gsis_id")],
        stub_source={
            "player_gsis_id": {
                "display_name": "player_display_name",
                "first_name": "player_first_name",
                "last_name": "player_last_name",
                "position": "player_position",
            }
        },
    ),
    "qbr": TableConfig(
        "qbr",
        update_mode="full_replace",
        fetch_fn=_fetch_qbr,
        foreign_keys=[("player_espn_id", "players", "player_espn_id")],
        stub_source={
            "player_espn_id": {
                "display_name": "name_short",
                "latest_team": "team_abb",
            }
        },
    ),
    "game_stats": TableConfig(
        "game_stats",
        update_mode="year_partition",
        fetch_fn=_fetch_game_stats,
        dedup_cols=["player_gsis_id", "season", "week"],
        # Deliberately no drop_na_col: nflverse's 1999-2000 data contains a
        # handful of rows with junk gsis_id ('0' / 'XX-*') that carry real
        # defensive tackles. Cleanup nulls the bogus IDs; the stats stay.
        # NULL gsis is permitted by the FK, so those rows don't join to
        # `players` but still contribute to aggregate queries correctly.
        foreign_keys=[
            ("player_gsis_id", "players", "player_gsis_id"),
            ("game_id",        "games",   "game_id"),
        ],
        stub_source={
            "player_gsis_id": {
                "display_name": "player_display_name",
                "position": "position",
            }
        },
    ),
    "season_stats": TableConfig(
        "season_stats",
        update_mode="year_partition",
        fetch_fn=_fetch_season_stats,
        # Same as game_stats: keep NULL-gsis rows so S.Fernando's 2000
        # 1-tackle + 1-assist season (which nflverse shipped with junk gsis
        # 'XX-0000001') survives aggregate queries. FK allows NULL.
        foreign_keys=[("player_gsis_id", "players", "player_gsis_id")],
        stub_source={
            "player_gsis_id": {
                "display_name": "player_display_name",
                "position": "position",
            }
        },
    ),
    "play_by_play": TableConfig(
        "play_by_play",
        update_mode="bulk_parquet",
        fetch_fn=_fetch_pbp,  # kept for --years incremental fallback
        parquet_glob=str(RAW_DATA_PATH / "pbp" / "play_by_play_*.parquet"),
        gsis_id_cols=PBP_PLAYER_ID_COLS,
        id_cols=["game_id"],
        force_varchar_cols=[
            "start_time", "time_of_day", "weather", "stadium", "nfl_api_id",
            "game_stadium", "stadium_id", "end_clock_time", "end_yard_line",
        ],
        foreign_keys=(
            [("game_id", "games", "game_id")]
            + [(col, "players", "player_gsis_id") for col in PBP_PLAYER_ID_COLS]
        ),
        stub_source=_pbp_stub_source(),
    ),
}


def main():
    parser = build_arg_parser("Build nflverse databases from local files in data/raw/")
    args = parser.parse_args()
    run(TABLE_CONFIGS, args, title=f"nflverse DB Build (source: {RAW_DATA_PATH})")


if __name__ == "__main__":
    main()
