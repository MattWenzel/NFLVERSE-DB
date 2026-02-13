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
# Column lists — reused from the original build scripts
# ---------------------------------------------------------------------------

GAME_STATS_COLS = [
    "gsis_id", "season", "week", "season_type", "team", "opponent",
    "completions", "pass_attempts", "passing_yards", "passing_tds",
    "interceptions", "sacks", "sack_yards", "sack_fumbles",
    "sack_fumbles_lost", "passing_air_yards", "passing_yards_after_catch",
    "passing_first_downs", "passing_epa", "passing_2pt_conversions",
    "carries", "rushing_yards", "rushing_tds", "rushing_fumbles",
    "rushing_fumbles_lost", "rushing_first_downs", "rushing_epa",
    "rushing_2pt_conversions", "targets", "receptions", "receiving_yards",
    "receiving_tds", "receiving_fumbles", "receiving_fumbles_lost",
    "receiving_air_yards", "receiving_yards_after_catch",
    "receiving_first_downs", "receiving_epa", "receiving_2pt_conversions",
    "target_share", "air_yards_share", "wopr", "racr", "pacr", "dakota",
    "special_teams_tds", "fantasy_points", "fantasy_points_ppr",
]

SEASON_STATS_COLS = [
    "gsis_id", "season", "season_type", "team", "games",
    "completions", "pass_attempts", "passing_yards", "passing_tds",
    "interceptions", "sacks", "sack_yards", "sack_fumbles",
    "sack_fumbles_lost", "passing_air_yards", "passing_yards_after_catch",
    "passing_first_downs", "passing_epa", "passing_2pt_conversions",
    "carries", "rushing_yards", "rushing_tds", "rushing_fumbles",
    "rushing_fumbles_lost", "rushing_first_downs", "rushing_epa",
    "rushing_2pt_conversions", "targets", "receptions", "receiving_yards",
    "receiving_tds", "receiving_fumbles", "receiving_fumbles_lost",
    "receiving_air_yards", "receiving_yards_after_catch",
    "receiving_first_downs", "receiving_epa", "receiving_2pt_conversions",
    "special_teams_tds", "fantasy_points", "fantasy_points_ppr",
]

PLAYERS_COLS = [
    "gsis_id", "display_name", "first_name", "last_name", "position",
    "position_group", "current_team", "jersey_number", "height", "weight",
    "birth_date", "college", "college_conference", "rookie_season",
    "last_season", "years_of_experience", "status", "headshot_url",
    "pfr_id", "espn_id", "pff_id", "draft_year", "draft_round",
    "draft_pick", "draft_team",
]

PLAYER_IDS_COLS = [
    "gsis_id", "name", "position", "team", "espn_id", "yahoo_id",
    "fantasypros_id", "sleeper_id", "pfr_id", "pff_id", "cbs_id",
    "rotowire_id", "rotoworld_id", "fantasy_data_id", "sportradar_id",
    "mfl_id", "fleaflicker_id", "stats_id", "stats_global_id",
    "cfbref_id", "nfl_id",
]

GAMES_COLS = [
    "game_id", "season", "game_type", "week", "gameday", "weekday",
    "gametime", "away_team", "home_team", "away_score", "home_score",
    "location", "result", "total", "overtime", "spread_line",
    "total_line", "away_moneyline", "home_moneyline", "away_rest",
    "home_rest", "stadium", "stadium_id", "roof", "surface", "temp",
    "wind", "away_coach", "home_coach", "referee",
]

DRAFT_PICKS_COLS = [
    "season", "round", "pick", "team", "gsis_id", "pfr_id",
    "player_name", "position", "college", "age",
]

COMBINE_COLS = [
    "season", "player_name", "position", "school", "height", "weight",
    "forty", "bench", "vertical", "broad_jump", "cone", "shuttle",
    "pfr_id", "cfb_id",
]

# PBP columns — same as build_pbp_db.py KEEP_COLUMNS
PBP_COLS = [
    "game_id", "play_id", "old_game_id", "season", "week", "season_type",
    "game_date", "game_half", "quarter_seconds_remaining", "half_seconds_remaining",
    "game_seconds_remaining", "qtr", "down", "ydstogo", "yardline_100",
    "posteam", "defteam", "posteam_score", "defteam_score", "score_differential",
    "home_team", "away_team",
    "desc", "play_type", "yards_gained", "air_yards", "yards_after_catch",
    "first_down", "rush", "pass", "sack", "touchdown", "interception",
    "fumble", "fumble_lost", "complete_pass", "incomplete_pass",
    "passer_player_id", "passer_player_name",
    "rusher_player_id", "rusher_player_name",
    "receiver_player_id", "receiver_player_name",
    "fantasy_player_id", "fantasy_player_name",
    "kicker_player_id", "kicker_player_name",
    "punter_player_id", "punter_player_name",
    "interception_player_id", "interception_player_name",
    "fumbled_1_player_id", "fumbled_1_player_name",
    "solo_tackle_1_player_id", "solo_tackle_1_player_name",
    "sack_player_id", "sack_player_name",
    "fantasy",
    "shotgun", "no_huddle", "qb_dropback", "qb_scramble", "qb_spike",
    "pass_location", "run_location", "run_gap",
    "field_goal_attempt", "field_goal_result", "kick_distance",
    "extra_point_attempt", "extra_point_result", "two_point_attempt", "two_point_conv_result",
    "penalty", "penalty_yards", "penalty_team",
    "epa", "wp", "wpa", "cp", "cpoe",
    "drive", "fixed_drive", "drive_play_count",
]

# ---------------------------------------------------------------------------
# nflreadpy column renames (differences from nfl_data_py)
# ---------------------------------------------------------------------------

NFLREADPY_GAME_STATS_RENAMES = {
    "player_id": "gsis_id",
    "opponent_team": "opponent",
    "attempts": "pass_attempts",
    "passing_interceptions": "interceptions",
    "sacks_suffered": "sacks",
    "sack_yards_lost": "sack_yards",
}

NFLREADPY_SEASON_STATS_RENAMES = {
    "player_id": "gsis_id",
    "attempts": "pass_attempts",
    "passing_interceptions": "interceptions",
    "sacks_suffered": "sacks",
    "sack_yards_lost": "sack_yards",
}

NFLREADPY_PLAYERS_RENAMES = {
    "latest_team": "current_team",
    "headshot": "headshot_url",
    "college_name": "college",
}

NFLREADPY_DRAFT_RENAMES = {
    "pfr_player_id": "pfr_id",
    "pfr_player_name": "player_name",
}

NFLREADPY_COMBINE_RENAMES = {
    "pos": "position",
    "ht": "height",
    "wt": "weight",
}

NFLREADPY_SNAP_COUNTS_RENAMES = {
    "game": "game_id",
    "pfr_game": "pfr_game_id",
}


# ---------------------------------------------------------------------------
# Table configuration
# ---------------------------------------------------------------------------

def _polars_to_pandas(df):
    """Convert a Polars DataFrame to pandas if needed."""
    if hasattr(df, "to_pandas"):
        return df.to_pandas()
    return df


def _filter_cols(df, col_list):
    """Keep only columns present in both the DataFrame and our schema."""
    available = [c for c in col_list if c in df.columns]
    return df[available]


class TableConfig:
    """Configuration for how to fetch and update a single table."""

    def __init__(self, name, *, db="main", update_mode="year_partition",
                 fetch_fn=None, renames=None, columns=None, dedup_cols=None):
        self.name = name
        self.db = db  # "main" or "pbp"
        self.update_mode = update_mode  # "year_partition" or "full_replace"
        self.fetch_fn = fetch_fn
        self.renames = renames or {}
        self.columns = columns or []
        self.dedup_cols = dedup_cols  # subset columns for drop_duplicates

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
    all_data = []
    for stat_type in ["passing", "rushing", "receiving"]:
        df = _polars_to_pandas(nflreadpy.load_nextgen_stats(stat_type=stat_type))
        df["stat_type"] = stat_type
        all_data.append(df)
    return pd.concat(all_data, ignore_index=True)


def _fetch_pfr_advanced(_years=None):
    all_data = []
    for stat_type in ["pass", "rush", "rec"]:
        df = _polars_to_pandas(
            nflreadpy.load_pfr_advstats(stat_type=stat_type, summary_level="season")
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


# All table configs
TABLE_CONFIGS = {
    "game_stats": TableConfig(
        "game_stats",
        update_mode="year_partition",
        fetch_fn=_fetch_game_stats,
        renames=NFLREADPY_GAME_STATS_RENAMES,
        columns=GAME_STATS_COLS,
        dedup_cols=["gsis_id", "season", "week"],
    ),
    "season_stats": TableConfig(
        "season_stats",
        update_mode="year_partition",
        fetch_fn=_fetch_season_stats,
        renames=NFLREADPY_SEASON_STATS_RENAMES,
        columns=SEASON_STATS_COLS,
    ),
    "games": TableConfig(
        "games",
        update_mode="year_partition",
        fetch_fn=_fetch_games,
        renames={},
        columns=GAMES_COLS,
    ),
    "players": TableConfig(
        "players",
        update_mode="full_replace",
        fetch_fn=_fetch_players,
        renames=NFLREADPY_PLAYERS_RENAMES,
        columns=PLAYERS_COLS,
        dedup_cols=["gsis_id"],
    ),
    "player_ids": TableConfig(
        "player_ids",
        update_mode="full_replace",
        fetch_fn=_fetch_player_ids,
        renames={},
        columns=PLAYER_IDS_COLS,
        dedup_cols=["gsis_id"],
    ),
    "draft_picks": TableConfig(
        "draft_picks",
        update_mode="full_replace",
        fetch_fn=_fetch_draft_picks,
        renames=NFLREADPY_DRAFT_RENAMES,
        columns=DRAFT_PICKS_COLS,
    ),
    "combine": TableConfig(
        "combine",
        update_mode="full_replace",
        fetch_fn=_fetch_combine,
        renames=NFLREADPY_COMBINE_RENAMES,
        columns=COMBINE_COLS,
    ),
    "snap_counts": TableConfig(
        "snap_counts",
        update_mode="year_partition",
        fetch_fn=_fetch_snap_counts,
        renames=NFLREADPY_SNAP_COUNTS_RENAMES,
        columns=None,  # Keep all columns (schema managed by add_supplementary_tables)
    ),
    "depth_charts": TableConfig(
        "depth_charts",
        update_mode="year_partition",
        fetch_fn=_fetch_depth_charts,
        renames={},
        columns=None,
    ),
    "ngs_stats": TableConfig(
        "ngs_stats",
        update_mode="full_replace",
        fetch_fn=_fetch_ngs_stats,
        renames={},
        columns=None,
    ),
    "pfr_advanced": TableConfig(
        "pfr_advanced",
        update_mode="full_replace",
        fetch_fn=_fetch_pfr_advanced,
        renames={},
        columns=None,
    ),
    "qbr": TableConfig(
        "qbr",
        update_mode="full_replace",
        fetch_fn=_fetch_qbr,
        renames={},
        columns=None,
    ),
    "play_by_play": TableConfig(
        "play_by_play",
        db="pbp",
        update_mode="year_partition",
        fetch_fn=_fetch_pbp,
        renames={},
        columns=PBP_COLS,
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

        # Apply renames
        if config.renames:
            df = df.rename(columns=config.renames)

        # Filter to schema columns
        if config.columns:
            df = _filter_cols(df, config.columns)

        # Deduplicate
        if config.dedup_cols:
            dedup_available = [c for c in config.dedup_cols if c in df.columns]
            if dedup_available:
                df = df.drop_duplicates(subset=dedup_available, keep="first")

        # Transaction: delete old + insert new
        try:
            conn.execute("BEGIN")
            conn.execute(
                f"DELETE FROM [{config.name}] WHERE season = ?", (year,)
            )
            df.to_sql(config.name, conn, if_exists="append", index=False)
            conn.execute("COMMIT")
            print(f"{len(df):,} rows inserted")
        except Exception as e:
            conn.execute("ROLLBACK")
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

    # Apply renames
    if config.renames:
        df = df.rename(columns=config.renames)

    # Filter to schema columns
    if config.columns:
        df = _filter_cols(df, config.columns)

    # Deduplicate
    if config.dedup_cols:
        dedup_available = [c for c in config.dedup_cols if c in df.columns]
        if dedup_available:
            if "gsis_id" in dedup_available:
                df = df.dropna(subset=["gsis_id"])
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
        year_clause = f"AND s.season IN ({placeholders})"
        params = tuple(years)

    try:
        conn.execute(f"""
            UPDATE season_stats
            SET team = (
                SELECT g.team
                FROM game_stats g
                WHERE g.gsis_id = season_stats.gsis_id
                  AND g.season = season_stats.season
                GROUP BY g.team
                ORDER BY COUNT(*) DESC
                LIMIT 1
            )
            WHERE EXISTS (
                SELECT 1 FROM game_stats g2
                WHERE g2.gsis_id = season_stats.gsis_id
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

    print("  Checking referential integrity...", end=" ", flush=True)
    cur = conn.cursor()

    cur.execute("""
        SELECT COUNT(*) FROM game_stats g
        WHERE NOT EXISTS (SELECT 1 FROM players p WHERE p.gsis_id = g.gsis_id)
    """)
    orphan_games = cur.fetchone()[0]

    cur.execute("""
        SELECT COUNT(*) FROM season_stats s
        WHERE NOT EXISTS (SELECT 1 FROM players p WHERE p.gsis_id = s.gsis_id)
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

    # Backup
    dbs_touched = set()
    for name in table_names:
        dbs_touched.add(TABLE_CONFIGS[name].get_db_path())

    if not args.no_backup and not args.dry_run:
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
            conn = connections[config.get_db_path()]

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
                        "depth_charts": range(2001, datetime.now().year + 1),
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
        main_conn = connections.get(DB_PATH)
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
