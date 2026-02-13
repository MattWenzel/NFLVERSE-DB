#!/usr/bin/env python3
"""
Build a complete NFL stats database from nflverse data.

Downloads ALL available data from nflverse and creates a normalized SQLite database.
No filtering by position - takes everything they have.

Usage:
    python3 build_nflverse_db.py                    # Build full DB (1999-2024)
    python3 build_nflverse_db.py --start-year 2020  # Build from 2020 onwards
    python3 build_nflverse_db.py --output my.db     # Custom output path

Data source: nflverse via nfl-data-py package (free, no API key)
"""

import argparse
import sqlite3
from datetime import datetime

import nfl_data_py as nfl
import pandas as pd

DEFAULT_DB_PATH = "/home/mattw813/Documents/fantasyDBv3/nflverse.db"
DEFAULT_START_YEAR = 1999
DEFAULT_END_YEAR = 2024

SCHEMA = """
-- ===========================================
-- PLAYERS: All NFL players with bio/career info
-- ===========================================
CREATE TABLE IF NOT EXISTS players (
    gsis_id TEXT PRIMARY KEY,
    display_name TEXT NOT NULL,
    first_name TEXT,
    last_name TEXT,
    position TEXT,
    position_group TEXT,
    current_team TEXT,
    jersey_number INTEGER,
    height REAL,
    weight REAL,
    birth_date TEXT,
    college TEXT,
    college_conference TEXT,
    rookie_season INTEGER,
    last_season INTEGER,
    years_of_experience INTEGER,
    status TEXT,
    headshot_url TEXT,

    -- External IDs for cross-referencing
    pfr_id TEXT,
    espn_id INTEGER,
    pff_id INTEGER,

    -- Draft info
    draft_year INTEGER,
    draft_round INTEGER,
    draft_pick INTEGER,
    draft_team TEXT
);

-- ===========================================
-- PLAYER_IDS: Cross-reference to other platforms
-- ===========================================
CREATE TABLE IF NOT EXISTS player_ids (
    gsis_id TEXT PRIMARY KEY,
    name TEXT,
    position TEXT,
    team TEXT,
    espn_id INTEGER,
    yahoo_id INTEGER,
    fantasypros_id INTEGER,
    sleeper_id TEXT,
    pfr_id TEXT,
    pff_id INTEGER,
    cbs_id INTEGER,
    rotowire_id INTEGER,
    rotoworld_id INTEGER,
    fantasy_data_id INTEGER,
    sportradar_id TEXT,
    mfl_id TEXT,
    fleaflicker_id INTEGER,
    stats_id INTEGER,
    stats_global_id INTEGER,
    cfbref_id TEXT,
    nfl_id INTEGER
);

-- ===========================================
-- GAMES: Schedule with scores, weather, betting
-- ===========================================
CREATE TABLE IF NOT EXISTS games (
    game_id TEXT PRIMARY KEY,
    season INTEGER NOT NULL,
    game_type TEXT,
    week INTEGER NOT NULL,
    gameday TEXT,
    weekday TEXT,
    gametime TEXT,

    away_team TEXT,
    home_team TEXT,
    away_score INTEGER,
    home_score INTEGER,

    location TEXT,
    result TEXT,
    total INTEGER,
    overtime INTEGER,

    -- Betting
    spread_line REAL,
    total_line REAL,
    away_moneyline INTEGER,
    home_moneyline INTEGER,

    -- Rest days
    away_rest INTEGER,
    home_rest INTEGER,

    -- Venue
    stadium TEXT,
    stadium_id TEXT,
    roof TEXT,
    surface TEXT,

    -- Weather
    temp INTEGER,
    wind INTEGER,

    -- Personnel
    away_coach TEXT,
    home_coach TEXT,
    referee TEXT
);

-- ===========================================
-- GAME_STATS: Weekly player stats (ALL positions, ALL stat types)
-- This is the main stats table - unified, not split by position
-- ===========================================
CREATE TABLE IF NOT EXISTS game_stats (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    gsis_id TEXT NOT NULL,
    season INTEGER NOT NULL,
    week INTEGER NOT NULL,
    season_type TEXT DEFAULT 'REG',
    team TEXT,
    opponent TEXT,

    -- PASSING (any position can have these - trick plays, etc.)
    completions INTEGER DEFAULT 0,
    pass_attempts INTEGER DEFAULT 0,
    passing_yards REAL DEFAULT 0,
    passing_tds INTEGER DEFAULT 0,
    interceptions INTEGER DEFAULT 0,
    sacks INTEGER DEFAULT 0,
    sack_yards REAL DEFAULT 0,
    sack_fumbles INTEGER DEFAULT 0,
    sack_fumbles_lost INTEGER DEFAULT 0,
    passing_air_yards REAL,
    passing_yards_after_catch REAL,
    passing_first_downs REAL,
    passing_epa REAL,
    passing_2pt_conversions INTEGER DEFAULT 0,

    -- RUSHING
    carries INTEGER DEFAULT 0,
    rushing_yards REAL DEFAULT 0,
    rushing_tds INTEGER DEFAULT 0,
    rushing_fumbles INTEGER DEFAULT 0,
    rushing_fumbles_lost INTEGER DEFAULT 0,
    rushing_first_downs REAL,
    rushing_epa REAL,
    rushing_2pt_conversions INTEGER DEFAULT 0,

    -- RECEIVING
    targets INTEGER DEFAULT 0,
    receptions INTEGER DEFAULT 0,
    receiving_yards REAL DEFAULT 0,
    receiving_tds INTEGER DEFAULT 0,
    receiving_fumbles INTEGER DEFAULT 0,
    receiving_fumbles_lost INTEGER DEFAULT 0,
    receiving_air_yards REAL,
    receiving_yards_after_catch REAL,
    receiving_first_downs REAL,
    receiving_epa REAL,
    receiving_2pt_conversions INTEGER DEFAULT 0,

    -- ADVANCED METRICS
    target_share REAL,
    air_yards_share REAL,
    wopr REAL,
    racr REAL,
    pacr REAL,
    dakota REAL,

    -- SPECIAL TEAMS
    special_teams_tds INTEGER DEFAULT 0,

    -- FANTASY POINTS (pre-calculated by nflverse)
    fantasy_points REAL,
    fantasy_points_ppr REAL,

    UNIQUE(gsis_id, season, week),
    FOREIGN KEY (gsis_id) REFERENCES players(gsis_id)
);

-- ===========================================
-- SEASON_STATS: Aggregated season totals
-- ===========================================
CREATE TABLE IF NOT EXISTS season_stats (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    gsis_id TEXT NOT NULL,
    season INTEGER NOT NULL,
    season_type TEXT DEFAULT 'REG',
    team TEXT,
    games INTEGER,

    -- PASSING
    completions INTEGER DEFAULT 0,
    pass_attempts INTEGER DEFAULT 0,
    passing_yards REAL DEFAULT 0,
    passing_tds INTEGER DEFAULT 0,
    interceptions INTEGER DEFAULT 0,
    sacks INTEGER DEFAULT 0,
    sack_yards REAL DEFAULT 0,
    sack_fumbles INTEGER DEFAULT 0,
    sack_fumbles_lost INTEGER DEFAULT 0,
    passing_air_yards REAL,
    passing_yards_after_catch REAL,
    passing_first_downs REAL,
    passing_epa REAL,
    passing_2pt_conversions INTEGER DEFAULT 0,

    -- RUSHING
    carries INTEGER DEFAULT 0,
    rushing_yards REAL DEFAULT 0,
    rushing_tds INTEGER DEFAULT 0,
    rushing_fumbles INTEGER DEFAULT 0,
    rushing_fumbles_lost INTEGER DEFAULT 0,
    rushing_first_downs REAL,
    rushing_epa REAL,
    rushing_2pt_conversions INTEGER DEFAULT 0,

    -- RECEIVING
    targets INTEGER DEFAULT 0,
    receptions INTEGER DEFAULT 0,
    receiving_yards REAL DEFAULT 0,
    receiving_tds INTEGER DEFAULT 0,
    receiving_fumbles INTEGER DEFAULT 0,
    receiving_fumbles_lost INTEGER DEFAULT 0,
    receiving_air_yards REAL,
    receiving_yards_after_catch REAL,
    receiving_first_downs REAL,
    receiving_epa REAL,
    receiving_2pt_conversions INTEGER DEFAULT 0,

    -- SPECIAL TEAMS
    special_teams_tds INTEGER DEFAULT 0,

    -- FANTASY
    fantasy_points REAL,
    fantasy_points_ppr REAL,

    UNIQUE(gsis_id, season, season_type),
    FOREIGN KEY (gsis_id) REFERENCES players(gsis_id)
);

-- ===========================================
-- DRAFT_PICKS: Historical draft data
-- ===========================================
CREATE TABLE IF NOT EXISTS draft_picks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    season INTEGER,
    round INTEGER,
    pick INTEGER,
    team TEXT,
    gsis_id TEXT,
    pfr_id TEXT,
    player_name TEXT,
    position TEXT,
    college TEXT,
    age REAL,
    UNIQUE(season, round, pick),
    FOREIGN KEY (gsis_id) REFERENCES players(gsis_id)
);

-- ===========================================
-- COMBINE: NFL Combine results
-- ===========================================
CREATE TABLE IF NOT EXISTS combine (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    season INTEGER,
    player_name TEXT,
    position TEXT,
    school TEXT,
    height REAL,
    weight REAL,
    forty REAL,
    bench INTEGER,
    vertical REAL,
    broad_jump INTEGER,
    cone REAL,
    shuttle REAL,
    pfr_id TEXT,
    cfb_id TEXT
);

-- ===========================================
-- INDEXES
-- ===========================================
CREATE INDEX IF NOT EXISTS idx_game_stats_player ON game_stats(gsis_id);
CREATE INDEX IF NOT EXISTS idx_game_stats_season ON game_stats(season);
CREATE INDEX IF NOT EXISTS idx_game_stats_week ON game_stats(season, week);
CREATE INDEX IF NOT EXISTS idx_game_stats_team ON game_stats(team);

CREATE INDEX IF NOT EXISTS idx_season_stats_player ON season_stats(gsis_id);
CREATE INDEX IF NOT EXISTS idx_season_stats_season ON season_stats(season);

CREATE INDEX IF NOT EXISTS idx_games_season ON games(season);
CREATE INDEX IF NOT EXISTS idx_games_week ON games(season, week);
CREATE INDEX IF NOT EXISTS idx_games_teams ON games(home_team, away_team);

CREATE INDEX IF NOT EXISTS idx_players_name ON players(display_name);
CREATE INDEX IF NOT EXISTS idx_players_position ON players(position);
CREATE INDEX IF NOT EXISTS idx_players_team ON players(current_team);

CREATE INDEX IF NOT EXISTS idx_draft_season ON draft_picks(season);
CREATE INDEX IF NOT EXISTS idx_draft_player ON draft_picks(gsis_id);

CREATE INDEX IF NOT EXISTS idx_player_ids_gsis_id ON player_ids(gsis_id);
CREATE INDEX IF NOT EXISTS idx_player_ids_pfr_id ON player_ids(pfr_id);
CREATE INDEX IF NOT EXISTS idx_player_ids_espn_id ON player_ids(espn_id);
"""


def create_schema(conn):
    """Create all tables and indexes."""
    conn.executescript(SCHEMA)
    conn.commit()
    print("Schema created.")


def import_players(conn):
    """Import all players."""
    print("\nImporting players...")
    df = nfl.import_players()

    # Map columns
    df = df.rename(columns={
        'latest_team': 'current_team',
        'headshot': 'headshot_url',
        'college_name': 'college',
    })

    cols = ['gsis_id', 'display_name', 'first_name', 'last_name', 'position',
            'position_group', 'current_team', 'jersey_number', 'height', 'weight',
            'birth_date', 'college', 'college_conference', 'rookie_season',
            'last_season', 'years_of_experience', 'status', 'headshot_url',
            'pfr_id', 'espn_id', 'pff_id', 'draft_year', 'draft_round',
            'draft_pick', 'draft_team']

    available = [c for c in cols if c in df.columns]
    df = df[available].drop_duplicates(subset=['gsis_id'])

    df.to_sql('players', conn, if_exists='replace', index=False)
    print(f"  {len(df):,} players")


def import_player_ids(conn):
    """Import player ID cross-reference."""
    print("\nImporting player IDs...")
    df = nfl.import_ids()

    cols = ['gsis_id', 'name', 'position', 'team', 'espn_id', 'yahoo_id',
            'fantasypros_id', 'sleeper_id', 'pfr_id', 'pff_id', 'cbs_id',
            'rotowire_id', 'rotoworld_id', 'fantasy_data_id', 'sportradar_id',
            'mfl_id', 'fleaflicker_id', 'stats_id', 'stats_global_id',
            'cfbref_id', 'nfl_id']

    available = [c for c in cols if c in df.columns]
    df = df[available].dropna(subset=['gsis_id']).drop_duplicates(subset=['gsis_id'])

    df.to_sql('player_ids', conn, if_exists='replace', index=False)
    print(f"  {len(df):,} player ID records")


def import_games(conn, start_year, end_year):
    """Import game schedules."""
    print(f"\nImporting games ({start_year}-{end_year})...")

    all_data = []
    for year in range(start_year, end_year + 1):
        try:
            df = nfl.import_schedules([year])
            all_data.append(df)
            print(f"  {year}: {len(df)} games")
        except Exception as e:
            print(f"  {year}: Error - {e}")

    if all_data:
        df = pd.concat(all_data, ignore_index=True)

        cols = ['game_id', 'season', 'game_type', 'week', 'gameday', 'weekday',
                'gametime', 'away_team', 'home_team', 'away_score', 'home_score',
                'location', 'result', 'total', 'overtime', 'spread_line',
                'total_line', 'away_moneyline', 'home_moneyline', 'away_rest',
                'home_rest', 'stadium', 'stadium_id', 'roof', 'surface', 'temp',
                'wind', 'away_coach', 'home_coach', 'referee']

        available = [c for c in cols if c in df.columns]
        df = df[available]

        df.to_sql('games', conn, if_exists='replace', index=False)
        print(f"  Total: {len(df):,} games")


def import_game_stats(conn, start_year, end_year):
    """Import weekly player stats."""
    print(f"\nImporting game stats ({start_year}-{end_year})...")

    total = 0
    for year in range(start_year, end_year + 1):
        try:
            df = nfl.import_weekly_data([year])

            # Rename columns to match schema
            df = df.rename(columns={
                'player_id': 'gsis_id',
                'recent_team': 'team',
                'opponent_team': 'opponent',
                'attempts': 'pass_attempts',
            })

            cols = ['gsis_id', 'season', 'week', 'season_type', 'team', 'opponent',
                    'completions', 'pass_attempts', 'passing_yards', 'passing_tds',
                    'interceptions', 'sacks', 'sack_yards', 'sack_fumbles',
                    'sack_fumbles_lost', 'passing_air_yards', 'passing_yards_after_catch',
                    'passing_first_downs', 'passing_epa', 'passing_2pt_conversions',
                    'carries', 'rushing_yards', 'rushing_tds', 'rushing_fumbles',
                    'rushing_fumbles_lost', 'rushing_first_downs', 'rushing_epa',
                    'rushing_2pt_conversions', 'targets', 'receptions', 'receiving_yards',
                    'receiving_tds', 'receiving_fumbles', 'receiving_fumbles_lost',
                    'receiving_air_yards', 'receiving_yards_after_catch',
                    'receiving_first_downs', 'receiving_epa', 'receiving_2pt_conversions',
                    'target_share', 'air_yards_share', 'wopr', 'racr', 'pacr', 'dakota',
                    'special_teams_tds', 'fantasy_points', 'fantasy_points_ppr']

            available = [c for c in cols if c in df.columns]
            df = df[available]

            # Drop duplicates (some years have duplicate player-week entries)
            df = df.drop_duplicates(subset=['gsis_id', 'season', 'week'], keep='first')

            df.to_sql('game_stats', conn, if_exists='append', index=False)
            total += len(df)
            print(f"  {year}: {len(df):,} rows")

        except Exception as e:
            print(f"  {year}: Error - {e}")

    print(f"  Total: {total:,} game stat rows")


def import_season_stats(conn, start_year, end_year):
    """Import seasonal aggregated stats."""
    print(f"\nImporting season stats ({start_year}-{end_year})...")

    total = 0
    for year in range(start_year, end_year + 1):
        try:
            df = nfl.import_seasonal_data([year])

            df = df.rename(columns={
                'player_id': 'gsis_id',
                'recent_team': 'team',
                'attempts': 'pass_attempts',
            })

            cols = ['gsis_id', 'season', 'season_type', 'team', 'games',
                    'completions', 'pass_attempts', 'passing_yards', 'passing_tds',
                    'interceptions', 'sacks', 'sack_yards', 'sack_fumbles',
                    'sack_fumbles_lost', 'passing_air_yards', 'passing_yards_after_catch',
                    'passing_first_downs', 'passing_epa', 'passing_2pt_conversions',
                    'carries', 'rushing_yards', 'rushing_tds', 'rushing_fumbles',
                    'rushing_fumbles_lost', 'rushing_first_downs', 'rushing_epa',
                    'rushing_2pt_conversions', 'targets', 'receptions', 'receiving_yards',
                    'receiving_tds', 'receiving_fumbles', 'receiving_fumbles_lost',
                    'receiving_air_yards', 'receiving_yards_after_catch',
                    'receiving_first_downs', 'receiving_epa', 'receiving_2pt_conversions',
                    'special_teams_tds', 'fantasy_points', 'fantasy_points_ppr']

            available = [c for c in cols if c in df.columns]
            df = df[available]

            df.to_sql('season_stats', conn, if_exists='append', index=False)
            total += len(df)
            print(f"  {year}: {len(df):,} rows")

        except Exception as e:
            print(f"  {year}: Error - {e}")

    print(f"  Total: {total:,} season stat rows")


def import_draft(conn):
    """Import draft picks."""
    print("\nImporting draft picks...")
    df = nfl.import_draft_picks()

    df = df.rename(columns={
        'pfr_player_id': 'pfr_id',
        'pfr_player_name': 'player_name',
    })

    cols = ['season', 'round', 'pick', 'team', 'gsis_id', 'pfr_id',
            'player_name', 'position', 'college', 'age']

    available = [c for c in cols if c in df.columns]
    df = df[available]

    df.to_sql('draft_picks', conn, if_exists='replace', index=False)
    print(f"  {len(df):,} draft picks")


def import_combine(conn):
    """Import combine results."""
    print("\nImporting combine data...")
    df = nfl.import_combine_data()

    df = df.rename(columns={
        'pos': 'position',
        'ht': 'height',
        'wt': 'weight',
    })

    cols = ['season', 'player_name', 'position', 'school', 'height', 'weight',
            'forty', 'bench', 'vertical', 'broad_jump', 'cone', 'shuttle',
            'pfr_id', 'cfb_id']

    available = [c for c in cols if c in df.columns]
    df = df[available]

    df.to_sql('combine', conn, if_exists='replace', index=False)
    print(f"  {len(df):,} combine results")


def print_summary(conn):
    """Print database summary."""
    print("\n" + "=" * 50)
    print("DATABASE SUMMARY")
    print("=" * 50)

    cur = conn.cursor()
    tables = ['players', 'player_ids', 'games', 'game_stats', 'season_stats',
              'draft_picks', 'combine']

    for table in tables:
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        count = cur.fetchone()[0]
        print(f"  {table}: {count:,} rows")


def main():
    parser = argparse.ArgumentParser(description="Build NFL database from nflverse")
    parser.add_argument("--output", default=DEFAULT_DB_PATH, help="Output database path")
    parser.add_argument("--start-year", type=int, default=DEFAULT_START_YEAR)
    parser.add_argument("--end-year", type=int, default=DEFAULT_END_YEAR)
    args = parser.parse_args()

    print(f"Building nflverse database")
    print(f"  Output: {args.output}")
    print(f"  Years: {args.start_year}-{args.end_year}")

    conn = sqlite3.connect(args.output)
    # Disable FKs during bulk import for performance, verify integrity after
    conn.execute("PRAGMA foreign_keys = OFF")
    start = datetime.now()

    create_schema(conn)
    import_players(conn)
    import_player_ids(conn)
    import_games(conn, args.start_year, args.end_year)
    import_game_stats(conn, args.start_year, args.end_year)
    import_season_stats(conn, args.start_year, args.end_year)
    import_draft(conn)
    import_combine(conn)

    # Verify referential integrity manually (FK pragma has issues with pandas)
    print("\nVerifying referential integrity...")
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
        print(f"  WARNING: {orphan_games} orphan game_stats, {orphan_seasons} orphan season_stats")
    else:
        print("  All references valid (0 orphan records)")

    print_summary(conn)
    conn.close()

    elapsed = datetime.now() - start
    print(f"\nCompleted in {elapsed}")
    print(f"Database: {args.output}")


if __name__ == "__main__":
    main()
