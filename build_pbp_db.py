#!/usr/bin/env python3
"""
Build a separate play-by-play database from nflverse data.
This is kept separate from nflverse_custom.db due to its large size (~2-3 GB).

The PBP data uses GSIS IDs for player references, which can be joined
to nflverse_custom.db.players via gsis_id.
"""

import sqlite3
import nfl_data_py as nfl
import pandas as pd
from pathlib import Path
import argparse

DB_PATH = Path(__file__).parent / "pbp.db"

# Key columns to keep (372 total is too many)
# Focus on fantasy-relevant and commonly-used columns
KEEP_COLUMNS = [
    # Game/play identification
    'game_id', 'play_id', 'old_game_id', 'season', 'week', 'season_type',
    'game_date', 'game_half', 'quarter_seconds_remaining', 'half_seconds_remaining',
    'game_seconds_remaining', 'qtr', 'down', 'ydstogo', 'yardline_100',

    # Teams and score
    'posteam', 'defteam', 'posteam_score', 'defteam_score', 'score_differential',
    'home_team', 'away_team',

    # Play description and type
    'desc', 'play_type', 'yards_gained', 'air_yards', 'yards_after_catch',
    'first_down', 'rush', 'pass', 'sack', 'touchdown', 'interception',
    'fumble', 'fumble_lost', 'complete_pass', 'incomplete_pass',

    # Player IDs (GSIS format)
    'passer_player_id', 'passer_player_name',
    'rusher_player_id', 'rusher_player_name',
    'receiver_player_id', 'receiver_player_name',
    'fantasy_player_id', 'fantasy_player_name',
    'kicker_player_id', 'kicker_player_name',
    'punter_player_id', 'punter_player_name',
    'interception_player_id', 'interception_player_name',
    'fumbled_1_player_id', 'fumbled_1_player_name',
    'solo_tackle_1_player_id', 'solo_tackle_1_player_name',
    'sack_player_id', 'sack_player_name',

    # Fantasy points
    'fantasy',

    # Situational
    'shotgun', 'no_huddle', 'qb_dropback', 'qb_scramble', 'qb_spike',
    'pass_location', 'run_location', 'run_gap',

    # Field goal/PAT
    'field_goal_attempt', 'field_goal_result', 'kick_distance',
    'extra_point_attempt', 'extra_point_result', 'two_point_attempt', 'two_point_conv_result',

    # Penalties
    'penalty', 'penalty_yards', 'penalty_team',

    # Advanced/EPA
    'epa', 'wp', 'wpa', 'cp', 'cpoe',

    # Red zone (yardline_100 already in play description columns)

    # Drive info
    'drive', 'fixed_drive', 'drive_play_count',
]

def create_pbp_database(years, batch_size=3):
    """Create PBP database, loading data in batches to manage memory."""
    print(f"Creating PBP database: {DB_PATH}")

    conn = sqlite3.connect(DB_PATH)

    # Process in batches
    for i in range(0, len(years), batch_size):
        batch_years = years[i:i + batch_size]
        print(f"\nLoading years {batch_years}...")

        try:
            # Load PBP for batch
            pbp = nfl.import_pbp_data(years=batch_years)
            print(f"  Raw rows: {len(pbp):,}")

            # Keep only columns that exist (deduplicate)
            cols_to_keep = list(dict.fromkeys(c for c in KEEP_COLUMNS if c in pbp.columns))
            pbp = pbp[cols_to_keep]
            print(f"  Kept {len(cols_to_keep)} columns")

            # Write to database
            if_exists = 'replace' if i == 0 else 'append'
            pbp.to_sql('play_by_play', conn, if_exists=if_exists, index=False)
            print(f"  Written to DB ({if_exists})")

            # Free memory
            del pbp

        except Exception as e:
            print(f"  Error: {e}")
            continue

    # Create indexes
    print("\nCreating indexes...")
    cur = conn.cursor()

    indexes = [
        "CREATE INDEX IF NOT EXISTS idx_pbp_game ON play_by_play(game_id)",
        "CREATE INDEX IF NOT EXISTS idx_pbp_season_week ON play_by_play(season, week)",
        "CREATE INDEX IF NOT EXISTS idx_pbp_passer ON play_by_play(passer_player_id)",
        "CREATE INDEX IF NOT EXISTS idx_pbp_rusher ON play_by_play(rusher_player_id)",
        "CREATE INDEX IF NOT EXISTS idx_pbp_receiver ON play_by_play(receiver_player_id)",
        "CREATE INDEX IF NOT EXISTS idx_pbp_fantasy ON play_by_play(fantasy_player_id)",
        "CREATE INDEX IF NOT EXISTS idx_pbp_play_type ON play_by_play(play_type)",
    ]

    for idx in indexes:
        print(f"  {idx.split('idx_pbp_')[1].split(' ')[0]}...")
        cur.execute(idx)

    conn.commit()

    # Summary
    cur.execute("SELECT COUNT(*) FROM play_by_play")
    total_rows = cur.fetchone()[0]

    cur.execute("SELECT MIN(season), MAX(season) FROM play_by_play")
    min_year, max_year = cur.fetchone()

    print(f"\n=== Summary ===")
    print(f"  Total plays: {total_rows:,}")
    print(f"  Seasons: {min_year} - {max_year}")

    conn.close()

    # File size
    import os
    size_mb = os.path.getsize(DB_PATH) / (1024 * 1024)
    print(f"  Database size: {size_mb:.1f} MB")

def main():
    parser = argparse.ArgumentParser(description='Build play-by-play database')
    parser.add_argument('--start-year', type=int, default=1999, help='Start year (default: 1999)')
    parser.add_argument('--end-year', type=int, default=2024, help='End year (default: 2024)')
    args = parser.parse_args()

    years = list(range(args.start_year, args.end_year + 1))
    print(f"Building PBP database for years {args.start_year}-{args.end_year}")

    create_pbp_database(years)

if __name__ == "__main__":
    main()
