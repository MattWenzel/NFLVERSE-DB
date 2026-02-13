#!/usr/bin/env python3
"""
Add supplementary nflverse tables to nflverse_custom.db:
- snap_counts (2015-2024)
- ngs_stats (2016-2024) - passing, rushing, receiving combined
- depth_charts (2001-2024)
- pfr_advanced (2018-2024) - passing, rushing, receiving combined
- qbr (2006-2024)
"""

import sqlite3
import nfl_data_py as nfl
import pandas as pd
from pathlib import Path

DB_PATH = Path(__file__).parent / "nflverse_custom.db"

def create_tables(conn):
    """Create the new supplementary tables."""
    cur = conn.cursor()

    # Snap counts - weekly snap count data per player
    cur.execute("""
        CREATE TABLE IF NOT EXISTS snap_counts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            game_id TEXT,
            pfr_game_id TEXT,
            season INTEGER,
            week INTEGER,
            team TEXT,
            player TEXT,
            position TEXT,
            pfr_player_id TEXT,
            offense_snaps INTEGER,
            offense_pct REAL,
            defense_snaps INTEGER,
            defense_pct REAL,
            st_snaps INTEGER,
            st_pct REAL
        )
    """)

    # NGS stats - Next Gen Stats (passing, rushing, receiving)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS ngs_stats (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            season INTEGER,
            season_type TEXT,
            week INTEGER,
            player_display_name TEXT,
            player_gsis_id TEXT,
            player_position TEXT,
            team_abbr TEXT,
            stat_type TEXT,
            -- Passing stats
            avg_time_to_throw REAL,
            avg_completed_air_yards REAL,
            avg_intended_air_yards REAL,
            avg_air_yards_differential REAL,
            aggressiveness REAL,
            max_completed_air_distance REAL,
            avg_air_yards_to_sticks REAL,
            passer_rating REAL,
            completions INTEGER,
            attempts INTEGER,
            pass_yards INTEGER,
            pass_touchdowns INTEGER,
            interceptions INTEGER,
            expected_completion_percentage REAL,
            completion_percentage_above_expectation REAL,
            -- Rushing stats
            efficiency REAL,
            percent_attempts_gte_eight_defenders REAL,
            avg_time_to_los REAL,
            rush_attempts INTEGER,
            rush_yards INTEGER,
            rush_touchdowns INTEGER,
            avg_rush_yards REAL,
            expected_rush_yards REAL,
            rush_yards_over_expected REAL,
            rush_yards_over_expected_per_att REAL,
            rush_pct_over_expected REAL,
            -- Receiving stats
            avg_cushion REAL,
            avg_separation REAL,
            avg_intended_air_yards_rec REAL,
            percent_share_of_intended_air_yards REAL,
            receptions INTEGER,
            targets INTEGER,
            rec_yards INTEGER,
            rec_touchdowns INTEGER,
            avg_yac REAL,
            avg_expected_yac REAL,
            avg_yac_above_expectation REAL,
            catch_percentage REAL,
            avg_target_share REAL
        )
    """)

    # Depth charts - weekly depth chart position
    cur.execute("""
        CREATE TABLE IF NOT EXISTS depth_charts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            season INTEGER,
            week INTEGER,
            team TEXT,
            position TEXT,
            depth_team INTEGER,
            full_name TEXT,
            first_name TEXT,
            last_name TEXT,
            gsis_id TEXT,
            jersey_number INTEGER,
            formation TEXT
        )
    """)

    # PFR advanced stats - seasonal advanced metrics
    cur.execute("""
        CREATE TABLE IF NOT EXISTS pfr_advanced (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            season INTEGER,
            player TEXT,
            team TEXT,
            pfr_id TEXT,
            stat_type TEXT,
            -- Passing
            pass_attempts INTEGER,
            throwaways INTEGER,
            spikes INTEGER,
            drops INTEGER,
            drop_pct REAL,
            bad_throws INTEGER,
            bad_throw_pct REAL,
            pocket_time REAL,
            times_blitzed INTEGER,
            times_hurried INTEGER,
            times_hit INTEGER,
            times_pressured INTEGER,
            pressure_pct REAL,
            batted_balls INTEGER,
            on_tgt_throws INTEGER,
            on_tgt_pct REAL,
            rpo_plays INTEGER,
            rpo_yards INTEGER,
            pa_pass_att INTEGER,
            pa_pass_yards INTEGER,
            intended_air_yards INTEGER,
            completed_air_yards INTEGER,
            pass_yards_after_catch INTEGER,
            scrambles INTEGER,
            scramble_yards_per_att REAL,
            -- Rushing
            rush_attempts INTEGER,
            rush_yards INTEGER,
            rush_yards_before_contact INTEGER,
            rush_yards_before_contact_per_att REAL,
            rush_yards_after_contact INTEGER,
            rush_yards_after_contact_per_att REAL,
            broken_tackles INTEGER,
            -- Receiving
            rec_targets INTEGER,
            receptions INTEGER,
            rec_yards INTEGER,
            rec_yards_before_catch INTEGER,
            rec_yards_before_catch_per_rec REAL,
            rec_yards_after_catch INTEGER,
            rec_yards_after_catch_per_rec REAL,
            rec_broken_tackles INTEGER,
            rec_drops INTEGER,
            rec_drop_pct REAL,
            rec_int_thrown INTEGER,
            rec_qb_rating REAL
        )
    """)

    # QBR - ESPN's Total QBR
    cur.execute("""
        CREATE TABLE IF NOT EXISTS qbr (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            season INTEGER,
            season_type TEXT,
            week INTEGER,
            team TEXT,
            player_id TEXT,
            name_display TEXT,
            qbr_total REAL,
            pts_added REAL,
            qb_plays INTEGER,
            epa_total REAL,
            pass_epa REAL,
            run_epa REAL,
            sack_epa REAL,
            penalty_epa REAL,
            raw_qbr REAL,
            sack_adj_qbr REAL
        )
    """)

    conn.commit()
    print("Tables created.")

def import_snap_counts(conn):
    """Import snap count data (2015-2024)."""
    print("\n=== Importing Snap Counts ===")
    years = list(range(2015, 2025))

    all_data = []
    for year in years:
        try:
            df = nfl.import_snap_counts(years=[year])
            if len(df) > 0:
                df['season'] = year
                all_data.append(df)
                print(f"  {year}: {len(df)} rows")
        except Exception as e:
            print(f"  {year}: failed ({e})")

    if all_data:
        combined = pd.concat(all_data, ignore_index=True)
        # Rename columns to match our schema
        combined = combined.rename(columns={
            'game': 'game_id',
            'pfr_game': 'pfr_game_id'
        })
        combined.to_sql('snap_counts', conn, if_exists='replace', index=False)
        print(f"  Total: {len(combined)} rows imported")

def import_ngs_stats(conn):
    """Import Next Gen Stats (2016-2024)."""
    print("\n=== Importing NGS Stats ===")
    years = list(range(2016, 2025))

    all_data = []
    for stat_type in ['passing', 'rushing', 'receiving']:
        print(f"  Loading {stat_type}...")
        try:
            df = nfl.import_ngs_data(stat_type=stat_type, years=years)
            df['stat_type'] = stat_type
            all_data.append(df)
            print(f"    {len(df)} rows")
        except Exception as e:
            print(f"    Failed: {e}")

    if all_data:
        combined = pd.concat(all_data, ignore_index=True)
        combined.to_sql('ngs_stats', conn, if_exists='replace', index=False)
        print(f"  Total: {len(combined)} rows imported")

def import_depth_charts(conn):
    """Import depth chart data (2001-2024)."""
    print("\n=== Importing Depth Charts ===")
    years = list(range(2001, 2025))

    all_data = []
    for year in years:
        try:
            df = nfl.import_depth_charts(years=[year])
            if len(df) > 0:
                all_data.append(df)
                print(f"  {year}: {len(df)} rows")
        except Exception as e:
            print(f"  {year}: failed ({e})")

    if all_data:
        combined = pd.concat(all_data, ignore_index=True)
        combined.to_sql('depth_charts', conn, if_exists='replace', index=False)
        print(f"  Total: {len(combined)} rows imported")

def import_pfr_advanced(conn):
    """Import PFR advanced stats (2018-2024)."""
    print("\n=== Importing PFR Advanced ===")
    years = list(range(2018, 2025))

    all_data = []
    for stat_type in ['pass', 'rush', 'rec']:
        print(f"  Loading {stat_type}...")
        try:
            df = nfl.import_seasonal_pfr(stat_type, years=years)
            df['stat_type'] = stat_type
            all_data.append(df)
            print(f"    {len(df)} rows")
        except Exception as e:
            print(f"    Failed: {e}")

    if all_data:
        combined = pd.concat(all_data, ignore_index=True)
        # Rename 'player' column if needed
        if 'player' in combined.columns:
            pass  # already correct
        combined.to_sql('pfr_advanced', conn, if_exists='replace', index=False)
        print(f"  Total: {len(combined)} rows imported")

def import_qbr(conn):
    """Import QBR data (2006-2024)."""
    print("\n=== Importing QBR ===")
    years = list(range(2006, 2025))

    try:
        df = nfl.import_qbr(years=years)
        if len(df) > 0:
            df.to_sql('qbr', conn, if_exists='replace', index=False)
            print(f"  Total: {len(df)} rows imported")
        else:
            print("  No data available")
    except Exception as e:
        print(f"  Failed: {e}")

def create_indexes(conn):
    """Create indexes for efficient lookups."""
    print("\n=== Creating Indexes ===")
    cur = conn.cursor()

    indexes = [
        "CREATE INDEX IF NOT EXISTS idx_snap_counts_player ON snap_counts(pfr_player_id)",
        "CREATE INDEX IF NOT EXISTS idx_snap_counts_season_week ON snap_counts(season, week)",
        "CREATE INDEX IF NOT EXISTS idx_ngs_gsis ON ngs_stats(player_gsis_id)",
        "CREATE INDEX IF NOT EXISTS idx_ngs_season_week ON ngs_stats(season, week)",
        "CREATE INDEX IF NOT EXISTS idx_ngs_type ON ngs_stats(stat_type)",
        "CREATE INDEX IF NOT EXISTS idx_depth_gsis ON depth_charts(gsis_id)",
        "CREATE INDEX IF NOT EXISTS idx_depth_season_week ON depth_charts(season, week)",
        "CREATE INDEX IF NOT EXISTS idx_pfr_id ON pfr_advanced(pfr_id)",
        "CREATE INDEX IF NOT EXISTS idx_pfr_season ON pfr_advanced(season)",
        "CREATE INDEX IF NOT EXISTS idx_qbr_player ON qbr(player_id)",
        "CREATE INDEX IF NOT EXISTS idx_qbr_season ON qbr(season)",
    ]

    for idx in indexes:
        cur.execute(idx)
        print(f"  Created: {idx.split('idx_')[1].split(' ')[0]}")

    conn.commit()

def main():
    print(f"Opening database: {DB_PATH}")
    conn = sqlite3.connect(DB_PATH)

    try:
        create_tables(conn)
        import_snap_counts(conn)
        import_ngs_stats(conn)
        import_depth_charts(conn)
        import_pfr_advanced(conn)
        import_qbr(conn)
        create_indexes(conn)

        # Summary
        print("\n=== Summary ===")
        cur = conn.cursor()
        for table in ['snap_counts', 'ngs_stats', 'depth_charts', 'pfr_advanced', 'qbr']:
            cur.execute(f"SELECT COUNT(*) FROM {table}")
            count = cur.fetchone()[0]
            print(f"  {table}: {count:,} rows")

        print("\nDone!")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
