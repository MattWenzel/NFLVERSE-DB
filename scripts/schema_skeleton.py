"""Auto-generated from data/nflverse_manifest.json by scripts/schema_generator.py.

DO NOT EDIT BY HAND. Regenerate with:
    python3 scripts/schema_generator.py

scripts/schema.py imports SKELETON and overrides entries (sets _enabled: True,
adds renames / expected_gaps / etc.). Any entry NOT overridden is skipped at
build time. A new manifest entry with no corresponding schema.py override
surfaces as an audit warning — you can't silently miss upstream files.
"""

from __future__ import annotations

# 62 candidate source(s) derived from the manifest.

SKELETON: dict = {
    'advstats_season_def': {
        '_enabled': False,
        'release_tag': 'pfr_advstats',
        'pattern': 'pfr_advstats/advstats_season_def.parquet',
        'renames': {},
        'id_cleanup': {'pfr_id': 'generic'},
        '_columns': ['season', 'player', 'pfr_id', 'tm', 'age', '...', 'comb', 'm_tkl', 'm_tkl_percent', 'loaded', 'bats'],  # 30 total
        '_year_span': None,
        '_sample_row_count': 7537,
    },
    'advstats_season_pass': {
        '_enabled': False,
        'release_tag': 'pfr_advstats',
        'pattern': 'pfr_advstats/advstats_season_pass.parquet',
        'renames': {},
        'id_cleanup': {'pfr_id': 'generic'},
        '_columns': ['player', 'team', 'pass_attempts', 'throwaways', 'spikes', '...', 'completed_air_yards_per_pass_attempt', 'pass_yards_after_catch', 'pass_yards_after_catch_per_completion', 'scrambles', 'scramble_yards_per_attempt'],  # 37 total
        '_year_span': None,
        '_sample_row_count': 848,
    },
    'advstats_season_rec': {
        '_enabled': False,
        'release_tag': 'pfr_advstats',
        'pattern': 'pfr_advstats/advstats_season_rec.parquet',
        'renames': {},
        'id_cleanup': {'pfr_id': 'generic'},
        '_columns': ['season', 'player', 'pfr_id', 'tm', 'age', '...', 'drop', 'drop_percent', 'int', 'rat', 'loaded'],  # 25 total
        '_year_span': None,
        '_sample_row_count': 4130,
    },
    'advstats_season_rush': {
        '_enabled': False,
        'release_tag': 'pfr_advstats',
        'pattern': 'pfr_advstats/advstats_season_rush.parquet',
        'renames': {},
        'id_cleanup': {'pfr_id': 'generic'},
        '_columns': ['season', 'player', 'pfr_id', 'tm', 'age', '...', 'yac', 'yac_att', 'brk_tkl', 'att_br', 'loaded'],  # 19 total
        '_year_span': None,
        '_sample_row_count': 2820,
    },
    'advstats_week_def': {
        '_enabled': False,
        'release_tag': 'pfr_advstats',
        'pattern': 'pfr_advstats/advstats_week_def_{year}.parquet',
        'renames': {},
        'id_cleanup': {'game_id': 'generic', 'pfr_game_id': 'generic', 'pfr_player_id': 'generic'},
        'year_range': ('auto', 'auto'),
        '_columns': ['game_id', 'pfr_game_id', 'season', 'week', 'game_type', '...', 'def_sacks', 'def_pressures', 'def_tackles_combined', 'def_missed_tackles', 'def_missed_tackle_pct'],  # 29 total
        '_year_span': [2018, 2025],
        '_sample_row_count': 7926,
    },
    'advstats_week_pass': {
        '_enabled': False,
        'release_tag': 'pfr_advstats',
        'pattern': 'pfr_advstats/advstats_week_pass_{year}.parquet',
        'renames': {},
        'id_cleanup': {'game_id': 'generic', 'pfr_game_id': 'generic', 'pfr_player_id': 'generic'},
        'year_range': ('auto', 'auto'),
        '_columns': ['game_id', 'pfr_game_id', 'season', 'week', 'game_type', '...', 'times_pressured', 'times_pressured_pct', 'def_times_blitzed', 'def_times_hurried', 'def_times_hitqb'],  # 24 total
        '_year_span': [2018, 2025],
        '_sample_row_count': 684,
    },
    'advstats_week_rec': {
        '_enabled': False,
        'release_tag': 'pfr_advstats',
        'pattern': 'pfr_advstats/advstats_week_rec_{year}.parquet',
        'renames': {},
        'id_cleanup': {'game_id': 'generic', 'pfr_game_id': 'generic', 'pfr_player_id': 'generic'},
        'year_range': ('auto', 'auto'),
        '_columns': ['game_id', 'pfr_game_id', 'season', 'week', 'game_type', '...', 'passing_drop_pct', 'receiving_drop', 'receiving_drop_pct', 'receiving_int', 'receiving_rat'],  # 17 total
        '_year_span': [2018, 2025],
        '_sample_row_count': 4533,
    },
    'advstats_week_rush': {
        '_enabled': False,
        'release_tag': 'pfr_advstats',
        'pattern': 'pfr_advstats/advstats_week_rush_{year}.parquet',
        'renames': {},
        'id_cleanup': {'game_id': 'generic', 'pfr_game_id': 'generic', 'pfr_player_id': 'generic'},
        'year_range': ('auto', 'auto'),
        '_columns': ['game_id', 'pfr_game_id', 'season', 'week', 'game_type', '...', 'rushing_yards_before_contact_avg', 'rushing_yards_after_contact', 'rushing_yards_after_contact_avg', 'rushing_broken_tackles', 'receiving_broken_tackles'],  # 16 total
        '_year_span': [2018, 2025],
        '_sample_row_count': 2355,
    },
    'combine': {
        '_enabled': False,
        'release_tag': 'combine',
        'pattern': 'combine/combine.parquet',
        'renames': {},
        'id_cleanup': {'pfr_id': 'generic'},
        '_columns': ['season', 'draft_year', 'draft_team', 'draft_round', 'draft_ovr', '...', 'bench', 'vertical', 'broad_jump', 'cone', 'shuttle'],  # 18 total
        '_year_span': None,
        '_sample_row_count': 8649,
    },
    'depth_charts_legacy': {
        '_enabled': False,
        'release_tag': 'depth_charts',
        'pattern': 'depth_charts/depth_charts_{year}.parquet',
        'renames': {},
        'id_cleanup': {'espn_id': 'generic', 'gsis_id': 'gsis'},
        'year_range': ('auto', 'auto'),
        '_columns': ['dt', 'team', 'player_name', 'espn_id', 'gsis_id', '...', 'pos_id', 'pos_name', 'pos_abb', 'pos_slot', 'pos_rank'],  # 12 total
        '_year_span': [2001, 2026],
        '_sample_row_count': 476501,
    },
    'draft_picks': {
        '_enabled': False,
        'release_tag': 'draft_picks',
        'pattern': 'draft_picks/draft_picks.parquet',
        'renames': {},
        'id_cleanup': {'gsis_id': 'gsis', 'pfr_player_id': 'generic'},
        '_columns': ['season', 'round', 'pick', 'team', 'gsis_id', '...', 'rec_yards', 'rec_tds', 'def_solo_tackles', 'def_ints', 'def_sacks'],  # 36 total
        '_year_span': None,
        '_sample_row_count': 12670,
    },
    'dynastyprocess_db_playerids': {
        '_enabled': False,
        'release_tag': '_external',
        'pattern': 'external/db_playerids.csv',
        'format': 'csv',
        'renames': {},
        'id_cleanup': {'gsis_id': 'gsis', 'pff_id': 'generic', 'nfl_id': 'generic', 'espn_id': 'generic', 'pfr_id': 'generic'},
        '_url': 'https://github.com/dynastyprocess/data/raw/master/files/db_playerids.csv',
        '_columns': ['mfl_id', 'sportradar_id', 'fantasypros_id', 'gsis_id', 'pff_id', '...', 'twitter_username', 'height', 'weight', 'college', 'db_season'],  # 35 total
    },
    'ftn_charting': {
        '_enabled': False,
        'release_tag': 'ftn_charting',
        'pattern': 'ftn_charting/ftn_charting_{year}.parquet',
        'renames': {},
        'id_cleanup': {'nflverse_game_id': 'generic'},
        'year_range': ('auto', 'auto'),
        '_columns': ['ftn_game_id', 'nflverse_game_id', 'season', 'week', 'ftn_play_id', '...', 'is_qb_sneak', 'n_blitzers', 'n_pass_rushers', 'is_qb_fault_sack', 'date_pulled'],  # 29 total
        '_year_span': [2022, 2025],
        '_sample_row_count': 47316,
    },
    'games': {
        '_enabled': False,
        'release_tag': 'schedules',
        'pattern': 'schedules/games.parquet',
        'renames': {},
        'id_cleanup': {'game_id': 'generic', 'old_game_id': 'generic'},
        '_columns': ['game_id', 'season', 'game_type', 'week', 'gameday', '...', 'away_coach', 'home_coach', 'referee', 'stadium_id', 'stadium'],  # 46 total
        '_year_span': None,
        '_sample_row_count': 7276,
    },
    'historical_contracts': {
        '_enabled': False,
        'release_tag': 'contracts',
        'pattern': 'contracts/historical_contracts.parquet',
        'renames': {},
        'id_cleanup': {'otc_id': 'generic', 'gsis_id': 'gsis'},
        '_columns': ['player', 'position', 'team', 'is_active', 'year_signed', '...', 'draft_year', 'draft_round', 'draft_overall', 'draft_team', 'cols'],  # 25 total
        '_year_span': None,
        '_sample_row_count': 50817,
    },
    'injuries': {
        '_enabled': False,
        'release_tag': 'injuries',
        'pattern': 'injuries/injuries_{year}.parquet',
        'renames': {},
        'id_cleanup': {'gsis_id': 'gsis'},
        'year_range': ('auto', 'auto'),
        '_columns': ['season', 'season_type', 'game_type', 'team', 'week', '...', 'report_secondary_injury', 'report_status', 'practice_primary_injury', 'practice_secondary_injury', 'practice_status'],  # 16 total
        '_year_span': [2009, 2025],
        '_sample_row_count': 6068,
    },
    'ngs_passing': {
        '_enabled': False,
        'release_tag': 'nextgen_stats',
        'pattern': 'nextgen_stats/ngs_passing.parquet',
        'renames': {},
        'id_cleanup': {'player_gsis_id': 'gsis'},
        '_columns': ['season', 'season_type', 'week', 'player_display_name', 'player_position', '...', 'player_gsis_id', 'player_first_name', 'player_last_name', 'player_jersey_number', 'player_short_name'],  # 29 total
        '_year_span': None,
        '_sample_row_count': 5933,
    },
    'ngs_receiving': {
        '_enabled': False,
        'release_tag': 'nextgen_stats',
        'pattern': 'nextgen_stats/ngs_receiving.parquet',
        'renames': {},
        'id_cleanup': {'player_gsis_id': 'gsis'},
        '_columns': ['season', 'season_type', 'week', 'player_display_name', 'player_position', '...', 'player_gsis_id', 'player_first_name', 'player_last_name', 'player_jersey_number', 'player_short_name'],  # 23 total
        '_year_span': None,
        '_sample_row_count': 14731,
    },
    'ngs_rushing': {
        '_enabled': False,
        'release_tag': 'nextgen_stats',
        'pattern': 'nextgen_stats/ngs_rushing.parquet',
        'renames': {},
        'id_cleanup': {'player_gsis_id': 'gsis'},
        '_columns': ['season', 'season_type', 'week', 'player_display_name', 'player_position', '...', 'player_short_name', 'expected_rush_yards', 'rush_yards_over_expected', 'rush_yards_over_expected_per_att', 'rush_pct_over_expected'],  # 22 total
        '_year_span': None,
        '_sample_row_count': 5992,
    },
    'officials': {
        '_enabled': False,
        'release_tag': 'officials',
        'pattern': 'officials/officials.parquet',
        'renames': {},
        'id_cleanup': {'game_id': 'generic', 'game_key': 'generic', 'official_id': 'generic'},
        '_columns': ['game_id', 'game_key', 'official_name', 'position', 'jersey_number', 'official_id', 'season', 'season_type', 'week'],
        '_year_span': None,
        '_sample_row_count': 21900,
    },
    'otc_players': {
        '_enabled': False,
        'release_tag': 'players_components',
        'pattern': 'players_components/otc_players.parquet',
        'renames': {},
        'id_cleanup': {'otc_id': 'generic', 'gsis_id': 'gsis', 'gsis_it_id': 'generic', 'pff_id': 'generic'},
        '_columns': ['otc_id', 'nfl_player_id', 'esbid', 'gsis_id', 'gsis_it_id', 'pff_id'],
        '_year_span': None,
        '_sample_row_count': 13074,
    },
    'pbp_participation': {
        '_enabled': False,
        'release_tag': 'pbp_participation',
        'pattern': 'pbp_participation/pbp_participation_{year}.parquet',
        'renames': {},
        'id_cleanup': {'nflverse_game_id': 'generic', 'old_game_id': 'generic'},
        'year_range': ('auto', 'auto'),
        '_columns': ['nflverse_game_id', 'old_game_id', 'play_id', 'possession_team', 'offense_formation', '...', 'defense_names', 'offense_positions', 'defense_positions', 'offense_numbers', 'defense_numbers'],  # 26 total
        '_year_span': [2016, 2025],
        '_sample_row_count': 45184,
    },
    'pbp_participation_old': {
        '_enabled': False,
        'release_tag': 'pbp_participation',
        'pattern': 'pbp_participation/pbp_participation_old_{year}.parquet',
        'renames': {},
        'id_cleanup': {'nflverse_game_id': 'generic', 'old_game_id': 'generic'},
        'year_range': ('auto', 'auto'),
        '_columns': ['nflverse_game_id', 'old_game_id', 'play_id', 'possession_team', 'offense_formation', '...', 'time_to_throw', 'was_pressure', 'route', 'defense_man_zone_type', 'defense_coverage_type'],  # 20 total
        '_year_span': [2023, 2023],
        '_sample_row_count': 47160,
    },
    'pfr_rosters': {
        '_enabled': False,
        'release_tag': 'misc',
        'pattern': 'misc/pfr_rosters.parquet',
        'renames': {},
        'id_cleanup': {'pfr_player_id': 'generic'},
        '_columns': ['season', 'pfr', 'nfl', 'pfr_player_id', 'no', '...', 'birth_date', 'yrs', 'av', 'drafted_tm_rnd_yr', 'salary'],  # 18 total
        '_year_span': None,
        '_sample_row_count': 49518,
    },
    'play_by_play': {
        '_enabled': False,
        'release_tag': 'pbp',
        'pattern': 'pbp/play_by_play_{year}.parquet',
        'renames': {},
        'id_cleanup': {'game_id': 'generic', 'old_game_id': 'generic'},
        'year_range': ('auto', 'auto'),
        '_columns': ['play_id', 'game_id', 'old_game_id', 'home_team', 'away_team', '...', 'xyac_median_yardage', 'xyac_success', 'xyac_fd', 'xpass', 'pass_oe'],  # 372 total
        '_year_span': [1999, 2025],
        '_sample_row_count': 48771,
    },
    'player_stats': {
        '_enabled': False,
        'release_tag': 'player_stats',
        'pattern': 'player_stats/player_stats.parquet',
        'renames': {},
        'id_cleanup': {'player_id': 'gsis'},
        '_columns': ['player_id', 'player_name', 'player_display_name', 'position', 'position_group', '...', 'air_yards_share', 'wopr', 'special_teams_tds', 'fantasy_points', 'fantasy_points_ppr'],  # 53 total
        '_year_span': None,
        '_sample_row_count': 134470,
    },
    'player_stats__player_stats': {
        '_enabled': False,
        'release_tag': 'player_stats',
        'pattern': 'player_stats/player_stats_{year}.parquet',
        'renames': {},
        'id_cleanup': {'player_id': 'gsis'},
        'year_range': ('auto', 'auto'),
        '_columns': ['player_id', 'player_name', 'player_display_name', 'position', 'position_group', '...', 'air_yards_share', 'wopr', 'special_teams_tds', 'fantasy_points', 'fantasy_points_ppr'],  # 53 total
        '_year_span': [1999, 2024],
        '_sample_row_count': 5597,
    },
    'player_stats__player_stats_def': {
        '_enabled': False,
        'release_tag': 'player_stats',
        'pattern': 'player_stats/player_stats_def_{year}.parquet',
        'renames': {},
        'id_cleanup': {'player_id': 'gsis'},
        'year_range': ('auto', 'auto'),
        '_columns': ['season', 'week', 'season_type', 'player_id', 'player_name', '...', 'def_fumble_recovery_opp', 'def_fumble_recovery_yards_opp', 'def_safety', 'def_penalty', 'def_penalty_yards'],  # 32 total
        '_year_span': [1999, 2024],
        '_sample_row_count': 9994,
    },
    'player_stats__player_stats_def_season': {
        '_enabled': False,
        'release_tag': 'player_stats',
        'pattern': 'player_stats/player_stats_def_season_{year}.parquet',
        'renames': {},
        'id_cleanup': {'player_id': 'gsis'},
        'year_range': ('auto', 'auto'),
        '_columns': ['season', 'season_type', 'player_id', 'player_name', 'player_display_name', '...', 'def_fumble_recovery_opp', 'def_fumble_recovery_yards_opp', 'def_safety', 'def_penalty', 'def_penalty_yards'],  # 32 total
        '_year_span': [1999, 2024],
        '_sample_row_count': 3117,
    },
    'player_stats__player_stats_kicking': {
        '_enabled': False,
        'release_tag': 'player_stats',
        'pattern': 'player_stats/player_stats_kicking_{year}.parquet',
        'renames': {},
        'id_cleanup': {'player_id': 'gsis'},
        'year_range': ('auto', 'auto'),
        '_columns': ['season', 'week', 'season_type', 'player_id', 'team', '...', 'gwfg_att', 'gwfg_distance', 'gwfg_made', 'gwfg_missed', 'gwfg_blocked'],  # 44 total
        '_year_span': [1999, 2024],
        '_sample_row_count': 567,
    },
    'player_stats__player_stats_kicking_season': {
        '_enabled': False,
        'release_tag': 'player_stats',
        'pattern': 'player_stats/player_stats_kicking_season_{year}.parquet',
        'renames': {},
        'id_cleanup': {'player_id': 'gsis'},
        'year_range': ('auto', 'auto'),
        '_columns': ['season', 'season_type', 'player_id', 'team', 'player_name', '...', 'gwfg_att', 'gwfg_distance_list', 'gwfg_made', 'gwfg_missed', 'gwfg_blocked'],  # 44 total
        '_year_span': [1999, 2024],
        '_sample_row_count': 126,
    },
    'player_stats__player_stats_season': {
        '_enabled': False,
        'release_tag': 'player_stats',
        'pattern': 'player_stats/player_stats_season_{year}.parquet',
        'renames': {},
        'id_cleanup': {'player_id': 'gsis'},
        'year_range': ('auto', 'auto'),
        '_columns': ['season', 'season_type', 'player_id', 'player_name', 'player_display_name', '...', 'air_yards_share', 'wopr', 'special_teams_tds', 'fantasy_points', 'fantasy_points_ppr'],  # 52 total
        '_year_span': [1999, 2024],
        '_sample_row_count': 1370,
    },
    'player_stats__stats_player_post': {
        '_enabled': False,
        'release_tag': 'player_stats',
        'pattern': 'player_stats/stats_player_post_{year}.parquet',
        'renames': {},
        'id_cleanup': {'player_id': 'gsis'},
        'year_range': ('auto', 'auto'),
        '_columns': ['player_id', 'player_name', 'player_display_name', 'position', 'position_group', '...', 'gwfg_missed', 'gwfg_blocked', 'gwfg_distance_list', 'fantasy_points', 'fantasy_points_ppr'],  # 113 total
        '_year_span': [1999, 2024],
        '_sample_row_count': 529,
    },
    'player_stats__stats_player_reg': {
        '_enabled': False,
        'release_tag': 'player_stats',
        'pattern': 'player_stats/stats_player_reg_{year}.parquet',
        'renames': {},
        'id_cleanup': {'player_id': 'gsis'},
        'year_range': ('auto', 'auto'),
        '_columns': ['player_id', 'player_name', 'player_display_name', 'position', 'position_group', '...', 'gwfg_missed', 'gwfg_blocked', 'gwfg_distance_list', 'fantasy_points', 'fantasy_points_ppr'],  # 113 total
        '_year_span': [1999, 2024],
        '_sample_row_count': 2020,
    },
    'player_stats__stats_player_regpost': {
        '_enabled': False,
        'release_tag': 'player_stats',
        'pattern': 'player_stats/stats_player_regpost_{year}.parquet',
        'renames': {},
        'id_cleanup': {'player_id': 'gsis'},
        'year_range': ('auto', 'auto'),
        '_columns': ['player_id', 'player_name', 'player_display_name', 'position', 'position_group', '...', 'gwfg_missed', 'gwfg_blocked', 'gwfg_distance_list', 'fantasy_points', 'fantasy_points_ppr'],  # 113 total
        '_year_span': [1999, 2024],
        '_sample_row_count': 2025,
    },
    'player_stats__stats_player_week': {
        '_enabled': False,
        'release_tag': 'player_stats',
        'pattern': 'player_stats/stats_player_week_{year}.parquet',
        'renames': {},
        'id_cleanup': {'player_id': 'gsis', 'game_id': 'generic'},
        'year_range': ('auto', 'auto'),
        '_columns': ['player_id', 'player_name', 'player_display_name', 'position', 'position_group', '...', 'gwfg_missed', 'gwfg_blocked', 'gwfg_distance', 'fantasy_points', 'fantasy_points_ppr'],  # 115 total
        '_year_span': [1999, 2024],
        '_sample_row_count': 19421,
    },
    'player_stats__stats_team_post': {
        '_enabled': False,
        'release_tag': 'player_stats',
        'pattern': 'player_stats/stats_team_post_{year}.parquet',
        'renames': {},
        'id_cleanup': {},
        'year_range': ('auto', 'auto'),
        '_columns': ['season', 'team', 'season_type', 'games', 'completions', '...', 'gwfg_made', 'gwfg_att', 'gwfg_missed', 'gwfg_blocked', 'gwfg_distance_list'],  # 101 total
        '_year_span': [1999, 2024],
        '_sample_row_count': 14,
    },
    'player_stats__stats_team_reg': {
        '_enabled': False,
        'release_tag': 'player_stats',
        'pattern': 'player_stats/stats_team_reg_{year}.parquet',
        'renames': {},
        'id_cleanup': {},
        'year_range': ('auto', 'auto'),
        '_columns': ['season', 'team', 'season_type', 'games', 'completions', '...', 'gwfg_made', 'gwfg_att', 'gwfg_missed', 'gwfg_blocked', 'gwfg_distance_list'],  # 101 total
        '_year_span': [1999, 2024],
        '_sample_row_count': 32,
    },
    'player_stats__stats_team_regpost': {
        '_enabled': False,
        'release_tag': 'player_stats',
        'pattern': 'player_stats/stats_team_regpost_{year}.parquet',
        'renames': {},
        'id_cleanup': {},
        'year_range': ('auto', 'auto'),
        '_columns': ['season', 'team', 'season_type', 'games', 'completions', '...', 'gwfg_made', 'gwfg_att', 'gwfg_missed', 'gwfg_blocked', 'gwfg_distance_list'],  # 101 total
        '_year_span': [1999, 2024],
        '_sample_row_count': 32,
    },
    'player_stats__stats_team_week': {
        '_enabled': False,
        'release_tag': 'player_stats',
        'pattern': 'player_stats/stats_team_week_{year}.parquet',
        'renames': {},
        'id_cleanup': {'game_id': 'generic'},
        'year_range': ('auto', 'auto'),
        '_columns': ['season', 'week', 'team', 'season_type', 'game_id', '...', 'gwfg_made', 'gwfg_att', 'gwfg_missed', 'gwfg_blocked', 'gwfg_distance'],  # 103 total
        '_year_span': [1999, 2024],
        '_sample_row_count': 570,
    },
    'player_stats_def': {
        '_enabled': False,
        'release_tag': 'player_stats',
        'pattern': 'player_stats/player_stats_def.parquet',
        'renames': {},
        'id_cleanup': {'player_id': 'gsis'},
        '_columns': ['season', 'week', 'season_type', 'player_id', 'player_name', '...', 'def_fumble_recovery_opp', 'def_fumble_recovery_yards_opp', 'def_safety', 'def_penalty', 'def_penalty_yards'],  # 32 total
        '_year_span': None,
        '_sample_row_count': 239955,
    },
    'player_stats_def_season': {
        '_enabled': False,
        'release_tag': 'player_stats',
        'pattern': 'player_stats/player_stats_def_season.parquet',
        'renames': {},
        'id_cleanup': {'player_id': 'gsis'},
        '_columns': ['season', 'season_type', 'player_id', 'player_name', 'player_display_name', '...', 'def_fumble_recovery_opp', 'def_fumble_recovery_yards_opp', 'def_safety', 'def_penalty', 'def_penalty_yards'],  # 32 total
        '_year_span': None,
        '_sample_row_count': 83147,
    },
    'player_stats_kicking': {
        '_enabled': False,
        'release_tag': 'player_stats',
        'pattern': 'player_stats/player_stats_kicking.parquet',
        'renames': {},
        'id_cleanup': {'player_id': 'gsis'},
        '_columns': ['season', 'week', 'season_type', 'player_id', 'team', '...', 'gwfg_att', 'gwfg_distance', 'gwfg_made', 'gwfg_missed', 'gwfg_blocked'],  # 44 total
        '_year_span': None,
        '_sample_row_count': 13300,
    },
    'player_stats_kicking_season': {
        '_enabled': False,
        'release_tag': 'player_stats',
        'pattern': 'player_stats/player_stats_kicking_season.parquet',
        'renames': {},
        'id_cleanup': {'player_id': 'gsis'},
        '_columns': ['season', 'season_type', 'player_id', 'team', 'player_name', '...', 'gwfg_att', 'gwfg_distance_list', 'gwfg_made', 'gwfg_missed', 'gwfg_blocked'],  # 44 total
        '_year_span': None,
        '_sample_row_count': 2504,
    },
    'player_stats_season': {
        '_enabled': False,
        'release_tag': 'player_stats',
        'pattern': 'player_stats/player_stats_season.parquet',
        'renames': {},
        'id_cleanup': {'player_id': 'gsis'},
        '_columns': ['season', 'season_type', 'player_id', 'player_name', 'player_display_name', '...', 'air_yards_share', 'wopr', 'special_teams_tds', 'fantasy_points', 'fantasy_points_ppr'],  # 52 total
        '_year_span': None,
        '_sample_row_count': 33636,
    },
    'players': {
        '_enabled': False,
        'release_tag': 'players_components',
        'pattern': 'players_components/players.parquet',
        'renames': {},
        'id_cleanup': {'gsis_id': 'gsis', 'esb_id': 'generic', 'nfl_id': 'generic', 'pfr_id': 'generic', 'pff_id': 'generic', 'otc_id': 'generic', 'espn_id': 'generic', 'smart_id': 'generic'},
        '_columns': ['gsis_id', 'display_name', 'common_first_name', 'first_name', 'last_name', '...', 'pff_status', 'draft_year', 'draft_round', 'draft_pick', 'draft_team'],  # 39 total
        '_year_span': None,
        '_sample_row_count': 24376,
    },
    'players_master': {
        '_enabled': False,
        'release_tag': 'players',
        'pattern': 'players/players.parquet',
        'renames': {},
        'id_cleanup': {'gsis_id': 'gsis', 'esb_id': 'generic', 'nfl_id': 'generic', 'pfr_id': 'generic', 'pff_id': 'generic', 'otc_id': 'generic', 'espn_id': 'generic', 'smart_id': 'generic'},
        '_columns': ['gsis_id', 'display_name', 'common_first_name', 'first_name', 'last_name', '...', 'pff_status', 'draft_year', 'draft_round', 'draft_pick', 'draft_team'],  # 39 total
        '_year_span': None,
        '_sample_row_count': 24376,
    },
    'qbr_season_level': {
        '_enabled': False,
        'release_tag': 'espn_data',
        'pattern': 'espn_data/qbr_season_level.parquet',
        'renames': {},
        'id_cleanup': {'player_id': 'generic'},
        '_columns': ['season', 'season_type', 'game_week', 'team_abb', 'player_id', '...', 'name_last', 'name_display', 'headshot_href', 'team', 'qualified'],  # 23 total
        '_year_span': None,
        '_sample_row_count': 1523,
    },
    'qbr_week_level': {
        '_enabled': False,
        'release_tag': 'espn_data',
        'pattern': 'espn_data/qbr_week_level.parquet',
        'renames': {},
        'id_cleanup': {'game_id': 'generic', 'player_id': 'generic'},
        '_columns': ['season', 'season_type', 'game_id', 'game_week', 'week_text', '...', 'opp_abb', 'opp_team', 'opp_name', 'week_num', 'qualified'],  # 30 total
        '_year_span': None,
        '_sample_row_count': 10709,
    },
    'roster': {
        '_enabled': False,
        'release_tag': 'rosters',
        'pattern': 'rosters/roster_{year}.parquet',
        'renames': {},
        'id_cleanup': {'gsis_id': 'gsis', 'espn_id': 'generic', 'pff_id': 'generic', 'pfr_id': 'generic', 'esb_id': 'generic', 'gsis_it_id': 'generic', 'smart_id': 'generic'},
        'year_range': ('auto', 'auto'),
        '_columns': ['season', 'team', 'position', 'depth_chart_position', 'jersey_number', '...', 'smart_id', 'entry_year', 'rookie_year', 'draft_club', 'draft_number'],  # 36 total
        '_year_span': [1920, 2026],
        '_sample_row_count': 3137,
    },
    'roster_weekly': {
        '_enabled': False,
        'release_tag': 'weekly_rosters',
        'pattern': 'weekly_rosters/roster_weekly_{year}.parquet',
        'renames': {},
        'id_cleanup': {'gsis_id': 'gsis', 'espn_id': 'generic', 'pff_id': 'generic', 'pfr_id': 'generic', 'esb_id': 'generic', 'gsis_it_id': 'generic', 'smart_id': 'generic'},
        'year_range': ('auto', 'auto'),
        '_columns': ['season', 'team', 'position', 'depth_chart_position', 'jersey_number', '...', 'smart_id', 'entry_year', 'rookie_year', 'draft_club', 'draft_number'],  # 36 total
        '_year_span': [2002, 2025],
        '_sample_row_count': 46849,
    },
    'snap_counts': {
        '_enabled': False,
        'release_tag': 'snap_counts',
        'pattern': 'snap_counts/snap_counts_{year}.parquet',
        'renames': {},
        'id_cleanup': {'game_id': 'generic', 'pfr_game_id': 'generic', 'pfr_player_id': 'generic'},
        'year_range': ('auto', 'auto'),
        '_columns': ['game_id', 'pfr_game_id', 'season', 'game_type', 'week', '...', 'offense_pct', 'defense_snaps', 'defense_pct', 'st_snaps', 'st_pct'],  # 16 total
        '_year_span': [2012, 2025],
        '_sample_row_count': 26612,
    },
    'stats_player_post': {
        '_enabled': False,
        'release_tag': 'stats_player',
        'pattern': 'stats_player/stats_player_post_{year}.parquet',
        'renames': {},
        'id_cleanup': {'player_id': 'gsis'},
        'year_range': ('auto', 'auto'),
        '_columns': ['player_id', 'player_name', 'player_display_name', 'position', 'position_group', '...', 'gwfg_missed', 'gwfg_blocked', 'gwfg_distance_list', 'fantasy_points', 'fantasy_points_ppr'],  # 113 total
        '_year_span': [1999, 2025],
        '_sample_row_count': 529,
    },
    'stats_player_reg': {
        '_enabled': False,
        'release_tag': 'stats_player',
        'pattern': 'stats_player/stats_player_reg_{year}.parquet',
        'renames': {},
        'id_cleanup': {'player_id': 'gsis'},
        'year_range': ('auto', 'auto'),
        '_columns': ['player_id', 'player_name', 'player_display_name', 'position', 'position_group', '...', 'gwfg_missed', 'gwfg_blocked', 'gwfg_distance_list', 'fantasy_points', 'fantasy_points_ppr'],  # 113 total
        '_year_span': [1999, 2025],
        '_sample_row_count': 2020,
    },
    'stats_player_regpost': {
        '_enabled': False,
        'release_tag': 'stats_player',
        'pattern': 'stats_player/stats_player_regpost_{year}.parquet',
        'renames': {},
        'id_cleanup': {'player_id': 'gsis'},
        'year_range': ('auto', 'auto'),
        '_columns': ['player_id', 'player_name', 'player_display_name', 'position', 'position_group', '...', 'gwfg_missed', 'gwfg_blocked', 'gwfg_distance_list', 'fantasy_points', 'fantasy_points_ppr'],  # 113 total
        '_year_span': [1999, 2025],
        '_sample_row_count': 2025,
    },
    'stats_player_week': {
        '_enabled': False,
        'release_tag': 'stats_player',
        'pattern': 'stats_player/stats_player_week_{year}.parquet',
        'renames': {},
        'id_cleanup': {'player_id': 'gsis', 'game_id': 'generic'},
        'year_range': ('auto', 'auto'),
        '_columns': ['player_id', 'player_name', 'player_display_name', 'position', 'position_group', '...', 'gwfg_missed', 'gwfg_blocked', 'gwfg_distance', 'fantasy_points', 'fantasy_points_ppr'],  # 115 total
        '_year_span': [1999, 2025],
        '_sample_row_count': 19421,
    },
    'stats_team_post': {
        '_enabled': False,
        'release_tag': 'stats_team',
        'pattern': 'stats_team/stats_team_post_{year}.parquet',
        'renames': {},
        'id_cleanup': {},
        'year_range': ('auto', 'auto'),
        '_columns': ['season', 'team', 'season_type', 'games', 'completions', '...', 'gwfg_made', 'gwfg_att', 'gwfg_missed', 'gwfg_blocked', 'gwfg_distance_list'],  # 101 total
        '_year_span': [1999, 2025],
        '_sample_row_count': 14,
    },
    'stats_team_reg': {
        '_enabled': False,
        'release_tag': 'stats_team',
        'pattern': 'stats_team/stats_team_reg_{year}.parquet',
        'renames': {},
        'id_cleanup': {},
        'year_range': ('auto', 'auto'),
        '_columns': ['season', 'team', 'season_type', 'games', 'completions', '...', 'gwfg_made', 'gwfg_att', 'gwfg_missed', 'gwfg_blocked', 'gwfg_distance_list'],  # 101 total
        '_year_span': [1999, 2025],
        '_sample_row_count': 32,
    },
    'stats_team_regpost': {
        '_enabled': False,
        'release_tag': 'stats_team',
        'pattern': 'stats_team/stats_team_regpost_{year}.parquet',
        'renames': {},
        'id_cleanup': {},
        'year_range': ('auto', 'auto'),
        '_columns': ['season', 'team', 'season_type', 'games', 'completions', '...', 'gwfg_made', 'gwfg_att', 'gwfg_missed', 'gwfg_blocked', 'gwfg_distance_list'],  # 101 total
        '_year_span': [1999, 2025],
        '_sample_row_count': 32,
    },
    'stats_team_week': {
        '_enabled': False,
        'release_tag': 'stats_team',
        'pattern': 'stats_team/stats_team_week_{year}.parquet',
        'renames': {},
        'id_cleanup': {'game_id': 'generic'},
        'year_range': ('auto', 'auto'),
        '_columns': ['season', 'week', 'team', 'season_type', 'game_id', '...', 'gwfg_made', 'gwfg_att', 'gwfg_missed', 'gwfg_blocked', 'gwfg_distance'],  # 103 total
        '_year_span': [1999, 2025],
        '_sample_row_count': 570,
    },
    'teams_colors_logos': {
        '_enabled': False,
        'release_tag': 'teams',
        'pattern': 'teams/teams_colors_logos.parquet',
        'renames': {},
        'id_cleanup': {},
        '_columns': ['team_abbr', 'team_name', 'team_id', 'team_nick', 'team_conf', '...', 'team_logo_espn', 'team_wordmark', 'team_conference_logo', 'team_league_logo', 'team_logo_squared'],  # 16 total
        '_year_span': None,
        '_sample_row_count': 36,
    },
    'trades': {
        '_enabled': False,
        'release_tag': 'trades',
        'pattern': 'trades/trades.parquet',
        'renames': {},
        'id_cleanup': {'pfr_id': 'generic'},
        '_columns': ['trade_id', 'season', 'trade_date', 'gave', 'received', '...', 'pick_round', 'pick_number', 'conditional', 'pfr_id', 'pfr_name'],  # 11 total
        '_year_span': None,
        '_sample_row_count': 4847,
    },
}
