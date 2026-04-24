"""Declarative description of the v2 DB shape.

Single source of truth. Every SOURCE we consume, every TABLE we produce,
every FK edge, every fill rule, every index. The engine (scripts/v2/engine.py)
interprets this file uniformly — no per-table special-case code elsewhere.

Structure:
  SOURCES     – input file patterns (parquet globs, CSV paths) with
                per-source renames, ID cleanup, and force-type overrides.
                Keys are stable source_ids referenced from TABLES and HUB_BUILD.
  TABLES      – output tables. Each names a source_id (or a list for unioned
                sources like ngs), declares its FKs, natural key, stubbing,
                ID backfill, and indexes.
  HUB_BUILD   – how `players` is constructed by priority-merging across
                many sources. Separate from TABLES because it's the one
                place in the engine that does a merge rather than a
                source-to-table projection.
  FILL_RULES  – cross-table value backfill (e.g., season_stats POST rows from
                game_stats aggregation; players.college from combine). Each
                rule is a pure declaration of target, source, join, and op.
  VIEWS       – declared views on top of tables.
  LOAD_ORDER  – FK-dependency order. Parents load before children. The engine
                walks this list; cycles are a config error.

Conventions:
  - id_cleanup kind: 'generic' (strip junk sentinels) or 'gsis' (require
    canonical GSIS regex). See scripts/v2/cleanup.py.
  - references syntax: "table.column" — engine parses to (table, column).
  - year_range: ("auto", "auto") means discover from raw files; otherwise
    (min_year, max_year) or a fixed list.
"""

from __future__ import annotations

# ---------------------------------------------------------------------------
# Helper: every column in play_by_play that holds a player GSIS reference.
# Flat enumeration here so the config stays declarative.
# ---------------------------------------------------------------------------

PBP_PLAYER_COLS = [
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


# ===========================================================================
# SOURCES — every raw input we read
# ===========================================================================
SOURCES: dict = {

    # -------- nflverse_releases: players_master --------
    "players_master": {
        "release_tag": "players",
        "pattern": "players/players.parquet",
        "renames": {"gsis_id": "player_gsis_id", "pfr_id": "player_pfr_id", "espn_id": "player_espn_id"},
        "id_cleanup": {
            # players.parquet contains pre-GSIS historical records with Elias
            # IDs (e.g. 'YOU597411') — use 'generic', not 'gsis', to keep them.
            "player_gsis_id": "generic",
            "player_pfr_id":  "generic",
            "player_espn_id": "generic",
        },
    },

    # -------- nflverse_releases: stats_player --------
    "stats_player_week": {
        "release_tag": "stats_player",
        "pattern": "stats_player/stats_player_week_{year}.parquet",
        "year_range": ("auto", "auto"),
        "renames": {"player_id": "player_gsis_id"},
        "id_cleanup": {"player_gsis_id": "gsis", "game_id": "generic"},
        "force_types": {"game_id": "VARCHAR"},
        "ensure_columns": {"game_id": "VARCHAR"},  # missing in pre-2022 files
    },
    "stats_player_reg": {
        "release_tag": "stats_player",
        "pattern": "stats_player/stats_player_reg_{year}.parquet",
        "year_range": ("auto", "auto"),
        "renames": {"player_id": "player_gsis_id"},
        "id_cleanup": {"player_gsis_id": "gsis"},
    },
    "stats_player_post": {
        "release_tag": "stats_player",
        "pattern": "stats_player/stats_player_post_{year}.parquet",
        "year_range": ("auto", "auto"),
        "renames": {"player_id": "player_gsis_id"},
        "id_cleanup": {"player_gsis_id": "gsis"},
    },

    # -------- nflverse_releases: schedules --------
    "schedules": {
        "release_tag": "schedules",
        "pattern": "schedules/games.parquet",
        "renames": {},
        "id_cleanup": {"game_id": "generic"},
        "force_types": {
            "gametime": "VARCHAR", "gameday": "VARCHAR",
            "weekday": "VARCHAR", "time_of_day": "VARCHAR", "start_time": "VARCHAR",
        },
    },

    # -------- nflverse_releases: snap_counts --------
    "snap_counts": {
        "release_tag": "snap_counts",
        "pattern": "snap_counts/snap_counts_{year}.parquet",
        "year_range": ("auto", "auto"),
        "renames": {"pfr_player_id": "player_pfr_id"},
        "id_cleanup": {"player_pfr_id": "generic"},
    },

    # -------- nflverse_releases: depth_charts --------
    "depth_charts_legacy": {
        # 2001-2024 schema (weekly, GSIS-keyed)
        "release_tag": "depth_charts",
        "pattern": "depth_charts/depth_charts_{year}.parquet",
        "year_range": ("auto", 2024),
        "renames": {"gsis_id": "player_gsis_id"},
        "id_cleanup": {"player_gsis_id": "gsis"},
    },
    "depth_charts_2025": {
        # 2025+ schema (daily, GSIS+ESPN keyed, granular pos_abb)
        "release_tag": "depth_charts",
        "pattern": "depth_charts/depth_charts_2025.parquet",
        "renames": {"gsis_id": "player_gsis_id", "espn_id": "player_espn_id"},
        "id_cleanup": {"player_gsis_id": "gsis", "player_espn_id": "generic"},
    },

    # -------- nflverse_releases: nextgen_stats --------
    "ngs_passing": {
        "release_tag": "nextgen_stats",
        "pattern": "nextgen_stats/ngs_passing.parquet",
        "renames": {},
        "id_cleanup": {"player_gsis_id": "gsis"},
        "add_literal_columns": {"stat_type": "passing"},
    },
    "ngs_rushing": {
        "release_tag": "nextgen_stats",
        "pattern": "nextgen_stats/ngs_rushing.parquet",
        "renames": {},
        "id_cleanup": {"player_gsis_id": "gsis"},
        "add_literal_columns": {"stat_type": "rushing"},
    },
    "ngs_receiving": {
        "release_tag": "nextgen_stats",
        "pattern": "nextgen_stats/ngs_receiving.parquet",
        "renames": {},
        "id_cleanup": {"player_gsis_id": "gsis"},
        "add_literal_columns": {"stat_type": "receiving"},
    },

    # -------- nflverse_releases: pfr_advstats (season) --------
    "pfr_advanced_season_pass": {
        "release_tag": "pfr_advstats",
        "pattern": "pfr_advstats/advstats_season_pass.parquet",
        "renames": {"pfr_id": "player_pfr_id"},
        "id_cleanup": {"player_pfr_id": "generic"},
        "add_literal_columns": {"stat_type": "pass"},
    },
    "pfr_advanced_season_rush": {
        "release_tag": "pfr_advstats",
        "pattern": "pfr_advstats/advstats_season_rush.parquet",
        "renames": {"pfr_id": "player_pfr_id", "tm": "team"},
        "id_cleanup": {"player_pfr_id": "generic"},
        "add_literal_columns": {"stat_type": "rush"},
    },
    "pfr_advanced_season_rec": {
        "release_tag": "pfr_advstats",
        "pattern": "pfr_advstats/advstats_season_rec.parquet",
        "renames": {"pfr_id": "player_pfr_id", "tm": "team"},
        "id_cleanup": {"player_pfr_id": "generic"},
        "add_literal_columns": {"stat_type": "rec"},
    },
    "pfr_advanced_season_def": {
        # NEW in v2 (v1 missed this file — 7,537 rows × 30 cols of defensive advanced stats)
        "release_tag": "pfr_advstats",
        "pattern": "pfr_advstats/advstats_season_def.parquet",
        "renames": {"pfr_id": "player_pfr_id", "tm": "team"},
        "id_cleanup": {"player_pfr_id": "generic"},
        "add_literal_columns": {"stat_type": "def"},
    },

    # -------- nflverse_releases: pfr_advstats (weekly) — NEW in v2 --------
    "pfr_advanced_week_pass": {
        "release_tag": "pfr_advstats",
        "pattern": "pfr_advstats/advstats_week_pass_{year}.parquet",
        "year_range": ("auto", "auto"),
        "renames": {"pfr_player_id": "player_pfr_id"},
        "id_cleanup": {"player_pfr_id": "generic", "game_id": "generic"},
        "add_literal_columns": {"stat_type": "pass"},
    },
    "pfr_advanced_week_rush": {
        "release_tag": "pfr_advstats",
        "pattern": "pfr_advstats/advstats_week_rush_{year}.parquet",
        "year_range": ("auto", "auto"),
        "renames": {"pfr_player_id": "player_pfr_id"},
        "id_cleanup": {"player_pfr_id": "generic", "game_id": "generic"},
        "add_literal_columns": {"stat_type": "rush"},
    },
    "pfr_advanced_week_rec": {
        "release_tag": "pfr_advstats",
        "pattern": "pfr_advstats/advstats_week_rec_{year}.parquet",
        "year_range": ("auto", "auto"),
        "renames": {"pfr_player_id": "player_pfr_id"},
        "id_cleanup": {"player_pfr_id": "generic", "game_id": "generic"},
        "add_literal_columns": {"stat_type": "rec"},
    },
    "pfr_advanced_week_def": {
        "release_tag": "pfr_advstats",
        "pattern": "pfr_advstats/advstats_week_def_{year}.parquet",
        "year_range": ("auto", "auto"),
        "renames": {"pfr_player_id": "player_pfr_id"},
        "id_cleanup": {"player_pfr_id": "generic", "game_id": "generic"},
        "add_literal_columns": {"stat_type": "def"},
    },

    # -------- nflverse_releases: combine --------
    "combine": {
        "release_tag": "combine",
        "pattern": "combine/combine.parquet",
        "renames": {"pfr_id": "player_pfr_id"},
        "id_cleanup": {"player_pfr_id": "generic"},
    },

    # -------- nflverse_releases: draft_picks --------
    "draft_picks": {
        "release_tag": "draft_picks",
        "pattern": "draft_picks/draft_picks.parquet",
        "renames": {"gsis_id": "player_gsis_id", "pfr_player_id": "player_pfr_id"},
        "id_cleanup": {"player_gsis_id": "gsis", "player_pfr_id": "generic"},
    },

    # -------- nflverse_releases: pbp --------
    "pbp": {
        "release_tag": "pbp",
        "pattern": "pbp/play_by_play_{year}.parquet",
        "year_range": ("auto", "auto"),
        "renames": {},
        "id_cleanup": {**{c: "gsis" for c in PBP_PLAYER_COLS}, "game_id": "generic"},
        "force_types": {
            "start_time": "VARCHAR", "time_of_day": "VARCHAR",
            "weather": "VARCHAR", "stadium": "VARCHAR",
            "nfl_api_id": "VARCHAR", "game_stadium": "VARCHAR",
            "stadium_id": "VARCHAR", "end_clock_time": "VARCHAR",
            "end_yard_line": "VARCHAR",
        },
    },

    # -------- nflverse_releases: weekly_rosters (NEW) --------
    "weekly_rosters": {
        "release_tag": "weekly_rosters",
        "pattern": "weekly_rosters/roster_weekly_{year}.parquet",
        "year_range": ("auto", "auto"),
        "renames": {
            "gsis_id": "player_gsis_id",
            "pfr_id": "player_pfr_id",
            "espn_id": "player_espn_id",
        },
        "id_cleanup": {
            "player_gsis_id": "gsis",
            "player_pfr_id": "generic",
            "player_espn_id": "generic",
            "gsis_it_id": "generic",
            "esb_id": "generic",
            "pff_id": "generic",
            "smart_id": "generic",
        },
    },

    # -------- nflverse_releases: injuries (NEW) --------
    "injuries": {
        "release_tag": "injuries",
        "pattern": "injuries/injuries_{year}.parquet",
        "year_range": ("auto", "auto"),
        "renames": {"gsis_id": "player_gsis_id"},
        "id_cleanup": {"player_gsis_id": "gsis"},
    },

    # -------- nflverse_releases: contracts (NEW) --------
    "contracts": {
        "release_tag": "contracts",
        "pattern": "contracts/historical_contracts.parquet",
        "renames": {"gsis_id": "player_gsis_id"},
        "id_cleanup": {"player_gsis_id": "gsis", "otc_id": "generic"},
    },

    # -------- nflverse_releases: pbp_participation (NEW) --------
    "pbp_participation": {
        "release_tag": "pbp_participation",
        "pattern": "pbp_participation/pbp_participation_{year}.parquet",
        "year_range": (2016, "auto"),
        "renames": {"nflverse_game_id": "game_id"},
        "id_cleanup": {"game_id": "generic", "old_game_id": "generic"},
    },

    # -------- nflverse_releases: ftn_charting (NEW) --------
    "ftn_charting": {
        "release_tag": "ftn_charting",
        "pattern": "ftn_charting/ftn_charting_{year}.parquet",
        "year_range": (2022, "auto"),
        "renames": {"nflverse_game_id": "game_id", "nflverse_play_id": "play_id"},
        "id_cleanup": {"game_id": "generic", "ftn_game_id": "generic"},
    },

    # -------- nflverse_releases: officials (NEW) --------
    "officials": {
        "release_tag": "officials",
        "pattern": "officials/officials.parquet",
        "renames": {},
        "id_cleanup": {"game_id": "generic", "game_key": "generic", "official_id": "generic"},
    },

    # -------- nflverse_releases: espn_data (NEW — replaces espnscrapeR CSV) --------
    "qbr_week": {
        "release_tag": "espn_data",
        "pattern": "espn_data/qbr_week_level.parquet",
        # ESPN's game_id is a numeric ESPN ID, not our nflverse game_id
        # (2024_01_KC_BUF format). Rename to avoid collision; keep as
        # informational column for ESPN cross-reference.
        "renames": {"player_id": "player_espn_id", "game_id": "espn_game_id"},
        "id_cleanup": {"player_espn_id": "generic", "espn_game_id": "generic"},
    },
    "qbr_season": {
        "release_tag": "espn_data",
        "pattern": "espn_data/qbr_season_level.parquet",
        "renames": {"player_id": "player_espn_id"},
        "id_cleanup": {"player_espn_id": "generic"},
    },

    # -------- nflverse_releases: stats_team (NEW) --------
    "stats_team_week": {
        "release_tag": "stats_team",
        "pattern": "stats_team/stats_team_week_{year}.parquet",
        "year_range": ("auto", "auto"),
        "renames": {},
        "id_cleanup": {"game_id": "generic"},
    },
    "stats_team_reg": {
        "release_tag": "stats_team",
        "pattern": "stats_team/stats_team_reg_{year}.parquet",
        "year_range": ("auto", "auto"),
        "renames": {},
        "id_cleanup": {},
    },
    "stats_team_post": {
        "release_tag": "stats_team",
        "pattern": "stats_team/stats_team_post_{year}.parquet",
        "year_range": ("auto", "auto"),
        "renames": {},
        "id_cleanup": {},
    },

    # -------- external: dynastyprocess db_playerids --------
    "db_playerids": {
        "release_tag": "_external",
        "pattern": "external/db_playerids.csv",
        "format": "csv",
        "renames": {},
        "id_cleanup": {
            "gsis_id": "gsis",
            "pfr_id": "generic",
            "espn_id": "generic",
            "nfl_id": "generic",
            "pff_id": "generic",
        },
        # espn_id is numeric-with-NaN in the CSV; pre-cast to Int64→string before cleanup.
        "pre_cast_numeric_to_string": ["espn_id"],
        "dedup_cols": ["gsis_id"],
        "dropna_cols": ["gsis_id"],
    },
}


# ===========================================================================
# HUB_BUILD — how `players` is constructed
# ===========================================================================
# Pandas-layer priority merge. Phase runs before any table writes.
# Each source contributes a DataFrame keyed on player_gsis_id (or on an
# alternate ID that gets resolved to GSIS via a prior source).
# Collision policy is per-column.
HUB_BUILD: dict = {
    "target_table": "players",
    "primary_key": "player_gsis_id",
    "unique_columns": ["player_pfr_id", "player_espn_id"],

    # Priority-ordered sources. Earlier = canonical; later sources fill NULLs.
    "sources": [
        # 1. Master players file: canonical biographical data.
        {
            "source_id": "players_master",
            "role": "master",
            "key": "player_gsis_id",
        },
        # 2. dynastyprocess bridge: adds cross-reference IDs + stubs for GSIS
        #    values the master missed.
        {
            "source_id": "db_playerids",
            "role": "id_bridge",
            "key": "gsis_id",           # will be renamed on read to player_gsis_id
            "column_map": {             # bridge column → hub column
                "gsis_id": "player_gsis_id",
                "pfr_id": "player_pfr_id",
                "espn_id": "player_espn_id",
                "name": "display_name",
                "position": "position",
                "team": "latest_team",
            },
        },
        # 3. Weekly rosters: closes ~680 fringe-player GSIS gaps per year
        #    (practice-squad callups, mid-season signings). Aggregated to
        #    one-row-per-GSIS using the LATEST week's values.
        {
            "source_id": "weekly_rosters",
            "role": "expansion",
            "key": "player_gsis_id",
            "aggregate": "latest_by_week",
            "column_map": {
                "player_gsis_id": "player_gsis_id",
                "player_pfr_id":  "player_pfr_id",
                "player_espn_id": "player_espn_id",
                "esb_id":         "esb_id",
                "pff_id":         "pff_id",
                "smart_id":       "smart_id",
                "full_name":      "display_name",
                "first_name":     "first_name",
                "last_name":      "last_name",
                "position":       "position",
                "team":           "latest_team",
                "birth_date":     "birth_date",
                "height":         "height",
                "weight":         "weight",
                "college":        "college_name",
                "jersey_number":  "jersey_number",
                "status":         "status",
                "headshot_url":   "headshot",
            },
        },
        # 4. Draft picks: biographical backfill for pre-GSIS draft picks
        #    (pre-1995 HoF picks etc.). PFR-keyed for pre-GSIS; GSIS-keyed
        #    for modern era.
        {
            "source_id": "draft_picks",
            "role": "biographical_backfill",
            "key_priority": ["player_gsis_id", "player_pfr_id"],
            "column_map": {
                "pfr_player_name": "display_name",
                "position": "position",
                "college": "college_name",
                "season":  "draft_year",
                "round":   "draft_round",
                "pick":    "draft_pick",
                "team":    "draft_team",
            },
        },
        # 5. Combine: adds prospect PFR ids that never made a roster.
        {
            "source_id": "combine",
            "role": "biographical_backfill",
            "key_priority": ["player_pfr_id"],
            "column_map": {
                # combine has native `draft_year`/`draft_team`/`draft_round` columns
                # (from the nflverse-draft join on the combine file), so we use
                # them directly rather than aliasing `season` → `draft_year`.
                "player_name":  "display_name",
                "pos":          "position",
                "school":       "college_name",
                "draft_year":   "draft_year",
                "draft_team":   "draft_team",
                "draft_round":  "draft_round",
                "ht":           "height",
                "wt":           "weight",
            },
        },
        # 6. Name-match preflight: for child-source PFR/ESPN IDs still not in
        #    the hub after steps 1-5, attempt a (name, position, team)-match
        #    against existing GSIS-bearing rows before emitting a fresh
        #    NULL-GSIS stub. Sources scanned in this step:
        {
            "source_id": "_name_match_preflight",
            "role": "name_match",
            "scan_sources": [
                "combine", "draft_picks", "snap_counts",
                "pfr_advanced_season_pass", "pfr_advanced_season_rush",
                "pfr_advanced_season_rec",  "pfr_advanced_season_def",
                "qbr_week", "depth_charts_2025",
            ],
            "match_columns": ("display_name", "position"),
            "safety": "reject_on_position_conflict",
        },
        # 7. Stub last-resort: any candidate ID still not in the hub gets a
        #    stub row. No name-match attempted; this is the "accept the gap"
        #    fallback. Engine emits one stub per unique unresolved ID.
        {
            "source_id": "_child_id_stubs",
            "role": "stub_source",
            "scan_sources": [
                "combine", "draft_picks", "snap_counts",
                "pfr_advanced_season_pass", "pfr_advanced_season_rush",
                "pfr_advanced_season_rec",  "pfr_advanced_season_def",
                "qbr_week", "depth_charts_2025",
            ],
        },
    ],

    # Per-column collision policy. 'first_non_null' default: earlier source wins.
    "column_policy": {
        "latest_team":    "latest_source_wins",   # temporal
        "jersey_number":  "latest_source_wins",
        "status":         "latest_source_wins",
        "headshot":       "latest_source_wins",
        "draft_year":     "earliest_non_null",    # biographical
        "draft_round":    "earliest_non_null",
        "draft_pick":     "earliest_non_null",
        "draft_team":     "earliest_non_null",
        "college_name":   "earliest_non_null",
        "birth_date":     "earliest_non_null",
        "height":         "earliest_non_null",
        "weight":         "earliest_non_null",
        # default for anything else: first_non_null (same as earliest_non_null)
    },
}


# ===========================================================================
# TABLES — every output table
# ===========================================================================
TABLES: dict = {

    "players": {
        "build_via": "hub",   # special: built by HUB_BUILD, not a direct source projection
        "primary_key": "player_gsis_id",
        "unique_columns": ["player_pfr_id", "player_espn_id"],
        "indexes": [("player_gsis_id",)],
    },

    "player_ids": {
        "source_id": "db_playerids",
        "dedup_cols": ["gsis_id"],
        "dropna_cols": ["gsis_id"],
        "foreign_keys": [
            {"column": "gsis_id", "references": "players.player_gsis_id"},
        ],
    },

    "games": {
        "source_id": "schedules",
        "primary_key": "game_id",
    },

    "snap_counts": {
        "source_id": "snap_counts",
        "dedup_cols": ["player_pfr_id", "season", "week", "team"],
        "foreign_keys": [
            {"column": "player_pfr_id", "references": "players.player_pfr_id"},
        ],
        "id_backfill": [
            {
                "new_column": "player_gsis_id",
                "via_hub_lookup": ("player_pfr_id", "player_gsis_id"),
                "add_fk": "players.player_gsis_id",
            },
        ],
        "_removed_stub_source": {
            "player_pfr_id": {
                "display_name": "player", "position": "position", "latest_team": "team",
            },
        },
    },

    "depth_charts": {
        "source_id": "depth_charts_legacy",
        "foreign_keys": [
            {"column": "player_gsis_id", "references": "players.player_gsis_id"},
        ],
        "_removed_stub_source": {
            "player_gsis_id": {
                "display_name": "full_name", "first_name": "first_name",
                "last_name": "last_name", "position": "position",
            },
        },
    },

    "depth_charts_2025": {
        "source_id": "depth_charts_2025",
        "foreign_keys": [
            {"column": "player_gsis_id", "references": "players.player_gsis_id"},
            {"column": "player_espn_id", "references": "players.player_espn_id"},
        ],
        "_removed_stub_source": {
            "player_gsis_id": {
                "display_name": "player_name", "latest_team": "team", "position": "pos_abb",
            },
            "player_espn_id": {
                "display_name": "player_name", "latest_team": "team", "position": "pos_abb",
            },
        },
    },

    "ngs_stats": {
        "source_ids": ["ngs_passing", "ngs_rushing", "ngs_receiving"],  # UNION
        "foreign_keys": [
            {"column": "player_gsis_id", "references": "players.player_gsis_id"},
        ],
        "_removed_stub_source": {
            "player_gsis_id": {
                "display_name": "player_display_name",
                "first_name": "player_first_name",
                "last_name": "player_last_name",
                "position": "player_position",
            },
        },
    },

    "pfr_advanced": {
        # Season-level. UNION across pass/rush/rec/def (def NEW in v2).
        "source_ids": [
            "pfr_advanced_season_pass", "pfr_advanced_season_rush",
            "pfr_advanced_season_rec",  "pfr_advanced_season_def",
        ],
        "foreign_keys": [
            {"column": "player_pfr_id", "references": "players.player_pfr_id"},
        ],
        "id_backfill": [
            {
                "new_column": "player_gsis_id",
                "via_hub_lookup": ("player_pfr_id", "player_gsis_id"),
                "add_fk": "players.player_gsis_id",
            },
        ],
        "_removed_stub_source": {
            "player_pfr_id": {
                "display_name": "player", "position": "pos", "latest_team": "team",
            },
        },
    },

    "pfr_advanced_weekly": {
        # NEW in v2. UNION across weekly pass/rush/rec/def.
        "source_ids": [
            "pfr_advanced_week_pass", "pfr_advanced_week_rush",
            "pfr_advanced_week_rec",  "pfr_advanced_week_def",
        ],
        "foreign_keys": [
            {"column": "player_pfr_id", "references": "players.player_pfr_id"},
            {"column": "game_id",       "references": "games.game_id"},
        ],
        "id_backfill": [
            {
                "new_column": "player_gsis_id",
                "via_hub_lookup": ("player_pfr_id", "player_gsis_id"),
                "add_fk": "players.player_gsis_id",
            },
        ],
    },

    "combine": {
        "source_id": "combine",
        "foreign_keys": [
            {"column": "player_pfr_id", "references": "players.player_pfr_id"},
        ],
        "id_backfill": [
            {
                "new_column": "player_gsis_id",
                "via_hub_lookup": ("player_pfr_id", "player_gsis_id"),
                "add_fk": "players.player_gsis_id",
            },
        ],
        "_removed_stub_source": {
            "player_pfr_id": {
                "display_name": "player_name", "position": "pos", "college_name": "school",
            },
        },
    },

    "draft_picks": {
        "source_id": "draft_picks",
        "foreign_keys": [
            {"column": "player_gsis_id", "references": "players.player_gsis_id"},
            {"column": "player_pfr_id",  "references": "players.player_pfr_id"},
        ],
        "_removed_stub_source": {
            "player_gsis_id": {"display_name": "pfr_player_name", "position": "position"},
            "player_pfr_id":  {"display_name": "pfr_player_name", "position": "position"},
        },
        "name_match_recovery": {
            "target_column": "player_gsis_id",
            "name_columns": ["pfr_player_name"],
        },
    },

    "game_stats": {
        "source_id": "stats_player_week",
        "dedup_cols": ["player_gsis_id", "season", "week"],
        "foreign_keys": [
            {"column": "player_gsis_id", "references": "players.player_gsis_id"},
            {"column": "game_id",        "references": "games.game_id"},
        ],
        "_removed_stub_source": {
            "player_gsis_id": {"display_name": "player_display_name", "position": "position"},
        },
        "indexes": [("player_gsis_id", "season")],
    },

    "season_stats": {
        # UNION of REG + POST files. v1 missed POST entirely; v2 loads both.
        "source_ids": ["stats_player_reg", "stats_player_post"],
        "foreign_keys": [
            {"column": "player_gsis_id", "references": "players.player_gsis_id"},
        ],
        "_removed_stub_source": {
            "player_gsis_id": {"display_name": "player_display_name", "position": "position"},
        },
        "indexes": [("player_gsis_id", "season")],
    },

    "qbr": {
        # Source swapped from espnscrapeR CSV (stopped at 2023) to nflverse
        # espn_data/qbr_week_level.parquet (covers 2024-2025, 21 more QBs).
        # espn_game_id stays as a non-FK column (ESPN's numeric ID, not ours).
        "source_id": "qbr_week",
        "foreign_keys": [
            {"column": "player_espn_id", "references": "players.player_espn_id"},
        ],
        "id_backfill": [
            {
                "new_column": "player_gsis_id",
                "via_hub_lookup": ("player_espn_id", "player_gsis_id"),
                "add_fk": "players.player_gsis_id",
            },
        ],
        "_removed_stub_source": {
            "player_espn_id": {"display_name": "name_short", "latest_team": "team_abb"},
        },
    },

    "weekly_rosters": {
        # NEW in v2. Weekly roster snapshots with full ID set (GSIS, PFR, ESPN,
        # ESB, PFF, SMART, NFL). Also feeds HUB_BUILD step 3.
        "source_id": "weekly_rosters",
        "foreign_keys": [
            {"column": "player_gsis_id", "references": "players.player_gsis_id"},
        ],
        "_removed_stub_source": {
            "player_gsis_id": {
                "display_name": "full_name", "first_name": "first_name",
                "last_name": "last_name", "position": "position",
                "latest_team": "team",
            },
        },
        "indexes": [("player_gsis_id", "season", "week")],
    },

    "injuries": {
        # NEW in v2. Weekly injury reports. 6K rows/season.
        "source_id": "injuries",
        "foreign_keys": [
            {"column": "player_gsis_id", "references": "players.player_gsis_id"},
        ],
        "_removed_stub_source": {
            "player_gsis_id": {
                "display_name": "full_name", "first_name": "first_name",
                "last_name": "last_name", "position": "position",
            },
        },
        "indexes": [("player_gsis_id", "season", "week")],
    },

    "contracts": {
        # NEW in v2. Historical player contracts. 50K rows.
        "source_id": "contracts",
        "foreign_keys": [
            {"column": "player_gsis_id", "references": "players.player_gsis_id"},
        ],
        "_removed_stub_source": {
            "player_gsis_id": {"display_name": "player", "position": "position"},
        },
    },

    "pbp_participation": {
        # NEW in v2. Per-play participation (2016+). 46K plays/year × 26 cols.
        "source_id": "pbp_participation",
        "foreign_keys": [
            {"column": "game_id", "references": "games.game_id"},
        ],
    },

    "ftn_charting": {
        # NEW in v2. FTN manual charting (2022+). 48K plays/year.
        "source_id": "ftn_charting",
        "foreign_keys": [
            {"column": "game_id", "references": "games.game_id"},
        ],
    },

    "officials": {
        # NEW in v2. Referee crews per game. Note: officials.game_id uses a
        # different ID space (NFL's internal YYYYMMDDGG, not our 2015_01_GB_CHI).
        # No FK until we work out a reliable crosswalk; join via (season, week, team)
        # against games if needed.
        "source_id": "officials",
        "foreign_keys": [],
    },

    "team_game_stats": {
        # NEW in v2. Team-level weekly aggregates. The source parquet doesn't
        # carry a game_id column — join via (season, week, team) against games
        # when you need cross-table joins.
        "source_id": "stats_team_week",
        "foreign_keys": [],
        "indexes": [("team", "season")],
    },

    "team_season_stats": {
        # NEW in v2. Team-level season aggregates (REG+POST unioned).
        "source_ids": ["stats_team_reg", "stats_team_post"],
        "indexes": [("team", "season")],
    },

    "play_by_play": {
        "source_id": "pbp",
        "foreign_keys": (
            [{"column": "game_id", "references": "games.game_id"}]
            + [{"column": c, "references": "players.player_gsis_id"} for c in PBP_PLAYER_COLS]
        ),
        "_removed_stub_source": {
            # Each GSIS column's companion *_player_name column serves as the
            # stub's display_name.
            c: {"display_name": (
                c.replace("_player_id", "_player_name")
                if c.endswith("_player_id")
                else c.replace("_id", "_player_name")
            )}
            for c in PBP_PLAYER_COLS
        },
    },
}


# ===========================================================================
# FILL_RULES — cross-table backfill applied after all tables are loaded
# ===========================================================================
# Rules run in declared order. Each produces a count logged at build end.

FILL_RULES: list = [
    # --- Season-stats safety net (v1's compute_missing_season_stats).
    # After nflverse's reg+post feeds are loaded, aggregate any (player, season,
    # type) still in game_stats but missing from season_stats. POST is fully
    # covered by nflverse's stats_player_post files now, so this mostly
    # no-ops. Kept as a safety net for future coverage drift.
    {
        "name": "season_stats_augment_from_game_stats",
        "target_table": "season_stats",
        "op": "aggregate_from_sibling",
        "source_table": "game_stats",
        "group_by": ["player_gsis_id", "season", "season_type"],
        "aggregation": {
            "sum_cols": "_auto",          # engine infers from SS_COLUMN_CLASSIFICATION
            "max_cols": ["fg_long"],
            "games": "COUNT(DISTINCT game_id)",
            "recent_team": "arg_max(team, week)",
            "compute_from_components": {
                "passer_rating": (
                    "CASE WHEN attempts > 0 THEN "
                    "((LEAST(GREATEST((completions*1.0/attempts - 0.3) * 5, 0), 2.375)"
                    "  + LEAST(GREATEST((passing_yards*1.0/attempts - 3) * 0.25, 0), 2.375)"
                    "  + LEAST(GREATEST((passing_tds*1.0/attempts) * 20, 0), 2.375)"
                    "  + LEAST(GREATEST(2.375 - (passing_interceptions*1.0/attempts * 25), 0), 2.375))"
                    " / 6.0 * 100) ELSE NULL END"
                ),
                "completion_percentage": "completions * 100.0 / NULLIF(attempts, 0)",
                "passing_cpoe": None,            # requires PBP xpass, skip
                "pacr": "passing_yards * 1.0 / NULLIF(passing_air_yards, 0)",
                "racr": "receiving_yards * 1.0 / NULLIF(receiving_air_yards, 0)",
                "target_share": None,            # team-relative, skip
                "air_yards_share": None,         # team-relative, skip
                "wopr": None,                    # derived from target_share, skip
                "fg_pct": "fg_made * 100.0 / NULLIF(fg_att, 0)",
                "pat_pct": "pat_made * 100.0 / NULLIF(pat_att, 0)",
            },
        },
    },

    # --- players.college_name from combine / draft_picks ---
    {
        "name": "players_college_from_combine",
        "target_table": "players",
        "op": "backfill_null",
        "target_column": "college_name",
        "source_table": "combine",
        "source_column": "school",
        "join": [("players.player_pfr_id", "combine.player_pfr_id")],
    },
    {
        "name": "players_college_from_draft_picks",
        "target_table": "players",
        "op": "backfill_null",
        "target_column": "college_name",
        "source_table": "draft_picks",
        "source_column": "college",
        "join": [("players.player_gsis_id", "draft_picks.player_gsis_id")],
    },
    # --- players draft info from draft_picks (for hub-missed pre-GSIS picks) ---
    {
        "name": "players_draft_year_from_draft_picks",
        "target_table": "players",
        "op": "backfill_null",
        "target_column": "draft_year",
        "source_table": "draft_picks",
        "source_column": "season",
        "join": [("players.player_pfr_id", "draft_picks.player_pfr_id")],
    },
    {
        "name": "players_draft_round_from_draft_picks",
        "target_table": "players",
        "op": "backfill_null",
        "target_column": "draft_round",
        "source_table": "draft_picks",
        "source_column": "round",
        "join": [("players.player_pfr_id", "draft_picks.player_pfr_id")],
    },
    {
        "name": "players_draft_team_from_draft_picks",
        "target_table": "players",
        "op": "backfill_null",
        "target_column": "draft_team",
        "source_table": "draft_picks",
        "source_column": "team",
        "join": [("players.player_pfr_id", "draft_picks.player_pfr_id")],
    },

    # --- season_stats.recent_team from game_stats (v1's backfill_season_stats_team) ---
    {
        "name": "season_stats_recent_team_from_game_stats",
        "target_table": "season_stats",
        "op": "backfill_null",
        "target_column": "recent_team",
        "source_table": "game_stats",
        # Most common team per (gsis, season) — ties broken by LAST week.
        "source_expression": (
            "(SELECT team FROM game_stats g WHERE g.player_gsis_id = season_stats.player_gsis_id "
            "AND g.season = season_stats.season AND g.team IS NOT NULL "
            "GROUP BY team ORDER BY COUNT(*) DESC, MAX(week) DESC LIMIT 1)"
        ),
        "scope": "where_null",
    },
]


# ===========================================================================
# VIEWS — declared views on top of tables
# ===========================================================================
VIEWS: dict = {
    "v_depth_charts": {
        # Composite: UNION of depth_charts (2001-2024) + depth_charts_2025 with
        # normalized column set. Same semantics as v1 (NFL-calendar-adjusted
        # season, games-lookup week for 2025, pos_abb-to-position mapping).
        # Definition lives in scripts/v2/views.py because the SQL is substantial.
        "defined_in": "scripts/v2/views.py::v_depth_charts_sql",
        "requires_tables": ["depth_charts", "depth_charts_2025", "games"],
    },
}


# ===========================================================================
# LOAD_ORDER — FK-dependency order (parents before children)
# ===========================================================================
LOAD_ORDER: list = [
    # Parents
    "players",
    "player_ids",
    "games",
    # Player-linked children (FK to players via various IDs)
    "weekly_rosters",        # load early; also contributes to hub but as own table
    "combine",
    "draft_picks",
    "snap_counts",
    "depth_charts",
    "depth_charts_2025",
    "pfr_advanced",
    "ngs_stats",
    "qbr",
    "injuries",
    "contracts",
    # Game-linked children
    "officials",
    "team_game_stats",
    "team_season_stats",
    # Both player- and game-linked
    "game_stats",
    "season_stats",
    "pfr_advanced_weekly",
    "pbp_participation",
    "ftn_charting",
    # Heaviest last — references everything
    "play_by_play",
]


# ===========================================================================
# Validation (self-check on config load)
# ===========================================================================

def validate_config(manifest: dict | None = None) -> list[str]:
    """Cross-check the config. Returns a list of error strings (empty = OK).

    - Every TABLE references a source_id that exists in SOURCES (or is 'hub')
    - Every FK references a table in TABLES
    - LOAD_ORDER covers every TABLE exactly once
    - FK edges respect LOAD_ORDER (parent appears before child)
    - If manifest provided: every SOURCE's release_tag exists in the manifest
      and the source's pattern matches one of the manifest's known patterns.
    """
    errors = []

    if manifest is not None:
        releases = manifest.get("nflverse_releases", {})
        for sid, s in SOURCES.items():
            tag = s.get("release_tag")
            if tag == "_external":
                continue
            if tag not in releases:
                errors.append(f"source {sid!r}: release_tag {tag!r} not in manifest")
                continue
            # Check the pattern — strip the subfolder prefix if present.
            # The config may reference a specific year (e.g. depth_charts_2025.parquet
            # for the schema-changed 2025+ file) that matches a general manifest
            # pattern (depth_charts_{year}.parquet). Accept either.
            import re as _re
            pattern = s["pattern"]
            pattern_file = pattern.rsplit("/", 1)[-1]
            known = {p["pattern"] for p in releases[tag]["patterns"]}
            if pattern_file in known:
                continue
            matches_general = False
            for k in known:
                if "{year}" in k:
                    regex = "^" + _re.escape(k).replace(r"\{year\}", r"\d{4}") + "$"
                    if _re.match(regex, pattern_file):
                        matches_general = True
                        break
            if not matches_general:
                errors.append(
                    f"source {sid!r}: pattern {pattern_file!r} not in manifest "
                    f"release {tag!r} (known: {sorted(known)})"
                )

    for tname, t in TABLES.items():
        if t.get("build_via") == "hub":
            continue
        src = t.get("source_id")
        srcs = t.get("source_ids", [])
        all_src = ([src] if src else []) + list(srcs)
        if not all_src:
            errors.append(f"{tname}: no source_id or source_ids declared")
        for s in all_src:
            if s not in SOURCES:
                errors.append(f"{tname}: source_id {s!r} not in SOURCES")

    valid_tables = set(TABLES.keys())
    for tname, t in TABLES.items():
        for fk in t.get("foreign_keys", []):
            ref = fk["references"]
            ref_table = ref.split(".")[0]
            if ref_table not in valid_tables:
                errors.append(f"{tname}.{fk['column']} → {ref}: unknown target table")

    missing = valid_tables - set(LOAD_ORDER)
    if missing:
        errors.append(f"LOAD_ORDER missing: {sorted(missing)}")
    extra = set(LOAD_ORDER) - valid_tables
    if extra:
        errors.append(f"LOAD_ORDER has unknown tables: {sorted(extra)}")

    pos = {t: i for i, t in enumerate(LOAD_ORDER)}
    for tname, t in TABLES.items():
        if tname not in pos:
            continue
        for fk in t.get("foreign_keys", []):
            parent = fk["references"].split(".")[0]
            if parent in pos and pos[parent] >= pos[tname]:
                errors.append(
                    f"LOAD_ORDER cycle: {tname} FK→{parent} but {parent} loads "
                    f"at/after {tname}"
                )

    return errors


if __name__ == "__main__":
    import json
    from pathlib import Path
    manifest_path = Path(__file__).resolve().parents[2] / "data" / "nflverse_manifest.json"
    manifest = None
    if manifest_path.exists():
        with manifest_path.open() as f:
            manifest = json.load(f)
        print(f"(manifest loaded: {len(manifest['nflverse_releases'])} releases)")
    else:
        print("(no manifest found — structural checks only; run scripts/v2/catalog.py first)")
    errs = validate_config(manifest)
    if errs:
        print("Config validation FAILED:")
        for e in errs:
            print(f"  {e}")
        raise SystemExit(1)
    print(f"Config OK: {len(SOURCES)} sources, {len(TABLES)} tables, "
          f"{len(FILL_RULES)} fill rules, {len(VIEWS)} views.")
