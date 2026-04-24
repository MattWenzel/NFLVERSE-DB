"""Declarative description of the NFLVERSE DB shape.

Single source of truth. Every SOURCE we consume, every TABLE we produce,
every FK edge, every fill rule, every index. The engine (scripts/engine.py)
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
    canonical GSIS regex). See scripts/cleanup.py.
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
        # PFR is the native ID on snap_counts. After id_backfill fills
        # player_gsis_id via hub lookup, both columns reach ~100% coverage
        # because weekly_rosters covers every player snap_counts tracks.
        # The 15% "NULL stats" pattern observed by v1's LLM consumer was
        # NOT a bug — filter `defense_snaps > 0` or `offense_snaps > 0` to
        # exclude zero-snap depth chart entries that didn't record stats.
        "expected_gaps": {
            "null_rate.player_pfr_id.max": 0.02,
        },
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
        # Keep native `tm` (v1 consumer-compat). The pass variant has `team`
        # natively; concat preserves both columns, matching v1 behavior.
        "renames": {"pfr_id": "player_pfr_id"},
        "id_cleanup": {"player_pfr_id": "generic"},
        "add_literal_columns": {"stat_type": "rush"},
    },
    "pfr_advanced_season_rec": {
        "release_tag": "pfr_advstats",
        "pattern": "pfr_advstats/advstats_season_rec.parquet",
        "renames": {"pfr_id": "player_pfr_id"},
        "id_cleanup": {"player_pfr_id": "generic"},
        "add_literal_columns": {"stat_type": "rec"},
    },
    "pfr_advanced_season_def": {
        # NEW in v2 (v1 missed this file — 7,537 rows × 30 cols of defensive advanced stats)
        "release_tag": "pfr_advstats",
        "pattern": "pfr_advstats/advstats_season_def.parquet",
        "renames": {"pfr_id": "player_pfr_id"},
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
        # ~20% of combine rows have no PFR ID — these are combine-only
        # prospects who never reached a PFR roster page. Not a bug; data reality.
        "expected_gaps": {
            "null_rate.player_pfr_id.max": 0.22,
        },
    },

    # -------- nflverse_releases: draft_picks --------
    "draft_picks": {
        "release_tag": "draft_picks",
        "pattern": "draft_picks/draft_picks.parquet",
        "renames": {"gsis_id": "player_gsis_id", "pfr_player_id": "player_pfr_id"},
        "id_cleanup": {"player_gsis_id": "gsis", "player_pfr_id": "generic"},
        # Source has 75% NULL GSIS (pre-1995 picks predate the GSIS era).
        # Phase 6 name-match recovery fills ~60pp of those, leaving ~15-20% NULL.
        # The survey measures the source (pre-recovery) rate; post-build the
        # table rate is ~20%.
        "expected_gaps": {
            "null_rate.player_gsis_id.max": 0.80,    # source feed: ≤80% NULL
            "null_rate.player_pfr_id.max": 0.16,     # PFR coverage ~86%
        },
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
        # GSIS coverage is near-100% (this is nflverse's canonical weekly feed).
        # PFR/ESPN NULL rates are high because upstream sources the bridge to
        # PFR/ESPN inconsistently. Not a bug.
        "expected_gaps": {
            "null_rate.player_gsis_id.max": 0.01,
            "null_rate.player_pfr_id.max": 0.70,
            "null_rate.player_espn_id.max": 0.60,
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
        # FTN publishes nflverse_play_id as INTEGER; pbp uses DOUBLE. Coerce
        # so the composite FK (game_id, play_id) → play_by_play has matching
        # types.
        "force_types": {"play_id": "DOUBLE"},
    },

    # -------- nflverse_releases: officials (NEW) --------
    "officials": {
        "release_tag": "officials",
        "pattern": "officials/officials.parquet",
        # officials.game_id is NFL's internal YYYYMMDDGG format — exactly
        # matches games.old_game_id. Rename at ingest so FK declaration
        # targets the correct namespace.
        "renames": {"game_id": "old_game_id"},
        "id_cleanup": {"old_game_id": "generic", "game_key": "generic", "official_id": "generic"},
    },

    # -------- nflverse_releases: espn_data (NEW — replaces espnscrapeR CSV) --------
    "qbr_week": {
        "release_tag": "espn_data",
        "pattern": "espn_data/qbr_week_level.parquet",
        # Keep native `game_id` column name (v1 consumer-compat). Values are
        # ESPN's numeric format (e.g. '260910009'), NOT our nflverse
        # `game_id` namespace — so NO FK to games.game_id. Documented in the
        # DATABASE.md schema notes as an ESPN-namespace column.
        "renames": {"player_id": "player_espn_id"},
        "id_cleanup": {"player_espn_id": "generic", "game_id": "generic"},
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
        # old_game_id is the NFL-internal YYYYMMDDGG format; 100% populated,
        # globally unique, and is how officials.game_id references into games.
        # Declare UNIQUE so officials can FK to it.
        "unique_columns": ["old_game_id"],
        "foreign_keys": [
            # Starting QBs per game (unwired in v1/v2; fully populated from
            # 1999 modern era). home_qb_id / away_qb_id are GSIS IDs.
            {"column": "home_qb_id", "references": "players.player_gsis_id"},
            {"column": "away_qb_id", "references": "players.player_gsis_id"},
        ],
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
        # game_id FK is safe. Composite FK on (game_id, play_id) is NOT
        # declared because the source has ~1% rows (~5,396) whose (game_id,
        # play_id) isn't in play_by_play — would reject the insert. The
        # audit documented this; consumers can still JOIN on (game_id, play_id)
        # in queries, with LEFT JOIN semantics for the drift.
        "source_id": "pbp_participation",
        "foreign_keys": [
            {"column": "game_id", "references": "games.game_id"},
        ],
        "expected_gaps": {
            # Documents the 1% (game_id, play_id) non-match — data reality.
            # No declared recovery; consumers use LEFT JOIN.
        },
    },

    "ftn_charting": {
        # NEW in v2. FTN manual charting (2022+). 48K plays/year.
        # Composite FK to play_by_play — 100% match on (game_id, play_id).
        "source_id": "ftn_charting",
        "foreign_keys": [
            {"column": "game_id", "references": "games.game_id"},
            {"column": ("game_id", "play_id"),
             "references": "play_by_play.(game_id, play_id)"},
        ],
    },

    "officials": {
        # NEW in v2. Referee crews per game. `game_id` holds NFL's internal
        # YYYYMMDDGG format (e.g. '2015091000') — matches `games.old_game_id`.
        # Rename at ingest so consumers know the namespace. No FK declared
        # because ~28 officials rows (out of 21,900, <0.2%) have an
        # old_game_id that doesn't resolve to any games row — real upstream
        # data drift. FK would reject those rows; accepting the gap is the
        # "don't lose data" principle.
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
        # Composite UNIQUE so ftn_charting and pbp_participation can declare
        # FK on (game_id, play_id). play_id is per-game unique; pair is global.
        "unique_columns": [("game_id", "play_id")],
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

    # --- game_stats.game_id derivation from games lookup (v3 audit finding) ---
    # Pre-2022 game_stats files don't carry game_id. 364,760 NULL rows can be
    # recovered via (season, week, team, opponent_team) → games join. Fills
    # game_stats.game_id from 12% → ~88%+ (only rows without a matching games
    # entry stay NULL, which is data reality — upstream mismatch).
    {
        "name": "game_stats_game_id_from_games",
        "target_table": "game_stats",
        "op": "backfill_null",
        "target_column": "game_id",
        "source_table": "games",
        "source_expression": (
            "(SELECT g.game_id FROM games g "
            "WHERE g.season = game_stats.season AND g.week = game_stats.week "
            "AND ((g.home_team = game_stats.team AND g.away_team = game_stats.opponent_team) "
            "OR (g.away_team = game_stats.team AND g.home_team = game_stats.opponent_team)) "
            "LIMIT 1)"
        ),
    },

    # --- combine.draft_year/team/round from draft_picks (v3 audit finding) ---
    # combine has ~38% NULL on draft info; draft_picks knows it all. Fill via
    # player_pfr_id lookup.
    {
        "name": "combine_draft_year_from_draft_picks",
        "target_table": "combine",
        "op": "backfill_null",
        "target_column": "draft_year",
        "source_table": "draft_picks",
        "source_column": "season",
        "join": [("combine.player_pfr_id", "draft_picks.player_pfr_id")],
    },
    {
        "name": "combine_draft_team_from_draft_picks",
        "target_table": "combine",
        "op": "backfill_null",
        "target_column": "draft_team",
        "source_table": "draft_picks",
        "source_column": "team",
        "join": [("combine.player_pfr_id", "draft_picks.player_pfr_id")],
    },
    {
        "name": "combine_draft_round_from_draft_picks",
        "target_table": "combine",
        "op": "backfill_null",
        "target_column": "draft_round",
        "source_table": "draft_picks",
        "source_column": "round",
        "join": [("combine.player_pfr_id", "draft_picks.player_pfr_id")],
    },

    # --- contracts.draft_round / draft_overall from draft_picks ---
    {
        "name": "contracts_draft_round_from_draft_picks",
        "target_table": "contracts",
        "op": "backfill_null",
        "target_column": "draft_round",
        "source_table": "draft_picks",
        "source_column": "round",
        "join": [("contracts.player_gsis_id", "draft_picks.player_gsis_id")],
    },
    {
        "name": "contracts_draft_overall_from_draft_picks",
        "target_table": "contracts",
        "op": "backfill_null",
        "target_column": "draft_overall",
        "source_table": "draft_picks",
        "source_column": "pick",
        "join": [("contracts.player_gsis_id", "draft_picks.player_gsis_id")],
    },
    # --- contracts.date_of_birth from players ---
    {
        "name": "contracts_birth_date_from_players",
        "target_table": "contracts",
        "op": "backfill_null",
        "target_column": "date_of_birth",
        "source_table": "players",
        "source_column": "birth_date",
        "join": [("contracts.player_gsis_id", "players.player_gsis_id")],
    },

    # --- weekly_rosters.player_pfr_id / player_espn_id from players hub ---
    # Source NULL rates: pfr 64%, espn 54%. Every GSIS-bearing row in
    # weekly_rosters has a matching players hub row (100% FK), so lookup
    # fills the 36%/46% of rows where the hub knows the cross-ID.
    {
        "name": "weekly_rosters_pfr_from_players",
        "target_table": "weekly_rosters",
        "op": "backfill_null",
        "target_column": "player_pfr_id",
        "source_table": "players",
        "source_column": "player_pfr_id",
        "join": [("weekly_rosters.player_gsis_id", "players.player_gsis_id")],
    },
    {
        "name": "weekly_rosters_espn_from_players",
        "target_table": "weekly_rosters",
        "op": "backfill_null",
        "target_column": "player_espn_id",
        "source_table": "players",
        "source_column": "player_espn_id",
        "join": [("weekly_rosters.player_gsis_id", "players.player_gsis_id")],
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
        # Definition lives in scripts/views.py because the SQL is substantial.
        "defined_in": "scripts/views.py::v_depth_charts_sql",
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
    # play_by_play must load BEFORE pbp_participation / ftn_charting now
    # that those declare FKs into (game_id, play_id).
    "play_by_play",
    "pbp_participation",
    "ftn_charting",
]


# ===========================================================================
# Validation (self-check on config load)
# ===========================================================================

# ===========================================================================
# Explicitly-skipped manifest patterns
# ===========================================================================
# Every entry in scripts/schema_skeleton.py:SKELETON must be either present
# in SOURCES (matched by `release_tag` + `pattern` filename), OR listed here
# with a reason. This makes "we saw this upstream and chose not to use it"
# an explicit, auditable decision rather than silent omission.
#
# The auditor in validate_config() fails the build if a skeleton entry is in
# NEITHER SOURCES nor this list, which is what guarantees we can't miss
# future upstream additions.

SKIPPED_SOURCES: dict[str, str] = {
    # Specific skeleton IDs we explicitly decline to consume.
    "stats_player_regpost":          "Union of reg+post; redundant with both loaded",
    "stats_team_regpost":            "Union of reg+post; redundant with both loaded",
    "roster":                        "Annual rosters 1920-; pre-modern era, no stat-table joins",
    "teams_colors_logos":            "Display-only metadata; low LLM-query value",
    "trades":                        "Trade history; low LLM-query value (no player FK)",
    "pfr_rosters":                   "In 'misc' release; superseded by weekly_rosters",
    "pbp_participation_old":         "Pre-2023 legacy format; superseded by pbp_participation",
    "qbr_season_level":              "We load qbr_week_level and aggregate as needed",
    "otc_players":                   "Players component file; superseded by main players.parquet",
    "players_components__players":   "Components release; superseded by main players release",
    "players":                       "Duplicate of players_master via players_components release",
    "dynastyprocess_db_playerids":   "Covered by SOURCES['db_playerids']",
}

# Whole-release skips — every skeleton entry under these release_tags is
# considered explicitly skipped. Reason declared once per release.
SKIPPED_RELEASE_TAGS: dict[str, str] = {
    "player_stats":  "Legacy deprecated release; superseded by stats_player. All file patterns inside are duplicates of stats_player content.",
    "rosters":       "Annual historical rosters 1920-2025; pre-modern era mostly has no stat-table joins. Low LLM value vs complexity.",
    "misc":          "Junk drawer release; currently only `pfr_rosters.parquet` which is superseded by weekly_rosters.",
    "trades":        "Trade history; no player FK; low LLM query value.",
    "teams":         "Colors/logos display metadata.",
}


def _source_file_stem(source_spec: dict) -> str:
    """Return the bare filename stem of a source's pattern (for audit match).
    E.g. 'players/players.parquet' -> 'players'; 'stats_player/stats_player_week_{year}.parquet' -> 'stats_player_week'."""
    pat = source_spec.get("pattern", "")
    fname = pat.rsplit("/", 1)[-1]
    fname = fname.replace(".parquet", "").replace(".csv", "").replace("_{year}", "")
    return fname


def audit_against_skeleton() -> list[str]:
    """Cross-check SOURCES + SKIPPED_SOURCES against the committed skeleton.

    Returns errors if a skeleton entry is in neither SOURCES nor SKIPPED_SOURCES.
    This is the v3 gate that prevents missing upstream additions.
    """
    import importlib.util
    from pathlib import Path as _Path
    skel_path = _Path(__file__).resolve().parent / "schema_skeleton.py"
    if not skel_path.exists():
        return [f"schema_skeleton.py missing at {skel_path}. Run scripts/schema_generator.py."]
    spec = importlib.util.spec_from_file_location("schema_skeleton", skel_path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    skeleton = mod.SKELETON

    # Match by (release_tag, bare filename stem) tuple
    sources_by_stem: dict[tuple[str, str], str] = {}
    for sid, spec_ in SOURCES.items():
        stem = _source_file_stem(spec_)
        tag = spec_.get("release_tag", "_external")
        sources_by_stem[(tag, stem)] = sid

    errors: list[str] = []
    for skel_id, skel_entry in skeleton.items():
        if skel_id in SKIPPED_SOURCES:
            continue
        if skel_entry.get("release_tag") in SKIPPED_RELEASE_TAGS:
            continue
        # Match via (release_tag, stem)
        skel_tag = skel_entry.get("release_tag", "_external")
        skel_stem = _source_file_stem(skel_entry)
        if (skel_tag, skel_stem) in sources_by_stem:
            continue
        # Also accept if any SOURCES entry has the same pattern (looser match)
        if any(s.get("pattern", "").endswith(skel_entry.get("pattern", "__never__")) or
               skel_entry.get("pattern", "").endswith(s.get("pattern", "__never__"))
               for s in SOURCES.values()):
            continue
        errors.append(
            f"skeleton entry {skel_id!r} (pattern {skel_entry.get('pattern')!r} "
            f"tag {skel_tag!r}) is in neither SOURCES nor SKIPPED_SOURCES. "
            f"Add an override entry in SOURCES or a reason to SKIPPED_SOURCES."
        )
    return errors


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
    manifest_path = Path(__file__).resolve().parents[1] / "data" / "nflverse_manifest.json"
    manifest = None
    if manifest_path.exists():
        with manifest_path.open() as f:
            manifest = json.load(f)
        print(f"(manifest loaded: {len(manifest['nflverse_releases'])} releases)")
    else:
        print("(no manifest found — structural checks only; run scripts/catalog.py first)")
    errs = validate_config(manifest)
    if errs:
        print("Config validation FAILED:")
        for e in errs:
            print(f"  {e}")
        raise SystemExit(1)

    skel_errs = audit_against_skeleton()
    if skel_errs:
        print("\nSkeleton audit FAILED (upstream drift not accounted for):")
        for e in skel_errs:
            print(f"  {e}")
        raise SystemExit(1)

    print(f"Config OK: {len(SOURCES)} sources, {len(TABLES)} tables, "
          f"{len(FILL_RULES)} fill rules, {len(VIEWS)} views.")
    print(f"Skeleton audit OK: all candidates either in SOURCES or SKIPPED_SOURCES "
          f"({len(SKIPPED_SOURCES)} explicit skips).")
