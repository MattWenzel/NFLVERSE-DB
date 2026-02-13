# nflverse DB

NFL player stats database built from [nflverse](https://github.com/nflverse/nflverse-data) data.

## Quick Reference

| Database | Size | Tables | Rows | Years |
|----------|------|--------|------|-------|
| `nflverse_v2.db` | 298 MB | 13 | ~2.2M | 1999-2025 |
| `pbp_v2.db` | 2,082 MB | 1 | 1.28M | 1999-2025 |

Legacy DBs (`nflverse_custom.db`, `pbp.db`) use old custom column renames — do not mix with v2.

## Key Tables

**Core**: `players`, `player_ids`, `games`, `game_stats`, `season_stats`, `draft_picks`, `combine`

**Supplementary**: `snap_counts` (2015+), `ngs_stats` (2016+), `depth_charts` (2001-2024), `depth_charts_2025` (2025+, different schema), `pfr_advanced` (2018+), `qbr` (2006-2023)

**Play-by-play**: 1.23M plays in separate `pbp.db` (too large to combine)

## Build Scripts

```bash
# Core DB — players, games, game_stats, season_stats, draft_picks, combine (1999-2024)
python3 build_nflverse_db.py

# Recent years only
python3 build_nflverse_db.py --start-year 2020

# Supplementary tables — snap_counts, ngs_stats, depth_charts, pfr_advanced, qbr
python3 add_supplementary_tables.py

# Play-by-play (separate DB, ~535 MB)
python3 build_pbp_db.py --start-year 1999 --end-year 2024
```

A pre-built snapshot is available as `nflverse_custom.db.zip` — just unzip to use.

## ID System & Column Names

All tables use **nflverse-native column names** (no custom renames). Key ID columns:

- **`player_id`** (`00-0033873`, GSIS format) — Primary key in `game_stats`, `season_stats`
- **`gsis_id`** (`00-0033873`) — Primary key in `players`, `player_ids`
- **PFR ID** (`MahoPa00`) — Used by `snap_counts`, `pfr_advanced`
- **ESPN ID** (`3139477`) — Used by `qbr`
- Join via `player_ids` table for cross-reference

Notable native names (differ from old custom renames):
- `game_stats`/`season_stats`: `player_id` (not `gsis_id`), `opponent_team` (not `opponent`), `attempts` (not `pass_attempts`), `passing_interceptions` (not `interceptions`), `sacks_suffered` (not `sacks`), `sack_yards_lost` (not `sack_yards`)
- `players`: `latest_team` (not `current_team`), `headshot` (not `headshot_url`), `college_name` (not `college`)
- `draft_picks`: `pfr_player_id` (not `pfr_id`), `pfr_player_name` (not `player_name`)
- `combine`: `pos` (not `position`), `ht` (not `height`), `wt` (not `weight`)

## Update Scripts

```bash
# Check for upstream data changes
python3 check_updates.py           # Human-readable report
python3 check_updates.py --json    # Machine-readable JSON
python3 check_updates.py --init    # Initialize metadata from current DB state

# Incremental updates (uses nflreadpy)
python3 update_db.py --years 2025                 # Update specific year(s)
python3 update_db.py --tables game_stats players   # Update specific table(s)
python3 update_db.py --pbp --years 2025            # Update play-by-play
python3 update_db.py --all                         # Force full refresh
python3 update_db.py --dry-run                     # Preview only
python3 update_db.py --check-first                 # Run check_updates first
python3 update_db.py --all --output nflverse_v2.db # Full refresh to new DB file
```

## Files

| File | Purpose |
|------|---------|
| `build_nflverse_db.py` | Builds core DB (players, game_stats, season_stats, etc.) |
| `build_pbp_db.py` | Builds play-by-play DB |
| `add_supplementary_tables.py` | Adds snap_counts, ngs_stats, depth_charts, pfr_advanced, qbr |
| `check_updates.py` | Checks GitHub releases for new/changed data vs local DB state |
| `update_db.py` | Incrementally updates databases with new data (uses `nflreadpy`) |
| `config.py` | DB path configuration (`DB_PATH`, `PBP_DB_PATH`, `METADATA_PATH`) |
| `requirements.txt` | Python dependencies (`nfl_data_py`, `nflreadpy`, `pandas`) |
| `docs/DATABASE.md` | Full schema reference |
| `nflverse_custom.db.zip` | Pre-built DB snapshot |
| `update_metadata.json` | Auto-generated tracking file (gitignored) |

## Notes

- `game_stats`/`season_stats` now include ALL ~114 nflverse columns (offensive, defensive, kicking, special teams, penalties, advanced). ~3x more rows than before (all position groups, not just skill positions)
- `season_stats.recent_team` is backfilled from `game_stats.team` (most common team per player-season); nflverse source data doesn't populate it
- `depth_charts` (2001-2024) and `depth_charts_2025` (2025+) are separate tables due to nflverse schema change in 2025
- `nfl-data-py` archived Sept 2025, successor is `nflreadpy`
- `build_nflverse_db.py` and `add_supplementary_tables.py` use old `nfl_data_py` with custom renames — kept for backwards compatibility but `update_db.py` is the primary update path
- To join stats to players: `game_stats.player_id = players.gsis_id` (same GSIS format, different column names)
- `game_id` column only populated for 2002+ in `game_stats` (nflverse doesn't provide it for 1999-2001)
- Schema drift between years is handled automatically — `update_db.py` adds missing columns via `ALTER TABLE`
- `combine` table has no join edges — query separately
- NGS `stat_type`: `passing`/`rushing`/`receiving`; `week=0` = season totals
- PFR `stat_type`: `pass`/`rush`/`rec` (different naming!)
- QBR `game_week` is a string (`"Season Total"`), `season_type` is `"Regular"`/`"Postseason"`
