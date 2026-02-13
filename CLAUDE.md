# nflverse DB

NFL player stats database built from [nflverse](https://github.com/nflverse/nflverse-data) data.

## Quick Reference

| Database | Size | Tables | Rows | Years |
|----------|------|--------|------|-------|
| `nflverse_custom.db` | 186 MB | 12 | 1.35M | 1999-2025 |
| `pbp.db` | 535 MB | 1 | 1.23M | 1999-2024 |

**Full schema**: [docs/DATABASE.md](docs/DATABASE.md)

## Key Tables

**Core**: `players`, `player_ids`, `games`, `game_stats`, `season_stats`, `draft_picks`, `combine`

**Supplementary**: `snap_counts` (2015+), `ngs_stats` (2016+), `depth_charts` (2001+), `pfr_advanced` (2018+), `qbr` (2006-2023)

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

## ID System

- **GSIS ID** (`00-0033873`) — Primary key for most tables
- **PFR ID** (`MahoPa00`) — Used by `snap_counts`, `pfr_advanced`
- **ESPN ID** (`3139477`) — Used by `qbr`
- Join via `player_ids` table for cross-reference

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

- 2025 stats migrated from old fantasyDB (nflverse has since released 2025 data — use `update_db.py --years 2025` to refresh); team data included in both game and season logs
- `season_stats.team` is backfilled from `game_stats` (most common team per player-season); nflverse source data doesn't populate it
- Kicker stats not tracked by nflverse
- `nfl-data-py` archived Sept 2025, successor is `nflreadpy`
- `combine` table has no join edges — query separately
- NGS `stat_type`: `passing`/`rushing`/`receiving`; `week=0` = season totals
- PFR `stat_type`: `pass`/`rush`/`rec` (different naming!)
- QBR `game_week` is a string (`"Season Total"`), `season_type` is `"Regular"`/`"Postseason"`
