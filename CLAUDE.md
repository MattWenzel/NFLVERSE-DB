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

## Files

| File | Purpose |
|------|---------|
| `build_nflverse_db.py` | Builds core DB (players, game_stats, season_stats, etc.) |
| `build_pbp_db.py` | Builds play-by-play DB |
| `add_supplementary_tables.py` | Adds snap_counts, ngs_stats, depth_charts, pfr_advanced, qbr |
| `config.py` | DB path configuration |
| `requirements.txt` | Python dependencies (`nfl_data_py`, `pandas`) |
| `docs/DATABASE.md` | Full schema reference |
| `nflverse_custom.db.zip` | Pre-built DB snapshot |

## Notes

- 2025 stats migrated from old fantasyDB (nflverse hasn't released 2025 yet); team data included in both game and season logs
- `season_stats.team` is backfilled from `game_stats` (most common team per player-season); nflverse source data doesn't populate it
- Kicker stats not tracked by nflverse
- `nfl-data-py` archived Sept 2025, successor is `nflreadpy`
- `combine` table has no join edges — query separately
- NGS `stat_type`: `passing`/`rushing`/`receiving`; `week=0` = season totals
- PFR `stat_type`: `pass`/`rush`/`rec` (different naming!)
- QBR `game_week` is a string (`"Season Total"`), `season_type` is `"Regular"`/`"Postseason"`
