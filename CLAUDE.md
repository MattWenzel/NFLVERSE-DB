# CLAUDE.md

This file points Claude Code at the canonical project documentation. See [`README.md`](README.md) for the full overview, quick start, and command reference, and [`docs/DATABASE.md`](docs/DATABASE.md) for the schema reference.

## Orientation cheatsheet

- Two databases: `data/nflverse.db` (13 tables, ~2.25M rows) and `data/pbp.db` (1 table, 1.28M rows).
- Build: `python3 scripts/download.py --all && python3 scripts/build_db.py --all`.
- Incremental pull: `python3 scripts/download.py --years {year} --force && python3 scripts/build_db.py --years {year}`.
- Shared pipeline lives in `scripts/pipeline.py`; `build_db.py` (local parquet) and `build_db_nflreadpy.py` (network via nflreadpy, used only as a fallback when the primary path breaks) are thin entry points that differ only in their fetch functions.
- All tables use nflverse-native column names — no custom renames.

## Quick gotchas

- `season_stats.recent_team` is backfilled from `game_stats.team` (nflverse doesn't populate it).
- `depth_charts` is split at 2025 (`depth_charts` for 2001–2024, `depth_charts_2025` for 2025+) because the upstream schema changed.
- PFR uses `pass`/`rush`/`rec` for `stat_type`; NGS uses `passing`/`rushing`/`receiving`. Don't mix them up.
- `game_id` in `game_stats` is missing for 1999–2001.
- Join stats to player bio via `game_stats.player_id = players.gsis_id` (same GSIS format, different column names).
