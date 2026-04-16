# nflverse DB

Scripts that turn [nflverse](https://github.com/nflverse/nflverse-data) — the gold-standard open NFL data source — into a local SQLite database you can query with plain SQL.

## Why a local SQLite DB?

nflverse publishes its data as per-season parquet files across a dozen release tags, and the mainstream way to use it (via [`nflreadpy`](https://github.com/nflverse/nflreadpy) or R's [`nflreadr`](https://github.com/nflverse/nflreadr)) loads those files into DataFrames on every call. That's great for a one-off analysis and nothing else: it costs bandwidth every session, can't answer a question without first loading the relevant files into memory, and can't cheaply join across tables (players ↔ weekly stats ↔ schedules ↔ snap counts ↔ …).

A single SQLite file fixes all of that:

- **Ask real questions in SQL.** Join `game_stats` to `players` to `games` to `snap_counts` in one query. No per-season stitching, no multi-file dataframes.
- **Offline and instant.** One `.db` file, indexed on the hot columns (`player_id`, `season`, `gsis_id`). Queries return in milliseconds.
- **Deterministic.** Build once, pin a snapshot, reproduce the same answer tomorrow.
- **Zero setup.** SQLite is file-based and every major language has bindings — Python (`sqlite3`), R (`DBI`/`RSQLite`), Go, JS, Rust, or the `sqlite3` CLI straight from a shell.
- **Small.** The full 1999–2025 player/team/stats database is ~327 MB; play-by-play (every single play, 1.28M rows) is a separate ~2 GB file.

Two databases, all nflverse-native column names, ~26 seasons of coverage:

| Database | Size | Tables | Rows | Years |
|----------|------|--------|------|-------|
| `data/nflverse.db` | 327 MB | 13 | ~2.25M | 1999–2025 |
| `data/pbp.db` | 2,082 MB | 1 | 1.28M | 1999–2025 |

Play-by-play lives in its own DB because it's too large to combine comfortably.

## Quick start

```bash
pip install -r requirements.txt

# Download raw files, then build locally
python3 scripts/download.py --all
python3 scripts/build_db.py --all

# Play-by-play (large — 466 MB download, ~2 GB DB)
python3 scripts/download.py --tables play_by_play --all
python3 scripts/build_db.py --pbp --all
```

See [Incremental updates](#incremental-updates) for pulling new seasons without a full rebuild, and [Alternative build path (nflreadpy)](#alternative-build-path-nflreadpy) if the primary download path breaks.

## Tables

**Core**: `players`, `player_ids`, `games`, `game_stats`, `season_stats`, `draft_picks`, `combine`

**Supplementary**: `snap_counts` (2012+), `ngs_stats` (2016+), `depth_charts` (2001–2024), `depth_charts_2025` (2025+, different schema), `pfr_advanced` (2018+), `qbr` (2006–2023)

**Play-by-play**: `play_by_play` in `pbp.db`.

`game_stats` and `season_stats` include all ~114 nflverse columns across every position group (offensive, defensive, kicking, special teams, penalties, advanced) — not just skill positions.

See [`docs/DATABASE.md`](docs/DATABASE.md) for the full schema reference.

## IDs and joins

Tables use the nflverse-native column names as-is. Key ID columns:

- **`player_id`** (`00-0033873`, GSIS format) — `game_stats`, `season_stats`
- **`gsis_id`** (`00-0033873`) — `players`, `player_ids`
- **PFR ID** (`MahoPa00`) — `snap_counts`, `pfr_advanced`
- **ESPN ID** (`3139477`) — `qbr`

Join stats to player bio: `game_stats.player_id = players.gsis_id`. Cross-reference IDs via `player_ids`.

`combine` has no join edges — query it independently.

## Downloading raw data

Raw parquet/CSV files are pulled from nflverse GitHub releases into `data/raw/`, organized by release tag (`data/raw/{tag}/{filename}.parquet`). These are gitignored — rebuild locally.

```bash
python3 scripts/download.py --all                              # Everything except PBP
python3 scripts/download.py --tables play_by_play --all        # PBP (~466 MB)
python3 scripts/download.py --tables game_stats --years 2025   # Specific tables/years
python3 scripts/download.py --force                            # Re-download existing
python3 scripts/download.py --dry-run --all                    # Preview
```

## Building the databases

```bash
python3 scripts/build_db.py --all                              # Full build from data/raw/
python3 scripts/build_db.py --tables game_stats --years 2025   # Specific tables/years
python3 scripts/build_db.py --pbp --all                        # Play-by-play
python3 scripts/build_db.py --dry-run --all                    # Preview only
python3 scripts/build_db.py --all --output data/nflverse.db    # Write to specific path
```

## Incremental updates

```bash
# Check what's new upstream
python3 scripts/check_updates.py           # Human-readable report
python3 scripts/check_updates.py --json    # Machine-readable JSON
python3 scripts/check_updates.py --init    # Initialize metadata from current DB

# Pull new year(s) or specific tables, then rebuild those slices
python3 scripts/download.py --years 2025 --force
python3 scripts/build_db.py --years 2025
```

## Alternative build path (nflreadpy)

`scripts/build_db_nflreadpy.py` is a second entry point that fetches data over the network through the [nflreadpy](https://github.com/nflverse/nflreadpy) Python client, maintained by the nflverse team, instead of reading from `data/raw/`.

Use it as a **fallback** when the primary path breaks — for example if nflverse reorganizes a release, renames assets, or the hardcoded URL patterns in `download.py` otherwise go stale. nflreadpy tracks upstream release-mapping changes, so a `pip install -U nflreadpy` will typically restore builds without code edits here.

Under normal conditions, prefer `download.py` + `build_db.py`: it's offline, deterministic, and has no reliance on the client's release-mapping staying in sync.

```bash
python3 scripts/build_db_nflreadpy.py --years 2025                  # Specific year
python3 scripts/build_db_nflreadpy.py --tables game_stats players   # Specific tables
python3 scripts/build_db_nflreadpy.py --pbp --years 2025            # Play-by-play
python3 scripts/build_db_nflreadpy.py --all                         # Full refresh of all tables
python3 scripts/build_db_nflreadpy.py --dry-run                     # Preview
python3 scripts/build_db_nflreadpy.py --check-first                 # Run check_updates first
```

Credit: [nflverse/nflreadpy](https://github.com/nflverse/nflreadpy) — the official Python client for nflverse data.

## Repository layout

| Path | Purpose |
|------|---------|
| `scripts/download.py` | Download raw parquet/CSV from GitHub into `data/raw/` |
| `scripts/build_db.py` | Build databases from local `data/raw/` files (primary path) |
| `scripts/build_db_nflreadpy.py` | Alternative build via [nflreadpy](https://github.com/nflverse/nflreadpy) — fallback when the primary path breaks |
| `scripts/check_updates.py` | Detect new/changed upstream data vs local DB |
| `scripts/pipeline.py` | Shared build logic (TableConfig, year-partition, backups, indexes) |
| `scripts/config.py` | DB path constants |
| `requirements.txt` | `nflreadpy`, `pandas`, `pyarrow` |
| `docs/DATABASE.md` | Full schema reference |
| `data/raw/` | Downloaded source files (gitignored) |
| `data/update_metadata.json` | Auto-generated update tracking (gitignored) |

## Notes

- `season_stats.recent_team` is backfilled from `game_stats.team` (most common team per player-season); nflverse source data doesn't populate it.
- `depth_charts` (2001–2024) and `depth_charts_2025` (2025+) are separate tables because nflverse changed the schema in 2025.
- `game_id` in `game_stats` is only populated for 2002+ (nflverse doesn't provide it for 1999–2001).
- Schema drift between years is handled automatically — missing columns are added via `ALTER TABLE` during load.
- NGS `stat_type`: `passing` / `rushing` / `receiving`; `week=0` is season totals.
- PFR `stat_type`: `pass` / `rush` / `rec` (different naming convention — watch out).
- QBR `game_week` is INTEGER, `week_text` is TEXT (use `week_text = "Season Total"` for season totals); `season_type` is `"Regular"` or `"Postseason"`.
- `nfl-data-py` was archived in September 2025 — [`nflreadpy`](https://github.com/nflverse/nflreadpy) is the successor and what this project uses.

## License

- **Code** (this repository): MIT — see [`LICENSE`](LICENSE).
- **Data**: see Data Attribution below.

## Data Attribution

The data itself is **not** part of this repository — it's downloaded from nflverse when you run the scripts. The following applies to the data and to any SQLite database you build from it.

- **Source**: [nflverse-data](https://github.com/nflverse/nflverse-data) © nflverse contributors
- **License**: [Creative Commons Attribution 4.0 International (CC-BY-4.0)](https://github.com/nflverse/nflverse-data/blob/main/LICENSE.md)
- **Disclaimer**: nflverse provides the data AS-IS and AS-AVAILABLE, with no warranties of any kind. See §5 of the license for the full warranty and liability disclaimer.

### Modifications applied by this project

Per CC-BY-4.0 §3(a)(1)(B), these build scripts modify the source data in the following ways while loading it into the database:

- Rows with null primary identifiers (`player_id` / `gsis_id`) are dropped.
- Duplicate rows are removed against `(player_id, season, week)` for `game_stats` and on `gsis_id` for `players` / `player_ids`.
- `season_stats.recent_team` is backfilled from `game_stats.team` (most common team per player-season), because nflverse does not populate it.
- A `stat_type` column is added to `ngs_stats` and `pfr_advanced` to distinguish the three sub-types (`passing`/`rushing`/`receiving` for NGS; `pass`/`rush`/`rec` for PFR).
- Schema drift across seasons is reconciled via `ALTER TABLE ADD COLUMN`; column set matches the union across all loaded years.

No column values are renamed, recalculated, or otherwise transformed — the numeric data remains as nflverse publishes it.

### If you redistribute a built database

CC-BY-4.0 requires that you carry the attribution downstream. A drop-in notice you can include with any `.db` you share:

> This database is built from [nflverse-data](https://github.com/nflverse/nflverse-data) (© nflverse contributors), licensed under [CC-BY-4.0](https://github.com/nflverse/nflverse-data/blob/main/LICENSE.md). The data has been modified during the build process (row de-duplication, null filtering, team backfill, schema reconciliation). See <https://github.com/MattWenzel/NFLVERSE-DB> for the build scripts.
