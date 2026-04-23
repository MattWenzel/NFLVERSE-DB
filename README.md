# nflverse DB

Scripts that turn [nflverse](https://github.com/nflverse/nflverse-data) — the gold-standard open NFL data source — into a local DuckDB database you can query with plain SQL.

## Why a local DuckDB?

nflverse publishes its data as per-season parquet files across a dozen release tags, and the mainstream way to use it (via [`nflreadpy`](https://github.com/nflverse/nflreadpy) or R's [`nflreadr`](https://github.com/nflverse/nflreadr)) loads those files into DataFrames on every call. That's great for a one-off analysis and nothing else: it costs bandwidth every session, can't answer a question without first loading the relevant files into memory, and can't cheaply join across tables (players ↔ weekly stats ↔ schedules ↔ snap counts ↔ …).

A single DuckDB file fixes all of that:

- **Ask real questions in SQL.** Join `game_stats` to `players` to `games` to `snap_counts` — or `play_by_play` to anything — in one query. No per-season stitching, no cross-database ATTACH.
- **Offline and fast.** One `.duckdb` file. Columnar storage with zone maps makes aggregates on the 1.28M-row, 372-column `play_by_play` table fly.
- **Deterministic.** Build once, pin a snapshot, reproduce the same answer tomorrow.
- **Zero setup.** DuckDB is file-based with bindings for Python (`duckdb`), R, Node, Rust, and a standalone CLI.
- **Small.** All 14 tables including play-by-play fit in a single ~740 MB file — the columnar format compresses the old 2.4 GB SQLite pair down by roughly 3×.

One database, ~26 seasons of coverage. Column names mostly match nflverse upstream, with player ID columns normalized to `player_gsis_id` / `player_pfr_id` / `player_espn_id` so cross-table joins are unambiguous (see [IDs and joins](#ids-and-joins)):

| Database | Size | Tables | Rows | Years |
|----------|------|--------|------|-------|
| `data/nflverse.duckdb` | ~740 MB | 14 | ~3.5M | 1999–2025 |

## Quick start

```bash
pip install -r requirements.txt

# Download raw files, then build locally
python3 scripts/download.py --all
python3 scripts/build_db.py --all

# Play-by-play (large — 466 MB download; table is ~1.28M rows)
python3 scripts/download.py --tables play_by_play --all
python3 scripts/build_db.py --pbp --all
```

See [Incremental updates](#incremental-updates) for pulling new seasons without a full rebuild, and [Alternative build path (nflreadpy)](#alternative-build-path-nflreadpy) if the primary download path breaks.

## Tables

**Core**: `players`, `player_ids`, `games`, `game_stats`, `season_stats`, `draft_picks`, `combine`

**Supplementary**: `snap_counts` (2015+), `ngs_stats` (2016+), `depth_charts` (2001–2024), `depth_charts_2025` (2025+, different schema), `pfr_advanced` (2018+), `qbr` (2006–2023)

**Play-by-play**: `play_by_play` (same file — joins directly against `games`, `players`, etc. with no ATTACH).

`game_stats` and `season_stats` include all ~114 nflverse columns across every position group (offensive, defensive, kicking, special teams, penalties, advanced) — not just skill positions.

**Cross-schema view**: `v_depth_charts` UNIONs `depth_charts` (2001–2024 weekly) and `depth_charts_2025` (2025+ daily) into a single normalized schema — `season`, `week`, `team`, `position`, `pos_abb`, `depth_rank`, `formation`, plus a `source` tag. Columns the 2025 data doesn't natively carry (`season`, `week`, `position`) are derived honestly from what it does have (`dt`, `pos_abb`, a `games`-table lookup for week). See [`docs/DATABASE.md`](docs/DATABASE.md#view-v_depth_charts-composite-across-schemas) for the column map and an example query.

See [`docs/DATABASE.md`](docs/DATABASE.md) for the full schema reference, and [`docs/INGESTION.md`](docs/INGESTION.md) for the ingestion-pipeline playbook (how to add a new season, how to add a new data source, failure modes and fixes, design principles).

## IDs and joins

Player-level tables use a normalized ID-column convention so joins are unambiguous:

- **`player_gsis_id`** (`00-0033873`, GSIS format) — on `players`, `game_stats`, `season_stats`, `depth_charts`, `depth_charts_2025`, `draft_picks`, `ngs_stats`
- **`player_pfr_id`** (`MahoPa00`) — on `players`, `draft_picks`, `combine`, `pfr_advanced`, `snap_counts`
- **`player_espn_id`** (`3139477`, stored as VARCHAR) — on `players`, `depth_charts_2025`, `qbr`

Join stats to player bio directly: `game_stats.player_gsis_id = players.player_gsis_id`. Cross-reference between ID systems via `player_ids`, which keeps its short bridge-table column names (`gsis_id`, `pfr_id`, `espn_id`, `yahoo_id`, `sleeper_id`, …).

Semantic `*_player_id` columns on `play_by_play` (`passer_player_id`, `rusher_player_id`, `kicker_player_id`, etc.) and `games` (`home_qb_id`, `away_qb_id`) are left as-is because they describe an actor's role on a play/game, not the ID system.

`combine` joins to `players` via `player_pfr_id`.

## Downloading raw data

Raw parquet/CSV files are pulled from nflverse GitHub releases into `data/raw/`, organized by release tag (`data/raw/{tag}/{filename}.parquet`). These are gitignored — rebuild locally.

```bash
python3 scripts/download.py --all                              # Everything except PBP
python3 scripts/download.py --tables play_by_play --all        # PBP (~466 MB)
python3 scripts/download.py --tables game_stats --years 2025   # Specific tables/years
python3 scripts/download.py --force                            # Re-download existing
python3 scripts/download.py --dry-run --all                    # Preview
```

## Building the database

```bash
python3 scripts/build_db.py --all                                  # Full build from data/raw/
python3 scripts/build_db.py --tables game_stats --years 2025       # Specific tables/years
python3 scripts/build_db.py --pbp --all                            # Play-by-play
python3 scripts/build_db.py --dry-run --all                        # Preview only
python3 scripts/build_db.py --all --output data/nflverse.duckdb    # Write to specific path
```

Each year-partitioned insert runs inside a single transaction — a failed fetch or integrity check rolls back cleanly and leaves the DB unchanged. Out-of-range years (e.g. `--years 2025 --tables depth_charts`, where the legacy `depth_charts` table ends in 2024) are skipped with a clear message rather than silently loading mismatched data.

### Optional: also build a SQLite sibling

After the DuckDB build, you can mirror the same data + schema + 60 FKs into a SQLite file for tools that prefer it:

```bash
python3 scripts/build_sqlite.py                     # writes data/nflverse.sqlite
python3 scripts/build_sqlite.py --no-vacuum         # skip VACUUM (~30s faster)
```

Takes ~2-3 min, produces ~2.5 GB file (vs ~940 MB for DuckDB — SQLite is row-store, no columnar compression). Row-count + FK + orphan parity with the DuckDB is verified as part of the build.

**Consumer-side caveat:** SQLite's FK enforcement is per-connection and **off by default**. Clients that want enforcement (or want to query FK metadata via `PRAGMA foreign_key_list`) must run `PRAGMA foreign_keys = ON` after connecting. The DDL is declared either way — it's just not enforced unless the pragma is set.

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
| `scripts/build_sqlite.py` | Mirror the built DuckDB to a SQLite sibling at `data/nflverse.sqlite` (same data, same 60 FKs, same indexes) |
| `scripts/config.py` | DB path constants |
| `requirements.txt` | `nflreadpy`, `pandas`, `pyarrow`, `duckdb` |
| `docs/DATABASE.md` | Full schema reference |
| `docs/INGESTION.md` | Pipeline playbook (adding seasons/tables, failure modes, design principles) |
| `data/raw/` | Downloaded source files (gitignored) |
| `data/update_metadata.json` | Auto-generated update tracking (gitignored) |

## Notes

- `season_stats.recent_team` is backfilled from `game_stats.team` (most common team per player-season); nflverse source data doesn't populate it.
- `depth_charts` (2001–2024) and `depth_charts_2025` (2025+) are separate tables because nflverse changed the schema in 2025.
- `game_id` in `game_stats` is populated only for some seasons (currently 2022, 2023, 2025). Seasons 1999–2021 and 2024 leave it NULL — upstream nflverse hasn't backfilled them. Filter on `game_id IS NOT NULL` or join on `(season, week, team, opponent_team)` against `games` when you need game context for the unpopulated years.
- Schema drift between years is handled automatically — missing columns are added via `ALTER TABLE` during load.
- NGS `stat_type`: `passing` / `rushing` / `receiving`; `week=0` is season totals.
- PFR `stat_type`: `pass` / `rush` / `rec` (different naming convention — watch out).
- QBR is weekly-only (no season-total rows) — aggregate with `AVG(qbr_total)` grouped by player + season. `game_week` is INTEGER, `season_type` is `"Regular"` or `"Postseason"`.
- `nfl-data-py` was archived in September 2025 — [`nflreadpy`](https://github.com/nflverse/nflreadpy) is the successor and what this project uses.

## License

- **Code** (this repository): MIT — see [`LICENSE`](LICENSE).
- **Data**: see Data Attribution below.

## Data Attribution

The data itself is **not** part of this repository — it's downloaded from nflverse when you run the scripts. The following applies to the data and to any database you build from it.

- **Source**: [nflverse-data](https://github.com/nflverse/nflverse-data) © nflverse contributors
- **License**: [Creative Commons Attribution 4.0 International (CC-BY-4.0)](https://github.com/nflverse/nflverse-data/blob/main/LICENSE.md)
- **Disclaimer**: nflverse provides the data AS-IS and AS-AVAILABLE, with no warranties of any kind. See §5 of the license for the full warranty and liability disclaimer.

### Modifications applied by this project

Per CC-BY-4.0 §3(a)(1)(B), these build scripts modify the source data in the following ways while loading it into the database:

- Rows with null primary identifiers (`player_gsis_id`) are dropped.
- Duplicate rows are removed against `(player_gsis_id, season, week)` for `game_stats` and on `player_gsis_id` / `gsis_id` for `players` / `player_ids`.
- `season_stats.recent_team` is backfilled from `game_stats.team` (most common team per player-season), because nflverse does not populate it.
- A `stat_type` column is added to `ngs_stats` and `pfr_advanced` to distinguish the three sub-types (`passing`/`rushing`/`receiving` for NGS; `pass`/`rush`/`rec` for PFR).
- Schema drift across seasons is reconciled via `ALTER TABLE ADD COLUMN`; column set matches the union across all loaded years.
- Player-ID columns on player-level tables are normalized to `player_gsis_id` / `player_pfr_id` / `player_espn_id` so cross-table joins don't require name translation. The `player_ids` bridge table keeps its short column names (`gsis_id`, `pfr_id`, `espn_id`, …) because it exists to cross-reference all ID systems.
- Two ID columns whose pandas-inferred types were lossy are coerced to `VARCHAR`: `qbr.player_espn_id` (was BIGINT) and `player_ids.espn_id` (was DOUBLE with null handling producing float artifacts).
- Obvious junk ID sentinels (`''`, `'0'`, `'XX-0000001'` etc.) that nflverse uses as "no ID" placeholders for pre-2001 data are normalized to NULL. Rows are preserved; only the invalid reference column is cleared.
- The `players` registry is enriched beyond nflverse's primary `players.parquet` to cover every ID referenced anywhere in the DB. Stub rows come from the `player_ids` bridge and from each child table's own metadata (name + position + team + school). This is what lets every declared foreign key resolve to an existing `players` row.
- Before children stub NULL-GSIS rows, a preflight pass reads each child source and attaches any PFR/ESPN IDs that uniquely name-match an existing GSIS-bearing player — so one person doesn't end up with a GSIS row and a separate NULL-GSIS stub under a different ID keyspace. See `docs/INGESTION.md` §12.3.
- Sixty foreign-key constraints are declared at table-creation time. Consumers can auto-derive a join graph by querying `duckdb_constraints()` (see `docs/DATABASE.md`).
- `season_stats` includes both REG and POST rows. Per-season aggregates come from nflverse's `stats_player_reg_{year}` and `stats_player_post_{year}` files. A safety-net pass (`compute_missing_season_stats` in `scripts/pipeline.py`) fills any (player, season, type) combination present in `game_stats` but missing from the downloaded aggregates by SUMming the weekly rows. Derived rows leave ratio columns (`passer_rating`, `fg_pct`, etc.) NULL — consumers recompute from the additive components. See `docs/INGESTION.md` §12.2.

Individual numeric stat values from nflverse's source feeds are not renamed, recalculated, or otherwise transformed — the SUM-of-weeks augmentation above only creates rows that the nflverse pre-aggregated feed omits.

### If you redistribute a built database

CC-BY-4.0 requires that you carry the attribution downstream. A drop-in notice you can include with any `.duckdb` you share:

> This database is built from [nflverse-data](https://github.com/nflverse/nflverse-data) (© nflverse contributors), licensed under [CC-BY-4.0](https://github.com/nflverse/nflverse-data/blob/main/LICENSE.md). The data has been modified during the build process (row de-duplication, null filtering, `season_stats.recent_team` backfill, `stat_type` column added to `ngs_stats`/`pfr_advanced`, cross-season schema reconciliation, normalization of player-ID column names to `player_gsis_id` / `player_pfr_id` / `player_espn_id`, and VARCHAR coercion of `qbr.player_espn_id` and `player_ids.espn_id`). See <https://github.com/MattWenzel/NFLVERSE-DB> for the build scripts.
