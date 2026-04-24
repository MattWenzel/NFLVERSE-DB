# nflverse DB

Scripts that turn [nflverse](https://github.com/nflverse/nflverse-data) — the gold-standard open NFL data source — into a local DuckDB database you can query with plain SQL.

## Why a local DuckDB?

nflverse publishes its data as per-season parquet files across ~24 release tags, and the mainstream way to use it (via [`nflreadpy`](https://github.com/nflverse/nflreadpy) or R's [`nflreadr`](https://github.com/nflverse/nflreadr)) loads those files into DataFrames on every call. Great for a one-off analysis; not for repeated cross-table questions. A single DuckDB file fixes that:

- **Ask real questions in SQL.** 25 tables wired together with 78 foreign keys — join `game_stats → players → games → snap_counts` or `play_by_play → ftn_charting → stadiums` in one query.
- **Offline and fast.** One `.duckdb` file. Columnar storage with zone maps makes aggregates on the 1.28M-row, 372-column `play_by_play` table fly.
- **Deterministic.** Build once, pin a snapshot, reproduce the same answer tomorrow. Catalog + survey gates prevent upstream drift from sneaking in.
- **LLM-friendly.** Every player-bearing table carries `player_gsis_id` regardless of the source's native ID; every game-bearing table carries `game_id`. One join pattern across the whole DB.

| Database | Size | Tables | Rows | Years |
|---|---|---|---|---|
| `data/nflverse.duckdb` | ~1.2 GB | 25 | ~5.5M | 1999–2025 |
| `data/nflverse.sqlite` (optional) | ~3.3 GB | 25 | ~5.5M | 1999–2025 |

## Quick start

```bash
pip install -r requirements.txt

# Catalog upstream nflverse releases (one-time + anytime upstream changes)
python3 scripts/catalog.py

# Download every file declared in the schema
python3 scripts/download.py --all

# Build the DuckDB (includes play_by_play by default)
python3 scripts/build.py

# Optional: mirror to SQLite
python3 scripts/build_sqlite.py
```

Full build is ~7-15 min depending on play_by_play load speed. Incremental refresh for a specific year is much faster (see below).

## Tables

**Parents**: `players`, `player_ids`, `games`, `stadiums`

**Player-linked**: `weekly_rosters`, `combine`, `draft_picks`, `snap_counts`, `depth_charts` (2001-2024), `depth_charts_2025` (2025+ daily), `pfr_advanced`, `ngs_stats`, `qbr`, `injuries`, `contracts`, `contracts_cap_breakdown`

**Game/team-linked**: `officials`, `team_game_stats`, `team_season_stats`

**Both-linked**: `game_stats`, `season_stats`, `pfr_advanced_weekly`, `play_by_play`, `pbp_participation`, `ftn_charting`

**Views**: `v_depth_charts` — cross-schema composite of the two depth_charts tables with normalized columns.

See [`docs/DATABASE.md`](docs/DATABASE.md) for the full schema and [`docs/DESIGN_RATIONALE.md`](docs/DESIGN_RATIONALE.md) for the rules behind every non-obvious decision.

## IDs and joins

Every player-bearing row carries `player_gsis_id` — even on tables whose upstream source is PFR-native (`snap_counts`, `pfr_advanced`, `combine`) or ESPN-native (`qbr`, `depth_charts_2025`). The build backfills canonical IDs from the `players` hub so consumers write one join pattern:

```sql
SELECT p.display_name, ss.passing_yards
FROM season_stats ss
JOIN players p USING (player_gsis_id)
WHERE ss.season = 2024 AND ss.season_type = 'REG'
ORDER BY ss.passing_yards DESC LIMIT 5;
```

Source-native IDs (`player_pfr_id`, `player_espn_id`) are also present on each table for direct access without the bridge. `player_ids` is the wide cross-reference table linking to 20+ other systems (yahoo, sleeper, pff, etc.).

Role-specific player columns on `play_by_play` (`passer_player_id`, `rusher_player_id`, ...) and `games` (`home_qb_id`, `away_qb_id`) are left as-is — they describe an actor's role on a play/game.

## Build pipeline

Eleven phases, each producing an artifact the next phase validates against:

| Phase | Artifact |
|---|---|
| 0. Catalog | `data/nflverse_manifest.json` — every upstream release + its files + column schemas |
| 1. Schema audit | `scripts/schema_skeleton.py` must match the manifest; unclaimed files fail the build |
| 2. Pre-build survey | `data/survey_report.json` — per-source ID coverage and cross-source overlap |
| 3. Hub construction | `players` DataFrame via priority-merge (pandas) |
| 4. Preflight stubs | Unresolved child FK targets added to hub before any child writes |
| 5. Table writes | Child tables loaded in FK-dependency order |
| 6. ID backfill | `player_gsis_id` populated on PFR/ESPN-native tables (pandas merge, bulk-replace) |
| 7. Name-match recovery | Pre-GSIS HoF draft picks resolved by `(name, position, season-active)` (pandas) |
| 8. Fill rules | Cross-table value backfills (pandas for >100K-row tables, SQL UPDATE for `players`) |
| 9. Views + indexes | `v_depth_charts`; hash indexes on frequent joins |
| 10. Validation | `data/canary_proof.json` — 17 committed LLM queries; plus FK orphan sweep, 0-tolerance integrity gates |

See [`docs/DESIGN_RATIONALE.md`](docs/DESIGN_RATIONALE.md) for the 18 rules that each phase exists to enforce, and [`docs/LESSONS_LEARNED.md`](docs/LESSONS_LEARNED.md) for the upstream-data reality that motivated the design. Phases 6-8 join in pandas and bulk-replace tables — `UPDATE ... SET ... (SELECT ... correlated)` on a 906K-row table measured 10+ minutes; the pandas path runs in under a minute (R18).

## CLI

```bash
# Catalog
python3 scripts/catalog.py                 # refresh data/nflverse_manifest.json
python3 scripts/catalog.py --diff          # fail if upstream changed without a schema update

# Download
python3 scripts/download.py --all                           # every enabled source, all years
python3 scripts/download.py --sources weekly_rosters qbr    # specific sources
python3 scripts/download.py --years 2024 2025               # specific years
python3 scripts/download.py --dry-run --all                 # preview

# Build
python3 scripts/build.py                   # full build (all tables, incl PBP)
python3 scripts/build.py --no-pbp          # skip play_by_play
python3 scripts/build.py --years 2025      # incremental rebuild of year-partitioned tables
python3 scripts/build.py --tables qbr      # fast iteration: rewrite ONE table (seconds)
python3 scripts/build.py --finalize        # rerun phases 5-9 on existing DB (pandas-first fills)
python3 scripts/build.py --no-validate     # skip post-build canary/invariant checks

# Survey (runs inside build; also callable standalone)
python3 scripts/survey.py                  # pre-build ID-space + coverage scan
python3 scripts/survey.py --strict         # exit non-zero on any declared-gap violation

# Canary queries
python3 scripts/canary_queries.py          # regenerate data/canary_proof.json
python3 scripts/canary_queries.py --verify # diff vs committed proof, exit non-zero on regression

# Optional: SQLite sibling
python3 scripts/build_sqlite.py            # mirror the DuckDB to data/nflverse.sqlite

# Upstream delta check
python3 scripts/check_updates.py           # compare nflverse GitHub against your local state
```

**SQLite consumer note**: SQLite's FK enforcement is per-connection and **off by default**. Run `PRAGMA foreign_keys = ON` after connecting to enable enforcement (or to use `PRAGMA foreign_key_list` for metadata).

## Incremental updates

`build.py --years 2025` reloads only year-partitioned tables for that year(s). Non-year-partitioned tables (combine, draft_picks, games, players_master, schedules) stay untouched. Hub is refreshed in pandas and new players are INSERTed only; existing rows are never UPDATEd (DuckDB's FK-parent-update restriction — see [R3 in DESIGN_RATIONALE.md](docs/DESIGN_RATIONALE.md)).

```bash
python3 scripts/check_updates.py           # detect what's new
python3 scripts/download.py --years 2025 --force
python3 scripts/build.py --years 2025      # ~60s vs full-rebuild minutes
```

## Repository layout

| Path | Purpose |
|---|---|
| `scripts/catalog.py` | Walk nflverse GitHub releases, emit `data/nflverse_manifest.json` |
| `scripts/schema_generator.py` | Derive `schema_skeleton.py` from the manifest |
| `scripts/schema.py` | The declarative DB shape: SOURCES, TABLES, FILL_RULES, HUB_BUILD, LOAD_ORDER |
| `scripts/schema_skeleton.py` | Auto-generated candidate-source list; cross-checked against SOURCES in validate |
| `scripts/cleanup.py` | Single `clean_id()` function (generic / gsis modes) |
| `scripts/loaders.py` | `load_source()` — reads one SOURCES entry, returns a cleaned DataFrame |
| `scripts/hub.py` | Priority-merge builder for the `players` hub |
| `scripts/engine.py` | Write / backfill / name-match / fill-rule / validate primitives |
| `scripts/views.py` | `v_depth_charts` SQL |
| `scripts/survey.py` | Pre-build ID-space + coverage gate |
| `scripts/canary_queries.py` | Committed LLM-style query regression suite |
| `scripts/build.py` | Orchestrator — composes all phases |
| `scripts/build_sqlite.py` | Mirror the built DuckDB to a SQLite sibling |
| `scripts/download.py` | Pull raw files declared in `schema.SOURCES` into `data/raw/` |
| `scripts/check_updates.py` | Detect new/changed upstream data vs local state |
| `scripts/config.py` | Path constants |
| `data/nflverse_manifest.json` | Committed upstream catalog |
| `data/canary_proof.json` | Committed query-result proof for regression detection |
| `data/survey_report.json` | Build artifact; coverage report |
| `data/raw/` | Downloaded source files (gitignored) |
| `docs/DATABASE.md` | Schema reference — tables, columns, FKs, join examples |
| `docs/DESIGN_RATIONALE.md` | Every non-obvious design decision + the incident it addresses |
| `docs/LESSONS_LEARNED.md` | Field guide of upstream-data reality and process lessons |
| `docs/V3_COMPARISON.md` | v1 → v3 delta |
| `docs/V3_THREE_WAY_COMPARISON.md` | v1 vs v2 vs v3 live-numbers comparison |

## Notes

- `players.position` / `weekly_rosters.position` use position GROUPS (DB, LB, CB, …); `snap_counts.position` uses fine-grained ROLES (FS, WLB, LCB, …). These are complementary, not conflicting — use whichever granularity fits your query.
- `qbr.game_id` holds ESPN's numeric game_id (e.g. `260910009`), not nflverse's `2024_01_KC_BUF` format — do NOT join to `games.game_id`. ESPN-namespace join target doesn't exist in our data.
- `officials.old_game_id` matches `games.old_game_id` (NFL's YYYYMMDDGG format) — that's how to join officials to games.
- `game_stats.game_id` is populated for ~89% of rows post-build (pre-2022 gap closed via the `game_id_from_games` fill rule).
- `players.otc_id` matches `contracts.otc_id` for 8,731 of 12,152 contracts rows (68%). Not declared as FK because the other 32% are non-players (coaches/retired) — join with LEFT JOIN.
- Pre-GSIS historical players (1950s-1990s) have Elias-format IDs like `VIT276861` as `player_gsis_id`. This is by design — preserved so draft_picks + HoF queries still join.

## License

- **Code** (this repository): MIT — see [`LICENSE`](LICENSE).
- **Data**: see Data Attribution below.

## Data Attribution

The data itself is **not** part of this repository — it's downloaded from nflverse when you run the scripts. The following applies to the data and to any database you build from it.

- **Source**: [nflverse-data](https://github.com/nflverse/nflverse-data) © nflverse contributors
- **License**: [Creative Commons Attribution 4.0 International (CC-BY-4.0)](https://github.com/nflverse/nflverse-data/blob/main/LICENSE.md)
- **Disclaimer**: nflverse provides the data AS-IS and AS-AVAILABLE, with no warranties of any kind. See §5 of the license for the full warranty and liability disclaimer.

### Modifications applied by this project

Per CC-BY-4.0 §3(a)(1)(B), these build scripts modify the source data while loading it:

- Junk ID sentinels (`''`, `'0'`, `'XX-0000001'`, etc.) are normalized to NULL. Rows are preserved; only the bad-reference column is cleared.
- Player-ID columns on player-level tables are normalized to `player_gsis_id` / `player_pfr_id` / `player_espn_id` so cross-table joins don't need name translation. The `player_ids` bridge keeps its short column names.
- Duplicate rows are removed on each table's natural key.
- The `players` registry is enriched beyond nflverse's primary `players.parquet`: a priority-merge across `players.parquet`, `db_playerids.csv` (dynastyprocess), `weekly_rosters`, `draft_picks`, and `combine` produces one canonical row per player. Stubs are added for any FK target not otherwise resolvable.
- Canonical `player_gsis_id` is backfilled onto every player-bearing table via hub lookup after initial load (see `id_backfill` rules in `scripts/schema.py`).
- `draft_picks.player_gsis_id` for pre-GSIS era rows is recovered via name-match against `players.display_name` (same-season active filter).
- `season_stats` includes both REG and POST. A safety-net pass (`compute_missing_season_stats`) aggregates weekly `game_stats` into `season_stats` for any (player, season, type) combo missing from the nflverse pre-aggregated feed.
- `game_stats.game_id` is derived from `games` on (season, week, team, opponent_team) for rows where upstream didn't populate it.
- `weekly_rosters.player_pfr_id` / `player_espn_id` are filled from the players hub where the upstream source has them NULL.
- `contracts.date_of_birth`, `.draft_round`, `.draft_overall` are filled from `players`/`draft_picks` where NULL.
- Two ID-adjacent columns are namespace-renamed: `officials.game_id → old_game_id` (NFL-internal format, not nflverse canonical); `qbr.game_id` stays but is explicitly ESPN-namespaced (no FK to games).
- A `stat_type` column is added to `ngs_stats` and `pfr_advanced` to distinguish the sub-types.
- Two derived reference tables: `stadiums` (62 rows, from `games.stadium_id`), `contracts_cap_breakdown` (302K rows, flattens `contracts.cols` struct array).
- 78 foreign-key constraints declared at table-creation time; consumers auto-derive the join graph via `duckdb_constraints()`.

Individual numeric stat values from nflverse's feeds are not altered. Derived aggregate rows (season_stats augmentation, contracts cap breakdown) are computed from their upstream sources.

### If you redistribute a built database

CC-BY-4.0 requires attribution downstream. A drop-in notice:

> This database is built from [nflverse-data](https://github.com/nflverse/nflverse-data) (© nflverse contributors), licensed under [CC-BY-4.0](https://github.com/nflverse/nflverse-data/blob/main/LICENSE.md). The data has been modified during the build process — see <https://github.com/MattWenzel/NFLVERSE-DB>/README.md#modifications-applied-by-this-project for the full list.
