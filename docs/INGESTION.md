# Ingestion Pipeline — Architecture, Conventions, and Playbook

This document exists so future additions (new seasons, new tables, new source types) can be ingested cleanly without rediscovering every trap. It pairs with:

- [`DATABASE.md`](DATABASE.md) — the schema reference (what's in the DB)
- `scripts/pipeline.py` — the shared build logic
- `scripts/build_db.py` — local-parquet fetch functions and TableConfig registry
- `scripts/build_db_nflreadpy.py` — network-fetch fallback

If something in the codebase disagrees with this doc, trust the code and update the doc — don't paper over a mismatch.

---

## 1. Pipeline in one picture

```
  nflverse GitHub releases ──► download.py ──► data/raw/**/*.parquet
                                                      │
                                                      ▼
                                      ┌────── fetch_fn(years) ────────┐
                                      │  (pandas-side cleanup here)   │
                                      │   • rename columns            │
                                      │   • clean junk IDs → NULL     │
                                      │   • explicit VARCHAR casts    │
                                      │   • players-only: merge       │
                                      │     player_ids bridge for     │
                                      │     registry enrichment       │
                                      └──────────────┬────────────────┘
                                                     │ DataFrame
                                                     ▼
                              ┌─ stub_players_for_config(conn, cfg, df) ─┐
                              │  Insert any parent rows a child will     │
                              │  reference that aren't in `players` yet. │
                              │  Minimal rows from child's own columns.  │
                              │  (INSERT ON CONFLICT DO NOTHING — never  │
                              │   updates existing player rows.)         │
                              └──────────────────┬───────────────────────┘
                                                 │
                                                 ▼
                              ┌─── one of four update modes ────────────┐
                              │                                         │
                              │   year_partition  (most year-sliced     │
                              │                    tables: game_stats,  │
                              │                    season_stats, ...)   │
                              │                                         │
                              │   year_partition_upsert  (FK-parent     │
                              │                    tables that are      │
                              │                    year-sliced: games)  │
                              │                                         │
                              │   full_replace    (single-file sources: │
                              │                    combine, draft_picks,│
                              │                    pfr_advanced, qbr)   │
                              │                                         │
                              │   upsert          (FK-parent table:     │
                              │                    players — can't      │
                              │                    DROP while children  │
                              │                    reference it)        │
                              │                                         │
                              │   bulk_parquet    (huge multi-year      │
                              │                    tables: games and    │
                              │                    play_by_play — read  │
                              │                    all parquets in one  │
                              │                    pass via DuckDB      │
                              │                    native reader)       │
                              │                                         │
                              └──────────────────┬──────────────────────┘
                                                 │
                                                 ▼
                                      DuckDB table with:
                                        • explicit DDL
                                        • UNIQUE / PRIMARY KEY
                                        • FOREIGN KEY constraints
                                                 │
                                                 ▼
                                    Post-build steps (pipeline.run):
                                        • backfill_season_stats_team
                                        • create_indexes  (on --all)
                                        • check_integrity
                                        • create_views (v_depth_charts)
```

---

## 2. `TableConfig` — the contract for every table

Every entry in `scripts/build_db.py:TABLE_CONFIGS` is a `TableConfig` instance (`scripts/pipeline.py:TableConfig`). Fields, in increasing order of "only set if needed":

| Field | Required | Purpose |
|-------|----------|---------|
| `name` | yes | Table name in DuckDB |
| `update_mode` | yes | One of the five modes above |
| `fetch_fn` | yes* | Callable returning a pandas DataFrame. *Omittable for `bulk_parquet` if you don't need the fallback — but `build_db_nflreadpy.py` still calls it for network fetches. |
| `dedup_cols` | optional | Subset of columns for `df.drop_duplicates`. Applied after fetch. |
| `drop_na_col` | optional | Drop rows where this column is NULL after fetch. |
| `primary_key` | optional | Column name. Emitted as `UNIQUE` in DDL (not `PRIMARY KEY` — see §4). Used as the conflict target for `upsert` / `year_partition_upsert`. |
| `unique_cols` | optional | Additional columns to get `UNIQUE` constraints. |
| `foreign_keys` | optional | List of `(col, ref_table, ref_col)` tuples. Emitted as `FOREIGN KEY` clauses in DDL and enforced on every INSERT. |
| `stub_source` | optional | Dict `{child_col: {player_col: source_col_in_df, ...}}`. Tells `stub_players_from_child` how to synthesize a minimal `players` row from this child's own data when a referenced ID isn't in `players` yet. |
| `parquet_glob` | for `bulk_parquet` | Absolute path (with wildcards allowed). `DuckDB.read_parquet` reads it natively. |
| `gsis_id_cols` | for `bulk_parquet` | Columns to scrub with the strict GSIS regex in SQL. |
| `id_cols` | for `bulk_parquet` | Columns to scrub with loose junk-only cleanup in SQL. |
| `force_varchar_cols` | for `bulk_parquet` | Columns to coerce to `VARCHAR` via `ALTER TABLE ALTER ... SET DATA TYPE`. Needed when a column is all-NULL in early years and DuckDB would otherwise infer it as `INT`. |

---

## 3. Update modes — when to use which

### 3.1 `year_partition` (default for year-sliced child tables)

Use when:
- The source ships per-year parquets (`stats_player_week_{year}.parquet`, etc.)
- The table is small enough per year that pandas handles it comfortably
- The table is a **child** (has FKs out but no FKs point at it)

Flow: for each year, fetch df → apply dedup/dropna → stub players if any FK → DELETE WHERE season = Y → INSERT.

### 3.2 `year_partition_upsert`

Use when:
- Same as `year_partition` shape (per-year source files)
- BUT this table is an **FK parent** (other tables hold FKs pointing at its PK) — e.g. `games`.

Flow: for each year, fetch df → apply dedup/dropna → INSERT ON CONFLICT (`primary_key`) DO UPDATE. No DELETE. Child tables keep their FK targets stable.

### 3.3 `full_replace`

Use when:
- The source is a single parquet/CSV covering all years (`combine.parquet`, `db_playerids.csv`)
- The table is a child (no FKs point at it)

Flow: fetch entire df → apply cleanups → stub players → DROP TABLE IF EXISTS → CREATE TABLE with DDL (including FKs) → INSERT.

### 3.4 `upsert` (players only, currently)

Use when:
- The table is an **FK parent** with children holding FKs to it
- Single-file source
- You want to preserve old rows that may no longer appear in the source parquet (retired players, etc.)

Flow: fetch df → apply cleanups → if table exists INSERT ON CONFLICT (`primary_key`) DO UPDATE; else CREATE + INSERT.

Key constraint: DuckDB can't DROP a parent table that has inbound FKs, so `full_replace` won't work once children exist. `upsert` sidesteps this.

### 3.5 `bulk_parquet` (for games and play_by_play)

Use when:
- The source is a year-globbed set of parquets (or a single parquet) and the table is large enough that pandas-based year-by-year loading is slow or hits schema-drift pain
- Especially: any column that can be all-NULL in early years (DuckDB infers `INT` from pandas NULLs and chokes on strings in later years)

Flow: pipeline calls `pipeline.bulk_load_from_parquet_glob` which:
1. Creates a TEMP table via `SELECT * FROM read_parquet('glob', union_by_name=true)` — one pass, DuckDB-native, multi-threaded, full schema inferred across every file.
2. Force-casts drift-prone columns to VARCHAR via ALTER.
3. Cleans junk IDs in a single SQL `UPDATE` with all column CASE-WHENs in one pass.
4. Stubs any missing `players` rows referenced by the staged data, via a single UNION ALL INSERT (not N separate INSERTs).
5. Builds final DDL from the staging table's column types + FK clauses, creates the real table, bulk-INSERTs staging into it, drops staging.

Why this exists: the year-by-year path hit pandas-level type inference mismatches on ~6 different PBP columns (`nfl_detail_id`, `nfl_api_id`, `start_time`, etc.) as each year brought different data. `bulk_parquet` eliminates that whole class of failure because DuckDB sees the full set of files before picking types. Also went from ~10 min per full PBP rebuild to ~1 min.

---

## 4. ID handling — the hard-won rules

### 4.1 Two cleanup functions, and when to use each

`pipeline.py` provides two cleanup helpers that MUST be applied appropriately:

- **`clean_id_series(s)`** — loose: empty string, `'0'`, `'None'`, `'nan'`, `'NaN'`, `'<NA>'` → NULL. Keeps anything else.
- **`clean_gsis_id_series(s)`** — strict: does `clean_id_series`, then additionally requires `^\d{2}-\d{7}$` (GSIS format like `00-0033873`); anything else → NULL.

**Rule:** use `clean_gsis_id_series` on GSIS columns in **child** tables (they only ever hold modern-era IDs; anything else is a real data error we want to null out). Use `clean_id_series` on the `players.player_gsis_id` source column itself — the primary `players.parquet` contains pre-GSIS-era records (Joe Montana, Dan Marino, Jerry Rice era) with Elias-style IDs like `VIT276861` that are real players we must not drop.

There's an easy trap: `_fetch_players` previously used `clean_gsis_id_series` and quietly dropped ~6,085 historical player records. The fix is in commit `eea6995`. If you find yourself editing `_fetch_players`, check for regression by diffing row counts against the prior build.

### 4.2 Why `UNIQUE` not `PRIMARY KEY` on `players` ID columns

`PRIMARY KEY` implies `NOT NULL`. Some stub rows legitimately have only one of the three ID columns populated (e.g., a combine-only college player has `player_pfr_id` but no GSIS or ESPN). `UNIQUE` allows NULLs, and DuckDB's FK target requirement is "referenced column is PK or UNIQUE" — `UNIQUE` satisfies it.

`games.game_id` is `PRIMARY KEY` because every game has an ID and we don't stub games from child data.

### 4.3 `players` enrichment happens in pandas, not SQL

DuckDB implements `UPDATE` as `DELETE + INSERT` internally. Any SQL `UPDATE` on `players` after children hold FKs triggers DuckDB's "can't delete row that's still referenced" error, even if the UPDATE touches a non-FK column.

**Rule:** never SQL-UPDATE an FK-target table. All `players` mutation happens before the first child INSERT:

1. `_fetch_players` in `build_db.py` does a pandas-level merge of `players.parquet` with the `player_ids` bridge. This adds stub rows for bridge-only GSIS IDs and backfills NULL `player_pfr_id` / `player_espn_id` from the bridge.
2. After `players` is written, child INSERTs proceed. Any still-missing FK targets get stubbed via `INSERT ... ON CONFLICT DO NOTHING` from the child's own metadata (`stub_players_for_config`).

No SQL `UPDATE` touches `players` at any point. The DuckDB limitation never fires.

### 4.4 Stub rows — what to expect in `players`

After a full build, `players` contains:

- ~24,400 rows from `players.parquet` (nflverse's canonical registry)
- ~60-70 stub rows from the `player_ids` bridge (gsis_ids in the bridge but not in the primary parquet)
- Child-sourced stubs:
  - ~300 from `combine.player_pfr_id` (college-only players who went to the combine but never made an NFL roster)
  - ~50-80 from `draft_picks.player_pfr_id` (historical draft picks nflverse didn't promote to the main players file)
  - ~20-30 from `snap_counts.player_pfr_id` per recent season (practice-squad / fringe roster players)
  - ~180-200 from `depth_charts_2025.player_espn_id` (practice-squad entries with only ESPN IDs)
  - Tiny numbers from `depth_charts`, `pfr_advanced`, `qbr`, `season_stats`, `ngs_stats`, PBP role columns

Total: ~700-1000 stubs depending on season. Stubs have a valid ID in one of the three columns and whatever metadata the source provided (often just name + position, sometimes also team or school).

You can identify stubs if needed: they typically have NULL `status` and NULL `rookie_season` and NULL measurables. But the main point of stubs is to keep child FKs valid — they're not intended to be queried as primary records.

### 4.5 Name-based GSIS recovery (`recover_gsis_by_name`)

When a child row has NULL `player_gsis_id` but a populated name column (e.g. `'R.Rodgers'`, `'Marshall Faulk'`, `'S.Gregory'`), the build attempts to fill in the canonical GSIS by looking it up in `players` via `(last_name, first-token, active-that-season)`. Only fills when exactly one player matches — ambiguous cases stay NULL.

Current coverage (per build):

- **draft_picks**: ~7,100 recoveries per build. The empty-string `gsis_id` for pre-1995 picks (Marshall Faulk, Trent Dilfer, Willie McGinest, etc.) gets normalized to NULL by the cleanup, then recovered by matching `pfr_player_name` against `players.display_name` + `players.last_name`.
- **depth_charts_2025**: ~1,050 recoveries. Many 2025 practice-squad entries ship with only ESPN ID + `player_name` and no GSIS; name-match fills them.
- **game_stats**: ~3 per build (initials-format names like `'R.Rodgers 2018 SEA'` → Richard Rodgers).
- **season_stats**: ~1 per build.

The helper searches these name columns in order (first present wins): `player_name`, `player_display_name`, `pfr_player_name`, `full_name`, `player`. Handles both initials-format (`'S.Fernando'`) and full-name format (`'Marshall Faulk'`) via one regex.

To extend recovery to a new child table, just ensure the fetched DataFrame has one of those name columns — the helper picks up the first match automatically. No TableConfig change needed.

### 4.6 NULL-gsis rows are legitimate — don't drop them

After `clean_gsis_id_series` normalizes junk (`''`, `'0'`, `'XX-*'`, non-regex-matching) to NULL, some rows will have NULL `player_gsis_id`. These fall into three categories:

| Kind | Example | FK behavior |
|------|---------|-------------|
| Team-level stats | `game_stats` rows with `player_name = 'Team'` and only `penalties`/`penalty_yards` populated | Correctly no player — NULL is the right value |
| Empty placeholder | `game_stats` rows with all NULLs except `(season, week, opponent_team)` | nflverse ships these; preserved for completeness |
| Unattributed historical stats | Pre-2001 defensive tackle rows with junk gsis and no player name | Real stat data, unknown author |

**Rule:** **never** set `drop_na_col='player_gsis_id'` on a child table. DuckDB's FK allows NULL; these rows contribute to aggregate queries (`SUM(def_tackles_solo) FROM game_stats WHERE season = 2000`) but won't join to `players`.

Historical note: we once had `drop_na_col='player_gsis_id'` on `game_stats` and `season_stats`. Audit vs pre-FK backup showed it silently dropped ~22 game_stats rows of real defensive-tackle data (plus S.Fernando's 2000 season) in the 1999-2000 era, plus ~21/year of team-level penalty stats modern-era. Fix in commit `79caee3`: removed the `drop_na_col` on both configs, preserved every row.

The only tables that keep `drop_na_col`:
- `players`: `drop_na_col='player_gsis_id'` — the primary registry needs a key column; stubs with only PFR/ESPN come via separate enrichment, not the parquet load path.
- `player_ids`: `drop_na_col='gsis_id'` — the bridge has no purpose without a GSIS.

---

## 5. Foreign keys — the 60-edge declaration

FKs are declared at `CREATE TABLE` time via `_create_table_from_df` emitting explicit DDL when the TableConfig carries `primary_key`/`unique_cols`/`foreign_keys`. DuckDB enforces them on every INSERT afterward.

The canonical query to list them:

```sql
SELECT
  table_name,
  constraint_column_names[1] AS column_name,
  referenced_table,
  referenced_column_names[1] AS referenced_column
FROM duckdb_constraints()
WHERE constraint_type = 'FOREIGN KEY'
ORDER BY table_name, column_name;
```

**Important:** `information_schema.constraint_column_usage` in DuckDB reports the child table as the `ref_table` (a known DuckDB limitation). `duckdb_constraints()` is the correct source for auto-derivation.

Consumer code (e.g., `NFL_AI_AGENT/tools/schema_metadata.py:JOIN_EDGES`) can be auto-generated from this query. See [`DATABASE.md § Foreign Keys`](DATABASE.md#foreign-keys) for the current edge list.

---

## 6. Adding a new season

### 6.1 Common case: nflverse ships 2026 regular-season data

1. Download the new data:
   ```bash
   python3 scripts/download.py --years 2026 --force
   ```
   This updates `data/raw/stats_player/*_2026.parquet`, `data/raw/pbp/play_by_play_2026.parquet`, etc.

2. Rebuild the affected slices:
   ```bash
   # For year-partition tables (game_stats, season_stats, snap_counts, depth_charts):
   python3 scripts/build_db.py --years 2026

   # For full_replace tables (combine, draft_picks, pfr_advanced, qbr):
   python3 scripts/build_db.py --tables combine draft_picks pfr_advanced qbr

   # For the full_replace bulk parquet tables (games, play_by_play):
   python3 scripts/build_db.py --tables games   # bulk_parquet reads all years in one pass
   python3 scripts/build_db.py --tables play_by_play --pbp
   ```

3. Verify:
   ```bash
   # Should report 0 orphans on every FK
   python3 -c "
   import duckdb
   c = duckdb.connect('data/nflverse.duckdb', read_only=True)
   fks = c.execute(\"SELECT table_name, constraint_column_names[1], referenced_table, referenced_column_names[1] FROM duckdb_constraints() WHERE constraint_type = 'FOREIGN KEY'\").fetchall()
   bad = 0
   for child, col, ref_t, ref_c in fks:
       n = c.execute(f'SELECT COUNT(*) FROM \"{child}\" ch WHERE ch.\"{col}\" IS NOT NULL AND NOT EXISTS (SELECT 1 FROM \"{ref_t}\" p WHERE p.\"{ref_c}\" = ch.\"{col}\")').fetchone()[0]
       if n > 0: print(f'  FAIL {child}.{col}: {n}'); bad += 1
   print(f'{len(fks)} edges, {bad} orphaned')
   "
   ```

### 6.2 What happens automatically

- **Junk IDs**: already-handled by the fetch functions on every rebuild.
- **New player records**: `_fetch_players` re-merges with the bridge; `stub_players_for_config` handles any child-only IDs (new rookies, practice-squad call-ups). No manual intervention needed.
- **Schema drift**: `_add_missing_columns` handles new columns for year_partition tables; `bulk_parquet` mode sees all years at once and always has the union schema.
- **FK validation**: all INSERTs are FK-validated. If the build succeeds, data is consistent by construction.

### 6.3 What to watch for

- If `build_db.py` reports "ERROR (rolled back)" on a year, data for that year is NOT in the DB. Read the error, fix the root cause, re-run. Never skip past a rollback.
- If a brand-new player's GSIS/PFR/ESPN ID isn't in the bridge and isn't in any already-declared `stub_source`, the first INSERT referencing them fails. Either add them to players.parquet upstream or extend `stub_source` for that table.
- If DuckDB reports `Conversion Error` from a column like `start_time`, `nfl_detail_id`, `nfl_api_id`, etc., nflverse changed the type or schema. Two options:
  - Add the column to `force_varchar_cols` on the relevant bulk_parquet config.
  - Or convert the table to `bulk_parquet` mode entirely (drops the per-year pandas inference problem forever).

---

## 7. Adding a new source table

Rough order of operations:

### 7.1 Decide the update mode

- **One parquet/CSV, all years in one file** → `full_replace`.
- **One-parquet-per-year** and small per year → `year_partition`.
- **One-parquet-per-year** and huge (or type-drift-prone) → `bulk_parquet` with a glob.
- **FK parent + single file** → `upsert`.
- **FK parent + per-year files** → `year_partition_upsert`.

### 7.2 Add a fetch function to `build_db.py`

```python
def _fetch_newtable(years):
    df = pd.read_parquet(RAW_DATA_PATH / "newsource" / "newtable.parquet")

    # 1. RENAME: align player-ID column names to our convention
    df = df.rename(columns={
        "gsis_id":       "player_gsis_id",     # if the source uses raw gsis_id
        "pfr_player_id": "player_pfr_id",
        # ... etc.
    })

    # 2. CLEAN: scrub junk IDs in every ID column
    if "player_gsis_id" in df.columns:
        df["player_gsis_id"] = clean_gsis_id_series(df["player_gsis_id"])   # strict for children
    if "player_pfr_id" in df.columns:
        df["player_pfr_id"] = clean_id_series(df["player_pfr_id"])          # loose for non-GSIS
    if "player_espn_id" in df.columns:
        df["player_espn_id"] = clean_id_series(df["player_espn_id"])

    # 3. VARCHAR-COERCE: any column that might be all-NULL in early years
    #    (DuckDB will infer INT from NULL pandas columns and fail later)
    for col in ("some_time_column", "some_uuid_column"):
        if col in df.columns:
            df[col] = df[col].astype("string")

    # 4. Ensure FK columns exist even if empty in early years
    if "game_id" not in df.columns:
        df["game_id"] = pd.Series([None] * len(df), dtype="string")

    return df
```

### 7.3 Add the TableConfig

Place it in `TABLE_CONFIGS` **after** any tables it references (the dict's insertion order is the processing order):

```python
"newtable": TableConfig(
    "newtable",
    update_mode="year_partition",   # or appropriate mode
    fetch_fn=_fetch_newtable,
    dedup_cols=["player_gsis_id", "season", "week"],  # if meaningful
    drop_na_col="player_gsis_id",                      # if required
    foreign_keys=[
        ("player_gsis_id", "players", "player_gsis_id"),
        # plus any others: ("game_id", "games", "game_id"), etc.
    ],
    stub_source={
        # For each FK, tell stub_players_from_child how to synthesize a
        # minimal `players` row from this new table's own metadata.
        "player_gsis_id": {
            "display_name": "some_name_column",
            "position": "some_position_column",
            "latest_team": "some_team_column",
        },
    },
),
```

### 7.4 Mirror in `build_db_nflreadpy.py`

Add a matching `_fetch_newtable` that pulls from nflreadpy. Register it in the `_FETCH_OVERRIDES` dict. The `_clone_with_fetch` helper will copy the FK/stub metadata from `build_db.py` automatically.

### 7.5 First rebuild and verify

1. `python3 scripts/build_db.py --tables newtable --all` (or `--years 2025` for a faster first pass).
2. Check that the table exists and has expected row count.
3. Run the orphan-sweep query (see §6.1 step 3). Must be 0 for every FK.
4. Run a sample cross-join query to confirm joins resolve correctly.
5. If FK violations happened during INSERT:
   - Check which FK column failed.
   - Check the source df for IDs not in the parent table.
   - Either extend `stub_source` with more metadata columns, or investigate upstream why the ID is unknown.

---

## 8. Known failure modes and how to fix them

### 8.1 "Conversion Error: Could not convert string 'X' to INT32"

**Cause:** DuckDB inferred a column type as `INT` from pandas NULLs in the first partition, and later partitions brought non-integer values.

**Fix:**
- Add the column to the table's `force_varchar_cols` (for `bulk_parquet` mode) or convert the fetch function's df via `df[col].astype("string")`.
- Better: convert the table to `bulk_parquet` mode so DuckDB sees all years at once.

**Historical examples:** `games.nfl_detail_id`, `play_by_play.start_time`, `play_by_play.nfl_api_id`.

### 8.2 "Violates foreign key constraint because key 'X' does not exist in the referenced table"

**Cause:** A row in the child has an ID that isn't in the parent table (`players` or `games`).

**Fix:**
- If it's a new player nflverse hasn't added to `players.parquet` yet: the `stub_source` on that TableConfig should handle it. If it doesn't, check that the `stub_source`'s `display_name` mapping uses a column actually present in the fetch df.
- If it's a real data quality bug: investigate the source parquet.
- Temporary workaround: extend the cleanup in the fetch function to null out the bad ID.

### 8.3 "Violates foreign key constraint because key 'X' is still referenced by a foreign key in a different table"

**Cause:** Someone tried to UPDATE or DELETE a row in an FK-parent table (most commonly `players`) while children hold FKs to that row. DuckDB's UPDATE implementation is DELETE+INSERT under the hood.

**Fix:**
- Don't UPDATE `players` via SQL. All `players` enrichment belongs in `_fetch_players` (pandas-level). Children stubs happen via INSERT ... ON CONFLICT DO NOTHING, never UPDATE.
- If you genuinely need to change existing player data (rename, reassign team, etc.), either:
  - Change it in the source parquet and let `upsert` mode pick up the change.
  - Or accept that this scenario is rare and handle it out-of-band.

### 8.4 "Duplicate key 'X' violates unique constraint"

**Cause:** Two rows in the source want the same unique-constrained ID (usually `player_pfr_id` or `player_espn_id`).

**Fix:**
- If it's in `_fetch_players`'s bridge backfill: the existing guard (`NOT EXISTS (SELECT 1 FROM players p2 WHERE p2.player_pfr_id = pi.pfr_id AND p2.player_gsis_id <> ...)`) handles this by leaving the target NULL. If you see this error, something bypassed that guard.
- If it's in a new fetch function: add `.drop_duplicates("player_pfr_id", keep="first")` or similar before the INSERT.

### 8.5 "column 'X' named in key does not exist"

**Cause:** A FK was declared on a column that isn't in the fetched df. Most commonly happens when a column is missing for early years (e.g., `game_stats.game_id` before 2022).

**Fix:** Ensure the column is always materialized by the fetch function, even if all-NULL for some years:

```python
if "game_id" not in df.columns:
    df["game_id"] = pd.Series([None] * len(df), dtype="string")
```

### 8.6 "players table shrank after rebuild"

**Cause:** Cleanup is too strict somewhere. The most common offender: using `clean_gsis_id_series` (strict regex) on `players.player_gsis_id`, which drops pre-GSIS-era records.

**Fix:** Use `clean_id_series` (loose) on the primary `players` source column. Strict regex is only appropriate for child tables.

**Regression guard:** before pushing a build change, compare row counts:
```bash
python3 -c "
import duckdb
new = duckdb.connect('data/nflverse.duckdb', read_only=True)
bak = duckdb.connect('data/nflverse.duckdb.pre-change.bak', read_only=True)
for t in ['players','player_ids','games','game_stats','season_stats','draft_picks',
          'combine','snap_counts','depth_charts','depth_charts_2025','ngs_stats',
          'pfr_advanced','qbr','play_by_play']:
    a = bak.execute(f'SELECT COUNT(*) FROM \"{t}\"').fetchone()[0]
    b = new.execute(f'SELECT COUNT(*) FROM \"{t}\"').fetchone()[0]
    print(f'  {t}: {a:,} -> {b:,} ({b-a:+,})')
"
```

### 8.7 "check_integrity reports hundreds of orphans after a rebuild"

**Cause:** The orphan query was written before NULL-gsis rows were allowed. `NOT EXISTS (SELECT 1 FROM players WHERE p.gsis_id = g.gsis_id)` evaluates to TRUE when `g.gsis_id` is NULL (because `NULL = anything` is NULL, and `NOT EXISTS` of an empty result is TRUE). So every NULL-gsis row gets flagged as an orphan even though the FK allows NULL.

**Fix:** add `IS NOT NULL` guard to the orphan query. The current `check_integrity` in `pipeline.py` already has this; if you ever hand-write a similar check, remember to add it:
```sql
-- Wrong (counts NULL as orphan):
WHERE NOT EXISTS (SELECT 1 FROM players p WHERE p.player_gsis_id = g.player_gsis_id)

-- Right (only counts real FK violations):
WHERE g.player_gsis_id IS NOT NULL
  AND NOT EXISTS (SELECT 1 FROM players p WHERE p.player_gsis_id = g.player_gsis_id)
```

### 8.8 "Aggregate stats are different from the last build"

If `SELECT SUM(def_tackles_solo) FROM game_stats WHERE season = 2000` returns a different number than before, the most likely culprit is that some rows with real stats but junk or NULL `player_gsis_id` were either dropped (bad) or added (good — recovered from historical data).

**Verify:** compare against a backup. For each year, compute totals on both and diff. If aggregates dropped, some stats are gone — investigate whether `drop_na_col` or a too-strict cleanup is filtering rows you should be keeping.

Historical note (see commit `79caee3`): removing `drop_na_col` on `game_stats`/`season_stats` added back ~60 previously-dropped defensive-tackle events from 1999-2000. The tackle totals increased back to their pre-drop values.

---

## 9. Debugging playbook

### 9.1 Confirm FK metadata shipped

```sql
SELECT COUNT(*) FROM duckdb_constraints() WHERE constraint_type = 'FOREIGN KEY';
-- Expect 60 (as of eea6995). If it drops, something isn't declaring FKs anymore.
```

### 9.2 Run the orphan sweep

```python
import duckdb
c = duckdb.connect("data/nflverse.duckdb", read_only=True)
fks = c.execute("""
    SELECT table_name, constraint_column_names[1], referenced_table, referenced_column_names[1]
    FROM duckdb_constraints() WHERE constraint_type = 'FOREIGN KEY'
""").fetchall()
for child, col, ref_t, ref_c in fks:
    n = c.execute(f'''
        SELECT COUNT(*) FROM "{child}" ch
        WHERE ch."{col}" IS NOT NULL
          AND NOT EXISTS (SELECT 1 FROM "{ref_t}" p WHERE p."{ref_c}" = ch."{col}")
    ''').fetchone()[0]
    if n > 0:
        print(f"ORPHANS: {child}.{col} -> {ref_t}.{ref_c}: {n}")
```

Zero output = clean. Any non-zero = FK violation snuck through somehow (shouldn't happen given FK enforcement, but verify).

### 9.3 Diff against a backup

Before any risky change, take a backup:
```bash
cp data/nflverse.duckdb data/nflverse.duckdb.pre-change.bak
```

After rebuilding, diff row counts (see §8.6) and spot-check known-good players:

```sql
SELECT s.season, s.passing_yards, s.passing_tds
FROM season_stats s JOIN players p ON s.player_gsis_id = p.player_gsis_id
WHERE p.display_name = 'Patrick Mahomes' AND s.season_type = 'REG'
ORDER BY s.season;
```

Mahomes is a good canary because his stats are well-known and unchanged across runs.

### 9.4 Investigate an FK failure

If a rebuild fails with an FK error:

```python
# 1. Which IDs are the child trying to reference that aren't in the parent?
c.execute("""
    SELECT DISTINCT child_col FROM <child> ch
    WHERE child_col IS NOT NULL
      AND NOT EXISTS (SELECT 1 FROM <parent> p WHERE p.ref_col = ch.child_col)
    LIMIT 20
""").fetchall()

# 2. Are they in player_ids?
c.execute("SELECT * FROM player_ids WHERE gsis_id IN (...)").fetchall()

# 3. Look up the offending rows to understand context
c.execute("SELECT * FROM <child> WHERE child_col = '<offending_id>' LIMIT 5").fetchall()
```

From there: decide whether to add them to `stub_source`, null them via cleanup, or fix the data upstream.

### 9.5 Check enforcement is live

A quick smoke test:

```python
import duckdb
con = duckdb.connect("data/nflverse.duckdb")
try:
    con.execute("INSERT INTO game_stats (player_gsis_id, season, week, player_name) VALUES ('00-FAKE-9999999', 2024, 1, 'Nobody')")
    print("FK FAIL — invalid ID accepted!")
    con.execute("DELETE FROM game_stats WHERE player_name = 'Nobody' AND season = 2024")
except duckdb.ConstraintException:
    print("FK enforcement live.")
con.close()
```

If the INSERT succeeds, something's wrong with the FK declarations.

---

## 10. Design principles (what we learned the hard way)

1. **Preserve data, don't drop.** Junk IDs become NULL; rows are kept. Real players without a registry entry get stubbed into `players`. `SELECT SUM(rushing_yards) FROM game_stats WHERE season = 2000` must return the same answer before and after any cleanup change.

2. **Explicit beats inferred.** Pandas-type-inferred-then-DuckDB-inferred is a two-layer lottery. Explicit VARCHAR casts, explicit DDL with types, explicit FK declarations. Every time we trusted inference, we got bitten (`gametime`, `nfl_detail_id`, `start_time`, `nfl_api_id`).

3. **Never SQL-UPDATE an FK-target table.** DuckDB treats UPDATE as DELETE+INSERT, which trips inbound FKs. All `players` mutation belongs in pandas at `_fetch_players` or as child-side INSERT ON CONFLICT DO NOTHING.

4. **Use `UNIQUE` for FK targets that allow partial records.** `PRIMARY KEY` implies `NOT NULL`; that's wrong for `players.player_gsis_id` because some stub rows only have PFR or ESPN IDs. `UNIQUE` permits NULLs and satisfies DuckDB's FK target requirement.

5. **For huge tables with cross-year type drift, use `bulk_parquet` mode.** DuckDB's `read_parquet('glob', union_by_name=true)` sees every file's schema at once. The year-by-year pandas path is a whack-a-mole with schema inference.

6. **Strict cleanup for children, loose cleanup for parents.** `clean_gsis_id_series` on `game_stats.player_gsis_id` (children only ever have modern IDs). `clean_id_series` on `players.player_gsis_id` (parents carry pre-GSIS-era records).

7. **Processing order matters.** FK parents must load before children. `TABLE_CONFIGS` dict order is the processing order: players → player_ids → games → every other child.

8. **Stubs are cheap, orphans are expensive.** Adding ~1,000 minimal stub rows to `players` is negligible storage. Silently dropping 300 orphan rows from `combine` is real data loss.

9. **Verify against a backup, not against your memory.** Row counts, famous-player spot checks, Mahomes-canary cross-join, **aggregate parity** (`SUM(def_tackles_solo)` per year), and **categorize NULL-gsis rows** (team-level vs empty vs recoverable-by-name). A build passing "0 orphans" but dropping 22 rows of real defensive tackles or 6,000 historical players is still a regression — the orphan check alone isn't sufficient proof.

10. **The `check_integrity` step runs every build.** If it reports warnings, investigate — don't tune them out. But also make sure the check itself handles NULL correctly (see §8.7): a naive orphan query will false-alarm on every NULL-gsis row.

11. **Recovery beats dropping.** When a row has enough metadata (a populated name column) to identify the player elsewhere, fill in the missing GSIS rather than leaving it NULL. `recover_gsis_by_name` in pipeline.py handles this automatically for child tables; most rebuilds recover ~8,000+ GSIS IDs that would otherwise remain NULL. See §4.5.

12. **drop_na_col is dangerous for child tables.** It silently removes rows whose key column is NULL after cleanup. For parent tables (players, player_ids) it's fine — the table's purpose requires the key. For child tables with FK-permitted-NULL columns, it's data loss. See §4.6.

---

## 11. SQLite sibling build

The canonical build is DuckDB (`data/nflverse.duckdb`). `scripts/build_sqlite.py` mirrors the already-built DuckDB into `data/nflverse.sqlite` for consumers that prefer SQLite tooling. No separate ingestion logic — SQLite is a **projection** of the DuckDB, so every cleanup, enrichment, recovery, stub, and FK declaration from the canonical build is preserved automatically.

### When to run it

After any `build_db.py` run that changed data. Typical full-rebuild sequence:

```bash
rm data/nflverse.duckdb
python3 scripts/build_db.py --all               # ~1 min
python3 scripts/build_db.py --pbp --no-backup   # ~1 min
python3 scripts/build_sqlite.py                 # ~2-3 min (includes VACUUM)
```

Skip it if nothing downstream needs SQLite — the DuckDB alone is always the source of truth.

### How it works

Hybrid approach (see `scripts/build_sqlite.py` for the code):

1. **DDL via Python `sqlite3`** — query DuckDB's `information_schema.columns` and `duckdb_constraints()` to discover each table's columns, types, UNIQUE constraints, and FKs. Emit explicit SQLite `CREATE TABLE` statements with the mapped types and the same constraint clauses. Close the sqlite3 connection when done.
2. **Bulk data transfer via DuckDB's `sqlite_scanner` extension** — `ATTACH` both the source DuckDB and the target SQLite, then `INSERT INTO dst.table SELECT * FROM src.main.table` for every table. DuckDB handles the multi-threaded row copy and automatic type conversion (e.g., `BOOLEAN → 0/1 INTEGER`).
3. **Post-build:** create the three indexes (same names as DuckDB), run row-count parity + FK-metadata + orphan checks, `VACUUM` to reclaim space.

### Type mapping

Only six DuckDB types appear in the schema, so the mapping is a finite lookup in `scripts/build_sqlite.py:TYPE_MAP`:

| DuckDB | SQLite |
|--------|--------|
| VARCHAR | TEXT |
| DOUBLE, FLOAT, REAL | REAL |
| BIGINT, INTEGER, HUGEINT, SMALLINT, TINYINT | INTEGER |
| BOOLEAN | INTEGER (0/1) |
| DATE, TIMESTAMP, TIME | TEXT (ISO format) |
| BLOB | BLOB |

Anything outside these maps to TEXT as a safe fallback. If nflverse ever ships a new type, the `sqlite_type_for` helper will warn via the fallback; update `TYPE_MAP` to add a proper mapping.

### `v_depth_charts` is a materialized table in SQLite

DuckDB's `v_depth_charts` is a live view with SQL that uses DuckDB-specific syntax (`EXTRACT`, `::INTEGER`, correlated subqueries with `CAST … AS TIMESTAMP`). Rather than re-implement the SQL in SQLite dialect, we materialize the view's result rows into a plain SQLite table with the same name and column set. Consumers querying `SELECT * FROM v_depth_charts WHERE team = 'KC'` get identical rows on both engines.

Trade-off: the SQLite `v_depth_charts` is static — it reflects the state at build time, not live. Since underlying tables only change on full rebuilds, this is a non-issue in practice.

### FK enforcement in SQLite — important consumer caveat

SQLite FKs are **declared unconditionally** but **enforced per-connection, default off**. Every FK edge the DuckDB has is also declared in the SQLite DDL (60 total, verified by `PRAGMA foreign_key_list` on every table after build). But to actually enforce them on write — or to use the FK metadata as a join graph — a consumer must:

```sql
PRAGMA foreign_keys = ON;
```

… after every `sqlite3.connect()`. The README and DATABASE.md document this for downstream users. The build script sets it itself during the verification pass so orphans can be caught live.

### Verification the script runs every time

- Row-count parity on every table + view (fails the build if any table differs).
- `PRAGMA foreign_key_list` total across all tables must return 60 (fails logged, build proceeds but orphan sweep catches the real issue).
- Orphan sweep with `PRAGMA foreign_keys = ON` — must report 0.
- `PRAGMA integrity_check` — must return `ok`.

### Size expectations

DuckDB's columnar layout compresses much better than SQLite's row-store. Expect:

- DuckDB: ~940 MB
- SQLite: ~2.5 GB (after VACUUM)

The size difference is engine-inherent, not a sign of duplication or bloat. Both hold the same rows.

### Adding a new table — what changes in build_sqlite.py

If you add a new TableConfig to `build_db.py`, also add the table name to `TABLE_ORDER` in `scripts/build_sqlite.py`. The list is deliberately hardcoded (not imported from build_db.py) so the SQLite build fails fast if the two drift, catching any accidental omission.

If the new table has a FK, nothing else needs changing — `build_ddl` reads the constraint from `duckdb_constraints()` automatically.

If the new table uses a DuckDB type not in `TYPE_MAP`, add it there.

If the new table should participate in a view (`v_depth_charts` style), add the view name to `VIEW_MATERIALIZATIONS`.

---

## 12. Completeness vs. data reality

A consumer (the NFL_AI_AGENT LLM) observed that a query — "top 2024 linebackers by defensive snaps, with season defensive stats" — returned NULL stats for ~15% of rows on first try, finding the data only on retry via a different join path. The hypothesis was ID-stitching breakage: `snap_counts.player_pfr_id → players.player_gsis_id → season_stats` losing rows somewhere.

We measured. The hypothesis was wrong, but the measurement surfaced two real gaps the consumer would otherwise hit silently, plus one class of "missing data" that's a real upstream reality (not a bug).

### 12.1 The hub is stitched. Don't chase it.

For the exact failing query, measured against the FK-bearing build:

| Stage | Count | Rate |
|---|---|---|
| Distinct PFR IDs in `snap_counts` 2024 REG LB | 315 | — |
| Matched in `players` on `player_pfr_id` | 315 | 100% |
| With non-NULL `player_gsis_id` | 315 | 100% |
| With a `season_stats` row for 2024 REG | 300 | 95.2% |

100% of PFR IDs from the child table reach `players` with a populated GSIS. No backfill gap. **If a consumer query returns NULL stats and retry finds them via a different path, the retry difference isn't a stitching bug — it's usually a missing aggregate row (gap 12.2) or missing source stats for that player (gap 12.4).**

When you touch the code here, don't chase ghost stitching problems. Re-run the §9.2 orphan sweep; if it's 0, the hub is fine.

### 12.2 POST-season `season_stats` was absent — now downloaded + augmented

The `season_stats` table was originally sourced only from `stats_player_reg_{year}.parquet`. nflverse also publishes `stats_player_post_{year}.parquet` under the same `stats_player` release tag, and the original config simply never downloaded them. Result: zero POST rows in `season_stats`, while `game_stats` carried every playoff week back to 2002.

Two changes close this:

1. **Download spec extended** (`scripts/download.py`). The `season_stats` entry now uses a `patterns` list covering both REG and POST parquets. Adds ~27 small files (<5 MB total).
2. **`_fetch_season_stats` loads both** (`scripts/build_db.py`). `pd.concat([reg, post])` before the usual rename + cleanup.

After (1) + (2), every (player, season, POST) combination nflverse publishes is in `season_stats` with full ratios (passer_rating, fg_pct, completion_percentage, etc.) populated from the source feed.

Anything still missing — combinations that exist in `game_stats` but not in either nflverse feed — is handled by `compute_missing_season_stats(conn)` (`scripts/pipeline.py`):

- Iterates `(player_gsis_id, season, season_type)` combinations in `game_stats` not in `season_stats`.
- For each, SUMs the additive weekly columns, takes `arg_max(team, week)` for `recent_team`, `COUNT(DISTINCT game_id)` for `games`, `MAX(fg_long)` for the longest FG.
- **Ratio columns are left NULL** (see lists `_SS_NULL_RATIO_COLS` and `_SS_NULL_LIST_COLS` in `pipeline.py`). Ratios cannot be summed from components in a single SQL aggregate without recomputing; consumers can recompute from the additive components (e.g. `passing_yards / attempts`, `fg_made / fg_att`).
- Safety: the classification sanity-check raises if a new column lands in either table without being added to one of the `_SS_*` lists. Build fails loudly rather than silently mis-aggregating.

Current effect: nflverse POST files cover all 12K+ combinations, and the augmentation typically inserts 0-1 extra rows. The function is a safety net — not a primary data source — but it's load-bearing if nflverse ever drops a player from the pre-aggregated feed.

**Runs on every build.** Cheap no-op when the gap is already closed. No `--augment` flag.

### 12.3 Preflight ID merge prevents duplicate NULL-GSIS stubs

Child tables (combine, snap_counts, pfr_advanced, draft_picks, qbr, depth_charts_2025) stub missing FK-target rows into `players` via their declared `stub_source` maps. When a child has a PFR or ESPN ID that isn't in `players`, a NULL-GSIS stub is created.

Some of those stubs are legitimate (pre-GSIS prospects, combine-only players, ESPN-only QBR entries without a known GSIS). But many are **duplicates** — the same player already exists in `players` under their GSIS with the same display name, just with the PFR/ESPN column still NULL. Without intervention these compete for future joins and clutter the registry.

Fix: `_preflight_id_merges(players_df)` in `scripts/build_db.py`. Runs inside `_fetch_players` after the bridge enrichment, **before any child INSERT**. For each PFR/ESPN-stubbing source:

1. Read the raw parquet; extract `(id, display_name, position, team)` tuples for IDs not already in `players[target_col]`.
2. For each candidate ID, look for an existing GSIS-bearing row in `players` with `target_col IS NULL` and a matching `display_name`.
3. Position tiebreaker: candidate and donor must match exactly or one side must be NULL. A strict position mismatch rejects the match (guard against two different players sharing a name).
4. Team tiebreaker: used only when there's still >1 viable donor. Prefer donors with matching `latest_team`.
5. If exactly 1 viable donor remains, attach the candidate's ID to the donor row. Mark the donor as consumed so later candidates don't re-use it.

Why pandas-side, not SQL-side: DuckDB treats `UPDATE` on a row with FK-pointing children as `DELETE+INSERT` internally, and `DELETE` is blocked by the FK. By running the merge before any child has been INSERTed (i.e. before any FK-child reference exists), we avoid the restriction entirely.

Sanity cap: if the match logic would merge more than 500 candidates, the function raises — the measured ceiling is ~65 and a 10× jump indicates a broken safety filter.

Current effect: ~65 child IDs attached to existing GSIS-bearing players per build (36 combine, 19 draft_picks, 10 snap_counts). `players` NULL-GSIS count drops from ~605 to ~540 as a result. No rows lost — the IDs move from would-be-stubs into existing GSIS rows.

### 12.4 "snap_counts row but no game_stats row" is real data, not a bug

Some players appear in `snap_counts` (on field for at least one play) but have no `game_stats` row — they simply didn't record any charted stat (tackle, INT, sack, PD, pass attempt, carry). Most have 0 defensive/offensive snaps; they're depth-chart entries or pure special-teams contributors who were active but not involved in the chart-worthy play types tracked in `game_stats`.

These players **will not appear** in `season_stats` either (there's no weekly data to aggregate from). If a consumer LEFT JOINs `snap_counts → season_stats` and sees NULL stats for a player with 0 offensive + 0 defensive snaps, **that's not a stitching bug** — the player truly recorded no stats. No retry will find them.

How to filter in a leaderboard query to avoid this confusion:
```sql
-- Top LBs 2024 REG by defensive snaps, only players with >0 snaps
SELECT p.display_name, sc.defense_snaps, ss.def_tackles_solo
FROM snap_counts sc
JOIN players p ON sc.player_pfr_id = p.player_pfr_id
LEFT JOIN season_stats ss
  ON ss.player_gsis_id = p.player_gsis_id
  AND ss.season = sc.season
  AND ss.season_type = 'REG'
WHERE sc.season = 2024 AND sc.game_type = 'REG'
  AND sc.position LIKE '%LB%'
  AND sc.defense_snaps > 0      -- excludes depth-chart-only entries
ORDER BY sc.defense_snaps DESC;
```

### 12.5 Validation invariants after every build

`run()` prints these; they should always hold:

- `Augmenting season_stats from game_stats... N rows inserted (gap was N, now 0)` — gap **always** closes to 0. If it doesn't, either the aggregation failed (raises) or a new column isn't classified (raises earlier).
- `Checking referential integrity... ok (0 orphan records)` — unchanged by the completeness work.
- `preflight ID-merge: +N child IDs attached to existing players` — N typically 40-100. A jump >500 trips the safety cap.
- No duplicate `player_gsis_id` in `players` (the UNIQUE constraint enforces this at INSERT time).

## 13. Current state summary

- **13 tables + 1 view** (`v_depth_charts`): see `DATABASE.md`.
- **60 foreign keys**: every edge the consumer (NFL_AI_AGENT) asked for, 0 orphans.
- **Full rebuild time**: ~2 min DuckDB + ~2-3 min optional SQLite mirror. `build_db.py --all` ≈ 1m15s (includes recovery + POST-augment), `build_db.py --pbp` ≈ 52s, `build_sqlite.py` ≈ 2m30s.
- **File sizes**: DuckDB ~940 MB, SQLite (optional) ~2.5 GB.
- **Players registry size**: ~25,000 rows (includes ~700 cross-referenced stubs beyond nflverse's primary parquet). ~540 rows have NULL GSIS (pre-GSIS historical, combine-only prospects).
- **`season_stats` row count**: ~61,600 (REG ~49,515 + POST ~12,074). POST was previously absent; now covered via nflverse POST files + a safety-net aggregation from `game_stats`.
- **Per-rebuild GSIS recoveries**: ~8,200 rows (7,100 draft_picks pre-1995 HoF picks + 1,050 depth_charts_2025 practice-squad + ~50 scattered). Without recovery, those rows would have NULL gsis; with it, they join to the correct `players` record.
- **Per-rebuild preflight merges**: ~65 child PFR/ESPN IDs attached to existing players instead of becoming duplicate stubs.

To rebuild from scratch (assumes parquets are already in `data/raw/`):
```bash
rm data/nflverse.duckdb
python3 scripts/build_db.py --all
python3 scripts/build_db.py --pbp --no-backup
```

To sanity-check a rebuild:
```bash
# Replace with your pre-change backup path
python3 -c "<row-count diff script from §8.6>"
python3 -c "<orphan-sweep script from §9.2>"
python3 -c "<FK enforcement smoke test from §9.5>"
```

If all three pass, the DB is correctly constructed and consumer queries will return accurate results.
