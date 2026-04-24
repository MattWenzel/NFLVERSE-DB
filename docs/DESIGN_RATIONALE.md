# NFLVERSE design rationale

Every non-obvious decision in this codebase was made to address a concrete
failure. This doc is the living record. Dropping any rule here requires
reading the "why," confirming the invariant is held by the new structure,
and updating this doc.

Companion: `docs/LESSONS_LEARNED.md` (the broader field guide of mistakes we
don't want to re-make; this doc is the mechanism-level implementation record).

---

## R1. UNIQUE, not PRIMARY KEY, on `players` ID columns

**Rule:** `players.player_gsis_id` is declared `UNIQUE`, not `PRIMARY KEY`.
Same for `player_pfr_id` and `player_espn_id`.

**Why:** `PRIMARY KEY` implies `NOT NULL` in DuckDB and SQLite. Stubs from
PFR-only or ESPN-only child sources (combine, qbr, depth_charts_2025) carry
only their native ID and have `player_gsis_id IS NULL`. A PK constraint
rejects those rows at INSERT.

**Origin:** v1 tried PK first; rejected stub rows broke the players hub.

**Code:** `scripts/engine.py:write_table` emits `UNIQUE` DDL clauses.
`scripts/schema.py:TABLES["players"]["primary_key"]` is the config key but
it emits UNIQUE — the name is historical.

**Don't change to PK** without a plan to either (a) filter all NULL-ID stubs
out before insert (which loses coverage) or (b) synthesize placeholder IDs
for stubs (which pollutes the hub).

---

## R2. Loose ID cleanup on `players` master, strict on child tables

**Rule:** `scripts/cleanup.py:clean_id(series, kind='generic')` on
`players_master.player_gsis_id`. `clean_id(series, kind='gsis')` on child
tables that carry GSIS.

**Why:** nflverse's `players.parquet` contains pre-GSIS historical records
(Joe Montana, Dan Marino, etc.) with Elias-format IDs like `YOU597411` or
`VIT276861`. These don't match the canonical `^\d{2}-\d{7}$` regex. Strict
cleanup dropped ~6,085 records in an early v1 build.

Child tables (game_stats, season_stats, depth_charts, snap_counts, etc.)
only carry modern-era IDs; any non-matching value is a real data error
(`XX-0000001` sentinels, stringified NaN, etc.).

**Origin:** v1 commit `eea6995` "Preserve pre-GSIS historical players" after
the accidental drop.

**Code:** `scripts/schema.py:SOURCES["players_master"]["id_cleanup"]` uses
`"generic"`; every other source uses `"gsis"` for GSIS columns.

---

## R3. No SQL UPDATE on `players` after children load (DuckDB FK-parent restriction)

**Rule:** All player-level mutations (bridge enrichment, name-match, stubs)
happen in pandas during hub construction, BEFORE any FK-bearing child
table is created. No SQL-side `UPDATE players` or `INSERT ON CONFLICT DO
UPDATE players` is permitted after children exist.

**Why:** DuckDB internally treats `UPDATE` as `DELETE + INSERT`. If any
child has an FK to a `players` row being updated, the implicit DELETE is
rejected by the FK constraint. The same applies to `ON CONFLICT DO UPDATE`
used as upsert.

**Origin:** v1 experimented with SQL-side player enrichment; every approach
hit this wall. The pandas-layer-only solution was arrived at after several
failed SQL patterns.

**Code:** `scripts/hub.py:build_hub` produces the complete `players`
DataFrame. `scripts/build.py:Phase 2.5` adds preflight stubs (still in
pandas). `scripts/engine.py:write_table` fires a single CREATE TABLE +
INSERT for players before any child runs.

**Tempting alternative (don't do it):** "Just disable FK enforcement during
the update and re-enable after." DuckDB doesn't cleanly support that;
SQLite consumers would need coordinated pragma toggles too; and the
pandas-side approach is actually cleaner code.

---

## R4. Canonical `player_gsis_id` on every player-bearing table

**Rule:** Every child table that references a player carries
`player_gsis_id` — even if the raw source is PFR-only (combine, snap_counts,
pfr_advanced) or ESPN-only (qbr, depth_charts_2025). The ID is populated
via `id_backfill` after child load (Phase 5).

**Why:** LLM-written SQL ergonomics. A query like "top rushers by snap
count" shouldn't require knowing that `snap_counts` is PFR-native and
`season_stats` is GSIS-native — requiring users to bridge-join per table
is a maintenance burden and a source of wrong joins.

**Origin:** v2's key ergonomic improvement over v1. Regression-free.

**Code:** `scripts/schema.py:TABLES["snap_counts"]["id_backfill"]` declares
the hub lookup; `scripts/engine.py:apply_id_backfill` runs the UPDATE after
write.

---

## R5. Name-match recovery is a first-class phase (not an implicit side-effect)

**Rule:** `scripts/engine.py:apply_name_match_recovery` is Phase 6 of
`build.py`. Declared per-table via `TABLES[x]["name_match_recovery"] = {target_column, name_columns}`.

**Why:** Some players legitimately exist under different IDs across sources.
A player drafted pre-1995 has a PFR ID in `draft_picks` but no GSIS
(didn't exist yet). Later, `players.parquet` has them with a GSIS. Join
`draft_picks.player_gsis_id = players.player_gsis_id` misses — the former
is NULL.

**Origin:** v1's `recover_gsis_by_name` closed this gap by matching names.
v2's first build dropped the mechanism (assumed `weekly_rosters` subsumed
it), producing a draft_picks coverage regression 81% → 25%. v3 restores it
as an explicit declared phase.

**Safety:** Match requires (a) equal display_name, (b) season within
rookie_season..last_season range on the donor, (c) exactly one candidate
donor — ambiguous names skip. No fuzzy matching (Michael vs Mike).

**Code:** `scripts/engine.py:apply_name_match_recovery`. Declared for
`draft_picks` currently; expand to `combine` or others if surveys show
need.

---

## R6. Hub merge UNIQUE-safety: first-established PFR/ESPN wins

**Rule:** In `scripts/hub.py:_apply_column_fill`, columns in `UNIQUE_COLS`
(`player_pfr_id`, `player_espn_id`) reject candidate values that are
already held by a different hub row.

**Why:** PFR IDs are not guaranteed globally unique across upstream sources.
Observed case: `SmitMa01` was the PFR ID for one Marcus Smith (DE) in one
source and for a different Marcus Smith (WR) in another. Without the
safety check, the second match would silently overwrite the first.

**Trade-off:** ~6pp lower coverage on these columns vs v1 (91% → 85% on
player_pfr_id). Accepted because the alternative is corrupted JOIN results
for ambiguous PFRs.

**Origin:** v2 investigation. Documented in `docs/LESSONS_LEARNED.md §1.7`.

**Code:** `scripts/hub.py:UNIQUE_COLS` constant + the `candidate.where()`
clause in `_apply_column_fill`.

---

## R7. `compute_missing_season_stats` is a load-bearing safety net

**Rule:** `scripts/engine.py:_aggregate_from_sibling` runs as a FILL_RULE
on every build, even when it inserts 0 rows today.

**Why:** nflverse's pre-aggregated `stats_player_reg` / `stats_player_post`
files may omit a player in a future season. `game_stats` (per-week) is
more complete. The safety net sums weekly rows into a season row when the
season-level row is missing. Today runs as a no-op (POST file loading
closed the gap). The day it's needed, not having it costs hours of debugging.

**Origin:** v1 found 12,048 missing POST rows that this function filled.
POST files were subsequently added to the download config, reducing the
function's workload to 0-1 rows. But the net stays.

**Code:** `scripts/schema.py:FILL_RULES[0]` (season_stats_augment_from_game_stats).
`scripts/engine.py:_aggregate_from_sibling`. Ratio formula companion:
`compute_season_ratios`.

---

## R8. `snap_counts` without corresponding `game_stats` row is REAL DATA

**Rule:** v3's `expected_gaps` annotation on `snap_counts` documents: a
player can appear in snap_counts (on field for ≥1 play) with zero rows in
game_stats — they didn't record any charted stat. This is data reality,
not a stitching bug. LLM consumers should filter `defense_snaps > 0` or
`offense_snaps > 0` in leaderboard queries.

**Why:** Misinterpreting this pattern as a bug leads to wasted investigation
cycles. v1's initial LLM-complaint triage was 2+ hours chasing a "stitching
gap" that turned out to be zero-snap depth chart entries.

**Origin:** v1 investigation ran a snap_counts-level analysis; 13 of 15
"failing" rows had 0 defensive snaps.

**Code:** `scripts/schema.py:TABLES["snap_counts"]["expected_gaps"]`.
Documented in `docs/DATABASE.md` under `snap_counts` with guidance for
LLM-query authors.

---

## R9. Foreign-namespace IDs don't get FKs

**Rule:** A column whose values are in a different ID namespace from our
canonical cannot have an FK declaration. `qbr.game_id` holds ESPN numeric
IDs like `260910009` — no FK to `games.game_id` (which uses
`2024_01_KC_BUF`). Similarly `officials.game_id` uses NFL's internal
`YYYYMMDDGG`.

**Why:** FK violations on INSERT. Silent join-miss if FK-constraint-less.

**Origin:** v2's first build tried to FK `qbr.game_id → games.game_id`;
FK violation on row 1.

**Code:** `scripts/schema.py:SOURCES["qbr_week"]` declares `id_cleanup`
on `game_id` but `TABLES["qbr"]["foreign_keys"]` omits it. Comment in the
SOURCES entry explains the namespace mismatch.

---

## R10. Multi-pattern releases require explicit pattern enumeration

**Rule:** An nflverse release tag can contain multiple file patterns. Each
pattern is its own source_id. `scripts/schema.py:SOURCES` enumerates every
pattern we consume; `scripts/schema_skeleton.py` enumerates every pattern
the release publishes. The `audit_against_skeleton` gate ensures every
skeleton entry is in SOURCES or explicitly SKIPPED.

**Why:** v1's `DOWNLOAD_MAP` had one pattern per release; `stats_player`
has 4+ patterns (reg/post/week/regpost), `pfr_advstats` has 8
(season × {pass, rush, rec, def} + week × {pass, rush, rec, def}). v1
missed the POST files because only `reg` was declared.

**Origin:** `docs/LESSONS_LEARNED.md §1.1`.

**Code:** `scripts/catalog.py` builds the manifest; `scripts/schema_generator.py`
emits the skeleton; `scripts/schema.py:audit_against_skeleton` is the gate.

---

## R11. `union_by_name=true` for year-partitioned loads

**Rule:** Any glob-based parquet load (DuckDB `read_parquet('path/*.parquet')`
or pandas `pd.concat` of multi-year reads) uses `union_by_name=True`
semantics — columns missing in some years are filled with NULL.

**Why:** nflverse adds/removes columns across years (`game_stats` pre-2022
had no `game_id`). Without union-by-name, DuckDB errors on schema mismatch;
pandas silently produces misaligned frames.

**Origin:** v1's game_stats load broke when 2022+ added `game_id`; required
a `force_types` patch and a union-aware loader.

**Code:** `scripts/loaders.py` uses pandas concat (schema-drift tolerant);
DuckDB sql-side loads use `read_parquet(..., union_by_name=true)`.

---

## R12. `arg_max(col, ordering_col)` for "last value by dimension"

**Rule:** For aggregations like "most recent team per player-season," use
DuckDB's `arg_max(team, week)`. Not window functions, not ordered subqueries.

**Why:** Cleaner SQL, single-statement. Works inside GROUP BY contexts.
Mirrors the `first_non_null / latest_source_wins` column policy in the hub.

**Origin:** v1's `compute_missing_season_stats` initial version used
`ORDER BY week DESC LIMIT 1` as correlated subquery; inefficient. Replaced
with `arg_max`.

**Code:** `scripts/engine.py:_aggregate_from_sibling` (recent_team case);
`scripts/schema.py:HUB_BUILD["sources"]["expansion"]["aggregate"] =
"latest_by_week"` for weekly_rosters.

---

## R13. Float numeric IDs require integer-roundtrip on cast-to-string

**Rule:** `clean_id` on a numeric-dtype Series does
`pd.to_numeric(errors='coerce').astype('Int64').astype('string')`. Never
naive `.astype(str)`.

**Why:** ESPN IDs arrive from CSV as float64 when the column also has NaN.
Naive string-cast produces `'3139477.0'` (trailing zero artifact) or
`'3.139477e+06'` (scientific notation on large magnitudes).

**Origin:** v1's db_playerids CSV bridge broke when ESPN IDs were stringified
as floats; real ID values were silently mangled. Fix in `scripts/cleanup.py`.

**Code:** `scripts/cleanup.py:clean_id` — the `is_numeric_dtype` branch.

---

## R14. `check_integrity` / `validate` runs on EVERY build

**Rule:** `scripts/engine.py:validate` is called as Phase 10 of every
build, regardless of `--all` vs incremental. Hard invariants (orphans,
duplicates, gap=0) fail the build.

**Why:** Without this, regressions ship silently. v1's orphan sweep caught
real FK violations during development; v2's expanded validate caught them
too. No build should complete without proving its own correctness.

**Origin:** v1's initial build shipped with orphans because validate was
optional. Retrofitted as mandatory.

**Code:** `scripts/engine.py:validate`, `scripts/build.py:Phase 10`.

---

## R15. Consumer-compat column aliases (v3's R15 from lessons §4.2, §5.6)

**Rule:** A column rename that breaks existing consumer queries requires
keeping the legacy name as an alias for a deprecation period. Declared via
`TABLES[x]["column_compat"]`.

**Why:** v1 consumers (NFL_AI_AGENT, any downstream LLM script) don't
expect silent breaking changes. `pfr_advanced.tm → team` and
`qbr.game_id → espn_game_id` (reverted in v2-redesign) are examples.

**Origin:** v2's atomic-swap plan flagged these as regressions;
v3 explicitly supports aliases.

**Code:** `scripts/schema.py:SOURCES["pfr_advanced_season_rush"]` keeps
native `tm` (no rename). `scripts/schema.py:SOURCES["qbr_week"]` keeps
native `game_id` (no rename). Renames that DO happen must be annotated.

---

## R16. Expected_gaps declarations are required for observed gaps > 10%

**Rule:** Any source whose survey shows an ID column with > 10% hub-gap
must declare `expected_gaps` on the schema entry OR have a recovery rule
(`name_match_recovery` or `id_backfill` in the TABLES entry). Otherwise
`scripts/survey.py` exits non-zero.

**Why:** Silent gaps are the source of every v1/v2 coverage regression.
Forcing a declaration at schema-time surfaces "did you mean to not have
coverage here?" to the human editing the config.

**Origin:** v2 → v3 lesson.

**Code:** `scripts/survey.py:check_unresolved_gaps`,
`scripts/schema.py:SOURCES[...]["expected_gaps"]`.

---

## R17. Institutional memory is code, not folklore

**Rule:** Every non-obvious design choice gets an entry in this doc with:
(a) the rule, (b) why it exists, (c) what would happen if it were dropped,
(d) the code file that implements it.

**Why:** v2 inherited v1's choices but not their rationales. The
draft_picks name-match recovery regression happened because the "why" was
in git commit messages, not in code comments. Future-me wouldn't read the
git log.

**Origin:** Every line of this doc.

**Code:** This doc. `docs/LESSONS_LEARNED.md` (the broader lesson record).

---

## R18. Pandas-first, DuckDB-second for large-table backfills

**Rule:** Any fill that touches more than ~100K rows and joins to another
table — id_backfill, fill_rules with `source_table` joins, name-match
recovery — must do the join in pandas, then bulk-replace the table (DROP
+ write_table via pandas registration). No SQL UPDATE with correlated
subqueries on large child tables.

**Why:** DuckDB's UPDATE with a correlated subquery is O(N·M) in practice:
for each of N target rows it re-scans the source. On weekly_rosters (906K
rows × 8 alt-ID fills) the pure-SQL path was measured at 10+ minutes;
the pandas version (read → merge → drop → write_table) ran in 47 seconds.
Same result, 15× faster.

**Scope:** Applies to `apply_id_backfill`, `apply_fill_rule(backfill_null)`
with `source_table` joins, and `apply_name_match_recovery`. Does NOT apply
to `aggregate_from_sibling` (INSERT ... GROUP BY is DuckDB-native and
fast) or view/index creation.

**How it's implemented:** `scripts/build.py:_finalize_pandas` is the
reference implementation. The `--finalize` path uses it unconditionally;
full build Phase 5-7 calls into the same helper (post-Phase-4 table load)
so both paths get the same speedup.

**Tempting alternative (don't do it):** "Keep it all SQL for consistency."
The correlated-subquery cost scales with table size; the pandas version
scales with memory. We have memory. Use it.

**Exception — small tables (`players`, ~26K rows):** UPDATE is cheap
there, and `players` can't be dropped anyway (R3 FK-parent restriction).
Apply fills via SQL UPDATE. The pandas-first rule kicks in above roughly
100K rows or when the UPDATE has multiple correlated joins.

**Code:** `scripts/build.py:_finalize_pandas`. Invoked by both `--finalize`
and the full-build's post-Phase-4 fill application. `scripts/engine.py:
apply_fill_rule` retains its SQL path for `players` fills and
`source_expression` rules that can't be expressed in pandas.

**Origin:** v3 build attempted a full rebuild via `--tables` batches.
Each batch ran fast, but the weekly_rosters alt-ID fills (8 separate
UPDATEs on 906K rows) dominated runtime. Switching to pandas merges cut
finalize from a projected 10+ minutes to 1:32.

---

## How to add a new rule

1. Identify the concrete incident the rule addresses (past failure, observed
   bug, or anticipated issue).
2. Write the rule as a short imperative statement.
3. Fill in: Rule, Why, Origin (commit SHA or branch name), Code (file paths),
   plus a "don't change without" clause naming the invariant that breaks.
4. Commit this doc alongside the code change that implements the rule.
5. Never delete a rule. If it's superseded, add a `~~strike-through~~` with
   a pointer to the new rule that replaces it. Rules removed in the code
   stay in history so we know why not to re-add them.

## How to drop a v1 or v2 mechanism

1. Find the rule here that justified its existence.
2. Confirm the invariant is held by a new structure (cite the new structure).
3. Update the rule entry: mark the old mechanism superseded; explain the
   new approach.
4. Only THEN remove the mechanism from code.

No "this looks unnecessary" removals. Ever.
