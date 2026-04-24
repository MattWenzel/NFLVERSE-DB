# What we learned building v1 and v2

This document is the institutional memory of the NFLVERSE build. Every rule here
was paid for with a concrete bug, a silent data loss, or a shipped regression.
v3 is designed to encode these lessons as build-time gates so the next version
can't rediscover them the hard way.

**Read this before dropping any v1 or v2 mechanism.** If a rule looks redundant,
the reason it looks redundant is usually that it's been quietly protecting an
invariant you haven't hit yet.

---

## 1. Upstream reality (how nflverse actually publishes data)

### 1.1 A release tag contains many file patterns, not one

The nflverse-data repo organizes releases by *tag*, not file. One tag (e.g.
`stats_player`) can contain 4+ file-pattern families: `stats_player_reg_{year}`,
`stats_player_post_{year}`, `stats_player_week_{year}`, `stats_player_regpost_{year}`.
The `pfr_advstats` release has 8: season × {pass, rush, rec, def} and week ×
{pass, rush, rec, def}.

**v1's mistake:** `DOWNLOAD_MAP` declared `stats_player_reg_{year}` only. POST
files existed upstream for years and were silently ignored. Result: zero POST
rows in `season_stats` until someone ran an audit.

**v1's mistake (recurrence):** `advstats_season_def.parquet` (7,537 rows × 30
defensive columns) was never downloaded because the config only listed
`{pass, rush, rec}`. Same pattern, different release.

**v3 rule:** Catalog the full asset list for every opted-in release tag. Derive
candidate sources from the catalog; don't handwrite the list.

### 1.2 Some "game_id" columns are not nflverse game_ids

nflverse's canonical game_id format is `2024_01_KC_BUF`. But:
- `qbr_week_level.parquet`'s `game_id` column holds ESPN numeric IDs like `260910009`.
- `officials.parquet`'s `game_id` holds NFL's internal `YYYYMMDDGG` format (`2015091301`).
- `pbp_participation.parquet` uses `nflverse_game_id` for the canonical ID, with
  `old_game_id` as a secondary namespace.

These are different ID *namespaces* that happen to share a column name. A naive
FK `qbr.game_id REFERENCES games.game_id` blows up on INSERT.

**v2's mistake:** First build declared `qbr.game_id → games.game_id` FK. FK
violation at load time: ESPN's `260910009` isn't in our games table.

**v3 rule:** When a column name collides with a canonical ID but uses a foreign
namespace, rename to the specific namespace (`espn_game_id`, `nfl_game_id`) OR
keep the native name with an explicit `no-fk, foreign-namespace` annotation. Never
declare an FK without verifying the two sides share a namespace.

### 1.3 Schema drift across year partitions is normal

`game_stats` files pre-2022 don't have a `game_id` column at all. `pfr_advstats/
advstats_season_pass.parquet` uses `team`; the `rush` and `rec` siblings use `tm`.
Game-schedule files have a nullable `gametime` column that was all-NULL in early
years and inferred as `INT` by DuckDB.

**v1's mistake:** Year-by-year pandas loading inferred dtypes from the first
partition and choked on type drift in later years. Required `force_types` escape
hatches for specific columns per source.

**v3 rule:** Every year-partitioned source declares `force_types` for known drift
columns, AND survey phase detects schema drift across the year set and surfaces
it as a warning. Use `read_parquet(..., union_by_name=true)` for SQL-side
multi-file loads.

### 1.4 Pre-GSIS historical players use Elias IDs (not GSIS format)

nflverse's `players.parquet` contains pre-GSIS era records (Joe Montana, Dan
Marino, etc.) with IDs like `YOU597411` or `VIT276861`. These fail the canonical
GSIS regex `^\d{2}-\d{7}$`.

**v1's mistake:** Strict GSIS-regex cleanup was applied to `players.parquet` at
one point, dropping ~6,085 historical records. Had to be reverted.

**v1's fix (and v3 rule):** Loose cleanup (`kind='generic'`) on the players
master source accepts any non-junk string. Strict cleanup (`kind='gsis'`) is
correct only for child tables, which only carry modern-era IDs.

### 1.5 nflverse uses multiple junk sentinels for "no ID"

Observed sentinel values: empty string, `'0'`, `'None'`, `'nan'`, `'NaN'`,
`'<NA>'`, `'XX-0000001'`, and various float encodings of NaN (which stringify
to `'nan'` or `'3.139477e+06'` when a NaN-bearing float column is cast to string
naively).

**v1's mistake:** Float ESPN IDs cast via `str(v)` produced `'3139477.0'` —
technically not NaN but garbage anyway. Lost several real ID values to
stringification artifacts.

**v3 rule:** `clean_id(series, kind)` handles numeric-dtype columns via
`pd.to_numeric(..., errors='coerce').astype('Int64').astype('string')`. All
sentinel forms collapse to `pd.NA`.

### 1.6 Year ranges differ across sources

Not every source covers 1999–current. `snap_counts` starts 2012. `depth_charts`
2001. `weekly_rosters` 2002. `pbp_participation` 2016. `ftn_charting` 2022.
`injuries` 2009. PFR weekly advstats 2018. Historical `rosters` go back to 1920.

**Implication:** "Player X not in weekly_rosters for year Y" is meaningful data —
either Y is outside the source's coverage, or X genuinely wasn't on a roster that
year. The difference matters for LLM queries.

**v3 rule:** Every source declares its year range explicitly (or `auto` with
detection from raw files). Survey phase records observed year range and compares
to declaration.

### 1.7 Some PFR IDs are aliased to multiple players

PFR's disambiguation convention is `SmitMa00`, `SmitMa01`, etc. — different
suffixes for players sharing first-4-of-last + first-2-of-first. But occasionally
the upstream sources disagree: a PFR ID gets linked to two different GSIS rows
across merge passes.

**Observed case:** `SmitMa01` ended up mapped to two Marcus Smiths (one DE, one
WR) in v2's hub merge because one source bridge associated it with GSIS `00-
0025713` and another attached it by name-match to GSIS `00-0026246`.

**v2's fix (v3 rule):** Hub merge enforces UNIQUE-safety on `player_pfr_id` and
`player_espn_id` — first source wins, later sources are silently rejected if
their value would create a collision. Trade-off: slightly lower coverage on
these columns (91% → 85%) but no corrupted joins.

### 1.8 snap_counts rows without matching game_stats rows are real data

A player can appear in `snap_counts` (on field for at least one play) with zero
rows in `game_stats` — they didn't record any charted stat (tackle, INT, sack,
pass defended, carry, attempt). Most have 0 snaps listed; they're depth chart
entries or pure special-teams contributors.

**v1's investigation:** 15% of 2024 LB snap_count rows had NULL season stats on
first LLM query. The original hypothesis was a stitching bug. The real cause:
these players genuinely have no defensive stats for 2024 REG. Nothing to
recover.

**v3 rule:** Document in `expected_gaps` per source. Example annotation on
`snap_counts`: "~1-2% of rows join to zero-stat season_stats; filter by
`defense_snaps > 0` or `offense_snaps > 0` in leaderboard queries."

### 1.9 Same player appears under different IDs in different sources

A player drafted pre-GSIS (say, 1985) has a PFR ID in `draft_picks` but no GSIS
(because GSIS didn't exist yet). If that player later appears in `players.parquet`
with a GSIS, the join `draft_picks.player_gsis_id = players.player_gsis_id`
misses — `draft_picks.player_gsis_id` is NULL.

**v1's fix:** `recover_gsis_by_name` runs a name-match pass — look up
`pfr_player_name` in `players.display_name`, restrict to season-active range, fill
on unique match. Recovers ~7,100 pre-1995 HoF picks.

**v3 rule:** Name-match recovery is a first-class phase, declared per-table in
schema, not an implicit side effect of load ordering. `draft_picks` declares
`name_match_recovery: {target: player_gsis_id, name_col: pfr_player_name}`.

### 1.10 The `dynastyprocess/data` bridge is not complete

The external `db_playerids.csv` (our `player_ids` table) covers ~12,000 players
with cross-references across ~20 ID systems. It's maintained by volunteers.
Missing: ~1,800 PFR-only players, the long tail of practice-squad callups, and
any player added to the NFL after the CSV was last updated.

**Implication:** The bridge is a necessary but insufficient source of truth.
Additional sources (weekly_rosters, child-table stubs, name-match) are required
for full hub coverage.

**v3 rule:** Hub construction uses a priority-merge across 6+ sources, not just
the bridge. Each source's contribution is logged. Hub coverage is asserted
post-merge — fails if GSIS coverage drops below historical baseline.

---

## 2. Technical constraints (DuckDB, pandas, SQLite)

### 2.1 DuckDB blocks UPDATE on FK-parent rows with children

`UPDATE players SET latest_team = 'KC' WHERE player_gsis_id = '00-0033873'`
fails if any child table has an FK to that row. DuckDB internally treats UPDATE
as DELETE + INSERT, and the DELETE is rejected by the FK.

This also applies to `ON CONFLICT DO UPDATE` (the upsert idiom) — `players`
upsert after children are loaded will fail.

**v1's fix:** `_fetch_players` did all player enrichment (bridge merge, stubs,
name-match) in pandas BEFORE any INSERT. No SQL-level UPDATE on `players` ever
ran after children existed.

**v2's fix:** Same approach via `hub.py`. Hub DataFrame is complete before the
first FK-bearing child write.

**v3 rule:** Never write a phase that modifies players after children load. All
player-level mutations happen in pandas, before the DB connection writes any
FK-bearing child. This is a hard architectural constraint, not a style choice.

### 2.2 PRIMARY KEY requires NOT NULL

`CREATE TABLE players (player_gsis_id VARCHAR PRIMARY KEY, ...)` rejects rows
where `player_gsis_id IS NULL`. This breaks stubs from PFR-only sources like
`combine` (combine-only prospects with no GSIS) or `qbr` (some ESPN-only entries).

**v1/v2 fix:** `UNIQUE` constraint instead of `PRIMARY KEY`. Functionally
equivalent for FK enforcement (FKs target either PK or UNIQUE), but allows NULL.

**v3 rule:** Primary key concept is kept for documentation/design, but the DDL
emits UNIQUE for all ID columns that might have legitimate NULL values.

### 2.3 FK enforcement happens on INSERT; NULL is allowed

DuckDB and SQLite both allow NULL values in FK columns by default. INSERT is
blocked only if the FK column has a non-NULL value that doesn't exist in the
referenced table.

**Implication:** We can declare FKs on columns that have many NULLs without
breaking the build, as long as the non-NULL values resolve. E.g., `game_stats.
game_id` is NULL for pre-2022 rows but FK'd — fine.

**v3 rule:** FKs declared universally; NULL coverage is a separate metric in
the coverage report.

### 2.4 SQLite FKs are opt-in per connection

SQLite's default is `PRAGMA foreign_keys = OFF`. Every consumer connection must
execute `PRAGMA foreign_keys = ON` to get enforcement. Without it, orphans can
be inserted silently.

**v2's fix:** Build-time enforcement is enabled via the pragma on the build
connection. Orphan sweep runs post-build with pragma on. Consumer-facing
documentation notes the requirement explicitly.

**v3 rule:** Same. Also add a `PRAGMA foreign_keys = ON` line to any
documentation code samples.

### 2.5 DuckDB's `union_by_name=true` is load-bearing for year partitions

When loading a year-partitioned glob like `game_stats_*.parquet` where the
column set differs across years, `read_parquet(..., union_by_name=true)`
produces a UNION ALL with NULL-fill for missing columns. Without it, DuckDB
errors on schema mismatch.

**v3 rule:** All multi-file glob loads use `union_by_name=true`. pandas-side
equivalent is `pd.concat(dfs, ignore_index=True, sort=False)`.

### 2.6 DuckDB's `arg_max` is the right primitive for "last value ordered by X"

For "most recent team per player" aggregations, `arg_max(team, week)` returns
the team value from the row with max week in the group. Cleaner than a
correlated subquery with ORDER BY ... LIMIT 1.

**v3 rule:** Used in `compute_missing_season_stats` and `backfill_recent_team`.
Prefer `arg_max` over window functions for single-value lookups.

### 2.7 Float ESPN IDs require integer-roundtrip on cast-to-string

ESPN IDs like `3139477` come out of CSV reads as float64 when the column also
contains NaNs. Naive `.astype(str)` produces `'3139477.0'` or `'3.139477e+06'`
depending on magnitude.

**v3 rule:** `clean_id` does `pd.to_numeric(errors='coerce').astype('Int64').
astype('string')` for any numeric column before sentinel stripping. Or declare
`pre_cast_numeric_to_string` in the source config.

---

## 3. Pipeline architecture (what works, what doesn't)

### 3.1 Declarative config beats distributed fetch functions

v1 had 14+ `_fetch_*` functions scattered across `build_db.py`, each with its
own renames, cleanup, and stubbing logic. Adding a new table meant edits in 3-4
places (fetch fn + TableConfig + stub_source dict + LOAD_ORDER).

v2's `schema.py` consolidates all of that into one declarative file. Adding a
new table is a config entry. Engine interprets uniformly.

**v3 rule:** Single-file declarative config is the only way. Resist the urge to
scatter "just one more helper function" across modules.

### 3.2 Uniform engine beats per-table modes

v1 had 5 update modes (`year_partition`, `year_partition_upsert`, `full_replace`,
`upsert`, `bulk_parquet`), each with custom code paths. Code duplication,
inconsistent error handling, different FK semantics.

v2 collapsed to effectively 2 modes (single-table write, partitioned write —
though the latter isn't implemented in v2's shipped version, it's the intended
shape). 5 primitives: `load_source`, `build_hub`, `write_table`, `apply_fill_rule`,
`validate`. Every table goes through the same primitives.

**v3 rule:** New primitives only when there's a genuinely new operation. Every
mode-flag alternative should be expressible as config, not engine code.

### 3.3 Cataloging upstream is load-bearing

v1 flew blind: whatever was in `DOWNLOAD_MAP` was the universe. v2 introduced
`catalog.py` but didn't make the manifest authoritative — SOURCES was still
handwritten.

**v3 rule:** The manifest is *the* upstream catalog. SOURCES is *derived*
from it (manifest-first). A committed `data/manifest.json` diffs against
regenerated output — any new file upstream surfaces immediately.

### 3.4 Stub creation without name-match creates duplicates

When a child table has a PFR ID that isn't in players yet, naive stubbing
creates a new NULL-GSIS row. If that player already exists in players with a
GSIS (but no PFR assigned), you now have TWO rows for the same person.

**v1's fix:** Preflight pass (`stub_players_for_config`) attaches IDs to existing
GSIS rows by name-match before creating stubs.

**v2's fix:** HUB_BUILD's `name_match` role formalizes this as a declarative
phase. Unique-safety rejects post-merge collisions.

**v3 rule:** No stub is created without first checking whether the candidate
player already exists under a different ID namespace. Name-match recovery is a
required phase in every source's declaration, explicit `accept-gap` annotation
required to skip it.

### 3.5 Safety nets are cheap when unused, expensive to re-add

v1 had `compute_missing_season_stats` for the case where nflverse's POST feed
omitted a player who had weekly game_stats. At the time of v1, it filled zero
rows (nflverse was complete). v2 kept it — and the first time POST dropped a
player, it'd catch it.

**v3 rule:** Keep every safety net v1 and v2 built, even if it currently no-ops.
The day it's needed, not having it costs hours of debugging. The day it's
needed is often the day before the consumer demo.

### 3.6 Validation must run on every build

v1's orphan sweep and v2's `validate.py` caught real bugs during development.
Without them, I'd have shipped orphans, duplicate IDs, and incomplete backfills.

**v3 rule:** `validate.py` runs as Phase 10 of every build, including
incremental. Hard invariants (orphans, duplicates, gap=0) fail the build. Soft
invariants (row-count deltas, NULL-rate drift) emit warnings.

### 3.7 Institutional knowledge rots without explicit porting

v2 inherited the decision "UNIQUE not PRIMARY KEY on players" but not the
explanation. Future-me (or the next contributor) might "simplify" to PK and
reintroduce the stubs-are-NOT-NULL bug.

**v3 rule:** `docs/DESIGN_RATIONALE.md` enumerates every non-obvious design
choice with a link to the code that implements it and a one-sentence "why" tied
to a concrete past incident. Dropping any mechanism requires updating the
rationale.

---

## 4. Consumer and query-ergonomics lessons

### 4.1 LLMs prefer canonical IDs everywhere

v1's split of `snap_counts.player_pfr_id` (PFR-native) vs `season_stats.
player_gsis_id` (GSIS-native) meant LLMs had to know which ID each table used.
A query like "top rushers by snap_count" required a PFR→GSIS bridge join.

v2's fix: every player-bearing table has `player_gsis_id` backfilled from the
hub. One join pattern works across all tables.

**v3 rule:** Every player-bearing table carries the canonical ID, regardless of
source-native keying. Backfilled via hub lookup in Phase 6. Source-native IDs
are preserved as cross-reference columns (don't drop `player_pfr_id` just
because `player_gsis_id` exists now).

### 4.2 Silent column renames break consumers

v2 renamed `pfr_advanced.tm → team` for internal consistency. Also renamed
`qbr.game_id → espn_game_id` for namespace clarity. Both are correct decisions
in isolation — but both break any existing `SELECT tm FROM pfr_advanced` or
`SELECT game_id FROM qbr` query.

**v3 rule:** Column renames require a `column_compat` annotation on the table.
The build emits both the new and the legacy name (as a simple view or alias).
Breaking changes require a CHANGELOG entry and a deprecation timeline.

### 4.3 Wide format beats normalized for LLMs

v1 kept `game_stats` as a wide 115-column table covering passing, rushing,
receiving, defense, kicking, returns, fumbles, penalties in one row. Normalizing
to per-position tables would have "cleaner" schema but would force LLMs to
write UNION ALL queries for cross-position leaderboards.

**v3 rule:** Keep the wide format. LLMs write `SELECT * FROM game_stats WHERE
position = 'QB'` and it works. Normalization is the consumer's job if they
need it.

### 4.4 ESPN numeric game_ids can't FK to nflverse game_ids

Different ID namespaces. See §1.2.

**v3 rule:** FK declarations require namespace alignment. When a source's
column is a foreign-namespace ID, rename to make the namespace explicit AND
don't declare an FK.

### 4.5 Some "missing" data is data reality

See §1.8. When the consumer asks "why does this player have NULL stats?",
the answer is sometimes "they didn't record any stats." That's not a bug.

**v3 rule:** `expected_gaps` annotations document the data-reality pattern.
Validation checks that observed matches expected. The annotations serve as
both validation inputs AND as guidance for LLM query authors.

---

## 5. Process lessons (how we got it wrong, how to not repeat)

### 5.1 v1 accreted reactively; the accretions encode real knowledge

v1's `pipeline.py` had ~30 hard-won comments explaining why specific things
were done specific ways. Each one was written because something broke. The
code looked messy, but the mess was informative.

**v3 rule:** Before removing any v1 or v2 mechanism, document what invariant
it held. If no invariant, safe to remove. If one, port the mechanism and the
rationale.

### 5.2 Rewrites that drop mechanisms without checking invariants break things

v2 dropped `recover_gsis_by_name` on the assumption that `weekly_rosters`
would subsume it. Reality: `weekly_rosters` starts in 2002, missing 7,100 pre-
1995 HoF draft picks. Their GSIS coverage regressed from 81% to 25%.

**v3 rule:** Rewrites use a checklist. For every v1 mechanism, explicitly
confirm:
- Is it subsumed by a new v2 structure? (If yes, which?)
- Is it still required? (If yes, port.)
- Is it genuinely obsolete? (If yes, why?)

No mechanism is dropped without an entry in this three-column table.

### 5.3 Manual audits miss what systematic ones catch

v1 missed POST files for months. Manual audits ("let's look at nflverse
releases") would catch it if done; they never were. A systematic diff
(`catalog --diff`) would catch it automatically on the first build after
nflverse published.

**v3 rule:** Anything that can be a systematic check, is. Manual audits are
not a substitute for automated gates.

### 5.4 Parity tests surface regressions ad-hoc queries miss

During v2 development, I ran canary queries against v1 and v2 manually to
compare. That's how I caught the draft_picks regression. If I'd skipped that
comparison, v2 would have shipped with a 56-percentage-point coverage drop
undetected.

**v3 rule:** Committed canary query suite with committed expected proofs.
Regressions detected automatically, reviewed explicitly.

### 5.5 "Fresh eyes" is valuable; so is institutional memory

The user's prompt "don't defer too much to the existing scripts and data from
the old version, sometimes fresh eyes and a fresh approach is best once you
have new data" was correct — v2 collapsed depth_charts into one table, switched
QBR sources, widened to 23 tables. Fresh eyes caught things v1 couldn't.

But the same prompt, applied too aggressively, would have dropped
`recover_gsis_by_name` permanently. The skill is knowing which v1 accretion
is cruft (upsert/full_replace mode duplication) and which is load-bearing
(name-match recovery, UNIQUE-not-PK, compute_missing_season_stats).

**v3 rule:** Fresh eyes for *architecture*, institutional memory for
*mechanisms*. The structure can be redesigned; the individual failure modes
each mechanism addresses can't.

### 5.6 Consumer stability has a real cost when broken

v2 renamed `pfr_advanced.tm → team` for internal consistency. Technically
correct. But if NFL_AI_AGENT was running `SELECT tm FROM pfr_advanced` queries,
they'd fail silently after the rename.

**v3 rule:** Consumer-facing schema stability is treated as a constraint, not
a suggestion. Renames require compat aliases during a deprecation period.
Removed columns require a CHANGELOG entry.

### 5.7 Scope discipline matters

v2 started as "a fix for the POST-season gap" and turned into a full DB
redesign. That was partially warranted (v1 needed modernization) but partially
scope creep that introduced the regressions we're now fixing.

**v3 rule:** Design docs commit to scope. New features added mid-implementation
require a design-doc update + explicit approval.

---

## 6. v3 design implications (what each lesson translates to)

Each lesson above maps to a v3 design decision:

| Lesson | v3 encoding |
|---|---|
| 1.1 Multi-file release patterns | Manifest catalog + manifest-derived schema |
| 1.2 Foreign-namespace IDs | FK declarations require namespace alignment; rename to disambiguate |
| 1.3 Schema drift | `force_types` + survey drift detection + `union_by_name` in SQL |
| 1.4 Pre-GSIS IDs | `clean_id(kind='generic')` on players master |
| 1.5 Junk sentinels | `clean_id` handles all observed forms |
| 1.6 Varying year ranges | Explicit year_range declaration per source; survey reports observed |
| 1.7 Aliased PFR IDs | Hub unique-safety rejects collisions |
| 1.8 snap_counts without stats | `expected_gaps` annotation |
| 1.9 Cross-era ID gaps | Name-match recovery as declared phase |
| 1.10 Bridge incompleteness | Multi-source priority merge, not bridge-only |
| 2.1 DuckDB FK-parent UPDATE | Pandas-layer hub before any child write |
| 2.2 PK requires NOT NULL | UNIQUE instead of PK on all ID cols |
| 2.3 FK allows NULL | FKs declared freely; NULL coverage tracked separately |
| 2.4 SQLite FK opt-in | `PRAGMA foreign_keys = ON` documented + enforced in consumer samples |
| 2.5 union_by_name | Used in every multi-file load |
| 2.6 arg_max | Used for all "last value by dimension" lookups |
| 2.7 Float ID stringification | `clean_id` numeric handling |
| 3.1 Declarative config | Single `schema.py`; engine interprets |
| 3.2 Uniform engine | 5 primitives; no per-table special cases |
| 3.3 Catalog as load-bearing | Manifest committed + diffed per build |
| 3.4 Stub-without-match creates dupes | Name-match mandatory in hub phase |
| 3.5 Safety nets | All v1/v2 safety mechanisms preserved (compute_missing_season_stats, backfill_season_stats_team, etc.) |
| 3.6 Validation every build | Phase 10 runs always, not just on --all |
| 3.7 Institutional knowledge | DESIGN_RATIONALE.md; every non-obvious choice documented |
| 4.1 Canonical ID everywhere | `id_backfill` from hub to every player-bearing table |
| 4.2 Rename compat | `column_compat` annotations; aliases emitted |
| 4.3 Wide format | Preserved |
| 4.4 Foreign-namespace FKs | FK declarations validate namespace |
| 4.5 Missing-as-reality | `expected_gaps` annotations with "filter by X" guidance |
| 5.1 Accretions encode knowledge | Porting checklist; rationale entries required |
| 5.2 Drop-without-invariant-check | Three-column audit table per mechanism |
| 5.3 Manual audits miss things | Automated `catalog --diff`, `survey --gate` |
| 5.4 Parity tests | Committed canary suite + proof file |
| 5.5 Fresh eyes + memory | Architecture can be redesigned, mechanisms can't be dropped without check |
| 5.6 Consumer stability | Column compat layer; CHANGELOG for breaking changes |
| 5.7 Scope discipline | Design-doc commits |

---

## How to use this document

- **Before starting v3 implementation**: Read top to bottom. Confirm the v3
  design doc (DESIGN_V3 or equivalent) addresses every lesson in §6.
- **Before dropping a v1 or v2 mechanism**: Find the lesson that justified its
  existence. Port or document the alternative that replaces it.
- **Before renaming a column or table**: Check §4.2 and confirm the
  `column_compat` path is followed.
- **When a consumer reports a query anomaly**: Check `expected_gaps`
  annotations for the source. If documented, the anomaly is data reality.
- **When nflverse publishes something new**: The manifest diff catches it.
  Follow the §1.1 / §3.3 flow.
- **When onboarding a contributor**: This doc is the starting point. It's the
  map of the landmines.

Keep this document up to date as v3 teaches new lessons.
