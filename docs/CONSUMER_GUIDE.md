# Consumer guide — what to tell a downstream agent

For an LLM agent or analyst connecting to `data/nflverse.duckdb` (or the
SQLite sibling), this is the minimum context needed to write correct queries.
Detailed schema lives in `DATABASE.md`; this doc is the short form optimized
for query-authoring correctness.

---

## Quick orientation

Single DuckDB file with **25 tables + 1 view**, **78 foreign keys**, covering
**1999–2025**. Every player-bearing table carries `player_gsis_id` as the
canonical join key.

Tables by role:
- **Parents:** `players`, `player_ids`, `games`, `stadiums`
- **Weekly/seasonal player stats:** `game_stats`, `season_stats`, `snap_counts`,
  `pfr_advanced`, `pfr_advanced_weekly`, `ngs_stats`, `qbr`, `weekly_rosters`,
  `depth_charts`, `depth_charts_2025`, `injuries`
- **Team:** `team_game_stats`, `team_season_stats`, `officials`
- **Player meta/contracts:** `combine`, `draft_picks`, `contracts`,
  `contracts_cap_breakdown`
- **Play-by-play:** `play_by_play`, `pbp_participation`, `ftn_charting`

## Join everywhere via `player_gsis_id`

```sql
SELECT p.display_name, ss.passing_yards
FROM season_stats ss
JOIN players p USING (player_gsis_id)
WHERE ss.season = 2024 AND ss.season_type = 'REG'
ORDER BY ss.passing_yards DESC LIMIT 5;
```

Even tables whose upstream source is PFR-native (`snap_counts`, `pfr_advanced`,
`combine`) or ESPN-native (`qbr`, `depth_charts_2025`) carry `player_gsis_id`
after build-time backfill. Use GSIS. Source-native IDs (`player_pfr_id`,
`player_espn_id`) are also present if you need them.

## Runtime introspection

```sql
-- List tables
SELECT table_name FROM information_schema.tables
WHERE table_schema = 'main' ORDER BY table_name;

-- Describe a table
DESCRIBE players;

-- Auto-derive the join graph
SELECT table_name, constraint_column_names,
       referenced_table, referenced_column_names
FROM duckdb_constraints()
WHERE constraint_type = 'FOREIGN KEY'
ORDER BY table_name;

-- Columns a table has in common with another (join candidates)
SELECT a.column_name
FROM information_schema.columns a
JOIN information_schema.columns b USING (column_name)
WHERE a.table_name = 'snap_counts' AND b.table_name = 'game_stats';
```

---

## Gotchas — read these BEFORE writing queries against these tables

These are the patterns that produce wrong-looking-right results if you miss them.

### 1. `qbr.game_id` is **ESPN's numeric ID**, not nflverse's

```sql
-- WRONG — silently joins nothing, returns 0 rows
SELECT * FROM qbr q JOIN games g ON g.game_id = q.game_id;

-- RIGHT — qbr is ESPN-namespace; join via player_gsis_id + (season, week) instead
SELECT p.display_name, q.qbr_total, g.home_team, g.away_team
FROM qbr q
JOIN players p USING (player_gsis_id)
JOIN games g ON g.season = q.season AND g.week = q.game_week
WHERE q.qualified = TRUE;
```

`qbr.game_id` values look like `'260910009'`. `games.game_id` values look like
`'2015_09_SEA_GB'`. They are different namespaces; no crosswalk exists.

### 2. `officials` joins to `games` via `old_game_id`, not `game_id`

`officials` uses NFL's internal YYYYMMDDGG format (e.g. `'2015091000'`), which
is what `games.old_game_id` holds. The raw column was named `game_id` upstream;
the build renames it to `old_game_id` to make the namespace explicit.

```sql
-- RIGHT
SELECT o.official_name, o.position, g.home_team, g.away_team, g.week
FROM officials o JOIN games g ON g.old_game_id = o.old_game_id
WHERE o.official_name = 'Ronald Torbert' AND g.season = 2023;
```

### 3. `snap_counts` with zero snaps is legitimate data, not a stitching bug

A player can appear in `snap_counts` with `defense_snaps = 0` and
`offense_snaps = 0` — they were on a depth chart or were active but didn't
play a charted snap. These rows legitimately have no corresponding
`game_stats`/`season_stats` row. If you're building a leaderboard, filter:

```sql
WHERE defense_snaps > 0      -- or offense_snaps > 0, depending on the query
```

Don't retry with a different join pattern when these come back with NULL stats;
the data isn't there.

### 4. `game_stats.game_id` is ~89% populated — know the gap pattern

Pre-2022 weekly stats files didn't carry `game_id` upstream. The build
derives `game_id` post-hoc via `(season, week, team, opponent_team) → games`
lookup, which fills most but not all. If you need a reliable join to `games`
for every `game_stats` row, use the derivation pattern directly:

```sql
SELECT gs.*, g.game_id, g.home_team, g.away_team
FROM game_stats gs
JOIN games g
  ON g.season = gs.season AND g.week = gs.week
 AND ((g.home_team = gs.team AND g.away_team = gs.opponent_team)
   OR (g.away_team = gs.team AND g.home_team = gs.opponent_team));
```

### 5. Position granularity differs across tables

- `players.position`, `weekly_rosters.position` → **position GROUPS** (`QB`, `RB`, `WR`, `TE`, `OL`, `DL`, `LB`, `DB`, `K`, `P`, `SPEC`).
- `snap_counts.position` → **fine-grained ROLES** (`FS`, `WLB`, `ILB`, `LCB`, `LT`, `RT`, `LG`, etc.).
- `depth_charts_2025.pos_abb` → also fine-grained.
- `v_depth_charts.position` → normalized to **position GROUPS** across both eras.

These are complementary, not conflicting. A player with
`snap_counts.position = 'FS'` will show up as `players.position = 'DB'`. Neither
is wrong.

### 6. Pre-GSIS historical players have non-GSIS-format IDs

`players.player_gsis_id` for pre-modern-era players (Joe Montana, Dan Marino,
etc.) uses Elias-format IDs like `'YOU597411'` or `'VIT276861'`. They don't
match the `^\d{2}-\d{7}$` GSIS regex. This is deliberate — those players are
preserved so `draft_picks` and HoF queries still join.

If you filter by `player_gsis_id LIKE '00-%'` you're excluding them.

### 7. `contracts.otc_id ≠ players.otc_id` for ~32% of contracts

`contracts` includes coaches and retired/non-player entries; ~6,300 of 19,700
contract rows won't join to `players`. Use LEFT JOIN if you want to preserve
them:

```sql
SELECT c.player, c.apy, p.display_name, p.position
FROM contracts c LEFT JOIN players p USING (otc_id)
WHERE c.is_active = TRUE;
```

### 8. `season_stats` ratio columns are NULL on derived rows

For (player, season, type) combinations nflverse ships as pre-aggregated, all
columns including ratios (`passer_rating`, `completion_percentage`, `fg_pct`,
`pacr`, `racr`, `wopr`) are populated. For rows derived by the build's safety
net (summed from `game_stats` when the pre-aggregated feed omitted that
combo), ratios are NULL — compute from components:

```sql
SELECT completions * 100.0 / NULLIF(attempts, 0) AS completion_pct,
       passing_yards * 1.0 / NULLIF(attempts, 0) AS yards_per_attempt
FROM season_stats;
```

Currently nflverse's feed is complete (derivation rule fills 0-1 rows per
build), but don't assume ratios are always non-null.

### 9. Units and magnitudes on `contracts`

- `apy`, `value`, `guaranteed`, `inflated_apy`, etc. are in **millions of dollars**. Daniel Jones' `apy = 44.0` means $44M/year.
- `cap_percent` in `contracts_cap_breakdown` is a decimal fraction (`0.032` = 3.2%).

### 10. `contracts_cap_breakdown` is the year-by-year view

The year-by-year salary details live in `contracts_cap_breakdown` (one row per
contract × cap-year). The parent `contracts` row has top-line deal terms;
`cols` on the parent is the raw STRUCT array that got flattened into
`contracts_cap_breakdown`. Query the flat table, not `cols[1].cap_number`.

```sql
SELECT p.display_name, c.cap_year, c.team, c.cap_number, c.cap_percent
FROM contracts_cap_breakdown c JOIN players p USING (player_gsis_id)
WHERE p.display_name = 'Patrick Mahomes' ORDER BY c.cap_year;
```

---

## 17 working example queries

`scripts/canary_queries.py` is a committed suite of LLM-style queries that
covers every table and every id_backfill path. The file is readable as
reference material — the `CANARY` list is just a list of `{id, description,
sql}` dicts. Expected behavior:

```bash
python3 scripts/canary_queries.py --verify
# Reads data/canary_proof.json (committed) and confirms the current DB still
# returns the same result shape. Useful for detecting regressions if you
# touch the build.
```

The 17 canaries:

| id | what it tests |
|---|---|
| Q1 | 2023 POST top rushers (`season_stats` POST coverage) |
| Q2 | 2024 LB defensive-snap leaders + tackles (`snap_counts` ↔ `season_stats` join) |
| Q3 | GSIS-native join on `snap_counts` (canonical-ID backfill completeness) |
| Q4 | Patrick Mahomes career passing year-by-year |
| Q5 | Highest-paid active QBs (`contracts`) |
| Q6 | Most-injured players 2024 (`injuries`) |
| Q7 | Top officiating crews by games since 2020 (`officials`, via `old_game_id`) |
| Q8 | Josh Allen weekly passing 2023 (`game_stats`) |
| Q9 | `v_depth_charts` cross-schema composite |
| Q10 | Pre-1995 HoF draft picks with GSIS populated (name-match recovery) |
| Q11 | Top weekly_rosters player appearances |
| Q12 | Defensive PFR advanced stats coverage |
| Q13 | QBR canonical GSIS join (`qbr` id_backfill) |
| Q14 | FTN charting play-count by season |
| Q15 | FK orphan sweep across all 78 FKs |
| Q16 | Starting QBs by games since 2020 (`games.home_qb_id`/`away_qb_id`) |
| Q17 | FTN charting joined to `play_by_play` on `(game_id, play_id)` |

---

## What v3 has that v1 (or a prior consumer) didn't

If the consumer was built against an older v1 DB, these are the new things
worth surfacing in their prompt:

**New tables:**
- `stadiums` — 62 rows, reference table (roof, surface, location, first_season, last_season)
- `contracts` + `contracts_cap_breakdown` — year-by-year cap + salary
- `injuries` — weekly injury reports (2009+)
- `officials` — referee crews per game
- `weekly_rosters` — week-level rosters with full cross-ID set (2002+)
- `pbp_participation` — who was on field for each play (2016+)
- `ftn_charting` — FTN manual play tagging (2022+)
- `pfr_advanced_weekly` — PFR week-level advanced stats (2018+)
- `team_game_stats`, `team_season_stats` — team-level aggregates

**Expanded coverage:**
- `pfr_advanced` now includes defensive stats (v1 had pass/rush/rec only)
- `qbr` covers 2006–2025 (v1 had espnscrapeR data only through 2023)
- `season_stats` includes POST (v1 was REG-only)
- `draft_picks.player_gsis_id` at 82% (v1 unchanged at 81%)
- `game_stats.game_id` at 89% populated (v1 was 12%)

**New canonical join paths:**
- `player_gsis_id` on every player-bearing table (v1 had some as PFR/ESPN-only)
- `games.home_qb_id` / `away_qb_id` now FK-declared to `players.player_gsis_id`
- `play_by_play.stadium_id` FK to `stadiums.stadium_id`

---

## What to hand the consumer agent

Minimum package (if limited context):
1. **This doc** (`CONSUMER_GUIDE.md`) — query-authoring correctness
2. **The DB file itself** — they can `DESCRIBE` and query `duckdb_constraints()`
   at runtime

Fuller package:
3. **`DATABASE.md`** — full schema reference (per-table column docs, example
   joins, column meanings); good for deep dives
4. **`scripts/canary_queries.py`** — 17 worked examples they can copy-adapt

Deeper still (usually overkill):
5. **`DESIGN_RATIONALE.md`** — the 17 rules behind the build; useful if the
   agent is debugging "why does this table look like this"
6. **`LESSONS_LEARNED.md`** — field guide of upstream-data reality; useful for
   the agent's reasoning about "should I retry this query or is this data
   genuinely absent"

For most consumer use cases, **CONSUMER_GUIDE.md + the DB file** is enough.
Point at `DATABASE.md` only when the agent needs per-column semantics.
