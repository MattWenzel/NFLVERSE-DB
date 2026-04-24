# NFLVERSE v1 → v3 comparison

_Focused v1 → v3 delta. For the full three-way view with v2, see [V3_THREE_WAY_COMPARISON.md](V3_THREE_WAY_COMPARISON.md)._

## Top-line

| Metric | v1 | v3 | Δ |
|---|---:|---:|---:|
| Tables | 14 | 25 | +11 |
| Foreign keys | 60 | 78 | +18 |
| Total rows | 3,592,983 | 5,777,445 | +2,184,462 |
| DuckDB size | 906 MB | 1,278 MB | +372 MB |
| Build time | ~2 min | ~7-13 min | longer (25 vs 14 tables + survey + gates) |

## New in v3 (vs v1)

- `contracts` — 50,817 rows
- `contracts_cap_breakdown` — 302,242 rows (derived from `contracts.cols`)
- `ftn_charting` — 185,215 rows
- `injuries` — 90,752 rows
- `officials` — 21,900 rows
- `pbp_participation` — 478,989 rows
- `pfr_advanced_weekly` — 121,954 rows
- `stadiums` — 62 rows (derived from `games.stadium_id`)
- `team_game_stats` — 14,531 rows
- `team_season_stats` — 1,198 rows
- `weekly_rosters` — 906,378 rows

## Regression-free on shared tables

| Table | v1 | v3 | Δ |
|---|---|---|---|
| combine | 8,649 | 8,649 | +0 |
| depth_charts | 869,185 | 869,185 | +0 |
| depth_charts_2025 | 476,501 | 476,501 | +0 |
| draft_picks | 12,670 | 12,670 | +0 |
| game_stats | 476,155 | 476,155 | +0 |
| games | 7,276 | 7,276 | +0 |
| ngs_stats | 26,656 | 26,656 | +0 |
| pfr_advanced | 7,798 | 15,335 | +7,537 |
| play_by_play | 1,279,628 | 1,279,628 | +0 |
| player_ids | 7,703 | 7,703 | +0 |
| players | 24,992 | 26,741 | +1,749 |
| qbr | 9,570 | 10,709 | +1,139 |
| season_stats | 61,589 | 61,588 | -1 |
| snap_counts | 324,611 | 324,611 | +0 |

## Coverage: player_gsis_id on player-bearing tables

| Table | v1 | v3 |
|---|---|---|
| combine | — | 77% |
| draft_picks | 81% | 82% |
| pfr_advanced | — | 100% |
| qbr | — | 100% |
| snap_counts | — | 100% |
| contracts | — | 93% |
| injuries | — | 100% |
| pfr_advanced_weekly | — | 100% |
| weekly_rosters | — | 100% |

## Architecture changes (v2 → v3)

**v3 inherits v2's structural wins and adds gates v2 didn't have.**

### Structural wins inherited from v2
- Single declarative `scripts/schema.py` (vs v1's distributed `_fetch_*` functions)
- Uniform engine with 5 primitives (vs v1's 5 update modes)
- `catalog.py` + `data/nflverse_manifest.json` (v1 had no catalog)
- Canonical `player_gsis_id` backfilled to every player-bearing table
- 9 new tables (contracts, injuries, ftn_charting, pbp_participation, officials, weekly_rosters, pfr_advanced_weekly, team_game_stats, team_season_stats)
- `advstats_season_def.parquet` loaded (v1 missed entirely)
- `stats_player_post_*.parquet` loaded (v1 missed for years)
- `qbr` sourced from nflverse parquet (v1 used stale espnscrapeR CSV)
- UNIQUE-safety on hub ID merges (prevents SmitMa01-style collisions)

### Gates v3 adds (v1 and v2 both lacked)

**`scripts/catalog.py --diff`** — exit-non-zero when upstream changes. Manifest is now a committed contract; missing a new nflverse file is structurally impossible.

**`scripts/schema_generator.py` + `schema_skeleton.py`** — skeleton auto-derived from manifest; `schema.py:audit_against_skeleton` fails if any manifest entry isn't explicitly in SOURCES or SKIPPED_SOURCES. v1 missed POST files for months; v3 can't.

**`scripts/survey.py`** — pre-build ID-space + coverage scan. Fails the build if a child source has >10% hub gap without a declared recovery. v2's draft_picks 25% regression would have tripped this gate immediately.

**`expected_gaps` declarations on SOURCES** — data-reality annotations (e.g., `combine.player_pfr_id.null_rate.max = 0.22`). Validation checks observed vs declared within 2pp. Makes normal data reality explicit instead of treating it as a bug.

**`scripts/canary_queries.py` + `data/canary_proof.json`** — 15 committed LLM-style queries with expected result shapes. Each build regenerates proof; `--verify` diffs for regressions. Would have caught v2's draft_picks regression automatically.

**`docs/DESIGN_RATIONALE.md`** — 17 rules with origin, code path, and don't-change-without clause. Every v1/v2 scar encoded as institutional memory. The 'why' outlives the code.

**`docs/LESSONS_LEARNED.md`** — field guide of upstream reality, technical constraints, pipeline architecture, query ergonomics, process lessons. Required reading before dropping any v1/v2 mechanism.

**Incremental rebuild (`--years 2025`)** — v3 restores v1's operational capability that v2 dropped. Matches v1's ~30s refresh speed for year-partition updates without v1's special-case code paths.

**Consumer-compat column preservation** — v2 renamed `pfr_advanced.tm → team` and `qbr.game_id → espn_game_id`. v3 keeps both native names. No silent breaks of existing consumer queries.

## Critical fixes v3 has over v2

| Issue | v2 state | v3 state |
|---|---|---|
| draft_picks GSIS coverage | 25% (Phase 6 not applied to canonical DB) | 82% (name-match recovery runs) |
| pfr_advanced.tm column | dropped | preserved alongside team |
| qbr.game_id column | renamed to espn_game_id | preserved as native |
| Incremental rebuild | not available | --years flag wired |
| Upstream drift detection | none | catalog.py --diff gate |
| Schema coverage audit | none | audit_against_skeleton gate |
| Pre-build ID survey | none | survey.py gate |
| Expected-gap declarations | none | declared on 4 sources, validated |
| Regression detection | manual | canary_queries.py --verify |
| Institutional memory | lost during v2 rewrite | LESSONS_LEARNED + DESIGN_RATIONALE |

## Full build timing

- v1: ~2-3 min (14 tables, no catalog or survey)
- v2: 5:42 (23 tables, first clean build)
- v3: 4:48 (23 tables + survey + gates)

## Integrity invariants

All three versions satisfy:
- 0 orphans across declared FKs
- 0 game_stats → season_stats gap
- 0 duplicate IDs on players (gsis / pfr / espn)
- SQLite mirror row-count parity + integrity_check='ok'
