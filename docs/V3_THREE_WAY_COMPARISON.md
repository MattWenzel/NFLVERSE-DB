# NFLVERSE v1 vs v2 vs v3 — three-way comparison

Built from:
- v1: `data/nflverse.duckdb.v1final.bak` (shipped on `main`)
- v2: `data/nflverse.duckdb.v2rebuild.bak` (`v2-redesign` branch build)
- v3: `data/nflverse.duckdb` (`v3` branch build)

## Top-line

| Metric | v1 | v2 | v3 |
|---|---:|---:|---:|
| Tables | 14 | 23 | 23 |
| Foreign keys | 60 | 72 | 72 |
| Total rows | 3,592,983 | 5,475,141 | 5,475,141 |
| DuckDB size | 906 MB | 1171 MB | 1168 MB |

## Table presence

| Table | v1 | v2 | v3 |
|---|---:|---:|---:|
| combine | 8,649 | 8,649 | 8,649 |
| contracts | — | 50,817 | 50,817 (NEW in v2+) |
| depth_charts | 869,185 | 869,185 | 869,185 |
| depth_charts_2025 | 476,501 | 476,501 | 476,501 |
| draft_picks | 12,670 | 12,670 | 12,670 |
| ftn_charting | — | 185,215 | 185,215 (NEW in v2+) |
| game_stats | 476,155 | 476,155 | 476,155 |
| games | 7,276 | 7,276 | 7,276 |
| injuries | — | 90,752 | 90,752 (NEW in v2+) |
| ngs_stats | 26,656 | 26,656 | 26,656 |
| officials | — | 21,900 | 21,900 (NEW in v2+) |
| pbp_participation | — | 478,989 | 478,989 (NEW in v2+) |
| pfr_advanced | 7,798 | 15,335 | 15,335 |
| pfr_advanced_weekly | — | 121,954 | 121,954 (NEW in v2+) |
| play_by_play | 1,279,628 | 1,279,628 | 1,279,628 |
| player_ids | 7,703 | 7,703 | 7,703 |
| players | 24,992 | 26,741 | 26,741 |
| qbr | 9,570 | 10,709 | 10,709 |
| season_stats | 61,589 | 61,588 | 61,588 |
| snap_counts | 324,611 | 324,611 | 324,611 |
| team_game_stats | — | 14,531 | 14,531 (NEW in v2+) |
| team_season_stats | — | 1,198 | 1,198 (NEW in v2+) |
| weekly_rosters | — | 906,378 | 906,378 (NEW in v2+) |

## Player-ID coverage (% populated)

| Table | ID | v1 | v2 | v3 |
|---|---|---:|---:|---:|
| combine | player_gsis_id | — | 77% | 77% ← backfilled in v2+ |
| combine | player_pfr_id | 80% | 80% | 80% |
| contracts | player_gsis_id | — | 93% | 93% ← backfilled in v2+ |
| depth_charts | player_gsis_id | 100% | 100% | 100% |
| depth_charts_2025 | player_gsis_id | 99% | 99% | 99% |
| depth_charts_2025 | player_espn_id | 100% | 100% | 100% |
| draft_picks | player_gsis_id | 81% | 82% | 82% |
| draft_picks | player_pfr_id | 86% | 86% | 86% |
| game_stats | player_gsis_id | 100% | 100% | 100% |
| injuries | player_gsis_id | — | 100% | 100% ← backfilled in v2+ |
| ngs_stats | player_gsis_id | 100% | 100% | 100% |
| pfr_advanced | player_gsis_id | — | 100% | 100% ← backfilled in v2+ |
| pfr_advanced | player_pfr_id | 100% | 100% | 100% |
| pfr_advanced_weekly | player_gsis_id | — | 100% | 100% ← backfilled in v2+ |
| pfr_advanced_weekly | player_pfr_id | — | 100% | 100% ← backfilled in v2+ |
| players | player_gsis_id | 98% | 98% | 98% |
| players | player_pfr_id | 91% | 85% | 85% |
| players | player_espn_id | 65% | 61% | 61% |
| qbr | player_gsis_id | — | 100% | 100% ← backfilled in v2+ |
| qbr | player_espn_id | 100% | 100% | 100% |
| season_stats | player_gsis_id | 100% | 100% | 100% |
| snap_counts | player_gsis_id | — | 100% | 100% ← backfilled in v2+ |
| snap_counts | player_pfr_id | 100% | 100% | 100% |
| weekly_rosters | player_gsis_id | — | 100% | 100% ← backfilled in v2+ |
| weekly_rosters | player_pfr_id | — | 36% | 36% ← backfilled in v2+ |
| weekly_rosters | player_espn_id | — | 46% | 46% ← backfilled in v2+ |

## Consumer-visible column stability

| Column | v1 | v2 | v3 |
|---|:-:|:-:|:-:|
| `pfr_advanced.tm` | ✓ | ✓ | ✓ |
| `pfr_advanced.team` | ✓ | ✓ | ✓ |
| `qbr.game_id` | ✓ | ✓ | ✓ |
| `qbr.espn_game_id` | — | — | — |
| `snap_counts.player_gsis_id` | — | ✓ | ✓ |
| `pfr_advanced.player_gsis_id` | — | ✓ | ✓ |
| `qbr.player_gsis_id` | — | ✓ | ✓ |
| `combine.player_gsis_id` | — | ✓ | ✓ |

## Integrity invariants

| Invariant | v1 | v2 | v3 |
|---|---:|---:|---:|
| FK orphans (all declared FKs) | 0 | 0 | 0 |
| game_stats → season_stats gap | 0 | 0 | 0 |
| Duplicate players.player_gsis_id | 0 | 0 | 0 |

## Architecture / process capabilities

| Capability | v1 | v2 | v3 |
|---|:-:|:-:|:-:|
| Declarative single-file config | — | ✓ | ✓ |
| Uniform 5-primitive engine | — | ✓ | ✓ |
| Data catalog + manifest | — | ✓ | ✓ |
| Canonical GSIS on every player table | — | ✓ | ✓ |
| UNIQUE-safety on hub ID merges | — | ✓ | ✓ |
| 9 new tables (contracts, injuries, etc.) | — | ✓ | ✓ |
| POST season_stats files loaded | — | ✓ | ✓ |
| PFR defensive advstats loaded | — | ✓ | ✓ |
| Name-match GSIS recovery | ✓ | — | ✓ |
| `pfr_advanced.tm` preserved | ✓ | — | ✓ |
| `qbr.game_id` preserved | ✓ | — | ✓ |
| Incremental rebuild (`--years`) | ✓ | — | ✓ |
| Institutional memory in docs | partial | — | ✓ |
| **Manifest drift detection (`--diff`)** | — | — | ✓ |
| **Schema-skeleton audit** | — | — | ✓ |
| **Pre-build ID-space survey** | — | — | ✓ |
| **`expected_gaps` validation** | — | — | ✓ |
| **Committed canary query suite** | — | — | ✓ |
| **LESSONS_LEARNED.md** | — | — | ✓ |
| **DESIGN_RATIONALE.md** | — | — | ✓ |

## Build timings

- v1: ~2-3 min (14 tables, no gates)
- v2: 7:04 (23 tables, first clean rebuild; current branch just measured)
- v3: ~5-6 min (23 tables + survey + all gates)

## Summary

- **v1 → v2**: broader coverage (+9 tables, POST + defensive PFR files loaded, canonical GSIS on all child tables). But v2 introduced regressions: draft_picks name-match GSIS recovery was dropped (coverage 81%→25%), `pfr_advanced.tm` and `qbr.game_id` columns renamed without aliases (consumer breaks), and incremental rebuild was dropped.
- **v2 → v3**: fixes every v2 regression (draft_picks back to 82%, tm/game_id preserved, `--years` incremental restored) AND adds 7 gates (manifest diff, schema-skeleton audit, pre-build survey, expected_gaps, canary suite, LESSONS_LEARNED, DESIGN_RATIONALE) that would have caught the v1 POST-files miss and the v2 draft_picks regression automatically.
- All three are integrity-clean (0 orphans, 0 duplicate IDs, 0 game→season gap).
- **v3 is the ship candidate.** `main` stays on v1 as the stable fallback until consumer (NFL_AI_AGENT) verifies v3. `v2-redesign` preserved as historical reference.
