# NFLVERSE v1 vs v2 vs v3 — three-way comparison

Live-number comparison, regenerated after v3's two audit passes landed.

Built from:
- v1: `data/nflverse.duckdb.v1final.bak` (shipped on `main`)
- v2: `data/nflverse.duckdb.v2rebuild.bak` (`v2-redesign` branch, post-fix state)
- v3: `data/nflverse.duckdb` (current `v3` branch, post-audit-2)

## Top-line

| Metric | v1 | v2 | v3 |
|---|---:|---:|---:|
| Tables | 14 | 23 | 25 |
| Foreign keys | 60 | 72 | 78 |
| Total rows | 3,592,983 | 5,475,141 | 5,777,445 |
| DuckDB size | 906 MB | 1171 MB | 1278 MB |

## Table presence

| Table | v1 | v2 | v3 |
|---|---:|---:|---:|
| combine | 8,649 | 8,649 | 8,649 |
| contracts | — | 50,817 | 50,817 |
| contracts_cap_breakdown | — | — | 302,242 |
| depth_charts | 869,185 | 869,185 | 869,185 |
| depth_charts_2025 | 476,501 | 476,501 | 476,501 |
| draft_picks | 12,670 | 12,670 | 12,670 |
| ftn_charting | — | 185,215 | 185,215 |
| game_stats | 476,155 | 476,155 | 476,155 |
| games | 7,276 | 7,276 | 7,276 |
| injuries | — | 90,752 | 90,752 |
| ngs_stats | 26,656 | 26,656 | 26,656 |
| officials | — | 21,900 | 21,900 |
| pbp_participation | — | 478,989 | 478,989 |
| pfr_advanced | 7,798 | 15,335 | 15,335 |
| pfr_advanced_weekly | — | 121,954 | 121,954 |
| play_by_play | 1,279,628 | 1,279,628 | 1,279,628 |
| player_ids | 7,703 | 7,703 | 7,703 |
| players | 24,992 | 26,741 | 26,741 |
| qbr | 9,570 | 10,709 | 10,709 |
| season_stats | 61,589 | 61,588 | 61,588 |
| snap_counts | 324,611 | 324,611 | 324,611 |
| stadiums | — | — | 62 |
| team_game_stats | — | 14,531 | 14,531 |
| team_season_stats | — | 1,198 | 1,198 |
| weekly_rosters | — | 906,378 | 906,378 |

## Player-ID coverage (% populated)

| Table | ID | v1 | v2 | v3 |
|---|---|---:|---:|---:|
| combine | player_gsis_id | — | 77% | 77% |
| combine | player_pfr_id | 80% | 80% | 80% |
| contracts | player_gsis_id | — | 93% | 93% |
| contracts_cap_breakdown | player_gsis_id | — | — | 97% |
| depth_charts | player_gsis_id | 100% | 100% | 100% |
| depth_charts_2025 | player_gsis_id | 99% | 99% | 99% |
| depth_charts_2025 | player_espn_id | 100% | 100% | 100% |
| draft_picks | player_gsis_id | 81% | 82% | 82% |
| draft_picks | player_pfr_id | 86% | 86% | 86% |
| game_stats | player_gsis_id | 100% | 100% | 100% |
| injuries | player_gsis_id | — | 100% | 100% |
| ngs_stats | player_gsis_id | 100% | 100% | 100% |
| pfr_advanced | player_gsis_id | — | 100% | 100% |
| pfr_advanced | player_pfr_id | 100% | 100% | 100% |
| pfr_advanced_weekly | player_gsis_id | — | 100% | 100% |
| pfr_advanced_weekly | player_pfr_id | — | 100% | 100% |
| players | player_gsis_id | 98% | 98% | 98% |
| players | player_pfr_id | 91% | 85% | 85% |
| players | player_espn_id | 65% | 61% | 61% |
| qbr | player_gsis_id | — | 100% | 100% |
| qbr | player_espn_id | 100% | 100% | 100% |
| season_stats | player_gsis_id | 100% | 100% | 100% |
| snap_counts | player_gsis_id | — | 100% | 100% |
| snap_counts | player_pfr_id | 100% | 100% | 100% |
| weekly_rosters | player_gsis_id | — | 100% | 100% |
| weekly_rosters | player_pfr_id | — | 36% | 98% |
| weekly_rosters | player_espn_id | — | 46% | 98% |

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
| Uniform engine primitives | — | ✓ | ✓ |
| Data catalog + manifest | — | ✓ | ✓ |
| Canonical GSIS on every player table | — | ✓ | ✓ |
| UNIQUE-safety on hub ID merges | — | ✓ | ✓ |
| POST season_stats files loaded | — | ✓ | ✓ |
| PFR defensive advstats loaded | — | ✓ | ✓ |
| Name-match GSIS recovery | ✓ | — | ✓ |
| `pfr_advanced.tm` preserved | ✓ | — | ✓ |
| `qbr.game_id` preserved | ✓ | — | ✓ |
| Incremental rebuild (`--years`) | ✓ | — | ✓ |
| game_stats.game_id fill from games | — | — | ✓ |
| weekly_rosters PFR/ESPN backfill | — | — | ✓ |
| contracts bio backfill | — | — | ✓ |
| stadiums reference table | — | — | ✓ |
| contracts_cap_breakdown (struct flatten) | — | — | ✓ |
| games home_qb_id/away_qb_id FK | — | — | ✓ |
| Manifest drift detection (`--diff`) | — | — | ✓ |
| Schema-skeleton audit | — | — | ✓ |
| Pre-build ID-space survey | — | — | ✓ |
| `expected_gaps` validation | — | — | ✓ |
| Committed canary query suite | — | — | ✓ |
| `LESSONS_LEARNED.md` | — | — | ✓ |
| `DESIGN_RATIONALE.md` | — | — | ✓ |

## Summary

- **v1 → v2** (13→23 tables): broader coverage (+9 new tables, POST + defensive PFR files loaded, canonical GSIS). v2 introduced regressions that were fixed on v2-redesign branch (draft_picks recovery, column compat).
- **v2 → v3** (23→25 tables): adds stadiums + contracts_cap_breakdown, 6 new FKs (+6), 6 new fill rules, full prevention-gate layer (manifest diff, survey, expected_gaps, canary suite, docs). All v1 mechanisms v2 dropped are restored.
- All three integrity-clean: 0 orphans, 0 duplicate IDs, 0 game→season gap.
- **v3 is the ship candidate.** `main` stays on v1 as the stable fallback until consumer verifies v3.
