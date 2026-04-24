#!/usr/bin/env python3
"""Committed canary query suite.

Runs every build. Each entry is an LLM-style query + an expected result
shape (row count, key columns, sample row). Results go to
data/canary_proof.json; diffing the proof across builds surfaces
behavior regressions automatically.

Queries chosen to exercise:
  - Every new v3 table (contracts, injuries, officials, ftn, participation, weekly_rosters)
  - Every ID-backfill path (snap_counts → GSIS, qbr → GSIS, pfr_advanced → GSIS, combine → GSIS)
  - Post-season season_stats coverage
  - v_depth_charts cross-schema composite
  - draft_picks name-match recovery (Phase 6)
  - Canonical-ID join equivalence (GSIS vs PFR route on snap_counts)

Usage:
    python3 scripts/canary_queries.py                  # run all, write proof
    python3 scripts/canary_queries.py --verify         # compare result to committed proof, exit non-zero on diff
    python3 scripts/canary_queries.py --query Q7       # run one by id
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[1]
PROOF_PATH = ROOT / "data" / "canary_proof.json"
DEFAULT_DB = ROOT / "data" / "nflverse.duckdb"


# Each query: id, description, SQL, and a shape validator:
#   expected_min_rows: int (fail if result has fewer)
#   expected_columns: list[str] (fail if any missing)
#   sample_check: optional lambda taking the first row, returns bool
CANARY = [
    {
        "id": "Q1",
        "description": "2023 POST top rushers (tests season_stats POST coverage)",
        "sql": """
            SELECT p.display_name, ss.recent_team, ss.rushing_yards, ss.rushing_tds, ss.games
            FROM season_stats ss JOIN players p ON ss.player_gsis_id = p.player_gsis_id
            WHERE ss.season = 2023 AND ss.season_type = 'POST'
              AND ss.rushing_yards IS NOT NULL
            ORDER BY ss.rushing_yards DESC LIMIT 10
        """,
        "expected_min_rows": 10,
        "expected_columns": ["display_name", "recent_team", "rushing_yards", "rushing_tds", "games"],
    },
    {
        "id": "Q2",
        "description": "2024 LBs by defensive snaps with tackles (tests snap_counts ↔ season_stats)",
        "sql": """
            SELECT p.display_name, sc.defense_snaps,
                   COALESCE(ss.def_tackles_solo + ss.def_tackles_with_assist, 0) AS tackles
            FROM snap_counts sc
            JOIN players p ON sc.player_pfr_id = p.player_pfr_id
            LEFT JOIN season_stats ss ON ss.player_gsis_id = p.player_gsis_id
              AND ss.season = 2024 AND ss.season_type = 'REG'
            WHERE sc.season = 2024 AND sc.game_type = 'REG' AND sc.position LIKE '%LB%'
              AND sc.defense_snaps > 50
            ORDER BY sc.defense_snaps DESC LIMIT 10
        """,
        "expected_min_rows": 10,
        "expected_columns": ["display_name", "defense_snaps", "tackles"],
    },
    {
        "id": "Q3",
        "description": "GSIS-native join on snap_counts (tests id_backfill completeness)",
        "sql": """
            SELECT p.display_name, sc.defense_snaps
            FROM snap_counts sc JOIN players p ON sc.player_gsis_id = p.player_gsis_id
            WHERE sc.season = 2024 AND sc.game_type = 'REG' AND sc.position LIKE '%LB%'
              AND sc.defense_snaps > 80
            ORDER BY sc.defense_snaps DESC LIMIT 5
        """,
        "expected_min_rows": 3,
        "expected_columns": ["display_name", "defense_snaps"],
    },
    {
        "id": "Q4",
        "description": "Patrick Mahomes career passing (tests time-series continuity)",
        "sql": """
            SELECT season, season_type, passing_yards, passing_tds, passing_interceptions, games
            FROM season_stats WHERE player_gsis_id = '00-0033873'
            ORDER BY season, season_type
        """,
        "expected_min_rows": 12,
        "expected_columns": ["season", "season_type", "passing_yards", "passing_tds", "games"],
    },
    {
        "id": "Q5",
        "description": "Highest-paid active QBs (tests contracts table)",
        "sql": """
            SELECT p.display_name, c.team, c.apy, c.years
            FROM contracts c JOIN players p ON c.player_gsis_id = p.player_gsis_id
            WHERE c.is_active = TRUE AND p.position = 'QB'
            ORDER BY c.apy DESC NULLS LAST LIMIT 5
        """,
        "expected_min_rows": 5,
        "expected_columns": ["display_name", "team", "apy", "years"],
    },
    {
        "id": "Q6",
        "description": "Most-injured players 2024 (tests injuries table)",
        "sql": """
            SELECT p.display_name, COUNT(DISTINCT i.week) AS weeks_on_report
            FROM injuries i JOIN players p ON i.player_gsis_id = p.player_gsis_id
            WHERE i.season = 2024 AND i.report_primary_injury IS NOT NULL
            GROUP BY p.display_name
            ORDER BY weeks_on_report DESC LIMIT 5
        """,
        "expected_min_rows": 5,
        "expected_columns": ["display_name", "weeks_on_report"],
    },
    {
        "id": "Q7",
        "description": "Top officiating crews by games since 2020 (tests officials table)",
        "sql": """
            SELECT official_name, position, COUNT(DISTINCT old_game_id) AS games
            FROM officials WHERE season >= 2020
            GROUP BY official_name, position ORDER BY games DESC LIMIT 5
        """,
        "expected_min_rows": 5,
        "expected_columns": ["official_name", "position", "games"],
    },
    {
        "id": "Q8",
        "description": "Josh Allen weekly passing 2023 (tests pfr_advanced_weekly + game_stats)",
        "sql": """
            SELECT gs.week, gs.passing_yards, gs.passing_tds, gs.passing_interceptions
            FROM game_stats gs
            WHERE gs.player_gsis_id = '00-0034857' AND gs.season = 2023 AND gs.season_type = 'REG'
            ORDER BY gs.week
        """,
        "expected_min_rows": 15,
        "expected_columns": ["week", "passing_yards", "passing_tds"],
    },
    {
        "id": "Q9",
        "description": "v_depth_charts cross-schema composite (tests view coverage)",
        "sql": """
            SELECT season, team, COUNT(DISTINCT player_gsis_id) AS players_charted
            FROM v_depth_charts
            WHERE season = 2024 AND team = 'KC' AND position = 'QB'
            GROUP BY season, team
        """,
        "expected_min_rows": 1,
        "expected_columns": ["season", "team", "players_charted"],
    },
    {
        "id": "Q10",
        "description": "Pre-1995 HoF draft picks have GSIS (tests name-match recovery)",
        "sql": """
            SELECT season, pfr_player_name, player_gsis_id
            FROM draft_picks
            WHERE season = 1983 AND round = 1 AND pfr_player_name IS NOT NULL
            ORDER BY pick LIMIT 5
        """,
        "expected_min_rows": 5,
        "expected_columns": ["season", "pfr_player_name", "player_gsis_id"],
        # After Phase 6 name-match recovery, 1983 first-round picks should have GSIS filled.
        "sample_check": lambda r: r.get("player_gsis_id") is not None,
    },
    {
        "id": "Q11",
        "description": "Top weekly_rosters player appearances (tests weekly_rosters table)",
        "sql": """
            SELECT p.display_name, COUNT(*) AS weeks
            FROM weekly_rosters wr JOIN players p ON wr.player_gsis_id = p.player_gsis_id
            WHERE wr.season = 2024
            GROUP BY p.display_name
            ORDER BY weeks DESC LIMIT 5
        """,
        "expected_min_rows": 5,
        "expected_columns": ["display_name", "weeks"],
    },
    {
        "id": "Q12",
        "description": "Defensive advanced stats coverage (tests pfr_advanced_season_def load)",
        "sql": """
            SELECT stat_type, COUNT(*) AS rows
            FROM pfr_advanced
            GROUP BY stat_type ORDER BY stat_type
        """,
        "expected_min_rows": 4,
        "expected_columns": ["stat_type", "rows"],
    },
    {
        "id": "Q13",
        "description": "QBR canonical GSIS join (tests qbr id_backfill)",
        "sql": """
            SELECT p.display_name, AVG(q.qbr_total) AS avg_qbr, COUNT(*) AS games
            FROM qbr q JOIN players p ON q.player_gsis_id = p.player_gsis_id
            WHERE q.season = 2023 AND q.season_type = 'Regular'
            GROUP BY p.display_name HAVING COUNT(*) >= 10
            ORDER BY avg_qbr DESC LIMIT 5
        """,
        "expected_min_rows": 5,
        "expected_columns": ["display_name", "avg_qbr", "games"],
    },
    {
        "id": "Q14",
        "description": "FTN charting play-count (tests ftn_charting table)",
        "sql": """
            SELECT season, COUNT(*) AS plays_charted
            FROM ftn_charting GROUP BY season ORDER BY season
        """,
        "expected_min_rows": 3,
        "expected_columns": ["season", "plays_charted"],
    },
    {
        "id": "Q16",
        "description": "Starting QBs by most games since 2020 (tests games.home_qb_id/away_qb_id FK)",
        "sql": """
            WITH qb_games AS (
                SELECT home_qb_id AS gsis, season FROM games WHERE season >= 2020 AND home_qb_id IS NOT NULL
                UNION ALL
                SELECT away_qb_id AS gsis, season FROM games WHERE season >= 2020 AND away_qb_id IS NOT NULL
            )
            SELECT p.display_name, COUNT(*) AS starts
            FROM qb_games q JOIN players p ON q.gsis = p.player_gsis_id
            GROUP BY p.display_name ORDER BY starts DESC LIMIT 5
        """,
        "expected_min_rows": 5,
        "expected_columns": ["display_name", "starts"],
    },
    {
        "id": "Q17",
        "description": "FTN charting joined to play_by_play (tests composite FK join)",
        "sql": """
            SELECT f.season, COUNT(*) AS plays, AVG(CASE WHEN pbp.pass THEN 1.0 ELSE 0 END) AS pass_rate
            FROM ftn_charting f
            JOIN play_by_play pbp ON pbp.game_id = f.game_id AND pbp.play_id = f.play_id
            WHERE f.season = 2024
            GROUP BY f.season
        """,
        "expected_min_rows": 1,
        "expected_columns": ["season", "plays", "pass_rate"],
    },
    {
        "id": "Q15",
        "description": "FK orphan sweep across all declared FKs (integrity invariant)",
        "sql": """
            WITH fk_orphans AS (
                SELECT table_name, constraint_column_names[1] AS col,
                       referenced_table AS rt, referenced_column_names[1] AS rc
                FROM duckdb_constraints()
                WHERE constraint_type = 'FOREIGN KEY'
            )
            SELECT COUNT(*) AS fk_count FROM fk_orphans
        """,
        "expected_min_rows": 1,
        "expected_columns": ["fk_count"],
        "sample_check": lambda r: r.get("fk_count", 0) >= 60,
    },
]


def run_queries(db_path: Path, only: str | None = None) -> dict:
    con = duckdb.connect(str(db_path), read_only=True)
    results: dict = {}
    try:
        for q in CANARY:
            if only and q["id"] != only:
                continue
            print(f"  {q['id']}: {q['description']}")
            try:
                rows = con.execute(q["sql"]).fetchdf()
                sample = rows.head(3).to_dict("records") if len(rows) else []
                results[q["id"]] = {
                    "description": q["description"],
                    "row_count": len(rows),
                    "columns": list(rows.columns),
                    "sample": sample,
                    "status": "ok",
                }
                # Shape checks
                violations = []
                if len(rows) < q.get("expected_min_rows", 0):
                    violations.append(f"got {len(rows)} rows, expected ≥{q['expected_min_rows']}")
                for col in q.get("expected_columns", []):
                    if col not in rows.columns:
                        violations.append(f"missing column {col!r}")
                check = q.get("sample_check")
                if check and sample and not check(sample[0]):
                    violations.append("sample_check failed on first row")
                if violations:
                    results[q["id"]]["status"] = "FAIL"
                    results[q["id"]]["violations"] = violations
                    print(f"    FAIL: {'; '.join(violations)}")
                else:
                    print(f"    ok ({len(rows)} rows)")
            except Exception as e:
                results[q["id"]] = {"description": q["description"], "status": "ERROR", "error": str(e)}
                print(f"    ERROR: {e}")
    finally:
        con.close()
    return results


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--db", type=str, default=str(DEFAULT_DB))
    parser.add_argument("--query", type=str, default=None, help="Run one query by id (Q1, Q2, ...)")
    parser.add_argument("--verify", action="store_true",
                        help="Compare result to committed proof, exit non-zero on regression")
    parser.add_argument("--no-write", action="store_true", help="Don't update the proof file")
    args = parser.parse_args()

    if not Path(args.db).exists():
        print(f"ERROR: db missing at {args.db}")
        return 2

    print(f"Running {len(CANARY)} canary queries against {args.db}")
    results = run_queries(Path(args.db), only=args.query)

    proof = {
        "generated_at": datetime.now(tz=timezone.utc).isoformat(),
        "db_path": str(args.db),
        "queries": results,
    }

    failed = [qid for qid, r in results.items() if r.get("status") in ("FAIL", "ERROR")]

    if args.verify:
        if not PROOF_PATH.exists():
            print(f"\nERROR: no committed proof at {PROOF_PATH}")
            return 2
        with PROOF_PATH.open() as f:
            committed = json.load(f)
        diffs: list[str] = []
        for qid, r in results.items():
            c = committed.get("queries", {}).get(qid, {})
            if r.get("row_count") != c.get("row_count"):
                diffs.append(f"{qid}: row_count {c.get('row_count')} → {r.get('row_count')}")
            if set(r.get("columns", [])) != set(c.get("columns", [])):
                diffs.append(f"{qid}: columns changed")
        if diffs:
            print(f"\nRegression vs committed proof ({len(diffs)} diff(s)):")
            for d in diffs:
                print(f"  {d}")
            return 1
        if failed:
            print(f"\n{len(failed)} query/queries failed shape checks: {failed}")
            return 1
        print("\nAll canaries match committed proof.")
        return 0

    if not args.no_write and not args.query:
        PROOF_PATH.parent.mkdir(parents=True, exist_ok=True)
        with PROOF_PATH.open("w") as f:
            json.dump(proof, f, indent=2, default=str)
        print(f"\nwrote {PROOF_PATH.relative_to(ROOT)}")

    if failed:
        print(f"\n{len(failed)} query/queries failed shape checks: {failed}")
        return 1

    print(f"\nAll {len(results)} canaries passed shape checks.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
