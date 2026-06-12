#!/usr/bin/env python3
"""Cross-table stat consistency audit.

The same question is answerable from season_stats, game_stats, or
play_by_play. This audit measures how well they agree so consumers know
which table is authoritative for what (results documented in
docs/CONSUMER_GUIDE.md "Which table is authoritative").

Pass 1 — season_stats vs SUM(game_stats) per (player, season, season_type):
    expected EXACT for every stat (same upstream feed at two grains).
    Any nonzero diff count here is a build regression — exit non-zero.

Pass 2 — play_by_play-derived season totals vs season_stats (REG):
    expected near-perfect but NOT exact. Known, accepted causes:
      - laterals: pbp role columns credit the original ball-carrier;
        official stats split yards with the lateral runner. The
        lateral_* columns recover most of it (receiving_yards exact-match
        rate goes ~97.9% -> ~99.4% when lateral yards are added).
      - official scoring corrections applied to stat feeds but never
        retrofitted into pbp (~70 player-seasons for receiving_yards).
    Pass 2 is informational: results are written to the report, and only
    a drop below the floors in PASS2_FLOORS fails the audit.

Usage:
    python3 scripts/consistency_audit.py          # run, write report, print summary
"""

from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[1]
DB = ROOT / "data" / "nflverse.duckdb"
REPORT = ROOT / "data" / "consistency_report.json"

PASS1_STATS = [
    "completions", "attempts", "passing_yards", "passing_tds", "passing_interceptions",
    "carries", "rushing_yards", "rushing_tds", "receptions", "targets",
    "receiving_yards", "receiving_tds", "def_sacks", "def_interceptions",
    "def_tackles_solo", "fg_made", "fg_att", "fantasy_points_ppr",
]

# Minimum acceptable exact-match rates for pbp-derived vs season_stats (REG).
# Set just below measured values (2026-06) so upstream drift is caught
# without flagging the known lateral/correction residue.
PASS2_FLOORS = {
    "passing_yards": 99.5, "passing_tds": 99.9, "passing_ints": 99.9,
    "rushing_yards": 99.0, "rushing_tds": 99.5,
    "receptions": 99.9, "receiving_yards": 97.0, "receiving_tds": 99.5,
    "def_sacks": 99.9,
}


def pass1(con) -> dict:
    sum_cols = ", ".join(f"SUM(g.{s}) AS {s}" for s in PASS1_STATS)
    cmp_cols = ", ".join(
        f"COUNT(CASE WHEN COALESCE(ABS(s.{s} - b.{s}), "
        f"CASE WHEN s.{s} IS NULL AND b.{s} IS NULL THEN 0 ELSE 999 END) > 0.01 "
        f"THEN 1 END) AS {s}"
        for s in PASS1_STATS
    )
    row = con.execute(f"""
        WITH b AS (
          SELECT player_gsis_id, season, season_type, {sum_cols}
          FROM game_stats g GROUP BY 1,2,3)
        SELECT COUNT(*) AS player_seasons, {cmp_cols}
        FROM season_stats s JOIN b USING (player_gsis_id, season, season_type)
    """).fetchone()
    cols = ["player_seasons"] + PASS1_STATS
    out = dict(zip(cols, row))
    out["diff_total"] = sum(out[s] for s in PASS1_STATS)
    return out


def pass2(con) -> dict:
    rows = con.execute("""
        WITH pass_pbp AS (
          SELECT passer_player_id AS pid, season,
                 SUM(COALESCE(passing_yards,0)) AS pass_yds,
                 SUM(COALESCE(pass_touchdown,0)) AS pass_tds,
                 SUM(COALESCE(interception,0)) AS ints
          FROM play_by_play WHERE season_type='REG' AND passer_player_id IS NOT NULL GROUP BY 1,2),
        rush_pbp AS (
          SELECT pid, season, SUM(y) AS rush_yds, SUM(td) AS rush_tds FROM (
            SELECT rusher_player_id AS pid, season, COALESCE(rushing_yards,0) AS y,
                   COALESCE(rush_touchdown,0) AS td
            FROM play_by_play WHERE season_type='REG' AND rusher_player_id IS NOT NULL
            UNION ALL
            SELECT lateral_rusher_player_id, season, COALESCE(lateral_rushing_yards,0), 0
            FROM play_by_play WHERE season_type='REG' AND lateral_rusher_player_id IS NOT NULL)
          GROUP BY 1,2),
        rec_pbp AS (
          SELECT pid, season, SUM(rec) AS rec, SUM(y) AS rec_yds, SUM(td) AS rec_tds FROM (
            SELECT receiver_player_id AS pid, season, COALESCE(complete_pass,0) AS rec,
                   COALESCE(receiving_yards,0) AS y, COALESCE(pass_touchdown,0) AS td
            FROM play_by_play WHERE season_type='REG' AND receiver_player_id IS NOT NULL
            UNION ALL
            SELECT lateral_receiver_player_id, season, 0, COALESCE(lateral_receiving_yards,0), 0
            FROM play_by_play WHERE season_type='REG' AND lateral_receiver_player_id IS NOT NULL)
          GROUP BY 1,2),
        sack_pbp AS (
          SELECT pid, season, SUM(s) AS sacks FROM (
            SELECT sack_player_id AS pid, season, 1.0 AS s FROM play_by_play
              WHERE season_type='REG' AND sack_player_id IS NOT NULL
            UNION ALL SELECT half_sack_1_player_id, season, 0.5 FROM play_by_play
              WHERE season_type='REG' AND half_sack_1_player_id IS NOT NULL
            UNION ALL SELECT half_sack_2_player_id, season, 0.5 FROM play_by_play
              WHERE season_type='REG' AND half_sack_2_player_id IS NOT NULL)
          GROUP BY 1,2),
        ss AS (SELECT player_gsis_id AS pid, season, passing_yards, passing_tds,
                      passing_interceptions, rushing_yards, rushing_tds, receptions,
                      receiving_yards, receiving_tds, def_sacks
               FROM season_stats WHERE season_type='REG')
        SELECT stat, COUNT(*) AS n,
               ROUND(100.0*COUNT(CASE WHEN d = 0 THEN 1 END)/COUNT(*), 2) AS pct_exact,
               MAX(d) AS max_diff
        FROM (
          SELECT 'passing_yards' AS stat, ABS(s.passing_yards - p.pass_yds) AS d
            FROM ss s JOIN pass_pbp p USING (pid, season)
            WHERE s.passing_yards IS NOT NULL AND (s.passing_yards>0 OR p.pass_yds>0)
          UNION ALL SELECT 'passing_tds', ABS(s.passing_tds - p.pass_tds)
            FROM ss s JOIN pass_pbp p USING (pid, season) WHERE s.passing_tds IS NOT NULL
          UNION ALL SELECT 'passing_ints', ABS(s.passing_interceptions - p.ints)
            FROM ss s JOIN pass_pbp p USING (pid, season) WHERE s.passing_interceptions IS NOT NULL
          UNION ALL SELECT 'rushing_yards', ABS(s.rushing_yards - p.rush_yds)
            FROM ss s JOIN rush_pbp p USING (pid, season)
            WHERE s.rushing_yards IS NOT NULL AND (s.rushing_yards<>0 OR p.rush_yds<>0)
          UNION ALL SELECT 'rushing_tds', ABS(s.rushing_tds - p.rush_tds)
            FROM ss s JOIN rush_pbp p USING (pid, season) WHERE s.rushing_tds IS NOT NULL
          UNION ALL SELECT 'receptions', ABS(s.receptions - p.rec)
            FROM ss s JOIN rec_pbp p USING (pid, season) WHERE s.receptions IS NOT NULL
          UNION ALL SELECT 'receiving_yards', ABS(s.receiving_yards - p.rec_yds)
            FROM ss s JOIN rec_pbp p USING (pid, season)
            WHERE s.receiving_yards IS NOT NULL AND (s.receiving_yards<>0 OR p.rec_yds<>0)
          UNION ALL SELECT 'receiving_tds', ABS(s.receiving_tds - p.rec_tds)
            FROM ss s JOIN rec_pbp p USING (pid, season) WHERE s.receiving_tds IS NOT NULL
          UNION ALL SELECT 'def_sacks', ABS(s.def_sacks - p.sacks)
            FROM ss s JOIN sack_pbp p USING (pid, season)
            WHERE s.def_sacks IS NOT NULL AND s.def_sacks > 0
        )
        GROUP BY 1 ORDER BY 1
    """).fetchall()
    return {r[0]: {"n": r[1], "pct_exact": r[2], "max_diff": r[3]} for r in rows}


def main() -> int:
    con = duckdb.connect(str(DB), read_only=True)
    p1 = pass1(con)
    p2 = pass2(con)
    con.close()

    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "pass1_season_vs_game_stats": p1,
        "pass2_pbp_vs_season_stats_reg": p2,
        "pass2_floors": PASS2_FLOORS,
    }
    REPORT.write_text(json.dumps(report, indent=2))

    failures = []
    print(f"Pass 1 — season_stats vs SUM(game_stats): "
          f"{p1['player_seasons']:,} player-seasons, {p1['diff_total']} mismatched cells")
    if p1["diff_total"] != 0:
        bad = {s: p1[s] for s in PASS1_STATS if p1[s]}
        failures.append(f"pass1 mismatches: {bad}")

    print("Pass 2 — pbp-derived vs season_stats (REG):")
    for stat, r in p2.items():
        floor = PASS2_FLOORS.get(stat)
        flag = ""
        if floor is not None and r["pct_exact"] < floor:
            flag = f"  << BELOW FLOOR {floor}"
            failures.append(f"pass2 {stat}: {r['pct_exact']}% < floor {floor}%")
        print(f"  {stat:<16} n={r['n']:>6,}  exact={r['pct_exact']:>6}%  max_diff={r['max_diff']}{flag}")

    print(f"\nwrote {REPORT}")
    if failures:
        print("\nAUDIT FAILURES:")
        for f in failures:
            print(f"  {f}")
        return 1
    print("All consistency gates passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
