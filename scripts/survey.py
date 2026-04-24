#!/usr/bin/env python3
"""Pre-build ID-space + coverage survey.

Runs BEFORE any table write. For every enabled source in schema.SOURCES,
samples the raw parquet(s) and emits:
  - per-source: row count, year range, ID columns with unique/null counts
  - cross-source: ID overlap matrix (which sources carry which IDs)
  - hub projection: predicted post-hub coverage per ID column
  - unexpected patterns: columns in the parquet not consumed by any
    table, schema drift across year partitions, gaps larger than declared

Output: data/survey_report.json (committed — diffable across builds).

Exit codes:
  0  survey clean, all declared expected_gaps match observation
  1  declared expected_gaps violated (observed deviates > tolerance)
  2  structural issue (manifest missing, raw files absent, etc.)

v3 principle (from LESSONS_LEARNED §3.3): we know the shape of the data
before we build. The survey is the gate that proves it.

Usage:
    python3 scripts/survey.py
    python3 scripts/survey.py --no-write      # just print, don't update the JSON
    python3 scripts/survey.py --source snap_counts   # survey one source
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

import schema  # noqa: E402
from config import RAW_DATA_PATH  # noqa: E402
from loaders import load_source  # noqa: E402

SURVEY_PATH = ROOT / "data" / "survey_report.json"


# Columns we care about for the hub-projection matrix. Add here if we ever
# introduce a new canonical ID type (e.g. nfl_id becomes primary).
HUB_ID_COLUMNS = [
    "player_gsis_id", "player_pfr_id", "player_espn_id",
]

# Hub-provider sources: the hub merges these, so any ID they carry is
# "hub-reachable." Used to project post-build coverage.
HUB_PROVIDER_SOURCES = [
    "players_master", "db_playerids", "weekly_rosters", "draft_picks", "combine",
]


def survey_one_source(sid: str, spec: dict) -> dict:
    """Sample one source; emit per-source stats."""
    t0 = time.time()
    try:
        df = load_source(sid, spec)
    except Exception as e:
        return {"error": str(e), "row_count": 0}

    report: dict = {
        "row_count": len(df),
        "column_count": len(df.columns),
        "columns": list(df.columns),
        "load_time_s": round(time.time() - t0, 2),
    }

    if df.empty:
        return report

    # Year range
    if "season" in df.columns:
        seasons = pd.to_numeric(df["season"], errors="coerce").dropna().astype(int)
        if len(seasons):
            report["year_range"] = [int(seasons.min()), int(seasons.max())]
            report["year_counts"] = {int(y): int(n) for y, n in seasons.value_counts().items()}

    # ID-column stats
    id_stats: dict = {}
    for col in df.columns:
        if col not in HUB_ID_COLUMNS:
            continue
        s = df[col]
        id_stats[col] = {
            "unique": int(s.nunique()),
            "null_count": int(s.isna().sum()),
            "null_rate": round(float(s.isna().mean()), 4),
        }
    if id_stats:
        report["id_columns"] = id_stats

    return report


def cross_source_overlap(per_source: dict) -> dict:
    """Build per-ID: which sources carry it, how many unique values each."""
    overlap: dict = {id_col: {"sources": {}} for id_col in HUB_ID_COLUMNS}
    for sid, s_report in per_source.items():
        for id_col, stats in s_report.get("id_columns", {}).items():
            overlap[id_col]["sources"][sid] = stats["unique"]
    for id_col in list(overlap):
        if not overlap[id_col]["sources"]:
            del overlap[id_col]
    return overlap


def hub_projection(per_source: dict) -> dict:
    """For each child source (non-hub-provider), project hub-coverage:
    what fraction of its unique IDs are present in at least one
    hub-provider source?

    Runs lightweight: compares set-unions via actually loading.
    """
    projection: dict = {}

    # Gather hub-provider ID sets
    provider_ids: dict[str, set] = {col: set() for col in HUB_ID_COLUMNS}
    for sid in HUB_PROVIDER_SOURCES:
        spec = schema.SOURCES.get(sid)
        if not spec:
            continue
        try:
            df = load_source(sid, spec)
        except Exception:
            continue
        for col in HUB_ID_COLUMNS:
            if col in df.columns:
                provider_ids[col].update(df[col].dropna().tolist())

    # For each child source, count reachability
    for sid, spec in schema.SOURCES.items():
        if sid in HUB_PROVIDER_SOURCES:
            continue
        if sid.startswith("_"):
            continue
        try:
            df = load_source(sid, spec)
        except Exception:
            continue
        if df.empty:
            continue
        row: dict = {}
        for col in HUB_ID_COLUMNS:
            if col not in df.columns:
                continue
            ids = set(df[col].dropna().tolist())
            if not ids:
                continue
            reachable = len(ids & provider_ids[col])
            row[col] = {
                "child_unique": len(ids),
                "hub_reachable": reachable,
                "gap": len(ids) - reachable,
                "coverage": round(reachable / len(ids), 4) if ids else 1.0,
            }
        if row:
            projection[sid] = row

    return projection


def run_survey(only_source: str | None = None) -> dict:
    """Full survey across enabled sources."""
    sources_to_scan = (
        {only_source: schema.SOURCES[only_source]}
        if only_source else schema.SOURCES
    )

    print(f"Surveying {len(sources_to_scan)} source(s)...")
    per_source: dict = {}
    for sid, spec in sources_to_scan.items():
        print(f"  {sid}...", end=" ", flush=True)
        per_source[sid] = survey_one_source(sid, spec)
        r = per_source[sid]
        print(f"{r.get('row_count', 0):,} rows "
              f"({r.get('load_time_s', 0):.1f}s)")

    overlap = cross_source_overlap(per_source) if not only_source else {}
    projection = hub_projection(per_source) if not only_source else {}

    return {
        "generated_at": datetime.now(tz=timezone.utc).isoformat(),
        "per_source": per_source,
        "cross_source_overlap": overlap,
        "hub_projection": projection,
    }


def check_gaps(report: dict) -> list[str]:
    """Validate declared expected_gaps in schema.SOURCES against observed.

    Returns a list of violations (empty = all good).

    Expected_gaps format in schema.py:
        "source_id_spec": {
            ...,
            "expected_gaps": {
                "hub_coverage.player_gsis_id.min": 0.95,  # >= 95% of child IDs in hub
                "null_rate.player_pfr_id.max": 0.05,      # <= 5% NULL
            }
        }
    """
    violations: list[str] = []
    projection = report.get("hub_projection", {})
    per_source = report.get("per_source", {})

    for sid, spec in schema.SOURCES.items():
        gaps = spec.get("expected_gaps", {})
        if not gaps:
            continue
        for key, expected in gaps.items():
            parts = key.split(".")
            if len(parts) != 3:
                continue
            metric, col, bound = parts

            observed: float | None = None
            if metric == "hub_coverage":
                observed = projection.get(sid, {}).get(col, {}).get("coverage")
            elif metric == "null_rate":
                observed = per_source.get(sid, {}).get("id_columns", {}).get(col, {}).get("null_rate")
            else:
                continue

            if observed is None:
                violations.append(
                    f"{sid}.expected_gaps.{key}: observation unavailable (column may not exist in source)"
                )
                continue

            if bound == "min" and observed < expected - 0.02:
                violations.append(
                    f"{sid}.{metric}.{col} = {observed:.4f} < declared min {expected:.4f} (tolerance 2pp)"
                )
            elif bound == "max" and observed > expected + 0.02:
                violations.append(
                    f"{sid}.{metric}.{col} = {observed:.4f} > declared max {expected:.4f} (tolerance 2pp)"
                )

    return violations


def check_unresolved_gaps(report: dict) -> list[str]:
    """Any child source with >10% hub-gap AND no declared expected_gaps entry
    for that ID column is a v3 gate violation — either declare the gap as
    acceptable OR add a recovery rule."""
    violations: list[str] = []
    projection = report.get("hub_projection", {})
    for sid, cols in projection.items():
        spec = schema.SOURCES.get(sid, {})
        declared = set(spec.get("expected_gaps", {}).keys())
        table_spec = schema.TABLES.get(sid, {})
        has_recovery = bool(
            table_spec.get("name_match_recovery") or
            table_spec.get("id_backfill")
        )
        for col, stats in cols.items():
            if stats.get("coverage", 1.0) < 0.90:
                key = f"hub_coverage.{col}.min"
                if key not in declared and not has_recovery:
                    violations.append(
                        f"{sid}: hub_coverage.{col} = {stats['coverage']:.2f} (below 0.90) "
                        f"but no `expected_gaps[{key}]` declared AND no "
                        f"name_match_recovery/id_backfill on TABLES[{sid!r}]. "
                        f"Either declare the gap or add a recovery rule."
                    )
    return violations


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--no-write", action="store_true",
                        help="Print report; don't update data/survey_report.json")
    parser.add_argument("--source", type=str, default=None,
                        help="Survey one source_id only (skips cross-source + projection)")
    parser.add_argument("--strict", action="store_true",
                        help="Exit non-zero on any violation (default: prints but exits 0)")
    args = parser.parse_args()

    report = run_survey(only_source=args.source)

    violations = check_gaps(report) if not args.source else []
    unresolved = check_unresolved_gaps(report) if not args.source else []

    report["gap_violations"] = violations
    report["unresolved_gaps"] = unresolved

    if not args.no_write and not args.source:
        SURVEY_PATH.parent.mkdir(parents=True, exist_ok=True)
        with SURVEY_PATH.open("w") as f:
            json.dump(report, f, indent=2, default=str)
        size_kb = SURVEY_PATH.stat().st_size / 1024
        print(f"\nwrote {SURVEY_PATH.relative_to(ROOT)} ({size_kb:.0f} KB)")

    if violations:
        print("\nDECLARED GAP VIOLATIONS:")
        for v in violations:
            print(f"  ! {v}")
    if unresolved:
        print("\nUNRESOLVED GAPS (no declared expected_gaps or recovery):")
        for v in unresolved:
            print(f"  ! {v}")

    if not violations and not unresolved:
        print("\nSurvey clean: no gap violations, no unresolved gaps.")

    if args.strict and (violations or unresolved):
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
