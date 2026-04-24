#!/usr/bin/env python3
"""NFLVERSE build orchestrator.

Composes loaders + hub + engine primitives into a full DB build. Single
phase ordering; no per-table special cases; config-driven by `schema.py`.

Usage:
    python3 scripts/build.py                                 # full build (all tables, incl PBP)
    python3 scripts/build.py --output data/my.duckdb
    python3 scripts/build.py --no-pbp                        # skip play_by_play
    python3 scripts/build.py --no-validate                   # skip post-build checks
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

import duckdb
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

import schema  # noqa: E402 — declarative DB config (SOURCES / TABLES / HUB_BUILD / FILL_RULES / LOAD_ORDER)
from config import DB_PATH  # noqa: E402 — path constants


def _augment_foreign_keys_with_backfill(table_spec: dict) -> list[dict]:
    """Return foreign_keys augmented with entries for id_backfill'd columns.

    The backfill adds e.g. player_gsis_id to snap_counts. We declare the FK
    for that column at table-creation time so DuckDB enforces it on the
    subsequent UPDATE fill.
    """
    fks = list(table_spec.get("foreign_keys", []))
    for bf in table_spec.get("id_backfill", []):
        fk_ref = bf.get("add_fk")
        if not fk_ref:
            continue
        new_col = bf["new_column"]
        # Avoid duplicate declarations
        if not any(fk["column"] == new_col for fk in fks):
            fks.append({"column": new_col, "references": fk_ref})
    return fks


def _preflight_child_fk_stubs(hub_df: pd.DataFrame, cfg) -> pd.DataFrame:
    """Scan every child table's source for FK-target IDs (GSIS/PFR/ESPN)
    not already in the hub. Append each as a minimal stub row.

    Runs purely in pandas between hub construction and table writes, so the
    hub is 100% complete before any FK-bearing table is created.
    """
    from loaders import load_source

    # Maps FK-parent-column → hub-row-column where we look for the ID
    FK_TO_HUB_COL = {
        "players.player_gsis_id":  "player_gsis_id",
        "players.player_pfr_id":   "player_pfr_id",
        "players.player_espn_id":  "player_espn_id",
        "players.pff_id":          "pff_id",
    }

    # Collect unresolved IDs per hub column
    missing: dict[str, set] = {v: set() for v in FK_TO_HUB_COL.values()}
    scanned_sources: set[str] = set()

    for tname, spec in cfg.TABLES.items():
        if spec.get("build_via") == "hub":
            continue
        # All FK columns pointing at players (incl. id_backfill-added)
        child_fks = list(spec.get("foreign_keys", [])) + [
            {"column": bf["new_column"], "references": bf.get("add_fk", "")}
            for bf in spec.get("id_backfill", [])
        ]
        player_fks = [fk for fk in child_fks
                      if fk.get("references", "").startswith("players.")]
        if not player_fks:
            continue

        # Use the child's source_id(s) to find the raw columns carrying those IDs.
        src_ids = [spec.get("source_id")] if spec.get("source_id") else list(spec.get("source_ids", []))
        for sid in src_ids:
            if sid is None or sid in scanned_sources:
                continue
            scanned_sources.add(sid)
            try:
                df = load_source(sid, cfg.SOURCES[sid])
            except Exception as e:
                print(f"    preflight skip {sid}: {e}")
                continue
            if df.empty:
                continue
            for fk in player_fks:
                col = fk["column"]
                # id_backfill columns don't exist in the raw source — they'll be
                # populated later via UPDATE. Skip those for preflight.
                if col in df.columns:
                    vals = df[col].dropna().unique()
                    hub_col = FK_TO_HUB_COL[fk["references"]]
                    missing[hub_col].update(vals)

    # Filter out IDs already in hub
    existing_gsis = set(hub_df["player_gsis_id"].dropna())
    existing_pfr  = set(hub_df["player_pfr_id"].dropna())
    existing_espn = set(hub_df["player_espn_id"].dropna())
    new_gsis = missing["player_gsis_id"] - existing_gsis
    new_pfr  = missing["player_pfr_id"]  - existing_pfr
    new_espn = missing["player_espn_id"] - existing_espn

    total_stubs = 0
    if new_gsis:
        stubs = pd.DataFrame({"player_gsis_id": list(new_gsis)})
        for c in hub_df.columns:
            if c not in stubs.columns:
                stubs[c] = pd.NA
        hub_df = pd.concat([hub_df, stubs[hub_df.columns]], ignore_index=True)
        total_stubs += len(stubs)
    if new_pfr:
        stubs = pd.DataFrame({"player_pfr_id": list(new_pfr)})
        for c in hub_df.columns:
            if c not in stubs.columns:
                stubs[c] = pd.NA
        hub_df = pd.concat([hub_df, stubs[hub_df.columns]], ignore_index=True)
        total_stubs += len(stubs)
    if new_espn:
        stubs = pd.DataFrame({"player_espn_id": list(new_espn)})
        for c in hub_df.columns:
            if c not in stubs.columns:
                stubs[c] = pd.NA
        hub_df = pd.concat([hub_df, stubs[hub_df.columns]], ignore_index=True)
        total_stubs += len(stubs)

    print(f"  preflight stubs added: "
          f"gsis={len(new_gsis)}  pfr={len(new_pfr)}  espn={len(new_espn)}  total={total_stubs}")
    return hub_df


def _ensure_backfill_columns(df: pd.DataFrame, table_spec: dict) -> pd.DataFrame:
    """Pre-add NULL columns that id_backfill will populate after write.
    Needed so the FK can be declared at CREATE TABLE time.
    """
    for bf in table_spec.get("id_backfill", []):
        col = bf["new_column"]
        if col not in df.columns:
            df[col] = pd.Series([None] * len(df), dtype="string")
    return df


def build(output_path: Path, include_pbp: bool = True, validate_after: bool = True,
          years: list[int] | None = None) -> dict:
    """Run the full build pipeline.

    Modes:
      - Full rebuild (years=None): clean start, build everything from scratch.
      - Incremental (years=[2025, ...]): open existing DB, update only
        year-partitioned tables for the specified years. Non-year-partitioned
        tables untouched. Hub is refreshed in pandas and new players are
        INSERTed (existing players never UPDATEd — DuckDB's FK-parent
        restriction; see docs/DESIGN_RATIONALE.md R3).
    """
    cfg = schema
    incremental = years is not None
    from hub import build_hub
    from engine import (
        table_source_df, write_table, write_partition, insert_new_hub_rows,
        apply_id_backfill, apply_fill_rule, compute_season_ratios,
        validate, print_report,
    )

    start = time.time()
    mode_label = f"incremental (years={years})" if incremental else "full rebuild"
    print(f"nflverse build → {output_path}  [{mode_label}]")
    print(f"  (source dir: {ROOT / 'data' / 'raw'})")

    if not incremental:
        # Clean start for full rebuild
        for suffix in ("", ".wal", ".bak"):
            p = Path(str(output_path) + suffix)
            if p.exists():
                p.unlink()
    else:
        # Incremental: require the DB to exist (it's a prerequisite)
        if not output_path.exists():
            raise SystemExit(
                f"Incremental build requires existing DB at {output_path}. "
                f"Run without --years to do a full build first."
            )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(output_path))

    try:
        # ---- Phase 1-2: hub + players ----
        print("\n[Phase 1-2] Build hub")
        hub_df = build_hub(cfg)
        print(f"\n  players hub: {len(hub_df):,} rows")

        if incremental:
            # Incremental: don't rewrite players. INSERT-only new rows; never
            # UPDATE (FK-parent restriction; docs/DESIGN_RATIONALE.md R3).
            print("\n[Phase 3-incr] Insert new hub rows only")
            added = insert_new_hub_rows(conn, hub_df, "player_gsis_id")
            print(f"  +{added:,} new players (existing {len(hub_df)-added:,} untouched)")
        else:
            # Full: preflight + rewrite
            print("\n[Phase 2.5] Pre-stub unresolved child FK targets")
            hub_df = _preflight_child_fk_stubs(hub_df, cfg)

            print("\n[Phase 3] Write players + other parents")
            players_spec = cfg.TABLES["players"]
            write_table(conn, "players", hub_df, players_spec)
            for idx_cols in players_spec.get("indexes", []):
                idx_name = "idx_players_" + "_".join(idx_cols)
                conn.execute(
                    f'CREATE INDEX IF NOT EXISTS "{idx_name}" ON players ({", ".join(idx_cols)})'
                )

        # ---- Phase 4: children in LOAD_ORDER ----
        print("\n[Phase 4] Load child tables in FK-dependency order")
        load_order = cfg.LOAD_ORDER
        for tname in load_order:
            if tname == "players":
                continue
            if tname == "play_by_play" and not include_pbp:
                print(f"  SKIP {tname} (--no-pbp)")
                continue
            spec = cfg.TABLES[tname]
            if spec.get("build_via") == "hub":
                continue

            # Derived tables (build_via='sql'): built from already-loaded
            # parents via a declared SQL query. Skipped during incremental.
            if spec.get("build_via") == "sql":
                if incremental:
                    print(f"  SKIP {tname:<24} (derived; rebuild not needed incrementally)")
                    continue
                sql = spec["sql_query"]
                pk = spec.get("primary_key")
                print(f"  {tname:<24}", end=" ", flush=True)
                t0 = time.time()
                conn.execute(f'DROP TABLE IF EXISTS "{tname}"')
                if pk:
                    # Two-step: stage → recreate with UNIQUE(pk) for FK targeting.
                    conn.execute(f'CREATE TEMP TABLE "_stage_{tname}" AS {sql}')
                    described = conn.execute(f'DESCRIBE "_stage_{tname}"').fetchall()
                    col_defs = [f'"{r[0]}" {r[1]}' for r in described]
                    body = ",\n    ".join(col_defs + [f'UNIQUE ("{pk}")'])
                    conn.execute(f'CREATE TABLE "{tname}" (\n    {body}\n)')
                    conn.execute(f'INSERT INTO "{tname}" SELECT * FROM "_stage_{tname}"')
                    conn.execute(f'DROP TABLE "_stage_{tname}"')
                else:
                    conn.execute(f'CREATE TABLE "{tname}" AS {sql}')
                n = conn.execute(f'SELECT COUNT(*) FROM "{tname}"').fetchone()[0]
                print(f"{n:>10,} rows  ({time.time()-t0:.1f}s)  [derived]")
                continue

            # Incremental: only touch year-partitioned tables
            src_id = spec.get("source_id") or (spec.get("source_ids") or [None])[0]
            is_year_partitioned = False
            if src_id:
                src_pat = cfg.SOURCES.get(src_id, {}).get("pattern", "")
                is_year_partitioned = "{year}" in src_pat
            if incremental and not is_year_partitioned:
                print(f"  SKIP {tname:<24} (not year-partitioned)")
                continue

            print(f"  {tname:<24}", end=" ", flush=True)
            t0 = time.time()
            df = table_source_df(tname, spec, cfg.SOURCES, years=years)
            if df.empty:
                print("no data")
                continue
            df = _ensure_backfill_columns(df, spec)
            augmented_spec = dict(spec)
            augmented_spec["foreign_keys"] = _augment_foreign_keys_with_backfill(spec)
            if incremental:
                total = write_partition(conn, tname, df, "season", years or [])
                print(f"{len(df):>10,} new rows → total {total:,}  ({time.time()-t0:.1f}s)")
            else:
                write_table(conn, tname, df, augmented_spec)
                print(f"{len(df):>10,} rows  ({time.time()-t0:.1f}s)")

        # ---- Phase 5: ID backfill (UPDATEs fill the pre-added columns) ----
        print("\n[Phase 5] ID backfill")
        for tname in load_order:
            if tname not in cfg.TABLES:
                continue
            rules = cfg.TABLES[tname].get("id_backfill", [])
            if rules:
                apply_id_backfill(conn, tname, rules)

        # ---- Phase 6: name-match recovery on declared tables ----
        # For child rows still missing a player_gsis_id after Phase 5 backfill
        # where a name column is populated, look up an unambiguous display_name
        # match in players (optionally season-active-range-gated) and fill.
        # Load-bearing for draft_picks (fills ~7,100 pre-GSIS HoF-era picks
        # whose GSIS isn't in any upstream source).
        print("\n[Phase 6] Name-match recovery")
        from engine import apply_name_match_recovery
        for tname in load_order:
            if tname not in cfg.TABLES:
                continue
            spec = cfg.TABLES[tname].get("name_match_recovery")
            if spec:
                apply_name_match_recovery(conn, tname, spec)

        # ---- Phase 7: fill rules ----
        print("\n[Phase 7] Fill rules")
        for rule in cfg.FILL_RULES:
            try:
                n = apply_fill_rule(conn, rule)
                print(f"  {rule['name']:<48} {n:>8,}")
                if rule["op"] == "aggregate_from_sibling":
                    r = compute_season_ratios(conn, rule)
                    if r:
                        print(f"    (ratio cells computed: {r:,})")
            except Exception as e:
                print(f"  {rule['name']}: FAILED — {e}")

        # ---- Phase 8: views ----
        print("\n[Phase 8] Views")
        # v_depth_charts — kept as a view (composite across depth_charts +
        # depth_charts_2025). SQL from scripts/views.py.
        try:
            from views import v_depth_charts_sql
            conn.execute("DROP VIEW IF EXISTS v_depth_charts")
            conn.execute(f"CREATE VIEW v_depth_charts AS {v_depth_charts_sql()}")
            n = conn.execute("SELECT COUNT(*) FROM v_depth_charts").fetchone()[0]
            print(f"  v_depth_charts: {n:,} rows")
        except Exception as e:
            print(f"  v_depth_charts: FAILED — {e}")

        # ---- Phase 9: declared indexes ----
        print("\n[Phase 9] Indexes")
        for tname, spec in cfg.TABLES.items():
            for idx_cols in spec.get("indexes", []):
                idx_name = f"idx_{tname}_" + "_".join(idx_cols)
                try:
                    conn.execute(
                        f'CREATE INDEX IF NOT EXISTS "{idx_name}" ON "{tname}" '
                        f'({", ".join(idx_cols)})'
                    )
                    print(f"  + {idx_name}")
                except duckdb.Error as e:
                    print(f"  ! {idx_name}: {e}")

        # ---- Phase 10: validate ----
        report = {}
        if validate_after:
            print("\n[Phase 10] Validate")
            report = validate(conn, cfg)
            print_report(report)

    finally:
        conn.close()

    elapsed = time.time() - start
    size_mb = output_path.stat().st_size / (1024 * 1024)
    print(f"\nDone in {elapsed:.1f}s. Output: {output_path} ({size_mb:,.0f} MB)")
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--output", type=str, default=str(DB_PATH))
    parser.add_argument("--no-pbp", action="store_true")
    parser.add_argument("--no-validate", action="store_true")
    parser.add_argument("--years", nargs="+", type=int, default=None,
                        help="Incremental: refresh only these year(s) in year-"
                             "partitioned tables. Requires existing DB. "
                             "Skips non-year-partitioned tables. Faster than "
                             "full rebuild for in-season weekly refreshes.")
    args = parser.parse_args()
    report = build(Path(args.output),
                   include_pbp=not args.no_pbp,
                   validate_after=not args.no_validate,
                   years=args.years)
    if report.get("hard_failures"):
        sys.exit(1)


if __name__ == "__main__":
    main()
