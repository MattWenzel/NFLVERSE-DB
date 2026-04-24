#!/usr/bin/env python3
"""NFLVERSE build orchestrator.

Composes loaders + hub + engine primitives into a full DB build. Single
phase ordering; no per-table special cases; config-driven by `schema.py`.

Large-table fills (id_backfill, fill_rules, name-match recovery) use the
pandas-first pattern: read target + source once, merge in memory, DROP +
bulk-recreate the table. See DESIGN_RATIONALE.md R18 — pure SQL UPDATE
with correlated subqueries is O(N·M) and ran 10× slower on weekly_rosters.
`_finalize_pandas` is the reference implementation.

Usage:
    python3 scripts/build.py                                 # full build (all tables, incl PBP)
    python3 scripts/build.py --output data/my.duckdb
    python3 scripts/build.py --no-pbp                        # skip play_by_play
    python3 scripts/build.py --no-validate                   # skip post-build checks
    python3 scripts/build.py --years 2025                    # incremental: year-partitioned only
    python3 scripts/build.py --tables ftn_charting           # tables-only: rewrite one table (seconds)
    python3 scripts/build.py --finalize                      # re-run phases 5-9 only (no rebuild)
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


def _finalize_pandas(conn, cfg, validate_after: bool, start: float) -> dict:
    """Pandas-first Phase 5-9 runner. Replaces per-row DuckDB UPDATEs with
    in-memory merges + bulk table replacement. Orders of magnitude faster
    on large tables (weekly_rosters: 906K rows × 8 fills in seconds).
    """
    import duckdb as _duckdb
    from engine import (
        write_table, apply_fill_rule, compute_season_ratios, validate, print_report,
    )

    existing_tables = {r[0] for r in conn.execute(
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_schema='main' AND table_type='BASE TABLE'"
    ).fetchall()}

    # Pre-load commonly-joined parents once
    players_df = conn.execute("SELECT * FROM players").df() if "players" in existing_tables else None
    player_ids_df = conn.execute("SELECT * FROM player_ids").df() if "player_ids" in existing_tables else None
    draft_picks_df = conn.execute("SELECT * FROM draft_picks").df() if "draft_picks" in existing_tables else None
    combine_df = conn.execute("SELECT * FROM combine").df() if "combine" in existing_tables else None

    # Group FILL_RULES by target so each table is processed once
    rules_by_target: dict = {}
    for rule in cfg.FILL_RULES:
        rules_by_target.setdefault(rule.get("target_table"), []).append(rule)

    def _parent_for(src_name):
        if src_name == "players":     return players_df
        if src_name == "player_ids":  return player_ids_df
        if src_name == "draft_picks": return draft_picks_df
        if src_name == "combine":     return combine_df
        # fallback: pull on demand
        if src_name in existing_tables:
            return conn.execute(f'SELECT * FROM "{src_name}"').df()
        return None

    def _apply_backfill_null(df, rule):
        """Pandas equivalent of apply_fill_rule backfill_null op."""
        col = rule["target_column"]
        src_tbl = rule.get("source_table")
        src_col = rule.get("source_column")
        src_expr = rule.get("source_expression")
        joins = rule.get("join") or []
        if col not in df.columns:
            # Might need to add the column first
            df[col] = pd.Series([None] * len(df), dtype="object")
        if src_expr:
            # Free-form expression; fall through to SQL path later
            return df, None  # signal: use SQL
        src = _parent_for(src_tbl)
        if src is None:
            return df, 0
        left_keys = [j[0].split(".")[-1] for j in joins]
        right_keys = [j[1].split(".")[-1] for j in joins]
        # Build right-side slice keyed on right_keys with the source_col
        subset_cols = list(dict.fromkeys(right_keys + [src_col]))
        missing_right = [c for c in subset_cols if c not in src.columns]
        if missing_right:
            return df, 0
        right = src[subset_cols].drop_duplicates(subset=right_keys)
        right = right[right[src_col].notna()]
        # Merge
        left_valid = all(c in df.columns for c in left_keys)
        if not left_valid:
            return df, 0
        before_null = df[col].isna().sum()
        merged = df.merge(
            right.rename(columns={src_col: "_fill_src"}),
            left_on=left_keys, right_on=right_keys, how="left", suffixes=("", "_rhs")
        )
        # fillna the target column
        df[col] = df[col].where(df[col].notna(), merged["_fill_src"].values)
        after_null = df[col].isna().sum()
        return df, int(before_null - after_null)

    def _apply_id_backfills_pandas(df, spec):
        """Replicate engine.apply_id_backfill via pandas merge to players."""
        for rule in spec.get("id_backfill", []):
            new_col = rule["new_column"]
            child_key, hub_col = rule["via_hub_lookup"]
            if child_key not in df.columns:
                continue
            if players_df is None or hub_col not in players_df.columns or child_key not in players_df.columns:
                continue
            # Build lookup: child_key -> hub_col (drop dup child_keys, NA-free on hub_col)
            lkp = players_df[[child_key, hub_col]].dropna(subset=[hub_col]).drop_duplicates(subset=[child_key])
            merged = df.merge(lkp, on=child_key, how="left", suffixes=("", "_bf"))
            # Populate new_col from the merged hub_col
            fill_src = merged[hub_col + "_bf"] if (hub_col + "_bf") in merged.columns else merged[hub_col]
            if new_col in df.columns:
                df[new_col] = df[new_col].where(df[new_col].notna(), fill_src.values)
            else:
                df[new_col] = fill_src.values
            filled = df[new_col].notna().sum()
            pct = 100.0 * filled / len(df) if len(df) else 0
            print(f"    id_backfill: {new_col:<24} {filled:>10,}/{len(df):,} ({pct:.0f}%)")
        return df

    def _apply_name_match_pandas(df, spec):
        """Pandas equivalent of apply_name_match_recovery."""
        target_col = spec["target_column"]
        name_cols = spec.get("name_columns", [])
        if target_col not in df.columns or players_df is None:
            return df, 0
        name_col = next((c for c in name_cols if c in df.columns), None)
        if name_col is None:
            return df, 0
        has_season = "season" in df.columns
        # Candidate rows: target NULL, name populated
        cand = df[df[target_col].isna() & df[name_col].notna()]
        if cand.empty:
            return df, 0
        # Players with non-null gsis + display_name
        p = players_df[players_df["player_gsis_id"].notna() & players_df["display_name"].notna()].copy()
        if has_season:
            # If season range gating applies, expand each player into an interval check
            # Do a cross-merge on name and filter by season range inline
            merged = cand[[name_col, "season"]].reset_index().merge(
                p[["display_name", "player_gsis_id", "rookie_season", "last_season"]],
                left_on=name_col, right_on="display_name", how="inner"
            )
            # Season must fall within rookie_season..last_season (NULL bounds accepted)
            ok = ((merged["rookie_season"].isna()) | (merged["season"] >= merged["rookie_season"])) & \
                 ((merged["last_season"].isna()) | (merged["season"] <= merged["last_season"]))
            merged = merged[ok]
        else:
            merged = cand[[name_col]].reset_index().merge(
                p[["display_name", "player_gsis_id"]],
                left_on=name_col, right_on="display_name", how="inner"
            )
        # Keep unambiguous matches (exactly one distinct gsis per index)
        grp = merged.groupby("index")["player_gsis_id"].agg(["nunique", "first"])
        unambiguous = grp[grp["nunique"] == 1]["first"]
        before = df[target_col].notna().sum()
        df.loc[unambiguous.index, target_col] = unambiguous.values
        after = df[target_col].notna().sum()
        return df, int(after - before)

    # ---- Pass 1: process each non-hub, non-sql child table ----
    # Order: LOAD_ORDER (parents first → draft_picks rebuilt before combine, so combine's fills get fresh data)
    t_phase = time.time()
    print("\n[Phase 5-7 pandas] Rebuilding child tables with in-memory fills")

    # Tables we will drop+recreate. Skip: players (FK parent, can't drop),
    # sql-derived tables (rebuild is cheap), games (FK parent), stadiums/teams (no fills needed).
    NO_DROP = {"players", "games"}

    refreshed_dfs: dict = {}  # keep enriched dfs in case later tables want them

    for tname in cfg.LOAD_ORDER:
        if tname not in cfg.TABLES or tname not in existing_tables:
            continue
        spec = cfg.TABLES[tname]
        if spec.get("build_via") in ("hub", "sql"):
            continue
        if tname in NO_DROP:
            continue

        has_bf = bool(spec.get("id_backfill"))
        has_fills = bool(rules_by_target.get(tname))
        has_nmr = bool(spec.get("name_match_recovery"))
        if not (has_bf or has_fills or has_nmr):
            continue

        t0 = time.time()
        df = conn.execute(f'SELECT * FROM "{tname}"').df()
        n_before = len(df)

        if has_bf:
            df = _apply_id_backfills_pandas(df, spec)

        deferred_sql_rules = []
        if has_fills:
            for rule in rules_by_target[tname]:
                if rule["op"] == "aggregate_from_sibling":
                    # aggregate_from_sibling is an INSERT — handled in Pass 3
                    continue
                try:
                    df, n = _apply_backfill_null(df, rule)
                    if n is None:
                        # source_expression path — can't express in pandas; run
                        # via SQL after the bulk replace.
                        deferred_sql_rules.append(rule)
                        print(f"    {rule['name']:<48} (defer to SQL)")
                    else:
                        print(f"    {rule['name']:<48} {n:>8,}")
                except Exception as e:
                    print(f"    {rule['name']}: FAILED — {e}")

        if has_nmr:
            df, n_nmr = _apply_name_match_pandas(df, spec["name_match_recovery"])
            filled = df[spec["name_match_recovery"]["target_column"]].notna().sum()
            print(f"    name_match_recovery({tname}): +{n_nmr:,} (total {filled:,}/{len(df):,})")

        # DROP + recreate via write_table
        augmented_spec = dict(spec)
        augmented_spec["foreign_keys"] = _augment_foreign_keys_with_backfill(spec)
        conn.execute(f'DROP TABLE IF EXISTS "{tname}"')
        df = _ensure_backfill_columns(df, spec)
        write_table(conn, tname, df, augmented_spec)
        refreshed_dfs[tname] = df
        print(f"  {tname:<24} {n_before:>10,} rows  ({time.time()-t0:.1f}s)")

        # Apply deferred source_expression rules via SQL on the freshly-rebuilt table
        for rule in deferred_sql_rules:
            try:
                n = apply_fill_rule(conn, rule)
                print(f"    sql: {rule['name']:<48} {n:>8,}")
            except Exception as e:
                print(f"    sql: {rule['name']}: FAILED — {e}")

    # ---- Pass 2: players fills (can't drop — UPDATE small 26K rows) ----
    players_rules = rules_by_target.get("players", [])
    if players_rules and "players" in existing_tables:
        print("\n[Phase 7 pandas] players fills (UPDATE, 26K rows)")
        for rule in players_rules:
            try:
                n = apply_fill_rule(conn, rule)
                print(f"  {rule['name']:<48} {n:>8,}")
            except Exception as e:
                print(f"  {rule['name']}: FAILED — {e}")

    # ---- Pass 3: aggregate_from_sibling rules (INSERT into target) ----
    for rule in cfg.FILL_RULES:
        if rule["op"] != "aggregate_from_sibling":
            continue
        if rule.get("target_table") not in existing_tables:
            continue
        try:
            n = apply_fill_rule(conn, rule)
            print(f"  {rule['name']:<48} {n:>8,}")
            r = compute_season_ratios(conn, rule)
            if r:
                print(f"    (ratio cells computed: {r:,})")
        except Exception as e:
            print(f"  {rule['name']}: FAILED — {e}")

    print(f"\nPhases 5-7 done in {time.time()-t_phase:.1f}s")

    # ---- Phase 8: views ----
    # Build order matters: v_draft_pick_careers selects from v_player_careers,
    # so create the upstream view first.
    print("\n[Phase 8] Views")
    from views import v_depth_charts_sql, v_player_careers_sql, v_draft_pick_careers_sql
    for vname, sql_fn in [
        ("v_depth_charts",       v_depth_charts_sql),
        ("v_player_careers",     v_player_careers_sql),
        ("v_draft_pick_careers", v_draft_pick_careers_sql),
    ]:
        try:
            conn.execute(f'DROP VIEW IF EXISTS "{vname}"')
            conn.execute(f'CREATE VIEW "{vname}" AS {sql_fn()}')
            n = conn.execute(f'SELECT COUNT(*) FROM "{vname}"').fetchone()[0]
            print(f"  {vname}: {n:,} rows")
        except Exception as e:
            print(f"  {vname}: FAILED — {e}")

    # ---- Phase 9: indexes ----
    print("\n[Phase 9] Indexes")
    existing_tables = {r[0] for r in conn.execute(
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_schema='main' AND table_type='BASE TABLE'"
    ).fetchall()}
    for tname, spec in cfg.TABLES.items():
        if tname not in existing_tables:
            continue
        for idx_cols in spec.get("indexes", []):
            idx_name = f"idx_{tname}_" + "_".join(idx_cols)
            try:
                conn.execute(
                    f'CREATE INDEX IF NOT EXISTS "{idx_name}" ON "{tname}" '
                    f'({", ".join(idx_cols)})'
                )
                print(f"  + {idx_name}")
            except _duckdb.Error as e:
                print(f"  ! {idx_name}: {e}")

    # ---- Phase 10: validate ----
    report = {}
    if validate_after:
        print("\n[Phase 10] Validate")
        report = validate(conn, cfg)
        print_report(report)

    print(f"\nFinalize done in {time.time()-start:.1f}s")
    return report


def build(output_path: Path, include_pbp: bool = True, validate_after: bool = True,
          years: list[int] | None = None, tables: list[str] | None = None,
          finalize: bool = False) -> dict:
    """Run the build pipeline.

    Modes:
      - Full rebuild (default): clean start, build everything from scratch.
      - Incremental by year (years=[2025, ...]): open existing DB, update only
        year-partitioned tables for the specified years. Non-year-partitioned
        tables untouched. Hub INSERT-only on new rows; never UPDATE
        (DuckDB's FK-parent restriction; see DESIGN_RATIONALE.md R3).
      - Tables-only (tables=[...]): open existing DB, drop + rewrite ONLY
        the listed tables. Skips hub rebuild, preflight, indexes, and validate.
        Fastest way to iterate on one table; ~seconds vs minutes. Requires
        that the table's upstream FK parents already exist.
      - Finalize (finalize=True): open existing DB, re-run Phases 5-9
        (id_backfill, name-match recovery, fill_rules, views, indexes) across
        ALL tables. No rebuild. Use after `--tables` batches, or to recover
        from a partial build that was killed mid-Phase-5.
    """
    cfg = schema
    incremental = years is not None
    tables_only = tables is not None and len(tables) > 0
    from hub import build_hub
    from engine import (
        table_source_df, write_table, write_partition, insert_new_hub_rows,
        apply_id_backfill, apply_fill_rule, compute_season_ratios,
        validate, print_report,
    )

    start = time.time()
    if finalize:
        mode_label = "finalize (phases 5-9)"
    elif tables_only:
        mode_label = f"tables-only ({','.join(tables)})"
    elif incremental:
        mode_label = f"incremental (years={years})"
    else:
        mode_label = "full rebuild"
    print(f"nflverse build → {output_path}  [{mode_label}]")
    print(f"  (source dir: {ROOT / 'data' / 'raw'})")

    if not incremental and not tables_only and not finalize:
        # Clean start for full rebuild
        for suffix in ("", ".wal", ".bak"):
            p = Path(str(output_path) + suffix)
            if p.exists():
                p.unlink()
    elif incremental or tables_only or finalize:
        if not output_path.exists():
            raise SystemExit(
                f"This mode requires existing DB at {output_path}. "
                f"Run without --years/--tables/--finalize to do a full build first."
            )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(output_path))

    try:
        # ---- Tables-only fast path: skip hub, skip preflight, skip indexes ----
        if tables_only:
            print(f"\n[Tables-only] Rebuilding {len(tables)} table(s): {tables}")
            for tname in tables:
                if tname not in cfg.TABLES:
                    raise SystemExit(f"Unknown table: {tname}. Known: {sorted(cfg.TABLES)}")
                spec = cfg.TABLES[tname]
                if spec.get("build_via") == "hub":
                    raise SystemExit(f"Cannot rebuild 'players' (hub-built) in tables-only mode. Use full rebuild.")

                print(f"  {tname:<24}", end=" ", flush=True)
                t0 = time.time()

                if spec.get("build_via") == "sql":
                    sql = spec["sql_query"]
                    pk = spec.get("primary_key")
                    conn.execute(f'DROP TABLE IF EXISTS "{tname}"')
                    if pk:
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

                df = table_source_df(tname, spec, cfg.SOURCES, years=None)
                if df.empty:
                    print("no data")
                    continue
                df = _ensure_backfill_columns(df, spec)
                augmented_spec = dict(spec)
                augmented_spec["foreign_keys"] = _augment_foreign_keys_with_backfill(spec)
                write_table(conn, tname, df, augmented_spec)
                print(f"{len(df):>10,} rows  ({time.time()-t0:.1f}s)")

                # Apply id_backfill for this table only
                bf_rules = spec.get("id_backfill", [])
                if bf_rules:
                    apply_id_backfill(conn, tname, bf_rules)

            # Apply fill_rules that target any of these tables (skipping others)
            for rule in cfg.FILL_RULES:
                if rule.get("target_table") not in tables:
                    continue
                try:
                    n = apply_fill_rule(conn, rule)
                    print(f"  fill: {rule['name']:<48} {n:>8,}")
                except Exception as e:
                    print(f"  fill: {rule['name']}: FAILED — {e}")

            elapsed = time.time() - start
            print(f"\nTables-only build done in {elapsed:.1f}s")
            return {}  # No full validation report in this mode

        # ---- Finalize fast path: pandas-first, then bulk-replace ----
        # For large child tables, a DuckDB UPDATE with correlated subqueries is
        # O(N*M) slow (weekly_rosters: 906K × 6 columns = minutes). Instead:
        # read each child into pandas, apply all id_backfill + targeted fills
        # + name_match in memory, then DROP + recreate via write_table.
        # `players` stays put (FK parent, can't drop); its fills run as UPDATEs
        # since it's only 26K rows.
        if finalize:
            report = _finalize_pandas(conn, cfg, validate_after, start)
            return report

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

        # ---- Phases 5-9: delegate to pandas-first finalize path (R18) ----
        # For full rebuilds, the post-Phase-4 state is identical to the
        # partial-build state that `--finalize` handles. Using the same
        # pandas-first path cuts runtime on the multi-million-row updates
        # (weekly_rosters alt-ID fills, snap_counts id_backfill) from
        # minutes of correlated SQL UPDATEs to seconds of in-memory merges.
        # Incremental builds keep their per-year SQL path below — deltas
        # are small enough that the rewrite cost wouldn't pay back.
        if not incremental:
            report = _finalize_pandas(conn, cfg, validate_after, start)
            return report

        # ---- Phase 5 (incremental only): ID backfill via SQL ----
        print("\n[Phase 5] ID backfill")
        for tname in load_order:
            if tname not in cfg.TABLES:
                continue
            rules = cfg.TABLES[tname].get("id_backfill", [])
            if rules:
                apply_id_backfill(conn, tname, rules)

        # ---- Phase 6 (incremental only): name-match recovery ----
        print("\n[Phase 6] Name-match recovery")
        from engine import apply_name_match_recovery
        for tname in load_order:
            if tname not in cfg.TABLES:
                continue
            spec = cfg.TABLES[tname].get("name_match_recovery")
            if spec:
                apply_name_match_recovery(conn, tname, spec)

        # ---- Phase 7 (incremental only): fill rules ----
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

        # ---- Phase 8 (incremental only): views ----
        print("\n[Phase 8] Views")
        from views import v_depth_charts_sql, v_player_careers_sql, v_draft_pick_careers_sql
        for vname, sql_fn in [
            ("v_depth_charts",       v_depth_charts_sql),
            ("v_player_careers",     v_player_careers_sql),
            ("v_draft_pick_careers", v_draft_pick_careers_sql),
        ]:
            try:
                conn.execute(f'DROP VIEW IF EXISTS "{vname}"')
                conn.execute(f'CREATE VIEW "{vname}" AS {sql_fn()}')
                n = conn.execute(f'SELECT COUNT(*) FROM "{vname}"').fetchone()[0]
                print(f"  {vname}: {n:,} rows")
            except Exception as e:
                print(f"  {vname}: FAILED — {e}")

        # ---- Phase 9 (incremental only): declared indexes ----
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
    parser.add_argument("--tables", nargs="+", default=None,
                        help="Tables-only: drop + rewrite ONLY the listed tables. "
                             "Skips hub, preflight, indexes, full validate. "
                             "Requires existing DB. Fastest way to iterate on one table.")
    parser.add_argument("--finalize", action="store_true",
                        help="Re-run Phases 5-9 on existing DB (id_backfill, "
                             "name-match recovery, fill_rules, views, indexes). "
                             "No table rebuild. Use after --tables batches or "
                             "to recover a partial build.")
    args = parser.parse_args()
    report = build(Path(args.output),
                   include_pbp=not args.no_pbp,
                   validate_after=not args.no_validate,
                   years=args.years,
                   tables=args.tables,
                   finalize=args.finalize)
    if report.get("hard_failures"):
        sys.exit(1)


if __name__ == "__main__":
    main()
