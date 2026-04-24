"""Primitive 3/4/5: write_table, apply_fill_rule, validate helpers.

DDL generation and table writes from config + a loaded DataFrame.
Fill-rule execution in SQL.
Post-build validation.

The build orchestrator lives in scripts/v2/build.py which composes these
primitives with hub.build_hub and loaders.load_source.
"""

from __future__ import annotations

import sys
from pathlib import Path

import duckdb
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from loaders import load_source  # noqa: E402


# ---------------------------------------------------------------------------
# Primitive 3: write_table
# ---------------------------------------------------------------------------

def write_table(conn: duckdb.DuckDBPyConnection, table_name: str,
                df: pd.DataFrame, table_spec: dict) -> None:
    """Create and populate a table from a DataFrame + config spec.

    Spec fields honored:
      - primary_key: str (emits UNIQUE constraint — not strict PK, to allow
        NULL in player_gsis_id for pre-GSIS rows)
      - unique_columns: list[str] (UNIQUE constraints)
      - foreign_keys: list of {column, references} dicts

    Column types are inferred from the DataFrame via DuckDB's DESCRIBE on
    a registered view of the df.
    """
    if df is None or df.empty:
        print(f"    write_table({table_name}): no data, skipping")
        return

    conn.register("_ingest_df", df)
    try:
        described = conn.execute("DESCRIBE SELECT * FROM _ingest_df").fetchall()
        col_defs = [f'"{row[0]}" {row[1]}' for row in described]

        constraints: list[str] = []
        uniques_seen: set[str] = set()

        pk = table_spec.get("primary_key")
        if pk:
            constraints.append(f'UNIQUE ("{pk}")')
            uniques_seen.add(pk)

        for uc in table_spec.get("unique_columns", []):
            if uc not in uniques_seen:
                constraints.append(f'UNIQUE ("{uc}")')
                uniques_seen.add(uc)

        for fk in table_spec.get("foreign_keys", []):
            ref_table, ref_col = fk["references"].split(".")
            constraints.append(
                f'FOREIGN KEY ("{fk["column"]}") REFERENCES "{ref_table}"("{ref_col}")'
            )

        body = ",\n    ".join(col_defs + constraints)
        ddl = f'CREATE TABLE "{table_name}" (\n    {body}\n)'

        conn.execute(f'DROP TABLE IF EXISTS "{table_name}"')
        conn.execute(ddl)
        # INSERT by explicit column list to sidestep any column-order drift
        col_list = ", ".join(f'"{r[0]}"' for r in described)
        conn.execute(
            f'INSERT INTO "{table_name}" ({col_list}) '
            f'SELECT {col_list} FROM _ingest_df'
        )
    finally:
        conn.unregister("_ingest_df")


def load_multi_source(source_ids: list[str], sources_config: dict) -> pd.DataFrame:
    """Load multiple source_ids and concat with union_by_name semantics.

    Used by TABLES entries that declare source_ids (a list) — e.g. ngs_stats
    which unions passing/rushing/receiving.
    """
    dfs = []
    for sid in source_ids:
        df = load_source(sid, sources_config[sid])
        if not df.empty:
            dfs.append(df)
    if not dfs:
        return pd.DataFrame()
    return pd.concat(dfs, ignore_index=True, sort=False)


def table_source_df(table_name: str, table_spec: dict, sources_config: dict) -> pd.DataFrame:
    """Resolve the DataFrame for one table per its config.

    Handles: single source_id, multiple source_ids (UNION), and dedup_cols /
    dropna_cols declared on the table.
    """
    src_id = table_spec.get("source_id")
    src_ids = table_spec.get("source_ids", [])
    if src_id:
        df = load_source(src_id, sources_config[src_id])
    elif src_ids:
        df = load_multi_source(src_ids, sources_config)
    else:
        return pd.DataFrame()

    if df.empty:
        return df

    for col in table_spec.get("dropna_cols", []):
        if col in df.columns:
            df = df.dropna(subset=[col])

    dedup = table_spec.get("dedup_cols")
    if dedup:
        available = [c for c in dedup if c in df.columns]
        if available:
            df = df.drop_duplicates(subset=available, keep="first")

    return df.reset_index(drop=True)


# ---------------------------------------------------------------------------
# ID backfill (post-table-write)
# ---------------------------------------------------------------------------

def apply_id_backfill(conn: duckdb.DuckDBPyConnection, table_name: str,
                      backfill_rules: list[dict]) -> None:
    """For each rule: add a new column to the table, populate it from the hub,
    then add the corresponding FK.

    Rule shape:
      {"new_column": "player_gsis_id",
       "via_hub_lookup": ("player_pfr_id", "player_gsis_id"),  # (child_key, hub_col)
       "add_fk": "players.player_gsis_id"}
    """
    for rule in backfill_rules:
        new_col = rule["new_column"]
        child_key, hub_col = rule["via_hub_lookup"]

        cols = conn.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema='main' AND table_name=?", [table_name]
        ).fetchall()
        existing = {r[0] for r in cols}
        if child_key not in existing:
            print(f"    id_backfill skipped: {table_name}.{child_key} not found")
            continue

        # The column is pre-added by build.py:_ensure_backfill_columns (so its
        # FK can be declared at CREATE TABLE). If missing for some reason, add
        # it now. Then UPDATE regardless — existence doesn't mean populated.
        if new_col not in existing:
            conn.execute(f'ALTER TABLE "{table_name}" ADD COLUMN "{new_col}" VARCHAR')

        conn.execute(f"""
            UPDATE "{table_name}" AS t
            SET "{new_col}" = p."{hub_col}"
            FROM players p
            WHERE p."{child_key}" = t."{child_key}"
              AND t."{child_key}" IS NOT NULL
        """)
        n = conn.execute(
            f'SELECT COUNT(*) FROM "{table_name}" WHERE "{new_col}" IS NOT NULL'
        ).fetchone()[0]
        total = conn.execute(f'SELECT COUNT(*) FROM "{table_name}"').fetchone()[0]
        pct = (n / total * 100) if total else 0
        print(f"    id_backfill: {table_name}.{new_col} filled in "
              f"{n:,}/{total:,} rows ({pct:.0f}%)")


# ---------------------------------------------------------------------------
# Phase 6: name-match GSIS recovery on child tables
# ---------------------------------------------------------------------------

def apply_name_match_recovery(conn: duckdb.DuckDBPyConnection, table_name: str,
                              recovery_spec: dict) -> int:
    """For rows in `table_name` where `target_column` is NULL but a name column
    is populated, look up a matching player in `players` by display_name and
    fill the target column when the match is unambiguous.

    Spec shape (from TABLES[x]['name_match_recovery']):
        {"target_column": "player_gsis_id",
         "name_columns": ["pfr_player_name", "player_display_name", ...]}

    Match policy:
      - display_name equals the child's name column (exact, case-sensitive).
      - If `season` exists on both sides, child.season must fall within
        players.rookie_season..last_season (NULL bounds accepted).
      - Exactly one unambiguous match required; multiple candidate GSIS
        values → skip the row (v1's "conservative" semantics).

    Mirrors v1's recover_gsis_by_name. For draft_picks this fills ~7,100
    pre-GSIS HoF-era picks whose GSIS isn't in any upstream source but whose
    name matches an existing hub player active in that season.
    """
    target_col = recovery_spec["target_column"]
    name_cols = recovery_spec.get("name_columns", [])

    table_cols = {r[0] for r in conn.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema='main' AND table_name=?", [table_name]
    ).fetchall()}
    if target_col not in table_cols:
        return 0

    # Pick the first available name column
    name_col = next((c for c in name_cols if c in table_cols), None)
    if name_col is None:
        return 0

    has_season = "season" in table_cols

    # Build (child_id, gsis) candidate pairs and keep only rows with a single
    # unambiguous match.
    season_filter = ""
    if has_season:
        season_filter = (
            " AND (p.rookie_season IS NULL OR c.season >= p.rookie_season) "
            " AND (p.last_season   IS NULL OR c.season <= p.last_season) "
        )

    # We need a row identity — DuckDB's rowid is stable within a transaction.
    conn.execute("DROP TABLE IF EXISTS _nmr_candidates")
    conn.execute(f"""
        CREATE TEMP TABLE _nmr_candidates AS
        SELECT c.rowid AS _child_rowid, p.player_gsis_id AS gsis
        FROM "{table_name}" c
        JOIN players p ON p.display_name = c."{name_col}"
        WHERE c."{target_col}" IS NULL
          AND p.player_gsis_id IS NOT NULL
          {season_filter}
    """)
    # Reduce to unambiguous matches
    n = conn.execute(f"""
        UPDATE "{table_name}"
        SET "{target_col}" = m.gsis
        FROM (
            SELECT _child_rowid, MIN(gsis) AS gsis
            FROM _nmr_candidates
            GROUP BY _child_rowid
            HAVING COUNT(DISTINCT gsis) = 1
        ) m
        WHERE "{table_name}".rowid = m._child_rowid
    """).fetchone()
    conn.execute("DROP TABLE _nmr_candidates")

    filled = conn.execute(
        f'SELECT COUNT(*) FROM "{table_name}" WHERE "{target_col}" IS NOT NULL'
    ).fetchone()[0]
    total = conn.execute(f'SELECT COUNT(*) FROM "{table_name}"').fetchone()[0]
    pct = (filled / total * 100) if total else 0
    print(f"    name_match_recovery({table_name}.{target_col}): "
          f"now {filled:,}/{total:,} ({pct:.0f}%)")
    return filled


# ---------------------------------------------------------------------------
# Primitive 4: apply_fill_rule
# ---------------------------------------------------------------------------

def apply_fill_rule(conn: duckdb.DuckDBPyConnection, rule: dict) -> int:
    """Execute one FILL_RULE. Returns rows affected."""
    op = rule["op"]
    name = rule["name"]
    target = rule["target_table"]

    if op == "backfill_null":
        col = rule["target_column"]
        src_tbl = rule.get("source_table")
        src_col = rule.get("source_column")
        src_expr = rule.get("source_expression")
        joins = rule.get("join") or []

        if src_expr:
            # Free-form expression that already correlates to target
            sql = f"""
                UPDATE "{target}"
                SET "{col}" = {src_expr}
                WHERE "{col}" IS NULL
            """
        else:
            # joins is a list of (target_expr, source_expr) where source_expr
            # uses the bare table name; we rewrite it to use alias 's' so the
            # correlated subquery resolves. Example:
            #   ("players.player_pfr_id", "combine.player_pfr_id")
            # becomes:
            #   target."player_pfr_id" = s."player_pfr_id"
            conditions = []
            for tk, sk in joins:
                # tk: "target_table.col" or just "col" - quote the last segment
                t_col = tk.split(".")[-1]
                s_col = sk.split(".")[-1]
                conditions.append(f'{target}."{t_col}" = s."{s_col}"')
            where = " AND ".join(conditions) if conditions else "1=1"
            sql = f"""
                UPDATE "{target}"
                SET "{col}" = (
                    SELECT s."{src_col}" FROM "{src_tbl}" s
                    WHERE {where}
                      AND s."{src_col}" IS NOT NULL
                    LIMIT 1
                )
                WHERE "{col}" IS NULL
            """
        before = conn.execute(
            f'SELECT COUNT(*) FROM "{target}" WHERE "{col}" IS NULL'
        ).fetchone()[0]
        conn.execute(sql)
        after = conn.execute(
            f'SELECT COUNT(*) FROM "{target}" WHERE "{col}" IS NULL'
        ).fetchone()[0]
        return before - after

    if op == "aggregate_from_sibling":
        # INSERT missing (key tuple) rows into target by aggregating source.
        # Used for season_stats augmentation from game_stats.
        # Delegated to a specialized function since columns are numerous.
        return _aggregate_from_sibling(conn, rule)

    raise ValueError(f"unknown fill rule op: {op!r}")


# ---------------------------------------------------------------------------
# aggregate_from_sibling — season_stats augmentation
# ---------------------------------------------------------------------------

# Column classification for season_stats aggregation. Same as v1's constants
# but moved here to keep the engine self-contained.
_SS_SUM_COLS = [
    "completions", "attempts", "passing_yards", "passing_tds",
    "passing_interceptions", "sacks_suffered", "sack_yards_lost",
    "sack_fumbles", "sack_fumbles_lost", "passing_air_yards",
    "passing_yards_after_catch", "passing_first_downs", "passing_epa",
    "passing_2pt_conversions",
    "carries", "rushing_yards", "rushing_tds", "rushing_fumbles",
    "rushing_fumbles_lost", "rushing_first_downs", "rushing_epa",
    "rushing_2pt_conversions",
    "receptions", "targets", "receiving_yards", "receiving_tds",
    "receiving_fumbles", "receiving_fumbles_lost", "receiving_air_yards",
    "receiving_yards_after_catch", "receiving_first_downs", "receiving_epa",
    "receiving_2pt_conversions",
    "special_teams_tds",
    "def_tackles_solo", "def_tackles_with_assist", "def_tackle_assists",
    "def_tackles_for_loss", "def_tackles_for_loss_yards",
    "def_fumbles_forced", "def_sacks", "def_sack_yards", "def_qb_hits",
    "def_interceptions", "def_interception_yards", "def_pass_defended",
    "def_tds", "def_fumbles", "def_safeties", "misc_yards",
    "fumble_recovery_own", "fumble_recovery_yards_own",
    "fumble_recovery_opp", "fumble_recovery_yards_opp",
    "fumble_recovery_tds", "penalties", "penalty_yards",
    "punt_returns", "punt_return_yards",
    "kickoff_returns", "kickoff_return_yards",
    "fg_made", "fg_att", "fg_missed", "fg_blocked",
    "fg_made_0_19", "fg_made_20_29", "fg_made_30_39", "fg_made_40_49",
    "fg_made_50_59", "fg_made_60_",
    "fg_missed_0_19", "fg_missed_20_29", "fg_missed_30_39",
    "fg_missed_40_49", "fg_missed_50_59", "fg_missed_60_",
    "fg_made_distance", "fg_missed_distance", "fg_blocked_distance",
    "pat_made", "pat_att", "pat_missed", "pat_blocked",
    "gwfg_made", "gwfg_att", "gwfg_missed", "gwfg_blocked",
    "fantasy_points", "fantasy_points_ppr",
]
_SS_MAX_COLS = ["fg_long"]
_SS_NULL_COLS = [
    # Ratios and list-valued columns; can't aggregate cleanly from weekly.
    # Computed post-hoc by a separate ratio-compute fill rule.
    "passing_cpoe", "pacr", "racr", "target_share", "air_yards_share",
    "wopr", "fg_pct", "pat_pct",
    "fg_made_list", "fg_missed_list", "fg_blocked_list", "gwfg_distance_list",
]
_SS_METADATA_COLS = [
    "player_name", "player_display_name", "position", "position_group",
    "headshot_url",
]
_SS_KEY_COLS = ["player_gsis_id", "season", "season_type"]


def _aggregate_from_sibling(conn: duckdb.DuckDBPyConnection, rule: dict) -> int:
    """Insert missing (player_gsis_id, season, season_type) combos into
    season_stats by aggregating game_stats weekly rows."""
    target = rule["target_table"]
    source = rule["source_table"]

    # Build SELECT expression per ordinal column order of target
    target_cols = [r[0] for r in conn.execute(
        f"SELECT column_name FROM information_schema.columns "
        f"WHERE table_schema='main' AND table_name='{target}' "
        f"ORDER BY ordinal_position"
    ).fetchall()]
    source_cols = {r[0] for r in conn.execute(
        f"SELECT column_name FROM information_schema.columns "
        f"WHERE table_schema='main' AND table_name='{source}'"
    ).fetchall()}

    classified = (set(_SS_SUM_COLS) | set(_SS_MAX_COLS) | set(_SS_NULL_COLS)
                  | set(_SS_METADATA_COLS) | set(_SS_KEY_COLS)
                  | {"games", "recent_team"})
    unclassified = set(target_cols) - classified
    if unclassified:
        # Don't fail hard — just leave them NULL with a warning. Schema drift
        # in season_stats should be investigated but shouldn't block the build.
        print(f"    WARN: unclassified season_stats cols (set NULL): {sorted(unclassified)}")

    before_rows = conn.execute(f'SELECT COUNT(*) FROM "{target}"').fetchone()[0]

    select_parts = []
    for c in target_cols:
        if c in _SS_KEY_COLS:
            select_parts.append(f'g."{c}"')
        elif c == "games":
            select_parts.append(
                'COALESCE(COUNT(DISTINCT g.game_id), COUNT(DISTINCT g.week)) AS games'
            )
        elif c == "recent_team":
            select_parts.append('arg_max(g."team", g."week") AS recent_team')
        elif c in _SS_METADATA_COLS:
            if c in source_cols:
                select_parts.append(f'MAX(g."{c}") AS "{c}"')
            else:
                select_parts.append(f'NULL::VARCHAR AS "{c}"')
        elif c in _SS_SUM_COLS:
            if c in source_cols:
                select_parts.append(f'SUM(g."{c}") AS "{c}"')
            else:
                select_parts.append(f'NULL AS "{c}"')
        elif c in _SS_MAX_COLS:
            if c in source_cols:
                select_parts.append(f'MAX(g."{c}") AS "{c}"')
            else:
                select_parts.append(f'NULL AS "{c}"')
        else:
            # _SS_NULL_COLS or unclassified
            select_parts.append(f'NULL AS "{c}"')

    col_list = ", ".join(f'"{c}"' for c in target_cols)
    select_sql = ",\n        ".join(select_parts)

    try:
        conn.execute(f"""
            INSERT INTO "{target}" ({col_list})
            SELECT
                {select_sql}
            FROM "{source}" g
            WHERE g.player_gsis_id IS NOT NULL
              AND NOT EXISTS (
                  SELECT 1 FROM "{target}" ss
                  WHERE ss.player_gsis_id = g.player_gsis_id
                    AND ss.season = g.season
                    AND ss.season_type = g.season_type
              )
            GROUP BY g.player_gsis_id, g.season, g.season_type
        """)
    except Exception as e:
        print(f"    aggregate_from_sibling failed: {e}")
        return 0

    after_rows = conn.execute(f'SELECT COUNT(*) FROM "{target}"').fetchone()[0]
    return after_rows - before_rows


# ---------------------------------------------------------------------------
# Ratio column computation (applied post-aggregation)
# ---------------------------------------------------------------------------

def compute_season_ratios(conn: duckdb.DuckDBPyConnection, rule: dict) -> int:
    """Fill ratio columns (fg_pct, pacr, ...) on season_stats rows where
    they're NULL but components are present. Applied after aggregate_from_sibling
    so newly-derived rows get ratios too.

    Silently skips formulas targeting columns not present in the table
    (e.g. nflverse's season_stats doesn't carry `passer_rating` — consumers
    compute it from completions/attempts/yards/tds/ints themselves). The
    formula-entry-with-None pattern in the config declares that explicitly.
    """
    target = rule["target_table"]
    aggregation = rule.get("aggregation", {})
    ratio_formulas = aggregation.get("compute_from_components", {})

    target_cols = {r[0] for r in conn.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema='main' AND table_name=?", [target]
    ).fetchall()}

    filled_total = 0
    for col, expr in ratio_formulas.items():
        if expr is None:
            continue
        if col not in target_cols:
            # Formula targets a column that doesn't exist — skip quietly.
            # This matches nflverse's reality for e.g. passer_rating which
            # isn't in season_stats.
            continue
        before = conn.execute(
            f'SELECT COUNT(*) FROM "{target}" WHERE "{col}" IS NULL'
        ).fetchone()[0]
        try:
            conn.execute(f"""
                UPDATE "{target}"
                SET "{col}" = {expr}
                WHERE "{col}" IS NULL
            """)
        except Exception as e:
            print(f"    compute_ratio {col} failed: {e}")
            continue
        after = conn.execute(
            f'SELECT COUNT(*) FROM "{target}" WHERE "{col}" IS NULL'
        ).fetchone()[0]
        filled_total += (before - after)
    return filled_total


# ---------------------------------------------------------------------------
# Primitive 5: validate
# ---------------------------------------------------------------------------

def validate(conn: duckdb.DuckDBPyConnection, config_module) -> dict:
    """Run hard + soft invariants and return a structured report."""
    report: dict = {
        "hard_failures": [],
        "warnings": [],
        "counts": {},
        "coverage": {},
    }

    # Table row counts — a missing table is a soft warning (it may have been
    # intentionally skipped via --no-pbp). Hard failures are reserved for
    # integrity issues (orphans, duplicates) not for skipped tables.
    for tname in config_module.TABLES.keys():
        try:
            n = conn.execute(f'SELECT COUNT(*) FROM "{tname}"').fetchone()[0]
            report["counts"][tname] = n
        except duckdb.Error:
            report["warnings"].append(f"table {tname} not present (skipped or failed to load)")

    # FK orphan sweep
    fks = conn.execute("""
        SELECT table_name, constraint_column_names[1],
               referenced_table, referenced_column_names[1]
        FROM duckdb_constraints()
        WHERE constraint_type='FOREIGN KEY'
        ORDER BY table_name, constraint_column_names[1]
    """).fetchall()
    orphan_total = 0
    for t, col, rt, rc in fks:
        n = conn.execute(
            f'SELECT COUNT(*) FROM "{t}" c WHERE c."{col}" IS NOT NULL '
            f'AND NOT EXISTS (SELECT 1 FROM "{rt}" p WHERE p."{rc}" = c."{col}")'
        ).fetchone()[0]
        if n > 0:
            report["hard_failures"].append(
                f"{t}.{col} → {rt}.{rc}: {n} orphans"
            )
            orphan_total += n
    report["counts"]["_foreign_keys"] = len(fks)
    report["counts"]["_orphans"] = orphan_total

    # game_stats → season_stats completeness
    if all(t in report["counts"] for t in ("game_stats", "season_stats")):
        gap = conn.execute("""
            SELECT COUNT(*) FROM (
                SELECT DISTINCT player_gsis_id, season, season_type FROM game_stats
                WHERE player_gsis_id IS NOT NULL
                EXCEPT
                SELECT DISTINCT player_gsis_id, season, season_type FROM season_stats
                WHERE player_gsis_id IS NOT NULL
            )
        """).fetchone()[0]
        report["counts"]["_game_season_gap"] = gap
        if gap > 0:
            report["hard_failures"].append(
                f"season_stats missing {gap} (player,season,type) combos present in game_stats"
            )

    # Duplicate ID check
    for col in ("player_gsis_id", "player_pfr_id", "player_espn_id"):
        dupes = conn.execute(f"""
            SELECT COUNT(*) FROM (
                SELECT "{col}" FROM players WHERE "{col}" IS NOT NULL
                GROUP BY 1 HAVING COUNT(*) > 1
            )
        """).fetchone()[0]
        if dupes > 0:
            report["hard_failures"].append(f"players: {dupes} duplicate {col}")

    # Coverage per player-linked table
    for tname in config_module.TABLES.keys():
        try:
            cols = {r[0] for r in conn.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema='main' AND table_name=?", [tname]
            ).fetchall()}
        except duckdb.Error:
            continue
        n = report["counts"].get(tname, 0)
        cov = {}
        for c in ("player_gsis_id", "player_pfr_id", "player_espn_id", "game_id"):
            if c in cols and n > 0:
                filled = conn.execute(
                    f'SELECT COUNT(*) FROM "{tname}" WHERE "{c}" IS NOT NULL'
                ).fetchone()[0]
                cov[c] = filled / n
        if cov:
            report["coverage"][tname] = cov

    return report


def print_report(report: dict) -> None:
    print()
    print("=" * 60)
    print("BUILD VALIDATION REPORT")
    print("=" * 60)
    print("\nRow counts:")
    for k, v in sorted(report["counts"].items()):
        print(f"  {k:<30} {v:>12,}")
    print("\nCoverage (% non-null):")
    for tname, cov in sorted(report["coverage"].items()):
        parts = ", ".join(f"{c}={p*100:.0f}%" for c, p in cov.items())
        print(f"  {tname:<22} {parts}")
    if report["warnings"]:
        print("\nWarnings:")
        for w in report["warnings"]:
            print(f"  ! {w}")
    if report["hard_failures"]:
        print("\nHARD FAILURES:")
        for f in report["hard_failures"]:
            print(f"  X {f}")
    else:
        print("\nAll hard invariants satisfied.")
    print("=" * 60)
