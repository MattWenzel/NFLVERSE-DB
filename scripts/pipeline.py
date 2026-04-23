#!/usr/bin/env python3
"""Shared pipeline for building/updating the nflverse database.

Provides the `TableConfig` class, year-partition and full-replace update modes,
schema-drift handling, backups, indexing, and the shared `run()` entry point
used by both `build_db.py` (local parquet source) and `build_db_nflreadpy.py`
(nflreadpy network source).
"""

import argparse
import re
import shutil
from datetime import datetime
from pathlib import Path

import duckdb
import pandas as pd

from config import DB_PATH, DEPTH_CHARTS_LEGACY_END, YEAR_RANGE_START


# ---------------------------------------------------------------------------
# ID-column cleanup helpers (nflverse carries empty-string and '0' sentinels
# for "no ID" in some old data; GSIS columns sometimes carry 'XX-0000001' etc.
# Normalize to NULL so downstream FK joins don't false-match).
# ---------------------------------------------------------------------------

_GSIS_RE = re.compile(r"^\d{2}-\d{7}$")


def clean_id(v):
    """Normalize obvious junk ID values to None. For PFR/ESPN/generic IDs."""
    if v is None:
        return None
    if isinstance(v, float) and v != v:  # NaN
        return None
    s = str(v).strip()
    if s in ("", "0", "None", "nan", "NaN", "<NA>"):
        return None
    return s


def clean_gsis_id(v):
    """GSIS IDs must look like '00-0033873'. Non-matching values → None."""
    s = clean_id(v)
    if s is None:
        return None
    return s if _GSIS_RE.fullmatch(s) else None


def clean_id_series(s):
    """Vectorized clean_id for a whole pandas Series. Much faster than .map()."""
    out = s.astype("string").str.strip()
    return out.replace(
        {"": pd.NA, "0": pd.NA, "None": pd.NA, "nan": pd.NA, "NaN": pd.NA, "<NA>": pd.NA}
    )


def clean_gsis_id_series(s):
    """Vectorized clean_gsis_id for a whole pandas Series."""
    out = clean_id_series(s)
    return out.where(out.str.match(r"^\d{2}-\d{7}$", na=False), other=pd.NA)


def to_string_id(s):
    """Cast a numeric-ID column (possibly float w/ NaN) to clean strings; NULLs preserved.

    Prevents '3139477.0' / '3.139477e+06' artifacts from casting a float column
    that carries NaNs straight to str.
    """
    return pd.to_numeric(s, errors="coerce").astype("Int64").astype("string")


# ---------------------------------------------------------------------------
# Table config
# ---------------------------------------------------------------------------

class TableConfig:
    """How to fetch and update a single table.

    FK-related fields enable declarative primary/unique/foreign-key constraints
    at CREATE TABLE time, plus per-table stubbing so FK targets exist before the
    child INSERT fires. See scripts/build_db.py for concrete configs.
    """

    def __init__(self, name, *, update_mode="year_partition",
                 fetch_fn=None, dedup_cols=None, drop_na_col=None,
                 primary_key=None, unique_cols=None, foreign_keys=None,
                 stub_source=None,
                 parquet_glob=None, gsis_id_cols=None, id_cols=None,
                 force_varchar_cols=None):
        self.name = name
        # update_mode:
        #   "year_partition"         — DELETE WHERE season=Y, INSERT per year
        #   "year_partition_upsert"  — per year, INSERT ON CONFLICT DO UPDATE (FK-parent tables)
        #   "full_replace"           — DROP + CREATE AS SELECT
        #   "upsert"                 — INSERT ON CONFLICT DO UPDATE (players)
        #   "bulk_parquet"           — one-pass native parquet load via DuckDB; for huge tables (PBP)
        self.update_mode = update_mode
        self.fetch_fn = fetch_fn
        self.dedup_cols = dedup_cols
        self.drop_na_col = drop_na_col
        # FK metadata
        self.primary_key = primary_key
        self.unique_cols = unique_cols or []
        self.foreign_keys = foreign_keys or []
        self.stub_source = stub_source or {}
        # bulk_parquet-mode config
        self.parquet_glob = parquet_glob          # absolute path w/ glob wildcards
        self.gsis_id_cols = gsis_id_cols or []    # SQL-side GSIS regex cleanup
        self.id_cols = id_cols or []              # SQL-side empty/'0' → NULL cleanup
        self.force_varchar_cols = force_varchar_cols or []  # force VARCHAR on drift-prone cols


def default_years_for(table_name):
    """Full year range for a year-partitioned table."""
    start = YEAR_RANGE_START.get(table_name, 1999)
    if table_name == "depth_charts":
        return list(range(start, DEPTH_CHARTS_LEGACY_END))
    return list(range(start, datetime.now().year + 1))


def _valid_years(table_name):
    """Same as default_years_for, used to filter explicit --years against a table's range."""
    return set(default_years_for(table_name))


# ---------------------------------------------------------------------------
# Backup
# ---------------------------------------------------------------------------

def create_backup(db_path):
    """Create a rolling .bak copy of a database file."""
    if not db_path.exists():
        return
    bak_path = db_path.with_suffix(db_path.suffix + ".bak")
    print(f"  Backing up {db_path.name} -> {bak_path.name}")
    shutil.copy2(db_path, bak_path)


# ---------------------------------------------------------------------------
# Schema helpers
# ---------------------------------------------------------------------------

def _table_exists(conn, table_name):
    row = conn.execute(
        "SELECT COUNT(*) FROM information_schema.tables "
        "WHERE table_schema = 'main' AND table_name = ?",
        [table_name],
    ).fetchone()
    return row[0] > 0


def _existing_columns(conn, table_name):
    rows = conn.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema = 'main' AND table_name = ?",
        [table_name],
    ).fetchall()
    return {r[0] for r in rows}


def _add_missing_columns(conn, table_name, df):
    """Add any columns present in df but missing from the table.

    Handles schema drift between years (e.g., game_id added in later seasons).
    """
    existing_cols = _existing_columns(conn, table_name)
    new_cols = [c for c in df.columns if c not in existing_cols]
    for col in new_cols:
        dtype = df[col].dtype
        if dtype.kind == "i":
            col_type = "INTEGER"
        elif dtype.kind == "f":
            col_type = "DOUBLE"
        elif dtype.kind == "b":
            col_type = "BOOLEAN"
        else:
            col_type = "VARCHAR"
        conn.execute(f'ALTER TABLE "{table_name}" ADD COLUMN "{col}" {col_type}')
    if new_cols:
        print(f"(added {len(new_cols)} cols: {', '.join(new_cols)}) ", end="", flush=True)


# ---------------------------------------------------------------------------
# DataFrame -> DuckDB helpers
# ---------------------------------------------------------------------------

def _insert_df(conn, table_name, df):
    """Append a DataFrame into an existing table by name-matching columns."""
    conn.register("_ingest_df", df)
    try:
        existing_cols = _existing_columns(conn, table_name)
        cols = [c for c in df.columns if c in existing_cols]
        col_list = ", ".join(f'"{c}"' for c in cols)
        conn.execute(
            f'INSERT INTO "{table_name}" ({col_list}) '
            f"SELECT {col_list} FROM _ingest_df"
        )
    finally:
        conn.unregister("_ingest_df")


def _create_table_from_df(conn, table_name, df, config=None):
    """Replace-or-create a table from a DataFrame.

    If `config` carries primary_key / unique_cols / foreign_keys metadata, the
    table is created with an explicit DDL that includes those constraints.
    Otherwise falls through to `CREATE TABLE AS SELECT`.
    """
    conn.register("_ingest_df", df)
    try:
        conn.execute(f'DROP TABLE IF EXISTS "{table_name}"')
        if config and (config.primary_key or config.unique_cols or config.foreign_keys):
            ddl = _build_create_ddl(conn, table_name, df, config)
            conn.execute(ddl)
            existing = _existing_columns(conn, table_name)
            cols = [c for c in df.columns if c in existing]
            col_list = ", ".join(f'"{c}"' for c in cols)
            conn.execute(
                f'INSERT INTO "{table_name}" ({col_list}) '
                f'SELECT {col_list} FROM _ingest_df'
            )
        else:
            conn.execute(f'CREATE TABLE "{table_name}" AS SELECT * FROM _ingest_df')
    finally:
        conn.unregister("_ingest_df")


def _build_create_ddl(conn, table_name, df, config):
    """Emit a CREATE TABLE statement with explicit columns + PK/UNIQUE/FK clauses.

    Column types are inferred from the DataFrame via a DuckDB DESCRIBE on a
    registered view of the df.
    """
    rows = conn.execute("DESCRIBE (SELECT * FROM _ingest_df)").fetchall()
    col_defs = [f'"{row[0]}" {row[1]}' for row in rows]

    # Treat `primary_key` as "unique column used as upsert target" — emit UNIQUE
    # instead of PRIMARY KEY so stub rows that lack the ID (e.g. players stubbed
    # from combine carry only player_pfr_id, not player_gsis_id) can coexist.
    # DuckDB FKs accept either UNIQUE or PRIMARY KEY as the referenced target,
    # so this doesn't break the outbound FK contract.
    constraints = []
    unique_emitted = set()
    if config.primary_key:
        constraints.append(f'UNIQUE ("{config.primary_key}")')
        unique_emitted.add(config.primary_key)
    for uc in config.unique_cols:
        if uc not in unique_emitted:
            constraints.append(f'UNIQUE ("{uc}")')
            unique_emitted.add(uc)
    for col, ref_table, ref_col in config.foreign_keys:
        constraints.append(
            f'FOREIGN KEY ("{col}") REFERENCES "{ref_table}" ("{ref_col}")'
        )

    body = ",\n    ".join(col_defs + constraints)
    return f'CREATE TABLE "{table_name}" (\n    {body}\n)'


def _upsert_df(conn, table_name, df, primary_key):
    """Insert df rows into table_name, updating on conflict of primary_key.

    Rows already in the target but not in df are kept. This is used for FK
    parent tables (players, games) so children's FK references survive rebuilds.
    """
    conn.register("_ingest_df", df)
    try:
        existing_cols = _existing_columns(conn, table_name)
        cols = [c for c in df.columns if c in existing_cols]
        col_list = ", ".join(f'"{c}"' for c in cols)
        update_set = ", ".join(
            f'"{c}" = EXCLUDED."{c}"' for c in cols if c != primary_key
        )
        conn.execute(
            f'INSERT INTO "{table_name}" ({col_list}) '
            f'SELECT {col_list} FROM _ingest_df '
            f'ON CONFLICT ("{primary_key}") DO UPDATE SET {update_set}'
        )
    finally:
        conn.unregister("_ingest_df")


def enrich_players_from_player_ids(conn):
    """Use player_ids bridge to expand the players registry.

    1. Insert stub players for any `player_ids.gsis_id` not in `players`.
    2. Backfill missing `player_pfr_id` / `player_espn_id` on existing players
       from the bridge.

    Must run after both `players` and `player_ids` are loaded.
    """
    if not (_table_exists(conn, "players") and _table_exists(conn, "player_ids")):
        return

    added = conn.execute(
        """
        SELECT COUNT(*) FROM player_ids pi
        WHERE pi.gsis_id IS NOT NULL
          AND NOT EXISTS (SELECT 1 FROM players p WHERE p.player_gsis_id = pi.gsis_id)
        """
    ).fetchone()[0]
    if added:
        conn.execute(
            """
            INSERT INTO players (
                player_gsis_id, player_pfr_id, player_espn_id,
                display_name, first_name, last_name, position, latest_team
            )
            SELECT DISTINCT ON (pi.gsis_id)
                pi.gsis_id,
                pi.pfr_id,
                pi.espn_id,
                pi.name,
                split_part(pi.name, ' ', 1),
                regexp_replace(pi.name, '^\\S+ ', ''),
                pi.position,
                pi.team
            FROM player_ids pi
            WHERE pi.gsis_id IS NOT NULL
              AND NOT EXISTS (SELECT 1 FROM players p WHERE p.player_gsis_id = pi.gsis_id)
            ON CONFLICT (player_gsis_id) DO NOTHING
            """
        )
    # Guard against UNIQUE violations: a bridge row sometimes carries an ID
    # that's already assigned to a different player in `players`. Skip those.
    conn.execute(
        """
        UPDATE players SET player_pfr_id = pi.pfr_id
        FROM player_ids pi
        WHERE players.player_gsis_id = pi.gsis_id
          AND players.player_pfr_id IS NULL
          AND pi.pfr_id IS NOT NULL
          AND NOT EXISTS (
              SELECT 1 FROM players p2
              WHERE p2.player_pfr_id = pi.pfr_id
                AND p2.player_gsis_id <> players.player_gsis_id
          )
        """
    )
    conn.execute(
        """
        UPDATE players SET player_espn_id = pi.espn_id
        FROM player_ids pi
        WHERE players.player_gsis_id = pi.gsis_id
          AND players.player_espn_id IS NULL
          AND pi.espn_id IS NOT NULL
          AND NOT EXISTS (
              SELECT 1 FROM players p2
              WHERE p2.player_espn_id = pi.espn_id
                AND p2.player_gsis_id <> players.player_gsis_id
          )
        """
    )
    if added:
        print(f"    enriched players: +{added:,} stubs from player_ids bridge")


def stub_players_from_child(conn, df, id_col, parent_col, meta_map):
    """Insert minimal players rows for df[id_col] values missing from players[parent_col].

    meta_map: {players_column: df_column_name}. E.g. {"display_name": "player_name"}.
    Uses ANY_VALUE aggregation so each parent_col value contributes at most one
    row, with metadata from some representative child row.
    """
    if id_col not in df.columns:
        return 0
    if not _table_exists(conn, "players"):
        return 0

    conn.register("_stub_src", df)
    try:
        # Build SELECT list: parent_col = any_value(id_col), metadata = any_value(src_col)
        agg_cols = ", ".join(
            f'any_value("{src}") AS "{dst}"' for dst, src in meta_map.items()
        )
        dst_cols = ", ".join(f'"{c}"' for c in meta_map.keys())
        added = conn.execute(
            f"""
            SELECT COUNT(DISTINCT "{id_col}") FROM _stub_src
            WHERE "{id_col}" IS NOT NULL
              AND NOT EXISTS (
                  SELECT 1 FROM players p WHERE p."{parent_col}" = _stub_src."{id_col}"
              )
            """
        ).fetchone()[0]
        if added:
            conn.execute(
                f"""
                INSERT INTO players ("{parent_col}", {dst_cols})
                SELECT "{id_col}" AS _pid, {agg_cols}
                FROM _stub_src
                WHERE "{id_col}" IS NOT NULL
                  AND NOT EXISTS (
                      SELECT 1 FROM players p WHERE p."{parent_col}" = _stub_src."{id_col}"
                  )
                GROUP BY "{id_col}"
                ON CONFLICT ("{parent_col}") DO NOTHING
                """
            )
        return added
    finally:
        conn.unregister("_stub_src")


def stub_players_for_config(conn, config, df):
    """For each FK declared by the child config, call stub_players_from_child
    using the stub_source[child_col] map if provided. Prints a summary line."""
    total_stubs = 0
    for col, ref_table, ref_col in config.foreign_keys:
        if ref_table != "players":
            continue
        meta_map = config.stub_source.get(col)
        if not meta_map:
            continue
        added = stub_players_from_child(conn, df, col, ref_col, meta_map)
        if added:
            print(f"    stubbed players from {config.name}.{col}: +{added:,} rows")
            total_stubs += added
    return total_stubs


_NAME_RE = re.compile(r"^(\w+)\.?(.+)$")


def recover_gsis_by_name(conn, df):
    """For rows where `player_gsis_id` is NULL but `player_name` is populated
    (e.g. 'S.Fernando' or 'R.Rodgers' — nflverse ships some old-era rows with
    only an initials-format name and a junk/missing GSIS), try to fill in the
    canonical GSIS by matching (last_name, first_initial) to the players table,
    restricted to players active that season.

    Only fills when the lookup resolves to exactly one player — ambiguous
    matches stay NULL. Mutates df in place. Returns the count recovered.
    """
    if "player_gsis_id" not in df.columns or "player_name" not in df.columns:
        return 0
    if not _table_exists(conn, "players"):
        return 0

    null_mask = df["player_gsis_id"].isna() & df["player_name"].notna() & (df["player_name"] != "Team")
    if not null_mask.any():
        return 0

    recovered = 0
    for idx in df.index[null_mask]:
        name = str(df.at[idx, "player_name"])
        m = _NAME_RE.match(name)
        if not m:
            continue
        first_init, lastname = m.group(1), m.group(2).strip()
        season = None
        if "season" in df.columns:
            s = df.at[idx, "season"]
            if s is not None and not (isinstance(s, float) and s != s):
                season = int(s)
        rows = conn.execute(
            """
            SELECT player_gsis_id FROM players
            WHERE last_name = ? AND display_name LIKE ?
              AND (? IS NULL OR rookie_season IS NULL OR last_season IS NULL
                   OR (rookie_season <= ? AND last_season >= ?))
              AND player_gsis_id IS NOT NULL
            """,
            [lastname, f"{first_init}%", season, season, season],
        ).fetchall()
        if len(rows) == 1:
            df.at[idx, "player_gsis_id"] = rows[0][0]
            recovered += 1
    return recovered


def bulk_load_from_parquet_glob(
    conn, table_name, parquet_glob, config,
    id_clean_cols=None, gsis_clean_cols=None,
    force_varchar_cols=None, stub_source=None,
):
    """Single-pass native-parquet load for very large tables (PBP).

    Reads every parquet in `parquet_glob` via DuckDB's multi-threaded
    `read_parquet`, scrubs junk IDs in SQL, stubs missing parents via a
    single UNION ALL, then creates the final FK-bearing table. Much faster
    and more schema-drift-robust than year-by-year pandas-based loading.

    Args:
        id_clean_cols: columns to normalize with clean_id semantics (empty/'0' → NULL).
        gsis_clean_cols: columns that must match the GSIS regex or become NULL.
        force_varchar_cols: columns to coerce to VARCHAR (handle DuckDB type drift).
        stub_source: {child_col: {player_col: source_col}} — same shape as
                     TableConfig.stub_source.
    """
    id_clean_cols = id_clean_cols or []
    gsis_clean_cols = gsis_clean_cols or []
    force_varchar_cols = force_varchar_cols or []
    stub_source = stub_source or {}
    stage = f"_{table_name}_stage"

    # 1. Stage raw data from the parquet glob. union_by_name lets schema drift
    #    across years resolve cleanly (NULLs for missing columns).
    conn.execute(f'DROP TABLE IF EXISTS "{stage}"')
    conn.execute(
        f'CREATE TEMP TABLE "{stage}" AS '
        f"SELECT * FROM read_parquet('{parquet_glob}', union_by_name=true)"
    )
    total = conn.execute(f'SELECT COUNT(*) FROM "{stage}"').fetchone()[0]
    print(f"    staged {total:,} rows from {parquet_glob}")

    # 2. Force VARCHAR on type-drift columns (time/date fields that can be all
    #    NULL in early years). Skip any that aren't already VARCHAR-compatible.
    for col in force_varchar_cols:
        try:
            conn.execute(
                f'ALTER TABLE "{stage}" ALTER "{col}" SET DATA TYPE VARCHAR '
                f'USING CAST("{col}" AS VARCHAR)'
            )
        except duckdb.Error:
            pass  # column may not exist in this dataset

    # 3. Junk-ID cleanup in a single UPDATE with per-column CASE WHEN.
    stage_cols = _existing_columns(conn, stage)
    set_clauses = []
    for col in gsis_clean_cols:
        if col in stage_cols:
            set_clauses.append(
                f'"{col}" = CASE WHEN "{col}" IS NOT NULL '
                f'AND regexp_matches("{col}", \'^[0-9]{{2}}-[0-9]{{7}}$\') '
                f'THEN "{col}" END'
            )
    for col in id_clean_cols:
        if col in stage_cols:
            set_clauses.append(
                f'"{col}" = CASE WHEN "{col}" IN (\'\', \'0\', \'None\', \'nan\', \'NaN\') '
                f'THEN NULL ELSE "{col}" END'
            )
    if set_clauses:
        conn.execute(f'UPDATE "{stage}" SET {", ".join(set_clauses)}')

    # 4. Stub missing FK-target player rows in ONE pass using UNION ALL of all
    #    role columns. Each tuple contributes (player_id, representative_name).
    if stub_source:
        stub_selects = []
        for col, meta in stub_source.items():
            if col not in stage_cols:
                continue
            name_col = meta.get("display_name")
            if name_col and name_col in stage_cols:
                stub_selects.append(
                    f'SELECT "{col}" AS _pid, "{name_col}" AS _nm FROM "{stage}"'
                )
            else:
                stub_selects.append(
                    f'SELECT "{col}" AS _pid, NULL::VARCHAR AS _nm FROM "{stage}"'
                )
        if stub_selects:
            union_sql = "\nUNION ALL\n".join(stub_selects)
            before = conn.execute("SELECT COUNT(*) FROM players").fetchone()[0]
            conn.execute(
                f"""
                INSERT INTO players (player_gsis_id, display_name)
                SELECT _pid, any_value(_nm)
                FROM ({union_sql}) u
                WHERE u._pid IS NOT NULL
                  AND NOT EXISTS (
                      SELECT 1 FROM players p WHERE p.player_gsis_id = u._pid
                  )
                GROUP BY _pid
                ON CONFLICT (player_gsis_id) DO NOTHING
                """
            )
            after = conn.execute("SELECT COUNT(*) FROM players").fetchone()[0]
            print(f"    stubbed players from {table_name}: +{after - before:,} rows")

    # 5. Build final DDL from staging schema + FK clauses, then CREATE + INSERT.
    describe = conn.execute(f'DESCRIBE "{stage}"').fetchall()
    col_defs = [f'"{r[0]}" {r[1]}' for r in describe]
    constraints = []
    if config.primary_key:
        constraints.append(f'UNIQUE ("{config.primary_key}")')
    for uc in config.unique_cols:
        constraints.append(f'UNIQUE ("{uc}")')
    for col, ref_table, ref_col in config.foreign_keys:
        if col in stage_cols:
            constraints.append(
                f'FOREIGN KEY ("{col}") REFERENCES "{ref_table}" ("{ref_col}")'
            )
    body = ",\n    ".join(col_defs + constraints)
    conn.execute(f'DROP TABLE IF EXISTS "{table_name}"')
    conn.execute(f'CREATE TABLE "{table_name}" (\n    {body}\n)')
    conn.execute(f'INSERT INTO "{table_name}" SELECT * FROM "{stage}"')
    conn.execute(f'DROP TABLE "{stage}"')
    print(f"    {total:,} rows loaded into {table_name}")


# ---------------------------------------------------------------------------
# Update modes
# ---------------------------------------------------------------------------

def update_year_partition(conn, config, years, dry_run=False):
    """Delete + re-insert data for specific years, one transaction per year.

    Years outside the table's valid range are skipped with a clear message —
    this prevents e.g. `--years 2025 --tables depth_charts` from loading the
    new-schema 2025 parquet into the frozen legacy `depth_charts` table.
    """
    valid = _valid_years(config.name)
    out_of_range = [y for y in years if y not in valid]
    if out_of_range:
        bounds = f"{min(valid)}-{max(valid)}" if valid else "(none)"
        for y in out_of_range:
            print(f"  {config.name} [{y}]: SKIPPED (outside valid range {bounds})")
    years = [y for y in years if y in valid]
    for year in years:
        print(f"  {config.name} [{year}]: ", end="", flush=True)

        if dry_run:
            if _table_exists(conn, config.name):
                existing = conn.execute(
                    f'SELECT COUNT(*) FROM "{config.name}" WHERE season = ?',
                    [year],
                ).fetchone()[0]
                print(f"would replace {existing:,} existing rows")
            else:
                print("table does not exist (would create)")
            continue

        try:
            df = config.fetch_fn([year])
        except Exception as e:
            print(f"FETCH ERROR: {e}")
            continue

        if df is None or len(df) == 0:
            print("no data returned")
            continue

        if config.drop_na_col and config.drop_na_col in df.columns:
            df = df.dropna(subset=[config.drop_na_col])

        if config.dedup_cols:
            dedup_available = [c for c in config.dedup_cols if c in df.columns]
            if dedup_available:
                df = df.drop_duplicates(subset=dedup_available, keep="first")

        # Try to recover NULL GSIS values via name-match against players
        # (e.g. 'R.Rodgers' 2018 SEA → Richard Rodgers TE).
        recovered = recover_gsis_by_name(conn, df)
        if recovered:
            print(f"    recovered {recovered} GSIS IDs via name-match ", end="", flush=True)

        # If this table has FK parents pointing at players, pre-stub any
        # missing parent rows from the child's own metadata before INSERT.
        stub_players_for_config(conn, config, df)

        if not _table_exists(conn, config.name):
            try:
                conn.begin()
                _create_table_from_df(conn, config.name, df, config=config)
                conn.commit()
                print(f"{len(df):,} rows inserted (table created)")
            except Exception as e:
                conn.rollback()
                print(f"ERROR creating table: {e}")
            continue

        try:
            conn.begin()
            _add_missing_columns(conn, config.name, df)
            if config.update_mode == "year_partition_upsert" and config.primary_key:
                # FK-parent tables (e.g. games) can't DELETE rows while children
                # reference them; upsert by primary key instead.
                _upsert_df(conn, config.name, df, config.primary_key)
            else:
                conn.execute(
                    f'DELETE FROM "{config.name}" WHERE season = ?', [year]
                )
                _insert_df(conn, config.name, df)
            conn.commit()
            print(f"{len(df):,} rows inserted")
        except Exception as e:
            conn.rollback()
            print(f"ERROR (rolled back): {e}")


def update_full_replace(conn, config, dry_run=False):
    """Replace the entire table with fresh data."""
    print(f"  {config.name}: ", end="", flush=True)

    if dry_run:
        if _table_exists(conn, config.name):
            existing = conn.execute(
                f'SELECT COUNT(*) FROM "{config.name}"'
            ).fetchone()[0]
            print(f"would replace {existing:,} existing rows")
        else:
            print("table does not exist (would create)")
        return

    try:
        df = config.fetch_fn()
    except Exception as e:
        print(f"FETCH ERROR: {e}")
        return

    if df is None or len(df) == 0:
        print("no data returned — skipping (kept existing)")
        return

    if config.drop_na_col and config.drop_na_col in df.columns:
        df = df.dropna(subset=[config.drop_na_col])

    if config.dedup_cols:
        dedup_available = [c for c in config.dedup_cols if c in df.columns]
        if dedup_available:
            df = df.drop_duplicates(subset=dedup_available, keep="first")

    # Try to recover NULL GSIS via name-match, then stub any genuinely
    # missing FK parents from this child's own metadata.
    recovered = recover_gsis_by_name(conn, df)
    if recovered:
        print(f"    recovered {recovered} GSIS IDs via name-match ", end="", flush=True)
    stub_players_for_config(conn, config, df)

    try:
        conn.begin()
        if config.update_mode == "upsert" and _table_exists(conn, config.name) and config.primary_key:
            # FK-parent table (players): can't DROP while children reference it.
            # Upsert by primary key; rows absent from the new source are retained.
            _add_missing_columns(conn, config.name, df)
            _upsert_df(conn, config.name, df, config.primary_key)
        else:
            _create_table_from_df(conn, config.name, df, config=config)
        conn.commit()
        print(f"{len(df):,} rows")
    except Exception as e:
        conn.rollback()
        print(f"ERROR (rolled back): {e}")


# ---------------------------------------------------------------------------
# Post-update steps
# ---------------------------------------------------------------------------

def backfill_season_stats_team(conn, years=None, dry_run=False):
    """Backfill season_stats.recent_team from game_stats (most common team per player-season)."""
    print("  Backfilling season_stats.recent_team from game_stats...", end=" ", flush=True)

    if dry_run:
        print("would backfill")
        return

    year_clause = ""
    params = []
    if years:
        placeholders = ",".join("?" for _ in years)
        year_clause = f"AND season_stats.season IN ({placeholders})"
        params = list(years)

    try:
        conn.begin()
        conn.execute(f"""
            UPDATE season_stats
            SET recent_team = (
                SELECT g.team
                FROM game_stats g
                WHERE g.player_gsis_id = season_stats.player_gsis_id
                  AND g.season = season_stats.season
                GROUP BY g.team
                ORDER BY COUNT(*) DESC
                LIMIT 1
            )
            WHERE EXISTS (
                SELECT 1 FROM game_stats g2
                WHERE g2.player_gsis_id = season_stats.player_gsis_id
                  AND g2.season = season_stats.season
            )
            {year_clause}
        """, params)
        conn.commit()
        print("done")
    except Exception as e:
        conn.rollback()
        print(f"ERROR: {e}")


def create_indexes(conn):
    """Create standard indexes on the database."""
    print("  Creating indexes...", end=" ", flush=True)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_game_stats_player_season ON game_stats(player_gsis_id, season)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_season_stats_player_season ON season_stats(player_gsis_id, season)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_players_player_gsis_id ON players(player_gsis_id)")
    print("done")


def drop_views(conn):
    """Drop convenience views. Call before any full_replace table rewrite so
    DuckDB's dependency check doesn't block the underlying DROP TABLE."""
    conn.execute("DROP VIEW IF EXISTS v_depth_charts")


def create_views(conn):
    """Create convenience views over the base tables.

    Currently: v_depth_charts unions the pre-2025 weekly table and the 2025+
    daily table with normalized column names and a `source` tag so consumers
    can write one cross-season query instead of a 25-line UNION.

    No-op if the required base tables aren't all present.
    """
    have = {
        row[0]
        for row in conn.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = 'main'"
        ).fetchall()
    }
    if not {"depth_charts", "depth_charts_2025"}.issubset(have):
        return

    print("  Creating views...", end=" ", flush=True)
    conn.execute("DROP VIEW IF EXISTS v_depth_charts")
    # On the 2025 side, `season`, `week`, and `position` are derived:
    #   - season: adjusted for the NFL calendar (Jan-Feb dates belong to the
    #     prior year's season, not the calendar year).
    #   - week: looked up from `games` by taking the latest game whose
    #     gameday <= dt within the same NFL season. NULL for preseason (no
    #     prior game yet). For playoff dates, the nearest regular-season
    #     game's week is returned (DuckDB handles this via correlated
    #     subquery fast enough — ~0.1s across 476K rows).
    #   - position: slot-specific `pos_abb` values (LDE, RCB, WLB, ...) are
    #     mapped to legacy-comparable general positions (DE, CB, OLB, ...)
    #     so `WHERE position = 'CB'` returns both eras. Values that are
    #     already general (QB, WR, TE, RB, ...) pass through unchanged.
    conn.execute("""
        CREATE VIEW v_depth_charts AS
        SELECT
            season,
            CAST(week AS INTEGER)                     AS week,
            NULL::VARCHAR                             AS dt,
            club_code                                 AS team,
            player_gsis_id,
            NULL::VARCHAR                             AS player_espn_id,
            position,
            depth_position                            AS pos_abb,
            TRY_CAST(depth_team AS INTEGER)           AS depth_rank,
            formation,
            NULL::VARCHAR                             AS pos_grp,
            'legacy'                                  AS source
        FROM depth_charts
        UNION ALL
        SELECT
            CASE
                WHEN EXTRACT(MONTH FROM CAST(dc.dt AS TIMESTAMP)) <= 2
                    THEN CAST(strftime(CAST(dc.dt AS TIMESTAMP), '%Y') AS INTEGER) - 1
                ELSE CAST(strftime(CAST(dc.dt AS TIMESTAMP), '%Y') AS INTEGER)
            END                                       AS season,
            (SELECT g.week FROM games g
             WHERE g.season = CASE
                        WHEN EXTRACT(MONTH FROM CAST(dc.dt AS TIMESTAMP)) <= 2
                            THEN CAST(strftime(CAST(dc.dt AS TIMESTAMP), '%Y') AS INTEGER) - 1
                        ELSE CAST(strftime(CAST(dc.dt AS TIMESTAMP), '%Y') AS INTEGER)
                    END
               AND CAST(g.gameday AS DATE) <= CAST(dc.dt AS DATE)
             ORDER BY CAST(g.gameday AS DATE) DESC
             LIMIT 1)::INTEGER                        AS week,
            dc.dt,
            dc.team,
            dc.player_gsis_id,
            dc.player_espn_id,
            CASE
                WHEN dc.pos_abb IN ('LT','RT')                   THEN 'T'
                WHEN dc.pos_abb IN ('LG','RG')                   THEN 'G'
                WHEN dc.pos_abb IN ('LDE','RDE')                 THEN 'DE'
                WHEN dc.pos_abb IN ('LDT','RDT')                 THEN 'DT'
                WHEN dc.pos_abb IN ('WLB','SLB','LOLB','ROLB')   THEN 'OLB'
                WHEN dc.pos_abb IN ('RILB','LILB','MLB')         THEN 'ILB'
                WHEN dc.pos_abb IN ('LCB','RCB')                 THEN 'CB'
                WHEN dc.pos_abb IN ('NB','NCB')                  THEN 'NB'
                WHEN dc.pos_abb = 'PK'                           THEN 'K'
                ELSE dc.pos_abb
            END                                       AS position,
            dc.pos_abb,
            dc.pos_rank                               AS depth_rank,
            CASE
                WHEN dc.pos_grp = 'Special Teams' THEN 'Special Teams'
                WHEN dc.pos_grp LIKE 'Base%'      THEN 'Defense'
                ELSE 'Offense'
            END                                       AS formation,
            dc.pos_grp,
            'v2025'                                   AS source
        FROM depth_charts_2025 dc
    """)
    print("done")


def check_integrity(conn, dry_run=False):
    """Check for orphan records in game_stats/season_stats."""
    if dry_run:
        return

    required = {"game_stats", "season_stats", "players"}
    existing = {
        row[0]
        for row in conn.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = 'main'"
        ).fetchall()
    }
    if not required.issubset(existing):
        missing = required - existing
        print(f"  Skipping integrity check (missing tables: {', '.join(sorted(missing))})")
        return

    print("  Checking referential integrity...", end=" ", flush=True)

    # NULL player_gsis_id is legitimate (team-level rows, empty placeholders,
    # unattributed pre-2001 stats) — those aren't orphans, the FK allows NULL.
    # Only count rows with a non-NULL gsis that doesn't match a player.
    orphan_games = conn.execute("""
        SELECT COUNT(*) FROM game_stats g
        WHERE g.player_gsis_id IS NOT NULL
          AND NOT EXISTS (SELECT 1 FROM players p WHERE p.player_gsis_id = g.player_gsis_id)
    """).fetchone()[0]

    orphan_seasons = conn.execute("""
        SELECT COUNT(*) FROM season_stats s
        WHERE s.player_gsis_id IS NOT NULL
          AND NOT EXISTS (SELECT 1 FROM players p WHERE p.player_gsis_id = s.player_gsis_id)
    """).fetchone()[0]

    if orphan_games or orphan_seasons:
        print(f"WARNING: {orphan_games} orphan game_stats, {orphan_seasons} orphan season_stats")
    else:
        print("ok (0 orphan records)")


# ---------------------------------------------------------------------------
# Shared CLI runner
# ---------------------------------------------------------------------------

def build_arg_parser(description):
    """Return an ArgumentParser with the common flags used by the build scripts."""
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("--tables", nargs="+", help="Specific table(s) to process")
    parser.add_argument("--years", nargs="+", type=int,
                        help="Specific year(s) for year-partitioned tables")
    parser.add_argument("--pbp", action="store_true", help="Include play-by-play table")
    parser.add_argument("--all", action="store_true",
                        help="Process all tables across all years")
    parser.add_argument("--dry-run", action="store_true",
                        help="Preview changes without modifying")
    parser.add_argument("--no-backup", action="store_true", help="Skip backup step")
    parser.add_argument("--output", type=str, help="Write DB to a different path")
    return parser


def run(table_configs, args, title="nflverse DB"):
    """Execute the build/update flow against a configs dict.

    `table_configs` maps table name -> TableConfig. Caller provides fetch
    functions via the configs; everything else is shared here.
    """
    start = datetime.now()
    print(f"{title} — {start.strftime('%Y-%m-%d %H:%M')}")
    if args.dry_run:
        print("*** DRY RUN — no changes will be made ***")
    print()

    # Pick tables. play_by_play is opt-in via --pbp or explicit --tables;
    # --all by itself still excludes it to match the old two-DB behavior.
    if args.tables:
        table_names = list(args.tables)
    elif args.pbp and not args.all:
        table_names = ["play_by_play"]
    else:
        table_names = [n for n in table_configs if n != "play_by_play"]

    if args.pbp and "play_by_play" not in table_names and "play_by_play" in table_configs:
        table_names.append("play_by_play")

    for name in table_names:
        if name not in table_configs:
            raise SystemExit(
                f"ERROR: Unknown table '{name}'\n"
                f"Available: {', '.join(sorted(table_configs.keys()))}"
            )

    # Pick years
    if args.years:
        years = sorted(args.years)
    elif args.all:
        years = None  # use default range per table
    else:
        years = [datetime.now().year]

    print(f"Tables: {', '.join(table_names)}")
    print(f"Years: {', '.join(str(y) for y in years) if years else 'all (full range)'}")
    print()

    output_db = Path(args.output) if args.output else DB_PATH

    if not args.no_backup and not args.dry_run and not args.output:
        print("Creating backups:")
        create_backup(output_db)
        print()

    output_db.parent.mkdir(parents=True, exist_ok=True)

    conn = duckdb.connect(str(output_db))

    try:
        # Drop views up front so full_replace's DROP TABLE doesn't hit
        # DuckDB's catalog dependency check. Recreated at the end.
        if not args.dry_run:
            drop_views(conn)

        updated_season_stats = updated_game_stats = False

        for name in table_names:
            cfg = table_configs[name]

            print(f"Processing {name}:")
            if cfg.update_mode in ("year_partition", "year_partition_upsert"):
                yr_list = years if years else default_years_for(name)
                update_year_partition(conn, cfg, yr_list, dry_run=args.dry_run)
                if name == "season_stats":
                    updated_season_stats = True
                elif name == "game_stats":
                    updated_game_stats = True
            elif cfg.update_mode in ("full_replace", "upsert"):
                update_full_replace(conn, cfg, dry_run=args.dry_run)
            elif cfg.update_mode == "bulk_parquet":
                if args.dry_run:
                    print(f"  {cfg.name}: would bulk-load from {cfg.parquet_glob}")
                else:
                    bulk_load_from_parquet_glob(
                        conn, cfg.name, cfg.parquet_glob, cfg,
                        id_clean_cols=cfg.id_cols,
                        gsis_clean_cols=cfg.gsis_id_cols,
                        force_varchar_cols=cfg.force_varchar_cols,
                        stub_source=cfg.stub_source,
                    )
            print()

            # Child-table INSERTs may need to stub missing parents on-the-fly
            # (e.g. a 2025 snap_counts row whose PFR id isn't in players yet).
            # That happens inside each update_* function via stub_players_for_config.
            # Players-level enrichment from player_ids is handled in
            # _fetch_players at the pandas layer, so no SQL UPDATE is needed here.

        if updated_season_stats and updated_game_stats:
            backfill_season_stats_team(conn, years=years, dry_run=args.dry_run)
            print()

        if not args.dry_run and args.all:
            create_indexes(conn)
            print()

        if not args.dry_run:
            check_integrity(conn)
            print()

        if not args.dry_run:
            create_views(conn)
            print()

    finally:
        conn.close()

    print(f"Completed in {datetime.now() - start}")
