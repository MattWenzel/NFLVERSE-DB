#!/usr/bin/env python3
"""Shared pipeline for building/updating the nflverse database.

Provides the `TableConfig` class, year-partition and full-replace update modes,
schema-drift handling, backups, indexing, and the shared `run()` entry point
used by both `build_db.py` (local parquet source) and `build_db_nflreadpy.py`
(nflreadpy network source).
"""

import argparse
import shutil
from datetime import datetime
from pathlib import Path

import duckdb
import pandas as pd

from config import DB_PATH, DEPTH_CHARTS_LEGACY_END, YEAR_RANGE_START


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
    """How to fetch and update a single table."""

    def __init__(self, name, *, update_mode="year_partition",
                 fetch_fn=None, dedup_cols=None, drop_na_col=None):
        self.name = name
        self.update_mode = update_mode  # "year_partition" or "full_replace"
        self.fetch_fn = fetch_fn
        self.dedup_cols = dedup_cols  # subset columns for drop_duplicates
        self.drop_na_col = drop_na_col  # column to dropna on (e.g. "player_id")


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


def _create_table_from_df(conn, table_name, df):
    """Replace-or-create a table from a DataFrame."""
    conn.register("_ingest_df", df)
    try:
        conn.execute(f'DROP TABLE IF EXISTS "{table_name}"')
        conn.execute(f'CREATE TABLE "{table_name}" AS SELECT * FROM _ingest_df')
    finally:
        conn.unregister("_ingest_df")


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

        if not _table_exists(conn, config.name):
            try:
                conn.begin()
                _create_table_from_df(conn, config.name, df)
                conn.commit()
                print(f"{len(df):,} rows inserted (table created)")
            except Exception as e:
                conn.rollback()
                print(f"ERROR creating table: {e}")
            continue

        try:
            conn.begin()
            _add_missing_columns(conn, config.name, df)
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

    try:
        conn.begin()
        _create_table_from_df(conn, config.name, df)
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

    orphan_games = conn.execute("""
        SELECT COUNT(*) FROM game_stats g
        WHERE NOT EXISTS (SELECT 1 FROM players p WHERE p.player_gsis_id = g.player_gsis_id)
    """).fetchone()[0]

    orphan_seasons = conn.execute("""
        SELECT COUNT(*) FROM season_stats s
        WHERE NOT EXISTS (SELECT 1 FROM players p WHERE p.player_gsis_id = s.player_gsis_id)
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
            if cfg.update_mode == "year_partition":
                yr_list = years if years else default_years_for(name)
                update_year_partition(conn, cfg, yr_list, dry_run=args.dry_run)
                if name == "season_stats":
                    updated_season_stats = True
                elif name == "game_stats":
                    updated_game_stats = True
            elif cfg.update_mode == "full_replace":
                update_full_replace(conn, cfg, dry_run=args.dry_run)
            print()

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
