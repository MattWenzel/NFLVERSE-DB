"""Primitive 1: load_source.

Reads one SOURCES entry from config, returns a cleaned pandas DataFrame
ready for either (a) direct insertion into a target table, or (b) consumption
by the hub builder.

Operations applied, in order:
  1. Resolve parquet path(s) per year_range
  2. Read + (for year-partitioned sources) concat via union_by_name
  3. Apply `renames`
  4. Apply `id_cleanup` (uses scripts/v2/cleanup.py:clean_id)
  5. Apply `force_types` casts (best-effort; skips columns that don't exist)
  6. Apply `ensure_columns` (add missing columns as typed NULLs)
  7. Apply `add_literal_columns` (add a constant column; e.g. stat_type='pass')
  8. Apply `pre_cast_numeric_to_string` (before id_cleanup for float-ID CSVs)
  9. For CSV sources, do a csv-specific read with pandas.read_csv

Intentionally does NOT drop rows, dedupe, or stub — those are table-specific
operations handled downstream.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

from cleanup import clean_id

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))
from config import RAW_DATA_PATH  # v1 scripts/config.py - path constants


def _expand_years(source_spec: dict, raw_root: Path) -> list[int] | None:
    """For year-partitioned sources, discover available years from disk.
    Returns None for non-partitioned sources.
    """
    pattern = source_spec["pattern"]
    if "{year}" not in pattern:
        return None
    subdir = pattern.rsplit("/", 1)[0] if "/" in pattern else source_spec.get("release_tag", "")
    folder = raw_root / subdir
    if not folder.exists():
        return []
    filename_pat = pattern.rsplit("/", 1)[-1]
    lo, hi = source_spec.get("year_range", ("auto", "auto"))
    years = []
    for y in range(1999, 2027):
        fname = filename_pat.replace("{year}", str(y))
        if (folder / fname).exists():
            if lo != "auto" and y < lo:
                continue
            if hi != "auto" and y > hi:
                continue
            years.append(y)
    return sorted(years)


def load_source(source_id: str, source_spec: dict, years: list[int] | None = None) -> pd.DataFrame:
    """Read a source per its config spec and return a cleaned DataFrame.

    Args:
        source_id: key into config.SOURCES (for logging).
        source_spec: the SOURCES[source_id] dict.
        years: optional year subset for year-partitioned sources.

    Returns:
        DataFrame, possibly empty if no files are present.
    """
    pattern = source_spec["pattern"]
    fmt = source_spec.get("format", "parquet")

    if fmt == "csv":
        path = RAW_DATA_PATH / pattern
        if not path.exists():
            print(f"    {source_id}: missing {path}")
            return pd.DataFrame()
        df = pd.read_csv(path, low_memory=False)
    else:
        # parquet — either a single file or year-partitioned glob
        if "{year}" in pattern:
            available = _expand_years(source_spec, RAW_DATA_PATH)
            if years is not None:
                available = [y for y in available if y in years]
            if not available:
                return pd.DataFrame()
            files = [RAW_DATA_PATH / pattern.replace("{year}", str(y)) for y in available]
            dfs = []
            for f in files:
                if f.exists():
                    dfs.append(pd.read_parquet(f))
            if not dfs:
                return pd.DataFrame()
            df = pd.concat(dfs, ignore_index=True)
        else:
            path = RAW_DATA_PATH / pattern
            if not path.exists():
                print(f"    {source_id}: missing {path}")
                return pd.DataFrame()
            df = pd.read_parquet(path)

    if df.empty:
        return df

    # Pre-cast numeric→string for float-ID columns (e.g. db_playerids.espn_id)
    for col in source_spec.get("pre_cast_numeric_to_string", []):
        if col in df.columns and pd.api.types.is_numeric_dtype(df[col]):
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64").astype("string")

    # Apply renames
    renames = source_spec.get("renames", {})
    if renames:
        df = df.rename(columns=renames)

    # Add literal columns (e.g. stat_type='pass')
    for col, val in source_spec.get("add_literal_columns", {}).items():
        df[col] = val

    # Ensure columns exist (for schema-drift sources like game_stats pre-2022)
    for col, dtype in source_spec.get("ensure_columns", {}).items():
        if col not in df.columns:
            if dtype == "VARCHAR":
                df[col] = pd.Series([None] * len(df), dtype="string")
            else:
                df[col] = pd.Series([None] * len(df))

    # Force types (VARCHAR coerce for type-drifted columns)
    for col, dtype in source_spec.get("force_types", {}).items():
        if col in df.columns:
            if dtype == "VARCHAR":
                df[col] = df[col].astype("string")

    # ID cleanup — applied after renames so the cleanup map keys match current column names
    id_cleanup = source_spec.get("id_cleanup", {})
    for col, kind in id_cleanup.items():
        if col in df.columns:
            df[col] = clean_id(df[col], kind=kind)

    return df
