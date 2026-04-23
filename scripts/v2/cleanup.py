"""Single ID-column cleanup function for v2.

Replaces v1's four-function maze (clean_id / clean_gsis_id / clean_id_series /
clean_gsis_id_series). One function, two modes, applied uniformly at source-
load time from the config's declared `id_columns` mapping.

Sentinels nflverse uses for "no ID":
  - Empty string
  - '0' (literal zero)
  - 'None' / 'nan' / 'NaN' / '<NA>' (stringified nulls)
  - 'XX-0000001' etc. (malformed GSIS placeholders)

The 'gsis' mode additionally requires the canonical GSIS format (^\\d{2}-\\d{7}$)
and nulls anything that doesn't match. That's correct for child tables (they
only carry modern-era IDs) but wrong for the `players` hub where pre-GSIS
historical Elias IDs like 'YOU597411' or 'VIT276861' are legitimate.
Config declares kind='generic' for those cases.
"""

from __future__ import annotations

import re

import pandas as pd

_JUNK = {"", "0", "None", "nan", "NaN", "<NA>", "NA"}
_GSIS_RE = re.compile(r"^\d{2}-\d{7}$")


def clean_id(series: pd.Series, kind: str = "generic") -> pd.Series:
    """Normalize an ID column, nulling junk sentinels.

    Args:
        series: pandas Series of ID values (any dtype).
        kind: 'generic' strips sentinels only; 'gsis' additionally requires the
              GSIS regex (^\\d{2}-\\d{7}$) and nulls non-matching values.

    Returns:
        Series with dtype 'string' (pandas nullable) and pd.NA for junk.
    """
    if kind not in ("generic", "gsis"):
        raise ValueError(f"unknown cleanup kind: {kind!r}")

    # Handle numeric→string conversion cleanly (e.g., ESPN IDs stored as float
    # with NaNs produce '3139477.0' / '3.139477e+06' if cast naively).
    if pd.api.types.is_numeric_dtype(series):
        s = pd.to_numeric(series, errors="coerce").astype("Int64").astype("string")
    else:
        s = series.astype("string").str.strip()

    # Strip junk sentinels
    s = s.where(~s.isin(_JUNK), other=pd.NA)

    if kind == "gsis":
        s = s.where(s.str.match(r"^\d{2}-\d{7}$", na=False), other=pd.NA)

    return s


def clean_value(value, kind: str = "generic") -> str | None:
    """Scalar version of clean_id. Returns None for junk values.

    Used in the hub builder when inspecting individual rows. Vectorized
    clean_id() is preferred; this is only for one-off checks.
    """
    if value is None:
        return None
    if isinstance(value, float) and value != value:  # NaN check
        return None
    s = str(value).strip()
    if s in _JUNK:
        return None
    if kind == "gsis" and not _GSIS_RE.fullmatch(s):
        return None
    return s
