"""Primitive 2: build_hub.

Constructs the `players` DataFrame by priority-merging across HUB_BUILD.sources.
Runs entirely in pandas; must complete before any child table is INSERTed
(so name-matches can attach IDs to existing hub rows, avoiding duplicate
NULL-GSIS stubs, and so DuckDB's UPDATE-on-FK-parent restriction never fires).

Algorithm:
  Phase A — Seed the hub from the master source (role='master').
  Phase B — For each subsequent source (in declared order):
      - Role 'id_bridge' / 'expansion': apply column_map, aggregate if needed,
        then (1) stub-in GSIS rows not already in hub, (2) fill NULL hub
        columns per column_policy.
      - Role 'biographical_backfill': fill NULLs only; may look up rows by
        PFR if GSIS isn't present (key_priority).
      - Role 'name_match': for each scan_source's unresolved PFR/ESPN IDs,
        attach to a hub row via (display_name, position) match.
      - Role 'stub_source': emit stub rows for any still-unresolved IDs.

Column policies (per column, from HUB_BUILD['column_policy']):
  latest_source_wins   — newer source always overwrites (e.g. latest_team)
  earliest_non_null    — earlier source wins; later only fills NULLs
  first_non_null       — default; same as earliest_non_null

Hub-visible column set (what `players` ends up with):
  player_gsis_id (PK, may be NULL for PFR/ESPN-only stubs)
  player_pfr_id, player_espn_id (UNIQUE, nullable)
  esb_id, pff_id, smart_id, nfl_id, otc_id (additional IDs, nullable)
  display_name, first_name, last_name, position, position_group
  latest_team, status, jersey_number, headshot
  birth_date, height, weight, college_name
  draft_year, draft_round, draft_pick, draft_team
  rookie_season, last_season, years_of_experience
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts" / "v2"))

from loaders import load_source

HUB_COLUMNS = [
    # Identity
    "player_gsis_id", "player_pfr_id", "player_espn_id",
    "esb_id", "pff_id", "smart_id", "nfl_id", "otc_id", "gsis_it_id",
    # Name
    "display_name", "first_name", "last_name", "short_name",
    "common_first_name", "football_name", "suffix",
    # Role
    "position_group", "position", "ngs_position_group", "ngs_position",
    "pff_position",
    # Team / status (temporal)
    "latest_team", "status", "ngs_status", "ngs_status_short_description",
    "pff_status", "jersey_number", "headshot",
    # Biographical
    "birth_date", "height", "weight", "college_name", "college_conference",
    # Career
    "rookie_season", "last_season", "years_of_experience",
    # Draft
    "draft_year", "draft_round", "draft_pick", "draft_team",
]


def build_hub(config) -> pd.DataFrame:
    """Build and return the players DataFrame.

    Args:
        config: the v2 config module (provides SOURCES, HUB_BUILD).

    Returns:
        DataFrame of hub rows, one per unique player, with every HUB_COLUMNS
        column present (NULL-filled where the sources don't know).
    """
    sources = config.SOURCES
    hub_spec = config.HUB_BUILD
    column_policy = hub_spec.get("column_policy", {})

    hub = pd.DataFrame(columns=HUB_COLUMNS)

    # Preload every source we'll need, once. Includes the scan_sources referenced
    # by name_match / stub_source roles.
    source_dfs: dict[str, pd.DataFrame] = {}
    needed_sources: set[str] = set()
    for s in hub_spec["sources"]:
        sid = s["source_id"]
        if not sid.startswith("_"):
            needed_sources.add(sid)
        needed_sources.update(s.get("scan_sources", []))
    for sid in sorted(needed_sources):
        print(f"    loading {sid}...", end=" ", flush=True)
        source_dfs[sid] = load_source(sid, sources[sid])
        print(f"{len(source_dfs[sid]):,} rows")

    for entry in hub_spec["sources"]:
        role = entry["role"]
        sid = entry["source_id"]
        print(f"  hub phase: {role} ({sid})", end=" ", flush=True)

        if role == "master":
            hub = _seed_master(source_dfs[sid])
            print(f"→ seeded {len(hub):,} rows")

        elif role == "id_bridge":
            hub = _merge_id_bridge(hub, source_dfs[sid], entry, column_policy)
            print(f"→ hub now {len(hub):,} rows")

        elif role == "expansion":
            hub = _merge_expansion(hub, source_dfs[sid], entry, column_policy)
            print(f"→ hub now {len(hub):,} rows")

        elif role == "biographical_backfill":
            hub = _merge_biographical(hub, source_dfs[sid], entry, column_policy)
            print(f"→ hub now {len(hub):,} rows")

        elif role == "name_match":
            hub, attached = _name_match_preflight(
                hub, source_dfs, entry
            )
            print(f"→ {attached} IDs attached via name-match")

        elif role == "stub_source":
            hub, stubbed = _emit_child_stubs(hub, source_dfs, entry, sources)
            print(f"→ {stubbed} stubs added; hub now {len(hub):,} rows")

        else:
            raise ValueError(f"unknown hub source role: {role!r}")

    # Ensure every column exists; fill missing with NA
    for col in HUB_COLUMNS:
        if col not in hub.columns:
            hub[col] = pd.NA

    return hub[HUB_COLUMNS].reset_index(drop=True)


# ---------------------------------------------------------------------------
# Role: master (phase A)
# ---------------------------------------------------------------------------

def _seed_master(df: pd.DataFrame) -> pd.DataFrame:
    """Seed the hub from players_master. Rows keep all HUB_COLUMNS present."""
    out = pd.DataFrame(columns=HUB_COLUMNS)
    for col in HUB_COLUMNS:
        if col in df.columns:
            out[col] = df[col].values
        elif col == "college_name" and "college_name" not in df.columns and "college" in df.columns:
            out[col] = df["college"].values
        else:
            out[col] = pd.NA
    # Dedup on player_gsis_id, keep first
    out = out[out["player_gsis_id"].notna()].drop_duplicates("player_gsis_id", keep="first")
    return out.reset_index(drop=True)


# ---------------------------------------------------------------------------
# Role: id_bridge (phase B)
# ---------------------------------------------------------------------------

def _merge_id_bridge(hub: pd.DataFrame, bridge_df: pd.DataFrame,
                     entry: dict, column_policy: dict) -> pd.DataFrame:
    """Merge db_playerids bridge: stub-in missing GSIS; fill NULL alt-IDs."""
    cmap = entry["column_map"]
    # Rename bridge columns → hub column names
    rn = {k: v for k, v in cmap.items() if k in bridge_df.columns}
    shaped = bridge_df.rename(columns=rn).copy()

    # Restrict to rows with GSIS (others don't bridge)
    shaped = shaped[shaped["player_gsis_id"].notna()]
    shaped = shaped.drop_duplicates("player_gsis_id", keep="first")

    # Stub in rows whose GSIS isn't in hub yet
    existing_gsis = set(hub["player_gsis_id"].dropna())
    new_rows = shaped[~shaped["player_gsis_id"].isin(existing_gsis)]
    if not new_rows.empty:
        stubs = pd.DataFrame(columns=HUB_COLUMNS)
        for col in HUB_COLUMNS:
            stubs[col] = new_rows[col].values if col in new_rows.columns else pd.NA
        hub = pd.concat([hub, stubs], ignore_index=True)

    # Fill NULLs on existing rows per column_policy
    shaped_indexed = shaped.set_index("player_gsis_id")
    hub = _apply_column_fill(hub, shaped_indexed, column_policy, match_col="player_gsis_id")

    return hub


# ---------------------------------------------------------------------------
# Role: expansion (weekly_rosters)
# ---------------------------------------------------------------------------

def _merge_expansion(hub: pd.DataFrame, source_df: pd.DataFrame,
                     entry: dict, column_policy: dict) -> pd.DataFrame:
    """Weekly rosters: aggregate to one-per-GSIS (latest-by-week), stub, fill."""
    cmap = entry["column_map"]
    rn = {k: v for k, v in cmap.items() if k in source_df.columns}
    shaped = source_df.rename(columns=rn).copy()
    shaped = shaped[shaped["player_gsis_id"].notna()]

    if entry.get("aggregate") == "latest_by_week":
        # Take the row with max (season, week) per GSIS
        if "season" in shaped.columns and "week" in shaped.columns:
            shaped = (shaped.sort_values(["player_gsis_id", "season", "week"])
                      .drop_duplicates("player_gsis_id", keep="last"))
        else:
            shaped = shaped.drop_duplicates("player_gsis_id", keep="last")
    else:
        shaped = shaped.drop_duplicates("player_gsis_id", keep="first")

    existing_gsis = set(hub["player_gsis_id"].dropna())
    new_rows = shaped[~shaped["player_gsis_id"].isin(existing_gsis)]
    if not new_rows.empty:
        stubs = pd.DataFrame(columns=HUB_COLUMNS)
        for col in HUB_COLUMNS:
            stubs[col] = new_rows[col].values if col in new_rows.columns else pd.NA
        hub = pd.concat([hub, stubs], ignore_index=True)

    shaped_indexed = shaped.set_index("player_gsis_id")
    hub = _apply_column_fill(hub, shaped_indexed, column_policy, match_col="player_gsis_id")
    return hub


# ---------------------------------------------------------------------------
# Role: biographical_backfill (draft_picks, combine)
# ---------------------------------------------------------------------------

def _merge_biographical(hub: pd.DataFrame, source_df: pd.DataFrame,
                        entry: dict, column_policy: dict) -> pd.DataFrame:
    """Fill NULL bio cols. Row matching by key_priority (first key that has
    a non-null value on the source row and a corresponding hub row)."""
    cmap = entry["column_map"]
    key_priority = entry["key_priority"]  # e.g. ['player_gsis_id', 'player_pfr_id']

    rn = {k: v for k, v in cmap.items() if k in source_df.columns}
    # Fail fast on rename collisions: if a rename target already exists as a
    # column, pandas silently produces a DataFrame with duplicate column names.
    existing = set(source_df.columns)
    dest_collisions = [
        (k, v) for k, v in rn.items() if v != k and v in existing
    ]
    if dest_collisions:
        raise ValueError(
            f"biographical_backfill rename collisions for source would create "
            f"duplicate columns: {dest_collisions}. Fix the column_map."
        )
    shaped = source_df.rename(columns=rn).copy()

    # For each row, resolve which hub row (by player_gsis_id) it maps to.
    # Build lookup indices for the priority keys.
    hub_by = {}
    for key in key_priority:
        if key in hub.columns:
            # Index: key_value -> hub.index (earliest position)
            idx = hub[hub[key].notna()].drop_duplicates(key)[[key]].copy()
            idx["_hub_idx"] = idx.index
            hub_by[key] = idx.set_index(key)["_hub_idx"]

    # Attach _hub_idx to shaped. For each row, walk key_priority; first resolved wins.
    shaped["_hub_idx"] = pd.NA
    for key in key_priority:
        if key not in shaped.columns or key not in hub_by:
            continue
        mapper = hub_by[key]
        # Only fill _hub_idx where it's still NA and the source has this key value
        mask = shaped["_hub_idx"].isna() & shaped[key].notna()
        candidate = shaped.loc[mask, key].map(mapper)
        shaped.loc[mask, "_hub_idx"] = candidate

    # Drop rows with no hub match (biographical_backfill doesn't add new rows)
    matched = shaped[shaped["_hub_idx"].notna()].copy()
    matched["_hub_idx"] = matched["_hub_idx"].astype(int)
    # If multiple source rows map to one hub row, keep first (deterministic)
    matched = matched.drop_duplicates("_hub_idx", keep="first").set_index("_hub_idx")

    hub = _apply_column_fill(hub, matched, column_policy, match_col=None)
    return hub


# ---------------------------------------------------------------------------
# Role: name_match (preflight)
# ---------------------------------------------------------------------------

def _name_match_preflight(hub: pd.DataFrame, source_dfs: dict,
                          entry: dict) -> tuple[pd.DataFrame, int]:
    """Scan each child source for PFR/ESPN IDs that aren't in hub. Attempt
    name+position match against GSIS-bearing hub rows with NULL in that ID
    column. If exactly one match, attach."""
    scan_sources = entry.get("scan_sources", [])
    total = 0

    # For each candidate ID column type, scan the sources that carry it
    for target_col, name_cols_by_source, pos_cols_by_source in [
        (
            "player_pfr_id",
            {
                "combine": "player_name", "draft_picks": "pfr_player_name",
                "snap_counts": "player",
                "pfr_advanced_season_pass": "player",
                "pfr_advanced_season_rush": "player",
                "pfr_advanced_season_rec":  "player",
                "pfr_advanced_season_def":  "player",
            },
            {
                "combine": "pos", "draft_picks": "position",
                "snap_counts": "position",
                "pfr_advanced_season_pass": None,
                "pfr_advanced_season_rush": "pos",
                "pfr_advanced_season_rec":  "pos",
                "pfr_advanced_season_def":  "pos",
            },
        ),
        (
            "player_espn_id",
            {"qbr_week": "name_short", "depth_charts_2025": "player_name"},
            {"qbr_week": None, "depth_charts_2025": "pos_abb"},
        ),
    ]:
        existing_ids = set(hub[target_col].dropna())
        for sid in scan_sources:
            if sid not in name_cols_by_source:
                continue
            df = source_dfs.get(sid, pd.DataFrame())
            if df.empty or target_col not in df.columns:
                continue
            name_col = name_cols_by_source[sid]
            pos_col = pos_cols_by_source.get(sid)

            cand = df[[c for c in [target_col, name_col, pos_col] if c is not None]].copy()
            cand = cand[cand[target_col].notna() & cand[name_col].notna()]
            cand = cand.drop_duplicates(target_col)
            cand = cand[~cand[target_col].isin(existing_ids)]
            if cand.empty:
                continue

            # Donors: hub rows with GSIS, target_col is NULL, display_name known
            donors = hub[
                hub["player_gsis_id"].notna()
                & hub[target_col].isna()
                & hub["display_name"].notna()
            ]
            if donors.empty:
                continue

            donors_by_name = {
                n: grp for n, grp in donors.groupby("display_name")
            }
            consumed = set()
            for _, row in cand.iterrows():
                nm = row[name_col]
                if nm not in donors_by_name:
                    continue
                grp = donors_by_name[nm]
                if consumed:
                    grp = grp[~grp.index.isin(consumed)]
                if grp.empty:
                    continue
                if pos_col and row.get(pos_col):
                    cand_pos = row[pos_col]
                    grp_ok = grp[grp["position"].isna() | (grp["position"] == cand_pos)]
                    if not grp_ok.empty:
                        grp = grp_ok
                    else:
                        continue  # strict position mismatch
                if len(grp) != 1:
                    continue  # ambiguous
                donor_idx = grp.index[0]
                hub.at[donor_idx, target_col] = row[target_col]
                consumed.add(donor_idx)
                existing_ids.add(row[target_col])
                total += 1

    return hub, total


# ---------------------------------------------------------------------------
# Role: stub_source (last-resort)
# ---------------------------------------------------------------------------

def _emit_child_stubs(hub: pd.DataFrame, source_dfs: dict, entry: dict,
                      sources: dict) -> tuple[pd.DataFrame, int]:
    """For each child source listed in scan_sources, emit stub hub rows for
    IDs that still don't resolve.

    Metadata columns (name, position, team) pulled from each source's own
    columns per a per-source map (hardcoded below — mirrors v1's stub_source
    config per child table)."""
    scan_sources = entry.get("scan_sources", [])
    total = 0

    # Hardcoded map: source_id -> (target_col, metadata_columns)
    # Matches the stub_source declarations in TABLES for consistency.
    stub_maps = {
        "combine": ("player_pfr_id", {
            "display_name": "player_name", "position": "pos", "college_name": "school",
        }),
        "draft_picks": ("player_pfr_id", {
            "display_name": "pfr_player_name", "position": "position",
        }),
        "snap_counts": ("player_pfr_id", {
            "display_name": "player", "position": "position", "latest_team": "team",
        }),
        "pfr_advanced_season_pass": ("player_pfr_id", {
            "display_name": "player", "latest_team": "team",
        }),
        "pfr_advanced_season_rush": ("player_pfr_id", {
            "display_name": "player", "position": "pos", "latest_team": "team",
        }),
        "pfr_advanced_season_rec": ("player_pfr_id", {
            "display_name": "player", "position": "pos", "latest_team": "team",
        }),
        "pfr_advanced_season_def": ("player_pfr_id", {
            "display_name": "player", "position": "pos", "latest_team": "team",
        }),
        "qbr_week": ("player_espn_id", {
            "display_name": "name_short", "latest_team": "team_abb",
        }),
        "depth_charts_2025": ("player_espn_id", {
            "display_name": "player_name", "latest_team": "team", "position": "pos_abb",
        }),
    }

    for sid in scan_sources:
        if sid not in stub_maps:
            continue
        target_col, meta = stub_maps[sid]
        df = source_dfs.get(sid, pd.DataFrame())
        if df.empty or target_col not in df.columns:
            continue

        existing_ids = set(hub[target_col].dropna())
        cand = df[df[target_col].notna() & ~df[target_col].isin(existing_ids)]
        # Distinct IDs with any one representative metadata row
        keep_cols = [target_col] + [c for c in meta.values() if c in cand.columns]
        cand = cand[keep_cols].copy()
        cand = cand.drop_duplicates(target_col, keep="first")
        if cand.empty:
            continue

        stubs = pd.DataFrame(columns=HUB_COLUMNS)
        stubs[target_col] = cand[target_col].values
        for hub_col, src_col in meta.items():
            if src_col in cand.columns:
                stubs[hub_col] = cand[src_col].values
        for col in HUB_COLUMNS:
            if col not in stubs.columns:
                stubs[col] = pd.NA
        hub = pd.concat([hub, stubs[HUB_COLUMNS]], ignore_index=True)
        total += len(stubs)

    return hub, total


# ---------------------------------------------------------------------------
# Column-fill helper (used by id_bridge, expansion, biographical_backfill)
# ---------------------------------------------------------------------------

def _apply_column_fill(hub: pd.DataFrame, shaped: pd.DataFrame,
                       column_policy: dict, match_col: str | None) -> pd.DataFrame:
    """Apply column_policy per column: merge values from `shaped` (indexed by
    key that matches hub[match_col] or hub.index when match_col is None)
    into hub.

    - 'latest_source_wins': overwrite with shaped's value when present.
    - 'earliest_non_null' / 'first_non_null' (default): only fill hub NULLs.
    """
    if match_col is not None:
        # shaped indexed by match_col values; map hub rows by that col
        hub_keys = hub[match_col]
        resolver = shaped.index.to_series() if shaped.index.name is None else None
    else:
        # shaped indexed by hub.index directly
        pass

    # Columns whose values must be unique across the hub. If a later source
    # offers a value already claimed by a different hub row, we reject it —
    # first-established wins, preventing e.g. PFR=SmitMa01 being attached to
    # two different Marcus Smiths from different upstream sources.
    UNIQUE_COLS = {"player_pfr_id", "player_espn_id"}

    for col in HUB_COLUMNS:
        if col not in shaped.columns:
            continue
        # Pull candidate values aligned to hub rows
        if match_col is not None:
            candidate = hub_keys.map(shaped[col])
        else:
            candidate = pd.Series(pd.NA, index=hub.index, dtype="object")
            valid = shaped.index.intersection(hub.index)
            candidate.loc[valid] = shaped.loc[valid, col].values

        if col in UNIQUE_COLS:
            # Reject candidate values already held elsewhere in hub[col].
            already = set(hub[col].dropna())
            candidate = candidate.where(
                ~(candidate.isin(already) & hub[col].isna()), other=pd.NA,
            )

        policy = column_policy.get(col, "first_non_null")
        if policy == "latest_source_wins":
            hub[col] = candidate.where(candidate.notna(), hub[col])
        else:  # first_non_null / earliest_non_null
            hub[col] = hub[col].where(hub[col].notna(), candidate)

    return hub
