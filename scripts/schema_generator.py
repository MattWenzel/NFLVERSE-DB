#!/usr/bin/env python3
"""Generate scripts/schema_skeleton.py from data/nflverse_manifest.json.

v3 principle: `schema.py`'s SOURCES must be *derivable* from upstream reality,
not a parallel handwritten assertion. This generator emits a skeleton with
every file pattern from the manifest as a candidate source (disabled by default).

scripts/schema.py then declares which skeleton entries are enabled + adds
renames + id_cleanup + expected_gaps. A new upstream file auto-lands as a
new skeleton entry on the next `scripts/catalog.py` + `scripts/schema_generator.py`
run; silently missing it is structurally impossible.

Usage:
    python3 scripts/schema_generator.py           # regenerate skeleton
    python3 scripts/schema_generator.py --check   # verify current skeleton up-to-date
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
from config import RAW_DATA_PATH  # noqa: E402

MANIFEST_PATH = ROOT / "data" / "nflverse_manifest.json"
SKELETON_PATH = ROOT / "scripts" / "schema_skeleton.py"


def source_id_from(tag: str, pattern: str) -> str:
    """Stable source_id from (release_tag, pattern).

    Examples:
      (stats_player, stats_player_week_{year}.parquet) -> 'stats_player_week'
      (players, players.parquet) -> 'players_master' (master collision-avoidance)
      (depth_charts, depth_charts_{year}.parquet) -> 'depth_charts_legacy'
      (depth_charts, depth_charts_2025.parquet) -> 'depth_charts_2025'
    """
    # Strip the .parquet and {year} and collapse
    base = pattern.replace(".parquet", "").replace("_{year}", "")
    # Avoid bare table-name collisions for the players release
    if tag == "players" and base == "players":
        return "players_master"
    if tag == "depth_charts" and base == "depth_charts":
        return "depth_charts_legacy"
    # For single-file releases, use the base directly (e.g. combine, draft_picks)
    # For multi-file releases, use base (stats_player_week, stats_player_reg, ...)
    return base


def infer_id_cleanup_kind(id_kind: str | None) -> str:
    """Map catalog's id_kind label to clean_id's kind argument."""
    if id_kind == "gsis":
        return "gsis"
    return "generic"  # everything else gets loose cleanup


def skeleton_entry(release_tag: str, pattern_info: dict) -> dict:
    """Build one SKELETON dict entry from a manifest pattern."""
    pattern = pattern_info["pattern"]
    entry: dict = {
        "_enabled": False,           # opt-in only; schema.py overrides to True
        "release_tag": release_tag,
        "pattern": pattern if "/" in pattern else f"{release_tag}/{pattern}",
        "renames": {},
        "id_cleanup": {},
    }
    if "{year}" in pattern:
        entry["year_range"] = ("auto", "auto")

    id_cols = pattern_info.get("id_columns", {})
    for col, kind in id_cols.items():
        entry["id_cleanup"][col] = infer_id_cleanup_kind(kind)

    # Reference metadata (engine ignores, humans read):
    entry["_columns"] = [c["name"] for c in pattern_info.get("columns", [])]
    entry["_year_span"] = pattern_info.get("year_span")
    entry["_sample_row_count"] = pattern_info.get("sample_row_count")
    return entry


def build_skeleton(manifest: dict) -> dict:
    """Walk the manifest, emit one skeleton entry per pattern.

    If two releases produce the same source_id (e.g. `player_stats` legacy
    release duplicates of `stats_player`), disambiguate by prefixing the
    tag to the later-encountered one. Preserves stable IDs for the canonical
    releases we care about."""
    skeleton = {}
    # Process canonical releases first so legacy duplicates get suffixed, not them.
    CANONICAL_FIRST = [
        "players", "stats_player", "stats_team", "schedules", "pbp",
        "snap_counts", "depth_charts", "nextgen_stats", "pfr_advstats",
        "combine", "draft_picks", "weekly_rosters", "injuries", "contracts",
        "pbp_participation", "ftn_charting", "officials", "espn_data",
    ]
    releases = manifest.get("nflverse_releases", {})
    ordered_tags = [t for t in CANONICAL_FIRST if t in releases] + [
        t for t in releases if t not in CANONICAL_FIRST
    ]
    for tag in ordered_tags:
        data = releases[tag]
        for pat in data.get("patterns", []):
            sid = source_id_from(tag, pat["pattern"])
            if sid in skeleton:
                # Collision — prefix the later entry's ID with its tag.
                sid = f"{tag}__{sid}"
            skeleton[sid] = skeleton_entry(tag, pat)

    # External sources (manifest.external_sources)
    for ext_key, ext in manifest.get("external_sources", {}).items():
        sid = ext_key  # e.g. "dynastyprocess_db_playerids"
        entry = {
            "_enabled": False,
            "release_tag": "_external",
            "pattern": f"external/{ext.get('local_path', '').split('/')[-1]}" or f"external/{sid}",
            "format": "csv" if ext.get("format") == "csv" else "parquet",
            "renames": {},
            "id_cleanup": {},
            "_url": ext.get("url"),
            "_columns": [c["name"] for c in ext.get("columns", [])],
        }
        for col in ext.get("columns", []):
            if col.get("is_id") and col.get("id_kind"):
                entry["id_cleanup"][col["name"]] = infer_id_cleanup_kind(col["id_kind"])
        skeleton[sid] = entry

    return skeleton


def render(skeleton: dict) -> str:
    """Render the skeleton dict to Python source."""
    lines = [
        '"""Auto-generated from data/nflverse_manifest.json by scripts/schema_generator.py.',
        "",
        "DO NOT EDIT BY HAND. Regenerate with:",
        "    python3 scripts/schema_generator.py",
        "",
        "scripts/schema.py imports SKELETON and overrides entries (sets _enabled: True,",
        "adds renames / expected_gaps / etc.). Any entry NOT overridden is skipped at",
        "build time. A new manifest entry with no corresponding schema.py override",
        "surfaces as an audit warning — you can't silently miss upstream files.",
        '"""',
        "",
        "from __future__ import annotations",
        "",
        f"# {len(skeleton)} candidate source(s) derived from the manifest.",
        "",
        "SKELETON: dict = {",
    ]
    for sid in sorted(skeleton):
        e = skeleton[sid]
        lines.append(f"    {sid!r}: {{")
        for k, v in e.items():
            if k == "_columns" and len(v) > 10:
                # Truncate long column lists for readability
                trunc = v[:5] + ["..."] + v[-5:]
                lines.append(f"        {k!r}: {trunc!r},  # {len(v)} total")
            else:
                lines.append(f"        {k!r}: {v!r},")
        lines.append("    },")
    lines.append("}")
    lines.append("")
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--check", action="store_true",
                        help="Verify committed skeleton matches manifest; exit non-zero on drift")
    args = parser.parse_args()

    if not MANIFEST_PATH.exists():
        print(f"ERROR: manifest missing at {MANIFEST_PATH}. Run scripts/catalog.py first.")
        return 2

    with MANIFEST_PATH.open() as f:
        manifest = json.load(f)

    skeleton = build_skeleton(manifest)
    fresh = render(skeleton)

    if args.check:
        if not SKELETON_PATH.exists():
            print(f"No skeleton at {SKELETON_PATH}. Run without --check to create.")
            return 1
        with SKELETON_PATH.open() as f:
            existing = f.read()
        if existing.strip() == fresh.strip():
            print(f"Skeleton up-to-date ({len(skeleton)} candidates).")
            return 0
        print("Skeleton is out of date. Regenerate with `python3 scripts/schema_generator.py`.")
        return 1

    SKELETON_PATH.write_text(fresh)
    print(f"wrote {SKELETON_PATH.relative_to(ROOT)} ({len(skeleton)} candidates).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
