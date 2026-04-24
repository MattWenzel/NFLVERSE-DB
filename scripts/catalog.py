#!/usr/bin/env python3
"""Discover and catalog every nflverse-data release + every external source.

Output: data/nflverse_manifest.json. Single source of truth for what sources
exist, their file patterns, year spans, column schemas, and which columns are
IDs. The build config (scripts/schema.py) references patterns from this
manifest; the build validates that every declared source resolves here.

Why this exists: v1 missed stats_player_post_*.parquet (12K POST-season rows)
for months because nobody systematically diffed GitHub against download.py.
Regenerating this file after each nflverse release surfaces drift.

Run:
    python3 scripts/catalog.py
    python3 scripts/catalog.py --no-probe     # skip column sampling
    python3 scripts/catalog.py --release snap_counts  # refresh one release
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import urllib.request
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
from config import RAW_DATA_PATH  # noqa: E402

MANIFEST_PATH = ROOT / "data" / "nflverse_manifest.json"
GH_API = "https://api.github.com/repos/nflverse/nflverse-data/releases?per_page=100"
GH_DOWNLOAD = "https://github.com/nflverse/nflverse-data/releases/download"

# ID-column classification. Order matters: first match wins, so the more-specific
# patterns come first. Release tag is consulted for ambiguous column names
# (player_id is GSIS in stats_player, ESPN in espn_data / qbr).
ID_PATTERNS = [
    (re.compile(r"^(player_)?gsis_id$"),      "gsis"),
    (re.compile(r"^gsis_it_id$"),             "gsis_it"),
    (re.compile(r"^(player_)?pfr_id$"),       "pfr"),
    (re.compile(r"^pfr_player_id$"),          "pfr"),
    (re.compile(r"^pfr_game_id$"),            "pfr_game_id"),
    (re.compile(r"^(player_)?espn_id$"),      "espn"),
    (re.compile(r"^(player_)?esb_id$"),       "esb"),
    (re.compile(r"^(player_)?pff_id$"),       "pff"),
    (re.compile(r"^(player_)?otc_id$"),       "otc"),
    (re.compile(r"^(player_)?smart_id$"),     "smart"),
    (re.compile(r"^(player_)?nfl_id$"),       "nfl"),
    (re.compile(r"^nflverse_game_id$"),       "game_id"),
    (re.compile(r"^old_game_id$"),            "old_game_id"),
    (re.compile(r"^game_id$"),                "game_id"),
    (re.compile(r"^game_key$"),               "game_key"),
    (re.compile(r"^official_id$"),            "official_id"),
]

# Release-tag-specific disambiguation for generic column names.
AMBIGUOUS_PLAYER_ID = {
    "stats_player":   "gsis",
    "player_stats":   "gsis",  # legacy; classified even if we don't use it
    "espn_data":      "espn",
}


def classify_id(column: str, release_tag: str) -> str | None:
    """Return an ID-system label for a column, or None if not an ID column."""
    for pat, label in ID_PATTERNS:
        if pat.fullmatch(column):
            return label
    if column == "player_id":
        return AMBIGUOUS_PLAYER_ID.get(release_tag)
    return None


def fetch_releases() -> list[dict]:
    """Pull every release from the GitHub API."""
    print(f"  GET {GH_API}")
    req = urllib.request.Request(
        GH_API,
        headers={"Accept": "application/vnd.github+json"},
    )
    with urllib.request.urlopen(req, timeout=60) as resp:
        return json.loads(resp.read())


_YEAR_RE = re.compile(r"(?<![0-9])(19\d{2}|20\d{2})(?![0-9])")


def extract_pattern(filename: str) -> tuple[str, int | None]:
    """'stats_player_reg_2024.parquet' -> ('stats_player_reg_{year}.parquet', 2024).
    'players.parquet' -> ('players.parquet', None).
    """
    m = _YEAR_RE.search(filename)
    if m:
        year = int(m.group())
        pattern = filename[: m.start()] + "{year}" + filename[m.end():]
        return pattern, year
    return filename, None


def group_assets(assets: list[dict]) -> list[dict]:
    """Collapse asset list into parquet-only pattern groups.

    Returns a list of {pattern, years, urls} dicts — one per distinct pattern.
    Non-parquet assets (csv/rds/qs/timestamp) are ignored — they're just
    alternate serializations of the parquet content or metadata files.
    """
    groups = defaultdict(lambda: {"years": [], "urls": {}})
    for a in assets:
        name = a["name"]
        if not name.endswith(".parquet"):
            continue
        pattern, year = extract_pattern(name)
        entry = groups[pattern]
        if year is not None:
            entry["years"].append(year)
        entry["urls"][year if year is not None else "_single"] = a["browser_download_url"]
    out = []
    for pattern, data in sorted(groups.items()):
        out.append({
            "pattern": pattern,
            "years": sorted(data["years"]) if data["years"] else None,
            "year_span": (min(data["years"]), max(data["years"])) if data["years"] else None,
            "file_count": len(data["urls"]),
            "_urls": data["urls"],  # internal; stripped before JSON save
        })
    return out


def probe_columns(pattern: str, urls: dict, release_tag: str,
                  raw_root: Path, cache_dir: Path) -> tuple[list[dict], int | None]:
    """Read one representative parquet's schema.

    Priority: local raw file > audit cache > download one sample from GitHub.
    Returns (columns, row_count) where columns is a list of
    {name, type, is_id, id_kind} dicts.
    """
    # Try local raw path first (no download, no cache pollution)
    local = _find_local_sample(pattern, raw_root)
    # Fallback to the audit cache where we pre-downloaded samples
    if not local:
        local = _find_cache_sample(pattern, cache_dir)
    # Last resort: download one year's file (prefer latest year)
    if not local:
        local = _download_sample(pattern, urls, cache_dir)
    if not local or not local.exists():
        return [], None

    con = duckdb.connect(":memory:")
    try:
        cols = con.execute(f"DESCRIBE SELECT * FROM read_parquet('{local}')").fetchall()
        n = con.execute(f"SELECT COUNT(*) FROM read_parquet('{local}')").fetchone()[0]
    finally:
        con.close()

    out = []
    for c in cols:
        name, dtype = c[0], c[1]
        id_kind = classify_id(name, release_tag)
        out.append({
            "name": name,
            "type": dtype,
            "is_id": id_kind is not None,
            "id_kind": id_kind,
        })
    return out, n


def _find_local_sample(pattern: str, raw_root: Path) -> Path | None:
    """Find one existing raw file matching the pattern."""
    # Pattern: stats_player_reg_{year}.parquet or players.parquet
    subdirs_to_try = [
        # known mapping of release tag to subfolder
        "stats_player", "pbp", "snap_counts", "depth_charts", "nextgen_stats",
        "pfr_advstats", "combine", "draft_picks", "players", "schedules",
        "external",
    ]
    if "{year}" in pattern:
        # Try the most recent year first, walk down
        for year in range(2025, 1998, -1):
            fname = pattern.replace("{year}", str(year))
            for sd in subdirs_to_try:
                p = raw_root / sd / fname
                if p.exists():
                    return p
    else:
        for sd in subdirs_to_try:
            p = raw_root / sd / pattern
            if p.exists():
                return p
    return None


def _find_cache_sample(pattern: str, cache_dir: Path) -> Path | None:
    """Find a matching file in the audit cache (/tmp/nflverse-audit)."""
    if "{year}" in pattern:
        for year in range(2025, 1998, -1):
            p = cache_dir / pattern.replace("{year}", str(year))
            if p.exists():
                return p
    else:
        p = cache_dir / pattern
        if p.exists():
            return p
    return None


def _download_sample(pattern: str, urls: dict, cache_dir: Path) -> Path | None:
    """Download the latest-year file (or single file) to the cache."""
    cache_dir.mkdir(parents=True, exist_ok=True)
    if urls.get("_single"):
        url = urls["_single"]
        fname = pattern
    else:
        if not urls:
            return None
        latest_year = max(k for k in urls.keys() if isinstance(k, int))
        url = urls[latest_year]
        fname = pattern.replace("{year}", str(latest_year))
    dest = cache_dir / fname
    try:
        urllib.request.urlretrieve(url, dest)
        return dest
    except Exception as e:
        print(f"    sample-download failed for {fname}: {e}")
        return None


def external_sources_catalog() -> dict:
    """Hardcoded entries for non-nflverse sources. Kept in the manifest so
    the config can reference them uniformly."""
    return {
        "dynastyprocess_db_playerids": {
            "url": "https://github.com/dynastyprocess/data/raw/master/files/db_playerids.csv",
            "local_path": "external/db_playerids.csv",
            "format": "csv",
            "purpose": "player_ids bridge table (GSIS↔PFR↔ESPN↔PFF↔...)",
            # Columns catalogued at build time by inspecting the local CSV.
            "columns": _probe_csv_columns(
                RAW_DATA_PATH / "external" / "db_playerids.csv", release_tag="_external"
            ),
        },
    }


def _probe_csv_columns(path: Path, release_tag: str) -> list[dict]:
    if not path.exists():
        return []
    con = duckdb.connect(":memory:")
    try:
        cols = con.execute(f"DESCRIBE SELECT * FROM read_csv_auto('{path}')").fetchall()
    finally:
        con.close()
    out = []
    for c in cols:
        name, dtype = c[0], c[1]
        # For db_playerids.csv, column names are the canonical short form
        # (gsis_id, pfr_id, espn_id, ...). Classify accordingly.
        id_kind = classify_id(name, release_tag)
        out.append({"name": name, "type": dtype, "is_id": id_kind is not None, "id_kind": id_kind})
    return out


def build_manifest(probe_columns_flag: bool = True, only_release: str | None = None) -> dict:
    """Walk the nflverse releases, probe schemas, emit the full manifest."""
    releases = fetch_releases()
    print(f"  found {len(releases)} releases")

    cache_dir = Path("/tmp/nflverse-audit")
    out: dict = {
        "generated_at": datetime.now(tz=timezone.utc).isoformat(),
        "source_repo": "nflverse/nflverse-data",
        "probe_columns": probe_columns_flag,
        "nflverse_releases": {},
        "external_sources": external_sources_catalog(),
    }

    for rel in releases:
        tag = rel["tag_name"]
        if only_release and tag != only_release:
            continue
        if tag in ("test",):  # nflverse scratch release
            continue
        groups = group_assets(rel["assets"])
        if not groups:
            continue
        print(f"  {tag}: {len(groups)} parquet pattern(s)")

        patterns = []
        for g in groups:
            entry = {
                "pattern": g["pattern"],
                "year_span": g["year_span"],
                "years": g["years"],
                "file_count": g["file_count"],
            }
            if probe_columns_flag:
                cols, row_count = probe_columns(g["pattern"], g["_urls"], tag,
                                                RAW_DATA_PATH, cache_dir)
                entry["columns"] = cols
                entry["sample_row_count"] = row_count
                entry["id_columns"] = {
                    c["name"]: c["id_kind"] for c in cols if c["is_id"]
                }
                entry["column_count"] = len(cols)
            patterns.append(entry)

        out["nflverse_releases"][tag] = {
            "github_url": f"https://github.com/nflverse/nflverse-data/releases/tag/{tag}",
            "patterns": patterns,
        }

    return out


def save_manifest(manifest: dict, path: Path = MANIFEST_PATH, merge: bool = False) -> None:
    """Write the manifest, optionally merging into an existing one (for
    --release updates)."""
    if merge and path.exists():
        with path.open() as f:
            existing = json.load(f)
        existing["generated_at"] = manifest["generated_at"]
        for tag, data in manifest["nflverse_releases"].items():
            existing["nflverse_releases"][tag] = data
        manifest = existing
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w") as f:
        json.dump(manifest, f, indent=2, sort_keys=False)
    size_kb = path.stat().st_size / 1024
    print(f"  wrote {path.relative_to(ROOT)} ({size_kb:.1f} KB)")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--no-probe", action="store_true",
                        help="Skip parquet column sampling (faster, patterns only)")
    parser.add_argument("--release", type=str, default=None,
                        help="Refresh only one release tag (merge into existing manifest)")
    args = parser.parse_args()

    print("Building nflverse manifest:")
    manifest = build_manifest(
        probe_columns_flag=not args.no_probe,
        only_release=args.release,
    )
    save_manifest(manifest, merge=bool(args.release))
    return 0


if __name__ == "__main__":
    sys.exit(main())
