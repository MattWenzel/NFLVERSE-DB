#!/usr/bin/env python3
"""Download every raw file declared in scripts/schema.py:SOURCES.

Derives URLs from the manifest (scripts/catalog.py output). Files land in
data/raw/<subfolder>/<filename>. Skips already-downloaded files unless --force.

Any source declared in schema.SOURCES gets pulled here — no parallel
DOWNLOAD_MAP to keep in sync.

Usage:
    python3 scripts/download.py                       # all sources, current year + prior
    python3 scripts/download.py --all                 # all years
    python3 scripts/download.py --sources weekly_rosters injuries
    python3 scripts/download.py --years 2024 2025
    python3 scripts/download.py --force               # re-download existing
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import urlretrieve

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from config import RAW_DATA_PATH  # noqa: E402 — path constants
from schema import SOURCES  # noqa: E402 — declarative DB config

MANIFEST_PATH = ROOT / "data" / "nflverse_manifest.json"
GH_DOWNLOAD = "https://github.com/nflverse/nflverse-data/releases/download"
CURRENT_YEAR = datetime.now().year


def load_manifest() -> dict:
    if not MANIFEST_PATH.exists():
        raise SystemExit(f"Manifest missing: {MANIFEST_PATH}. Run scripts/catalog.py first.")
    with MANIFEST_PATH.open() as f:
        return json.load(f)


def resolve_source_files(source_id: str, source_spec: dict, manifest: dict,
                         years: list[int] | None) -> list[tuple[str, Path]]:
    """For one SOURCES entry, return (url, local_path) pairs to fetch.

    Handles external sources (direct URL) and nflverse releases (computed
    from release_tag + pattern).
    """
    pattern = source_spec["pattern"]  # e.g. "snap_counts/snap_counts_{year}.parquet"
    release_tag = source_spec.get("release_tag")

    # External sources: direct URL stored in manifest under external_sources
    if release_tag == "_external":
        ext = manifest.get("external_sources", {})
        # Match by the local_path == pattern
        matched = None
        for k, v in ext.items():
            if v.get("local_path") == pattern.removeprefix("external/"):
                matched = v
                break
            if pattern.endswith(v.get("local_path", "__never__")):
                matched = v
                break
        # Fall back: db_playerids special-case
        if matched is None and "db_playerids" in source_id:
            matched = ext.get("dynastyprocess_db_playerids")
        if matched is None:
            print(f"  WARN: external source {source_id!r} unresolved in manifest")
            return []
        local = RAW_DATA_PATH / pattern
        return [(matched["url"], local)]

    # nflverse release — patterns live under the release's subfolder locally.
    # Determine the release asset pattern (the filename portion only):
    asset_pattern = pattern.rsplit("/", 1)[-1]
    local_subdir = pattern.rsplit("/", 1)[0] if "/" in pattern else release_tag

    out = []
    if "{year}" in asset_pattern:
        # year-partitioned; resolve years from the manifest
        rel = manifest["nflverse_releases"].get(release_tag, {})
        manifest_pats = rel.get("patterns", [])
        manifest_years: set[int] = set()
        for p in manifest_pats:
            # A specific filename like "depth_charts_2025.parquet" can be
            # satisfied by a manifest pattern like "depth_charts_{year}.parquet"
            # with year 2025 in its years list. Check both direct match and
            # regex match.
            if p["pattern"] == asset_pattern:
                manifest_years.update(p.get("years", []) or [])
            else:
                regex = "^" + re.escape(p["pattern"]).replace(r"\{year\}", r"(\d{4})") + "$"
                m = re.match(regex, asset_pattern)
                if m:
                    manifest_years.update(p.get("years", []) or [])
        # Intersect with requested years (if any)
        year_range = source_spec.get("year_range", ("auto", "auto"))
        lo, hi = year_range
        if lo == "auto":
            lo = min(manifest_years) if manifest_years else 1999
        if hi == "auto":
            hi = max(manifest_years) if manifest_years else CURRENT_YEAR
        eligible = {y for y in manifest_years if lo <= y <= hi}
        if years:
            eligible &= set(years)
        for y in sorted(eligible):
            filename = asset_pattern.replace("{year}", str(y))
            url = f"{GH_DOWNLOAD}/{release_tag}/{filename}"
            local = RAW_DATA_PATH / local_subdir / filename
            out.append((url, local))
    else:
        # single file
        url = f"{GH_DOWNLOAD}/{release_tag}/{asset_pattern}"
        local = RAW_DATA_PATH / local_subdir / asset_pattern
        out.append((url, local))
    return out


def download_one(url: str, local: Path, force: bool) -> tuple[str, str]:
    if local.exists() and local.stat().st_size > 0 and not force:
        return "skip", f"{local.name} (exists)"
    local.parent.mkdir(parents=True, exist_ok=True)
    try:
        urlretrieve(url, local)
        kb = local.stat().st_size / 1024
        return "ok", f"{local.name} ({kb:.0f} KB)"
    except (HTTPError, URLError, OSError) as e:
        if local.exists():
            local.unlink()
        return "error", f"{local.name} FAILED: {e}"


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--sources", nargs="+",
                        help="Restrict to these source_ids (default: all)")
    parser.add_argument("--years", nargs="+", type=int,
                        help="Restrict to these years for year-partitioned sources")
    parser.add_argument("--all", action="store_true",
                        help="Download all years (default: current year only)")
    parser.add_argument("--force", action="store_true",
                        help="Re-download files that already exist")
    parser.add_argument("--workers", type=int, default=8,
                        help="Parallel download workers")
    parser.add_argument("--dry-run", action="store_true",
                        help="List what would download, don't fetch")
    args = parser.parse_args()

    manifest = load_manifest()

    source_ids = args.sources if args.sources else list(SOURCES.keys())
    for sid in source_ids:
        if sid not in SOURCES:
            print(f"ERROR: unknown source {sid!r}. Known: {sorted(SOURCES)}")
            sys.exit(1)

    # Default to current year only, --all for full range
    years = args.years if args.years else (None if args.all else [CURRENT_YEAR - 1, CURRENT_YEAR])

    files: list[tuple[str, Path]] = []
    for sid in source_ids:
        files.extend(resolve_source_files(sid, SOURCES[sid], manifest, years))

    print(f"nflverse downloader: {len(files)} files across {len(source_ids)} sources")
    if args.dry_run:
        for url, local in files:
            mark = "exists" if local.exists() and local.stat().st_size > 0 else "missing"
            print(f"  [{mark:<7}] {local.relative_to(RAW_DATA_PATH)}")
        return

    counts = {"ok": 0, "skip": 0, "error": 0}
    with ThreadPoolExecutor(max_workers=max(1, args.workers)) as pool:
        futures = {pool.submit(download_one, u, l, args.force): (u, l) for u, l in files}
        for i, fut in enumerate(as_completed(futures), 1):
            status, msg = fut.result()
            counts[status] += 1
            print(f"[{i}/{len(files)}] {msg}", flush=True)

    print(f"\nDone: {counts['ok']} downloaded, {counts['skip']} skipped, {counts['error']} errors")


if __name__ == "__main__":
    main()
