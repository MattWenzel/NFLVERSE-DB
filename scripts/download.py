#!/usr/bin/env python3
"""
Download raw nflverse data files from GitHub into data/raw/.

Downloads parquet files from nflverse-data GitHub releases and CSV files
from external sources. Files are organized by release tag.

Usage:
    python3 scripts/download.py --all                              # Everything
    python3 scripts/download.py --tables game_stats players        # Specific tables
    python3 scripts/download.py --tables game_stats --years 2025   # Specific years
    python3 scripts/download.py --pbp --all                        # Play-by-play
    python3 scripts/download.py --pbp --years 2025                 # PBP for one year
    python3 scripts/download.py --force                            # Re-download existing
    python3 scripts/download.py --dry-run                          # Preview only
"""

import argparse
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import urlretrieve

from config import RAW_DATA_PATH, YEAR_RANGE_START

NFLVERSE_BASE = "https://github.com/nflverse/nflverse-data/releases/download"

CURRENT_YEAR = datetime.now().year

# table name -> download spec
DOWNLOAD_MAP = {
    # Year-partitioned parquet from nflverse-data releases
    "game_stats": {
        "tag": "stats_player",
        "pattern": "stats_player_week_{year}.parquet",
        "years": (YEAR_RANGE_START["game_stats"], CURRENT_YEAR),
    },
    "season_stats": {
        "tag": "stats_player",
        # Both REG and POST aggregates from nflverse. POST was overlooked in
        # the original config; without it we had zero season-level POST rows
        # (visible as the 12K gap game_stats → season_stats).
        "patterns": [
            "stats_player_reg_{year}.parquet",
            "stats_player_post_{year}.parquet",
        ],
        "years": (YEAR_RANGE_START["season_stats"], CURRENT_YEAR),
    },
    "games": {
        "tag": "schedules",
        "files": ["games.parquet"],
    },
    "snap_counts": {
        "tag": "snap_counts",
        "pattern": "snap_counts_{year}.parquet",
        "years": (YEAR_RANGE_START["snap_counts"], CURRENT_YEAR),
    },
    "depth_charts": {
        "tag": "depth_charts",
        "pattern": "depth_charts_{year}.parquet",
        "years": (YEAR_RANGE_START["depth_charts"], CURRENT_YEAR),
    },
    # Single-file parquet from nflverse-data releases
    "players": {
        "tag": "players",
        "files": ["players.parquet"],
    },
    "player_ids": {
        "url": "https://github.com/dynastyprocess/data/raw/master/files/db_playerids.csv",
        "subfolder": "external",
        "files": ["db_playerids.csv"],
    },
    "draft_picks": {
        "tag": "draft_picks",
        "files": ["draft_picks.parquet"],
    },
    "combine": {
        "tag": "combine",
        "files": ["combine.parquet"],
    },
    "ngs_stats": {
        "tag": "nextgen_stats",
        "files": ["ngs_passing.parquet", "ngs_rushing.parquet", "ngs_receiving.parquet"],
    },
    "pfr_advanced": {
        "tag": "pfr_advstats",
        "files": ["advstats_season_pass.parquet", "advstats_season_rush.parquet", "advstats_season_rec.parquet"],
    },
    "qbr": {
        "url": "https://raw.githubusercontent.com/nflverse/espnscrapeR-data/master/data/qbr-nfl-weekly.csv",
        "subfolder": "external",
        "files": ["qbr-nfl-weekly.csv"],
    },
    # Play-by-play (separate DB, very large)
    "play_by_play": {
        "tag": "pbp",
        "pattern": "play_by_play_{year}.parquet",
        "years": (YEAR_RANGE_START["play_by_play"], CURRENT_YEAR),
    },
}

# Tables that belong to the PBP database
PBP_TABLES = {"play_by_play"}


def download_file(url, local_path, force=False):
    """Download a single file. Returns (status, message) where status is one of
    'ok', 'skip', 'error'."""
    if local_path.exists() and local_path.stat().st_size > 0 and not force:
        return "skip", f"  {local_path.name} (exists)"

    local_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        urlretrieve(url, local_path)
        size_mb = local_path.stat().st_size / (1024 * 1024)
        return "ok", f"  {local_path.name} ({size_mb:.1f} MB)"
    except (HTTPError, URLError, OSError) as e:
        if local_path.exists():
            local_path.unlink()
        return "error", f"  {local_path.name} FAILED: {e}"


def get_files_for_table(table_name, years=None):
    """Return list of (url, local_path) tuples for a table."""
    spec = DOWNLOAD_MAP[table_name]
    results = []

    if "pattern" in spec or "patterns" in spec:
        # Year-partitioned. `patterns` (list) supports multiple files per
        # year under the same release tag (e.g. REG + POST season stats).
        start, end = spec["years"]
        if years:
            file_years = [y for y in years if start <= y <= end]
        else:
            file_years = list(range(start, end + 1))

        tag = spec["tag"]
        subfolder = spec.get("subfolder", tag)
        patterns = spec.get("patterns") or [spec["pattern"]]
        for year in file_years:
            for pat in patterns:
                filename = pat.format(year=year)
                url = f"{NFLVERSE_BASE}/{tag}/{filename}"
                local = RAW_DATA_PATH / subfolder / filename
                results.append((url, local))
    elif "url" in spec:
        # External source (single direct URL)
        subfolder = spec.get("subfolder", "external")
        for filename in spec["files"]:
            local = RAW_DATA_PATH / subfolder / filename
            results.append((spec["url"], local))
    else:
        # Single-file from nflverse-data release
        tag = spec["tag"]
        subfolder = spec.get("subfolder", tag)
        for filename in spec["files"]:
            url = f"{NFLVERSE_BASE}/{tag}/{filename}"
            local = RAW_DATA_PATH / subfolder / filename
            results.append((url, local))

    return results


def main():
    parser = argparse.ArgumentParser(description="Download nflverse data files")
    parser.add_argument("--tables", nargs="+", help="Specific table(s) to download")
    parser.add_argument("--years", nargs="+", type=int, help="Specific year(s) for year-partitioned data")
    parser.add_argument("--pbp", action="store_true", help="Include play-by-play (large)")
    parser.add_argument("--all", action="store_true", help="Download all years (not just current)")
    parser.add_argument("--force", action="store_true", help="Re-download existing files")
    parser.add_argument("--dry-run", action="store_true", help="Preview what would download")
    parser.add_argument("--workers", type=int, default=8,
                        help="Parallel download workers (default: 8). "
                             "Progress is shown in completion order, not input order.")
    args = parser.parse_args()

    # Determine which tables
    if args.tables:
        table_names = args.tables
    else:
        # All non-PBP tables by default
        table_names = [t for t in DOWNLOAD_MAP if t not in PBP_TABLES]

    if args.pbp:
        if "play_by_play" not in table_names:
            table_names.append("play_by_play")

    # Validate
    for name in table_names:
        if name not in DOWNLOAD_MAP:
            print(f"ERROR: Unknown table '{name}'")
            print(f"Available: {', '.join(sorted(DOWNLOAD_MAP.keys()))}")
            sys.exit(1)

    # Determine years
    years = None
    if args.years:
        years = sorted(args.years)
    elif not args.all:
        years = [CURRENT_YEAR]

    # Build file list
    all_files = []
    for table in table_names:
        all_files.extend(get_files_for_table(table, years))

    print(f"nflverse Data Download — {len(all_files)} files")
    if args.dry_run:
        print("*** DRY RUN ***\n")
        for url, local in all_files:
            exists = "exists" if local.exists() else "missing"
            print(f"  {local.relative_to(RAW_DATA_PATH)} ({exists})")
        return

    print()
    total = len(all_files)
    counts = {"ok": 0, "skip": 0, "error": 0}

    with ThreadPoolExecutor(max_workers=max(1, args.workers)) as pool:
        futures = [
            pool.submit(download_file, url, local, args.force)
            for url, local in all_files
        ]
        for i, fut in enumerate(as_completed(futures), 1):
            status, message = fut.result()
            counts[status] += 1
            print(f"[{i}/{total}] {message.lstrip()}", flush=True)

    ok, skip, errors = counts["ok"], counts["skip"], counts["error"]
    print(f"\nDone: {ok} downloaded, {skip} skipped (already exist), {errors} errors")
    if skip and not args.force:
        print("Use --force to re-download existing files")


if __name__ == "__main__":
    main()
