#!/usr/bin/env python3
"""
Check for new/updated nflverse data by comparing GitHub releases against local DB state.

Queries the GitHub Releases API for nflverse-data and compares timestamps/asset lists
against a local metadata file. Also scans local DBs for current row counts and max seasons.

The release map is derived from ``schema.SOURCES``/``HUB_BUILD`` at import time, so
every nflverse release the pipeline actually consumes is tracked — there's no
hand-kept duplicate list to drift out of sync. External sources are pulled from
the committed ``data/nflverse_manifest.json``.

Usage:
    python3 scripts/check_updates.py           # Full check, human-readable report
    python3 scripts/check_updates.py --json    # Machine-readable JSON output
    python3 scripts/check_updates.py --init    # Initialize metadata from current DB state
"""

import argparse
import json
import re
import sys
from datetime import datetime, timezone
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

import duckdb

import schema
from config import DB_PATH, METADATA_PATH

GITHUB_API = "https://api.github.com"
NFLVERSE_REPO = "nflverse/nflverse-data"


def _derive_release_map():
    """Build {release_tag: {tables, year_partitioned, asset_pattern}} from schema.SOURCES.

    Auto-derived so it can never drift from the build pipeline. Every source
    declares its release_tag and file pattern; we invert TABLES to find which
    tables consume each source. Year-partitioned releases get a regex that
    extracts the year from asset filenames (for new-year-file detection).
    """
    source_to_tables: dict[str, list[str]] = {}
    for tname, t in schema.TABLES.items():
        if t.get("build_via") in ("hub", "sql"):
            continue
        sids = []
        if t.get("source_id"):
            sids.append(t["source_id"])
        sids.extend(t.get("source_ids", []))
        for s in sids:
            source_to_tables.setdefault(s, []).append(tname)

    for hub_src in schema.HUB_BUILD.get("sources", []):
        sid = hub_src.get("source_id")
        if sid:
            source_to_tables.setdefault(sid, []).append("players")

    m: dict[str, dict] = {}
    for source_id, info in schema.SOURCES.items():
        tag = info.get("release_tag")
        pattern = info.get("pattern", "")
        if not tag or pattern.startswith("external/"):
            continue
        tables = source_to_tables.get(source_id, [])
        if not tables:
            continue
        entry = m.setdefault(tag, {"tables": [], "year_partitioned": False, "asset_pattern": None})
        for t in tables:
            if t not in entry["tables"]:
                entry["tables"].append(t)
        if "{year}" in pattern:
            entry["year_partitioned"] = True
            if entry["asset_pattern"] is None:
                filename = pattern.rsplit("/", 1)[-1].replace(".parquet", "")
                entry["asset_pattern"] = re.escape(filename).replace(r"\{year\}", r"(\d{4})")
    return m


def _derive_external_sources():
    """Build {name: {url, tables}} from manifest.external_sources cross-referenced
    against schema.SOURCES (so tables come from the pipeline, URL from the manifest)."""
    source_to_tables: dict[str, list[str]] = {}
    for tname, t in schema.TABLES.items():
        if t.get("build_via") in ("hub", "sql"):
            continue
        sids = []
        if t.get("source_id"):
            sids.append(t["source_id"])
        sids.extend(t.get("source_ids", []))
        for s in sids:
            source_to_tables.setdefault(s, []).append(tname)

    from pathlib import Path as _Path
    manifest_path = _Path(__file__).resolve().parents[1] / "data" / "nflverse_manifest.json"
    if not manifest_path.exists():
        return {}
    with manifest_path.open() as f:
        manifest = json.load(f)

    out = {}
    for mname, minfo in manifest.get("external_sources", {}).items():
        local_path = minfo.get("local_path", "")
        url = minfo.get("url")
        if not url or not local_path:
            continue
        matching_source_id = None
        for sid, sinfo in schema.SOURCES.items():
            pattern = sinfo.get("pattern", "")
            if pattern == f"external/{local_path}" or pattern.endswith(local_path):
                matching_source_id = sid
                break
        if not matching_source_id:
            continue
        tables = source_to_tables.get(matching_source_id, [])
        if not tables:
            continue
        out[matching_source_id] = {"url": url, "tables": tables}
    return out


RELEASE_MAP = _derive_release_map()
EXTERNAL_SOURCES = _derive_external_sources()


def github_get(path):
    """GET from GitHub API. Returns parsed JSON."""
    url = f"{GITHUB_API}{path}"
    req = Request(url, headers={"Accept": "application/vnd.github.v3+json"})
    try:
        with urlopen(req, timeout=15) as resp:
            return json.loads(resp.read().decode())
    except HTTPError as e:
        if e.code == 403:
            print(f"  WARNING: GitHub API rate limit hit ({url})", file=sys.stderr)
            return None
        raise
    except URLError as e:
        print(f"  WARNING: Network error fetching {url}: {e}", file=sys.stderr)
        return None


def http_head(url):
    """HTTP HEAD request. Returns Last-Modified or ETag header for change detection."""
    req = Request(url, method="HEAD")
    try:
        with urlopen(req, timeout=15) as resp:
            # Prefer Last-Modified, fall back to ETag
            return resp.headers.get("Last-Modified") or resp.headers.get("ETag")
    except (HTTPError, URLError) as e:
        print(f"  WARNING: HEAD request failed for {url}: {e}", file=sys.stderr)
        return None


def get_release_info(tag):
    """Fetch release info for a given nflverse-data tag."""
    data = github_get(f"/repos/{NFLVERSE_REPO}/releases/tags/{tag}")
    if data is None:
        return None

    assets = [a["name"] for a in data.get("assets", [])]
    return {
        "updated_at": data.get("published_at", data.get("created_at")),
        "assets": assets,
    }


def extract_max_year(assets, pattern):
    """Find the highest year number matching a pattern in asset names."""
    years = []
    regex = re.compile(pattern)
    for name in assets:
        m = regex.search(name)
        if m:
            years.append(int(m.group(1)))
    return max(years) if years else None


def scan_db_state(db_path, tables):
    """Scan a DuckDB database for row counts and max season per table."""
    state = {}
    if not db_path.exists():
        return state

    conn = duckdb.connect(str(db_path), read_only=True)

    existing_tables = {
        row[0]
        for row in conn.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = 'main'"
        ).fetchall()
    }

    for table in tables:
        if table not in existing_tables:
            state[table] = {"row_count": 0, "max_season": None}
            continue

        try:
            row_count = conn.execute(
                f'SELECT COUNT(*) FROM "{table}"'
            ).fetchone()[0]

            max_season = None
            columns = {
                row[0]
                for row in conn.execute(
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE table_schema = 'main' AND table_name = ?",
                    [table],
                ).fetchall()
            }
            if "season" in columns:
                max_season = conn.execute(
                    f'SELECT MAX(season) FROM "{table}"'
                ).fetchone()[0]

            state[table] = {"row_count": row_count, "max_season": max_season}
        except duckdb.Error:
            state[table] = {"row_count": 0, "max_season": None}

    conn.close()
    return state


def load_metadata():
    """Load saved metadata, or return empty structure."""
    if METADATA_PATH.exists():
        with open(METADATA_PATH) as f:
            return json.load(f)
    return {"last_checked": None, "releases": {}, "external": {}, "db_state": {}}


def save_metadata(metadata):
    """Save metadata to disk."""
    metadata["last_checked"] = datetime.now(timezone.utc).isoformat()
    with open(METADATA_PATH, "w") as f:
        json.dump(metadata, f, indent=2)


def init_metadata():
    """Initialize metadata from current DB state and GitHub releases."""
    print("Initializing metadata from current DB state...")

    metadata = {"last_checked": None, "releases": {}, "external": {}, "db_state": {}}

    all_tables = []
    for info in RELEASE_MAP.values():
        all_tables.extend(info["tables"])
    for info in EXTERNAL_SOURCES.values():
        all_tables.extend(info["tables"])

    db_state = scan_db_state(DB_PATH, all_tables)
    metadata["db_state"] = db_state

    # Fetch current release timestamps
    print("Fetching current GitHub release info...")
    for tag in RELEASE_MAP:
        print(f"  {tag}...", end=" ", flush=True)
        info = get_release_info(tag)
        if info:
            entry = {"updated_at": info["updated_at"]}
            cfg = RELEASE_MAP[tag]
            if cfg["year_partitioned"] and cfg["asset_pattern"]:
                max_year = extract_max_year(info["assets"], cfg["asset_pattern"])
                if max_year:
                    entry["max_year_file"] = max_year
            metadata["releases"][tag] = entry
            print("ok")
        else:
            print("failed")

    # Fetch external source timestamps
    print("Checking external sources...")
    for name, src in EXTERNAL_SOURCES.items():
        print(f"  {name}...", end=" ", flush=True)
        last_mod = http_head(src["url"])
        if last_mod:
            metadata["external"][name] = {"last_modified": last_mod}
            print("ok")
        else:
            print("failed")

    save_metadata(metadata)
    print(f"\nMetadata saved to {METADATA_PATH}")

    # Print summary
    print("\nDB state:")
    for table, info in sorted(db_state.items()):
        season_str = f", max season {info['max_season']}" if info["max_season"] else ""
        print(f"  {table}: {info['row_count']:,} rows{season_str}")


def check_updates():
    """Check for updates and return structured results."""
    metadata = load_metadata()
    results = {"new_data": [], "updated": [], "no_change": [], "errors": []}

    # Scan current DB state
    all_tables = []
    for info in RELEASE_MAP.values():
        all_tables.extend(info["tables"])
    for info in EXTERNAL_SOURCES.values():
        all_tables.extend(info["tables"])

    current_db = scan_db_state(DB_PATH, all_tables)

    # Check each nflverse-data release
    for tag, cfg in RELEASE_MAP.items():
        release_info = get_release_info(tag)
        if release_info is None:
            results["errors"].append({"release": tag, "error": "Failed to fetch"})
            continue

        stored = metadata.get("releases", {}).get(tag, {})
        remote_max = None
        if cfg["year_partitioned"] and cfg["asset_pattern"]:
            remote_max = extract_max_year(
                release_info["assets"], cfg["asset_pattern"]
            )

        # Record latest remote state for this release before evaluating diffs.
        entry = {"updated_at": release_info["updated_at"]}
        if remote_max is not None:
            entry["max_year_file"] = remote_max
        metadata["releases"][tag] = entry

        if cfg["year_partitioned"] and cfg["asset_pattern"]:
            stored_max = stored.get("max_year_file")

            # Also check what the DB has
            db_max_seasons = []
            for table in cfg["tables"]:
                db_info = current_db.get(table, {})
                if db_info.get("max_season"):
                    db_max_seasons.append(db_info["max_season"])

            # For game_stats/season_stats, the DB may have 2025 data from fantasyDB
            # but nflverse data only goes through max_year_file. Compare against stored.
            if remote_max and stored_max and remote_max > stored_max:
                new_years = list(range(stored_max + 1, remote_max + 1))
                results["new_data"].append({
                    "release": tag,
                    "tables": cfg["tables"],
                    "new_years": new_years,
                    "remote_max": remote_max,
                    "stored_max": stored_max,
                    "db_max_seasons": db_max_seasons,
                })
            elif remote_max and not stored_max:
                # First check — treat remote_max as new if DB doesn't have it
                db_max = max(db_max_seasons) if db_max_seasons else None
                if db_max is None or remote_max > db_max:
                    results["new_data"].append({
                        "release": tag,
                        "tables": cfg["tables"],
                        "new_years": [remote_max] if remote_max else [],
                        "remote_max": remote_max,
                        "stored_max": None,
                        "db_max_seasons": db_max_seasons,
                    })
                else:
                    results["no_change"].append({"release": tag})
            else:
                # Also check timestamp changes for in-season updates
                remote_ts = release_info.get("updated_at")
                stored_ts = stored.get("updated_at")
                if remote_ts and stored_ts and remote_ts != stored_ts:
                    results["updated"].append({
                        "release": tag,
                        "tables": cfg["tables"],
                        "remote_updated": remote_ts,
                        "stored_updated": stored_ts,
                    })
                else:
                    results["no_change"].append({"release": tag})
        else:
            # Timestamp-based detection
            remote_ts = release_info.get("updated_at")
            stored_ts = stored.get("updated_at")

            if remote_ts and stored_ts and remote_ts != stored_ts:
                results["updated"].append({
                    "release": tag,
                    "tables": cfg["tables"],
                    "remote_updated": remote_ts,
                    "stored_updated": stored_ts,
                })
            elif not stored_ts:
                # No stored data — first run
                results["updated"].append({
                    "release": tag,
                    "tables": cfg["tables"],
                    "remote_updated": remote_ts,
                    "stored_updated": None,
                })
            else:
                results["no_change"].append({"release": tag})

    # Check external sources
    for name, src in EXTERNAL_SOURCES.items():
        last_mod = http_head(src["url"])
        stored_mod = metadata.get("external", {}).get(name, {}).get("last_modified")

        if last_mod:
            metadata["external"][name] = {"last_modified": last_mod}

        if last_mod and stored_mod and last_mod != stored_mod:
            results["updated"].append({
                "release": name,
                "tables": src["tables"],
                "remote_updated": last_mod,
                "stored_updated": stored_mod,
            })
        elif last_mod and not stored_mod:
            results["updated"].append({
                "release": name,
                "tables": src["tables"],
                "remote_updated": last_mod,
                "stored_updated": None,
            })
        else:
            results["no_change"].append({"release": name})

    metadata["db_state"] = current_db
    save_metadata(metadata)

    return results, current_db


def print_report(results):
    """Print human-readable update report."""
    today = datetime.now().strftime("%Y-%m-%d")
    print(f"=== nflverse Update Check ({today}) ===\n")

    if results["new_data"]:
        print("NEW DATA AVAILABLE:")
        for item in results["new_data"]:
            years_str = ", ".join(str(y) for y in item["new_years"])
            db_max = item["db_max_seasons"]
            db_str = f"DB has nflverse data through {item['stored_max']}" if item["stored_max"] else "no stored metadata"
            if db_max:
                db_str += f" (DB max season: {max(db_max)})"
            print(f"  {item['release']}: year files for {years_str} found ({db_str})")
        print()

    if results["updated"]:
        print("RELEASES UPDATED:")
        for item in results["updated"]:
            remote = item["remote_updated"] or "unknown"
            stored = item["stored_updated"] or "never checked"
            # Truncate to date if ISO format
            if isinstance(remote, str) and "T" in remote:
                remote = remote[:10]
            if isinstance(stored, str) and "T" in stored:
                stored = stored[:10]
            print(f"  {item['release']}: updated {remote} (stored: {stored})")
        print()

    if results["no_change"]:
        print("NO CHANGES:")
        names = ", ".join(item["release"] for item in results["no_change"])
        print(f"  {names}")
        print()

    if results["errors"]:
        print("ERRORS:")
        for item in results["errors"]:
            print(f"  {item['release']}: {item['error']}")
        print()

    # Suggest commands
    new_years = set()
    for item in results["new_data"]:
        new_years.update(item["new_years"])
    updated_tables = []
    for item in results["updated"]:
        updated_tables.extend(item["tables"])

    suggestions = []
    if new_years:
        years_str = " ".join(str(y) for y in sorted(new_years))
        suggestions.append(f"python3 scripts/download.py --years {years_str}")
        suggestions.append(f"python3 scripts/build.py --years {years_str}")
    if updated_tables:
        tables_str = " ".join(updated_tables)
        suggestions.append(f"python3 scripts/download.py --sources {tables_str} --force")
        suggestions.append("python3 scripts/build.py  # full rebuild recommended after source refresh")
    if not suggestions and (results["new_data"] or results["updated"]):
        suggestions.append("python3 scripts/download.py --all && python3 scripts/build.py")

    if suggestions:
        print("Suggested commands:")
        for s in suggestions:
            print(f"  {s}")


def main():
    parser = argparse.ArgumentParser(description="Check for nflverse data updates")
    parser.add_argument(
        "--json", action="store_true", help="Output machine-readable JSON"
    )
    parser.add_argument(
        "--init",
        action="store_true",
        help="Initialize metadata from current DB state",
    )
    args = parser.parse_args()

    if args.init:
        init_metadata()
        return

    results, db_state = check_updates()

    if args.json:
        output = {
            "checked_at": datetime.now(timezone.utc).isoformat(),
            "results": results,
            "db_state": db_state,
        }
        print(json.dumps(output, indent=2))
    else:
        print_report(results)


if __name__ == "__main__":
    main()
