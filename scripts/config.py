"""Database configuration."""

from pathlib import Path

ROOT = Path(__file__).parent.parent

DB_PATH = ROOT / "data" / "nflverse.duckdb"
SQLITE_DB_PATH = ROOT / "data" / "nflverse.sqlite"
METADATA_PATH = ROOT / "data" / "update_metadata.json"
RAW_DATA_PATH = ROOT / "data" / "raw"

# First year of available data for each year-partitioned table.
# Shared path constants; referenced by catalog/download/loaders/build etc.
YEAR_RANGE_START = {
    "game_stats": 1999,
    "season_stats": 1999,
    "games": 1999,
    "snap_counts": 2012,
    "depth_charts": 2001,
    "play_by_play": 1999,
}

# depth_charts schema changed in 2025 — pre-2025 lives in `depth_charts`,
# 2025+ lives in `depth_charts_2025` (handled as full_replace).
DEPTH_CHARTS_LEGACY_END = 2025  # exclusive upper bound for the old table
