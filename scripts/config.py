"""Database configuration."""

from pathlib import Path

ROOT = Path(__file__).parent.parent

# v2 DBs use nflverse-native column names (no custom renames)
DB_PATH = ROOT / "data" / "nflverse_v2.db"
PBP_DB_PATH = ROOT / "data" / "pbp_v2.db"
METADATA_PATH = ROOT / "data" / "update_metadata.json"
