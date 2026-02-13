"""Database configuration."""

from pathlib import Path

# v2 DBs use nflverse-native column names (no custom renames)
DB_PATH = Path(__file__).parent / "nflverse_v2.db"
PBP_DB_PATH = Path(__file__).parent / "pbp_v2.db"
METADATA_PATH = Path(__file__).parent / "update_metadata.json"
