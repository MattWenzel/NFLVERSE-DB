"""Database configuration."""

from pathlib import Path

ROOT = Path(__file__).parent.parent

DB_PATH = ROOT / "data" / "nflverse.db"
PBP_DB_PATH = ROOT / "data" / "pbp.db"
METADATA_PATH = ROOT / "data" / "update_metadata.json"
RAW_DATA_PATH = ROOT / "data" / "raw"
