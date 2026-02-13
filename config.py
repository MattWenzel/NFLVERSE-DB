"""Database configuration."""

from pathlib import Path

# Use custom DB for edits, keeps original nflverse.db clean
DB_PATH = Path(__file__).parent / "nflverse_custom.db"
PBP_DB_PATH = Path(__file__).parent / "pbp.db"
