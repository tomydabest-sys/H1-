"""Put src/ on sys.path and load .env; imported by every script in this directory."""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from dotenv import load_dotenv  # noqa: E402

load_dotenv(ROOT / ".env")

DATA_ROOT = ROOT / "data"
