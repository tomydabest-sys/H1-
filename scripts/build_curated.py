"""Decode raw events, dedup, join with Gamma metadata, write curated Parquet tables.

Usage:
    uv run python scripts/build_curated.py
"""

import _bootstrap
from normalize.build_curated import build


def main() -> None:
    stats = build(_bootstrap.DATA_ROOT)
    for k, v in stats.items():
        print(f"{k}: {v}")


if __name__ == "__main__":
    main()
