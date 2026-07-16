"""Decode raw events, dedup, join with Gamma metadata, write curated Parquet tables.

Usage:
    uv run python scripts/build_curated.py
    uv run python scripts/build_curated.py --no-trades   # skip the fill-level sink
                                                          # (daily_bars only) when disk is tight
"""

import argparse

import _bootstrap
from normalize.build_curated import build


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--no-trades", action="store_true", help="skip curated/trades.parquet")
    args = ap.parse_args()
    stats = build(_bootstrap.DATA_ROOT, write_trades=not args.no_trades)
    for k, v in stats.items():
        print(f"{k}: {v}")


if __name__ == "__main__":
    main()
