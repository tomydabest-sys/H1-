"""Pull a full Gamma market-metadata snapshot via keyset pagination (resumable).

Usage:
    uv run python scripts/backfill_gamma.py
    uv run python scripts/backfill_gamma.py --snapshot 2026-07-14 --max-pages 5
"""

import argparse

import _bootstrap
from ingest.gamma_markets import run_snapshot


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--snapshot", default=None, help="snapshot label (default: today UTC)")
    ap.add_argument("--max-pages", type=int, default=None, help="stop after N pages (resumable)")
    args = ap.parse_args()

    stats = run_snapshot(
        _bootstrap.DATA_ROOT, snapshot=args.snapshot, max_pages=args.max_pages
    )
    print(stats)


if __name__ == "__main__":
    main()
