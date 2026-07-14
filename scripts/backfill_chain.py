"""Backfill on-chain event streams via HyperSync (resumable; safe to kill/re-run).

Usage:
    uv run python scripts/backfill_chain.py --stream all
    uv run python scripts/backfill_chain.py --stream orderfilled_ctf_v2 --to-block 85000000
"""

import argparse
import asyncio

import _bootstrap
from ingest.contracts import STREAMS
from ingest.hypersync_backfill import run_streams


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--stream",
        default="all",
        choices=["all", *STREAMS],
        help="stream name or 'all'",
    )
    ap.add_argument("--to-block", type=int, default=None, help="inclusive end block (default: tip minus confirmations)")
    args = ap.parse_args()

    names = list(STREAMS) if args.stream == "all" else [args.stream]
    results = asyncio.run(
        run_streams(names, data_root=_bootstrap.DATA_ROOT, to_block=args.to_block)
    )
    for r in results:
        print(
            f"{r.stream}: +{r.events} events in {r.parts_written} parts, "
            f"checkpoint at block {r.last_committed_block}"
        )


if __name__ == "__main__":
    main()
