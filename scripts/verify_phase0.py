"""Phase 0 verification report (operating rule 2): exit 0 = PASS, 1 = FAIL.

Fails on missing streams, checkpoints short of the target end block, gaps in
retention coverage, or missing curated tables — an empty dataset fails.
See src/normalize/verify_phase0.py for the full list of checks.

Usage:
    uv run python scripts/verify_phase0.py
    uv run python scripts/verify_phase0.py --target-block 95199593
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import _bootstrap
from ingest.contracts import PINNED_END_BLOCK
from normalize.verify_phase0 import run


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--target-block", type=int, default=PINNED_END_BLOCK,
                    help=f"inclusive end block every stream must reach (default: pinned {PINNED_END_BLOCK})")
    ap.add_argument("--data-root", type=Path, default=_bootstrap.DATA_ROOT)
    args = ap.parse_args()
    return run(args.data_root, args.target_block)


if __name__ == "__main__":
    sys.exit(main())
