"""Compact raw HyperSync part files into data/interim/raw_compacted (raw untouched).

Re-runnable: a stream whose manifest already lists the current raw parts is
skipped. build_curated and verify_phase0 read the compacted files only when
they match the raw part set exactly.

Usage:
    uv run python scripts/compact_raw.py
"""

import _bootstrap
from ingest.contracts import STREAMS
from normalize.compact import compact_stream


def main() -> None:
    for name in STREAMS:
        print(compact_stream(_bootstrap.DATA_ROOT, name), flush=True)


if __name__ == "__main__":
    main()
