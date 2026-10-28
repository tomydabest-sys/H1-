"""Compact raw HyperSync part files into a few large files (rule 7: raw untouched).

The backfill writes one part file per HyperSync batch, which left ~280k small
files. Opening each costs ~23ms here, so polars scans over the raw parts take
hours. Compaction reads the parts in block order with a thread pool (I/O-bound,
~280 files/s) and writes byte-identical rows into
``data/interim/raw_compacted/<stream>/chunk-*.parquet``.

``_manifest.json`` records the exact raw part names that went in. Readers use
the compacted files only when that list equals the current raw part set, and
fall back to the raw parts otherwise. Overlapping part block ranges (the only
way duplicate rows can arise, from a re-scan after a crash) are recorded.
"""

from __future__ import annotations

import json
import os
import shutil
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq


def raw_parts(data_root: Path, stream: str) -> list[Path]:
    # Zero-padded part-<from>-<to> names sort in block order.
    return sorted((data_root / "raw" / "hypersync" / stream).glob("part-*.parquet"))


def compacted_dir(data_root: Path, stream: str) -> Path:
    return data_root / "interim" / "raw_compacted" / stream


def _block_range(p: Path) -> tuple[int, int]:
    _, lo, hi = p.stem.split("-")
    return int(lo), int(hi)


def stream_files(data_root: Path, stream: str) -> list[Path]:
    """Files to read for a stream: compacted chunks if they cover exactly the
    current raw parts, else the raw parts themselves."""
    parts = raw_parts(data_root, stream)
    cdir = compacted_dir(data_root, stream)
    manifest = cdir / "_manifest.json"
    if parts and manifest.exists():
        doc = json.loads(manifest.read_text())
        if doc.get("parts") == [p.name for p in parts]:
            return [cdir / c for c in doc["chunks"]]
    return parts


def iter_windows(files: list[Path], columns: list[str] | None = None, window: int = 2000,
                 workers: int = 16, max_bytes: int = 32 * 2**20, filters=None):
    """Yield polars frames of up to `window` files and `max_bytes` of Parquet
    each (always at least one file), read with a thread pool (~280 files/s here
    vs ~40/s for polars' serial opens). Hex-string columns expand ~40x in
    memory, so the byte cap is what bounds RSS: a 2000-file ctf_events window
    was 9GB resident. `filters` (pyarrow DNF) drops rows per file before concat."""
    import polars as pl

    groups: list[list[Path]] = []
    size = 0
    for f in files:
        b = f.stat().st_size
        if not groups or len(groups[-1]) >= window or size + b > max_bytes:
            groups.append([])
            size = 0
        groups[-1].append(f)
        size += b
    with ThreadPoolExecutor(workers) as ex:
        for group in groups:
            tables = list(ex.map(lambda f: pq.read_table(f, columns=columns, filters=filters), group))
            yield pl.from_arrow(pa.concat_tables(tables))


def compact_stream(
    data_root: Path, stream: str, rows_per_chunk: int = 2_000_000, workers: int = 16
) -> dict:
    parts = raw_parts(data_root, stream)
    names = [p.name for p in parts]
    cdir = compacted_dir(data_root, stream)
    manifest = cdir / "_manifest.json"
    if manifest.exists() and json.loads(manifest.read_text()).get("parts") == names:
        return {"stream": stream, "status": "up_to_date"}

    ranges = [_block_range(p) for p in parts]
    overlaps = [
        [names[i - 1], names[i]] for i in range(1, len(ranges)) if ranges[i][0] <= ranges[i - 1][1]
    ]

    tmp = cdir.with_name(cdir.name + ".tmp")
    shutil.rmtree(tmp, ignore_errors=True)
    tmp.mkdir(parents=True)
    chunks: list[str] = []
    buf: list[pa.Table] = []
    buf_rows = 0
    total = 0

    def flush() -> None:
        nonlocal buf, buf_rows
        if not buf:
            return
        name = f"chunk-{len(chunks):05d}.parquet"
        pq.write_table(pa.concat_tables(buf), tmp / name, compression="zstd", compression_level=3)
        chunks.append(name)
        buf, buf_rows = [], 0

    window = 1000  # bound read-ahead: map() would otherwise queue every part at once
    with ThreadPoolExecutor(workers) as ex:
        for start in range(0, len(parts), window):
            for table in ex.map(pq.read_table, parts[start : start + window]):  # keeps order
                buf.append(table)
                buf_rows += table.num_rows
                total += table.num_rows
                if buf_rows >= rows_per_chunk:
                    flush()
    flush()

    (tmp / "_manifest.json").write_text(
        json.dumps({"parts": names, "chunks": chunks, "rows": total, "overlaps": overlaps})
    )
    shutil.rmtree(cdir, ignore_errors=True)
    os.replace(tmp, cdir)
    return {
        "stream": stream,
        "status": "compacted",
        "parts": len(names),
        "chunks": len(chunks),
        "rows": total,
        "overlaps": len(overlaps),
    }
