"""Compaction: byte-identical rows, overlap detection, raw fallback when stale."""

import polars as pl

from conftest import v1_order_filled_row, write_raw_part
from normalize.compact import compact_stream, iter_windows, raw_parts, stream_files

S = "orderfilled_ctf_v1"


def _row(block, idx):
    return v1_order_filled_row(maker="0x" + "11" * 20, taker="0x" + "22" * 20,
                               maker_asset_id=0, taker_asset_id=7, maker_amount=1,
                               taker_amount=2, block_number=block, log_index=idx,
                               tx_hash=f"0x{block:064x}")


def test_compact_roundtrip_overlaps_and_fallback(data_root):
    write_raw_part(data_root, S, [_row(10, 0), _row(11, 0)], "part-0000000010-0000000019.parquet")
    write_raw_part(data_root, S, [_row(20, 0)], "part-0000000020-0000000029.parquet")
    write_raw_part(data_root, S, [_row(25, 0)], "part-0000000025-0000000030.parquet")  # overlaps

    raw = pl.read_parquet(raw_parts(data_root, S)).sort("block_number", "log_index")
    stats = compact_stream(data_root, S, rows_per_chunk=2, workers=2)
    assert stats["rows"] == 4 and stats["chunks"] == 2 and stats["overlaps"] == 1
    files = stream_files(data_root, S)
    assert all("raw_compacted" in str(f) for f in files)
    assert pl.read_parquet(files).sort("block_number", "log_index").equals(raw)

    assert compact_stream(data_root, S)["status"] == "up_to_date"

    # A new raw part makes the compacted set stale: readers fall back to raw.
    write_raw_part(data_root, S, [_row(40, 0)], "part-0000000040-0000000049.parquet")
    assert stream_files(data_root, S) == raw_parts(data_root, S)


def test_iter_windows_byte_cap_and_row_filter(data_root):
    for b in (10, 20, 30):
        write_raw_part(data_root, S, [_row(b, 0), _row(b, 1)], f"part-{b:010d}-{b + 9:010d}.parquet")
    parts = raw_parts(data_root, S)
    ref = pl.read_parquet(parts)
    # byte cap below one file: every window still gets exactly one file, in order
    wins = list(iter_windows(parts, max_bytes=1, workers=2))
    assert [w.height for w in wins] == [2, 2, 2]
    assert pl.concat(wins).equals(ref)
    # row filter applied per file before concat
    got = pl.concat(list(iter_windows(parts, filters=[("log_index", "=", 1)], workers=2)))
    assert got.equals(ref.filter(pl.col("log_index") == 1))
