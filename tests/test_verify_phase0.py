"""verify_phase0 must report a real pass/fail (rule 2): empty or incomplete data FAILS."""

import json

import polars as pl
import pytest

from conftest import v1_order_filled_row, v2_order_filled_row, write_raw_part
from ingest.contracts import MIGRATION_TS, STREAMS
from normalize.verify_phase0 import coverage_gaps, run

TARGET = 95_000_000
MAKER = "0x1111111111111111111111111111111111111111"
TAKER = "0x2222222222222222222222222222222222222222"
TOKEN = 111_222_333


def _write_stream(data_root, name, segments, checkpoint):
    spec = STREAMS[name]
    if spec.generation == 2:
        row = v2_order_filled_row(maker=MAKER, taker=TAKER, side=0, token_id=TOKEN,
                                  maker_amount=10, taker_amount=20,
                                  block_timestamp=MIGRATION_TS + 10)
    else:
        row = v1_order_filled_row(maker=MAKER, taker=TAKER, maker_asset_id=0,
                                  taker_asset_id=TOKEN, maker_amount=10, taker_amount=20,
                                  block_timestamp=MIGRATION_TS - 10)
    write_raw_part(data_root, name, [row])
    sdir = data_root / "raw" / "hypersync" / name
    (sdir / "_checkpoint.json").write_text(json.dumps({"last_committed_block": checkpoint}))
    (sdir / "_retention.json").write_text(json.dumps({"segments": [
        {"mode": m, "from_block": lo, "to_block": hi} for m, lo, hi in segments
    ]}))


def _write_curated(data_root):
    c = data_root / "curated"
    pl.DataFrame({"is_politics": [True, False], "cohort": ["resolved_onchain", "open"],
                  "condition_id": ["0xc1", "0xc2"]}).write_parquet(c / "markets.parquet")
    pl.DataFrame({"condition_id": ["0xc1"], "payout_numerators": [[1, 0]]}).write_parquet(
        c / "resolutions.parquet")
    pl.DataFrame({
        "token_id_hex": [hex(TOKEN)], "date": ["2026-05-01"], "n_fills": [1],
        "usd_volume": [10.0], "token_volume": [20.0], "vwap": [0.5], "last_price": [0.5],
        "is_politics": [True], "condition_id": ["0xc1"],
    }).write_parquet(c / "daily_bars.parquet")


@pytest.fixture
def complete(data_root):
    (data_root / "curated").mkdir()
    for name, spec in STREAMS.items():
        _write_stream(data_root, name, [("politics_only", spec.from_block, TARGET)], TARGET)
    _write_curated(data_root)
    return data_root


def test_empty_dataset_fails(data_root, capsys):
    assert run(data_root, TARGET) == 1
    assert "FAIL" in capsys.readouterr().out


def test_complete_dataset_passes(complete):
    assert run(complete, TARGET) == 0


def test_checkpoint_short_of_target_fails(complete):
    name = "orderfilled_ctf_v2"
    lo = STREAMS[name].from_block
    _write_stream(complete, name, [("politics_only", lo, TARGET - 1000)], TARGET - 1000)
    assert run(complete, TARGET) == 1


def test_retention_gap_fails(complete):
    # checkpoint reached the target, but a middle range was never recorded
    name = "ctf_events"
    lo = STREAMS[name].from_block
    _write_stream(complete, name, [("politics_only", lo, 50_000_000),
                                   ("politics_only", 50_000_101, TARGET)], TARGET)
    assert run(complete, TARGET) == 1


def test_missing_curated_table_fails(complete):
    (complete / "curated" / "resolutions.parquet").unlink()
    assert run(complete, TARGET) == 1


def test_coverage_gaps_merges_adjacent_and_overlapping():
    segs = [{"mode": "politics_only", "from_block": 10, "to_block": 20},
            {"mode": "full", "from_block": 21, "to_block": 30},
            {"mode": "politics_only", "from_block": 25, "to_block": 40},
            {"mode": "politics_only", "from_block": 50, "to_block": 60}]
    assert coverage_gaps(segs, 10, 60) == [(41, 49)]
    assert coverage_gaps(segs, 5, 70) == [(5, 9), (41, 49), (61, 70)]
    assert coverage_gaps([], 1, 3) == [(1, 3)]
