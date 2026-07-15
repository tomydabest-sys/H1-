"""Politics-only retention: row filters and the retention manifest."""

import asyncio
import json

import polars as pl

from conftest import condition_resolution_row, raw_row, v1_order_filled_row, v2_order_filled_row
from ingest import hypersync_backfill as hb
from ingest.contracts import CTF_TOPIC0
from ingest.filters import ctf_row_filter, load_politics_sets, orderfilled_row_filter
from test_backfill_checkpoint import RESPONSES, SPEC, FakeClient

POL_TOKEN = 0xABC123
OTHER_TOKEN = 0xDEF456
POL_COND = "0x" + "c1".rjust(64, "0")
OTHER_COND = "0x" + "c2".rjust(64, "0")
MAKER = "0x1111111111111111111111111111111111111111"
TAKER = "0x2222222222222222222222222222222222222222"


def test_v1_filter_buy_and_sell_paths():
    keep = orderfilled_row_filter(1, {hex(POL_TOKEN)})
    buy_pol = v1_order_filled_row(maker=MAKER, taker=TAKER, maker_asset_id=0,
                                  taker_asset_id=POL_TOKEN, maker_amount=1, taker_amount=1)
    sell_pol = v1_order_filled_row(maker=MAKER, taker=TAKER, maker_asset_id=POL_TOKEN,
                                   taker_asset_id=0, maker_amount=1, taker_amount=1)
    buy_other = v1_order_filled_row(maker=MAKER, taker=TAKER, maker_asset_id=0,
                                    taker_asset_id=OTHER_TOKEN, maker_amount=1, taker_amount=1)
    assert keep(buy_pol) and keep(sell_pol)
    assert not keep(buy_other)


def test_v2_filter():
    keep = orderfilled_row_filter(2, {hex(POL_TOKEN)})
    pol = v2_order_filled_row(maker=MAKER, taker=TAKER, side=0, token_id=POL_TOKEN,
                              maker_amount=1, taker_amount=1)
    other = v2_order_filled_row(maker=MAKER, taker=TAKER, side=1, token_id=OTHER_TOKEN,
                                maker_amount=1, taker_amount=1)
    assert keep(pol)
    assert not keep(other)


def test_ctf_filter():
    keep = ctf_row_filter({POL_COND})
    resolution_other = condition_resolution_row(condition_id=OTHER_COND, oracle=MAKER,
                                                payout_numerators=[1, 0])
    assert keep(resolution_other)  # resolutions always kept, even non-politics

    def split(cond):
        return raw_row(block_number=1, block_timestamp=1, tx_hash="0x" + "11" * 32,
                       log_index=0, address="0x" + "22" * 20,
                       topic0=CTF_TOPIC0["PositionSplit"], topic1="0x" + "33" * 32,
                       topic2="0x" + "00" * 32, topic3=cond, data="0x")

    assert keep(split(POL_COND))
    assert not keep(split(OTHER_COND))

    def redemption(cond):
        return raw_row(block_number=1, block_timestamp=1, tx_hash="0x" + "11" * 32,
                       log_index=0, address="0x" + "22" * 20,
                       topic0=CTF_TOPIC0["PayoutRedemption"], topic1="0x" + "33" * 32,
                       topic2="0x" + "44" * 32, topic3="0x" + "00" * 32,
                       data=cond + "00" * 64)  # conditionId is data word 0

    assert keep(redemption(POL_COND))
    assert not keep(redemption(OTHER_COND))


def test_load_politics_sets(tmp_path):
    pl.DataFrame(
        {
            "is_politics": [True, False, True],
            "condition_id": [POL_COND, OTHER_COND, ""],
            "clob_token_ids": [[str(POL_TOKEN)], [str(OTHER_TOKEN)], []],
        }
    ).write_parquet(tmp_path / "markets.parquet")
    tokens, conditions = load_politics_sets(tmp_path / "markets.parquet")
    assert tokens == {hex(POL_TOKEN)}
    assert conditions == {POL_COND}  # empty condition ids excluded


def test_retention_manifest_written(data_root):
    client = FakeClient(RESPONSES, height=10_000)
    asyncio.run(hb.run_stream(SPEC, data_root=data_root, client=client, to_block=300,
                              log_every=0, row_filter=lambda r: False))
    sdir = hb.stream_dir(data_root, SPEC.name)
    doc = json.loads((sdir / "_retention.json").read_text())
    assert doc["segments"] == [
        {
            "mode": "politics_only",
            "from_block": 100,
            "to_block": 300,
            "recorded_at": doc["segments"][0]["recorded_at"],
        }
    ]
    # filter dropped everything: checkpoint advanced, no parts written
    assert hb.load_checkpoint(sdir) == 300
    assert not list(sdir.glob("part-*.parquet"))
