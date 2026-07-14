"""ConditionResolution / ConditionPreparation decoding."""

import polars as pl
from eth_abi import encode as abi_encode

from conftest import condition_resolution_row, pad_topic_address, raw_row
from ingest.contracts import CTF_TOPIC0
from normalize.decode import decode_condition_preparations, decode_condition_resolutions

COND = "0x" + "c1".rjust(64, "0")
ORACLE = "0x3333333333333333333333333333333333333333"


def test_condition_resolution_binary_yes():
    row = condition_resolution_row(
        condition_id=COND, oracle=ORACLE, payout_numerators=[1, 0]
    )
    out = decode_condition_resolutions(pl.DataFrame([row]))
    r = out.to_dicts()[0]
    assert r["condition_id"] == COND
    assert r["oracle"] == ORACLE
    assert r["outcome_slot_count"] == 2
    assert r["payout_numerators"] == [1, 0]
    assert r["resolved_timestamp"] == 1_750_000_000


def test_condition_resolution_ignores_other_events():
    row = condition_resolution_row(
        condition_id=COND, oracle=ORACLE, payout_numerators=[0, 1]
    )
    other = dict(row, topic0=CTF_TOPIC0["PositionSplit"], log_index=7)
    out = decode_condition_resolutions(pl.DataFrame([row, other]))
    assert out.height == 1


def test_condition_preparation():
    data = "0x" + abi_encode(["uint256"], [2]).hex()
    row = raw_row(
        block_number=10,
        block_timestamp=1_600_000_000,
        tx_hash="0x" + "aa" * 32,
        log_index=3,
        address="0x4d97dcd97ec945f40cf65f87097ace5ea0476045",
        topic0=CTF_TOPIC0["ConditionPreparation"],
        topic1=COND,
        topic2=pad_topic_address(ORACLE),
        topic3="0x" + "q1".encode().hex().rjust(64, "0"),
        data=data,
    )
    out = decode_condition_preparations(pl.DataFrame([row]))
    r = out.to_dicts()[0]
    assert r["condition_id"] == COND
    assert r["outcome_slot_count"] == 2
    assert r["prepared_timestamp"] == 1_600_000_000
