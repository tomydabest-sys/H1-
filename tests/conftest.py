"""Shared fixture helpers: construct raw log rows exactly as the backfill writes them."""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from eth_abi import encode as abi_encode

RAW_COLUMNS = [
    "block_number",
    "block_timestamp",
    "tx_hash",
    "log_index",
    "address",
    "topic0",
    "topic1",
    "topic2",
    "topic3",
    "data",
]


def pad_topic_address(addr: str) -> str:
    return "0x" + addr.lower().removeprefix("0x").rjust(64, "0")


def bytes32_hex(label: str) -> str:
    return "0x" + label.encode().hex().rjust(64, "0")


def raw_row(
    *,
    block_number: int,
    block_timestamp: int,
    tx_hash: str,
    log_index: int,
    address: str,
    topic0: str,
    topic1: str,
    topic2: str,
    topic3: str,
    data: str,
) -> dict:
    return {
        "block_number": block_number,
        "block_timestamp": block_timestamp,
        "tx_hash": tx_hash,
        "log_index": log_index,
        "address": address.lower(),
        "topic0": topic0,
        "topic1": topic1,
        "topic2": topic2,
        "topic3": topic3,
        "data": data,
    }


def v1_order_filled_row(
    *,
    maker: str,
    taker: str,
    maker_asset_id: int,
    taker_asset_id: int,
    maker_amount: int,
    taker_amount: int,
    fee: int = 0,
    block_number: int = 100,
    block_timestamp: int = 1_700_000_000,
    tx_hash: str = "0x" + "ab" * 32,
    log_index: int = 0,
    address: str = "0x4bfb41d5b3570defd03c39a9a4d8de6bd8b8982e",
) -> dict:
    from ingest.contracts import TOPIC0_ORDER_FILLED_V1

    data = "0x" + abi_encode(
        ["uint256"] * 5,
        [maker_asset_id, taker_asset_id, maker_amount, taker_amount, fee],
    ).hex()
    return raw_row(
        block_number=block_number,
        block_timestamp=block_timestamp,
        tx_hash=tx_hash,
        log_index=log_index,
        address=address,
        topic0=TOPIC0_ORDER_FILLED_V1,
        topic1=bytes32_hex(f"order-{tx_hash[-4:]}-{log_index}"),
        topic2=pad_topic_address(maker),
        topic3=pad_topic_address(taker),
        data=data,
    )


def v2_order_filled_row(
    *,
    maker: str,
    taker: str,
    side: int,
    token_id: int,
    maker_amount: int,
    taker_amount: int,
    fee: int = 0,
    block_number: int = 90_000_000,
    block_timestamp: int = 1_780_000_000,
    tx_hash: str = "0x" + "cd" * 32,
    log_index: int = 0,
    address: str = "0xe111180000d2663c0091e4f400237545b87b996b",
) -> dict:
    from ingest.contracts import TOPIC0_ORDER_FILLED_V2

    data = "0x" + abi_encode(
        ["uint8", "uint256", "uint256", "uint256", "uint256", "bytes32", "bytes32"],
        [side, token_id, maker_amount, taker_amount, fee, b"\x00" * 32, b"\x00" * 32],
    ).hex()
    return raw_row(
        block_number=block_number,
        block_timestamp=block_timestamp,
        tx_hash=tx_hash,
        log_index=log_index,
        address=address,
        topic0=TOPIC0_ORDER_FILLED_V2,
        topic1=bytes32_hex(f"order-{tx_hash[-4:]}-{log_index}"),
        topic2=pad_topic_address(maker),
        topic3=pad_topic_address(taker),
        data=data,
    )


def condition_resolution_row(
    *,
    condition_id: str,
    oracle: str,
    payout_numerators: list[int],
    block_number: int = 50_000_000,
    block_timestamp: int = 1_750_000_000,
    tx_hash: str = "0x" + "ef" * 32,
    log_index: int = 0,
) -> dict:
    from ingest.contracts import CTF_TOPIC0

    data = "0x" + abi_encode(
        ["uint256", "uint256[]"], [len(payout_numerators), payout_numerators]
    ).hex()
    return raw_row(
        block_number=block_number,
        block_timestamp=block_timestamp,
        tx_hash=tx_hash,
        log_index=log_index,
        address="0x4d97dcd97ec945f40cf65f87097ace5ea0476045",
        topic0=CTF_TOPIC0["ConditionResolution"],
        topic1=condition_id,
        topic2=pad_topic_address(oracle),
        topic3=bytes32_hex("question"),
        data=data,
    )


def write_raw_part(data_root: Path, stream: str, rows: list[dict], part_name: str = "part-0000000001-0000000999.parquet") -> Path:
    d = data_root / "raw" / "hypersync" / stream
    d.mkdir(parents=True, exist_ok=True)
    path = d / part_name
    pq.write_table(pa.Table.from_pylist(rows), path)
    return path


@pytest.fixture
def data_root(tmp_path: Path) -> Path:
    root = tmp_path / "data"
    root.mkdir()
    return root
