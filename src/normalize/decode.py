"""Decode raw HyperSync logs into a unified labeled-trade schema.

V1 OrderFilled (8 params, topic0 0xd0a08e8c…):
    topics: 1=orderHash, 2=maker, 3=taker
    data words: 0=makerAssetId, 1=takerAssetId, 2=makerAmountFilled,
                3=takerAmountFilled, 4=fee
    Side is derived: makerAssetId == 0 means the maker paid collateral, i.e.
    the maker BOUGHT takerAssetId (price = makerAmountFilled/takerAmountFilled).
    Otherwise the maker SOLD makerAssetId (price = takerAmountFilled/makerAmountFilled).

V2 OrderFilled (10 params, explicit side):
    topics: 1=orderHash, 2=maker, 3=taker
    data words: 0=side (uint8, BUY=0/SELL=1), 1=tokenId, 2=makerAmountFilled,
                3=takerAmountFilled, 4=fee, 5=builder (bytes32), 6=metadata (bytes32)
    Mapping implemented as: side is the MAKER order's side; BUY ⇒ maker pays
    collateral (makerAmountFilled) for tokenId (takerAmountFilled).
    ⚠ VERIFY-AT-SMOKE-RUN: this maker-perspective reading must be confirmed
    against real fills (all prices must land in (0,1); spot-check one tx on
    Polygonscan). A flipped reading inverts prices, which the verification
    script's price-range check would catch.

Collateral units: V1 settles in USDC.e (6 decimals). V2 settles in the wrapped
collateral introduced at the migration (pUSD/PMCT, a USDC wrapper — assumed
6 decimals). ⚠ VERIFY-AT-SMOKE-RUN: the V2 collateral token's decimals() must
be confirmed as 6 before trusting usd_notional on the V2 side.

Both rows of a match: each match emits one OrderFilled per maker order plus an
aggregate row for the taker order, whose recorded counterparty is the exchange
itself. Rows are flagged with taker_is_exchange rather than dropped, so the
maker/taker asymmetry analysis (Phase 1) can choose its own treatment.
"""

from __future__ import annotations

import polars as pl
from eth_abi import decode as abi_decode

from ingest.contracts import CTF_TOPIC0

WORD = 64  # hex chars per ABI word

TRADE_COLUMNS = [
    "block_number",
    "block_timestamp",
    "tx_hash",
    "log_index",
    "exchange",
    "generation",
    "order_hash",
    "maker",
    "taker",
    "side",
    "token_id_hex",
    "token_amount",
    "collateral_amount",
    "fee",
    "price",
    "usd_notional",
    "taker_is_exchange",
]


def _word(i: int) -> pl.Expr:
    """i-th 32-byte word of the (0x-prefixed) data column, as 64 hex chars."""
    return pl.col("data").str.slice(2 + i * WORD, WORD)


def _word_int(i: int) -> pl.Expr:
    return _word(i).str.to_integer(base=16, strict=False)


def _word_is_zero(i: int) -> pl.Expr:
    return _word(i).str.strip_chars_start("0") == ""


def _word_hex_norm(i: int) -> pl.Expr:
    """Word as minimal 0x-hex (leading zeros stripped) for token-id join keys."""
    stripped = _word(i).str.strip_chars_start("0")
    return (
        pl.when(stripped == "")
        .then(pl.lit("0x0"))
        .otherwise(pl.concat_str([pl.lit("0x"), stripped]))
    )


def _topic_addr(name: str) -> pl.Expr:
    """Address from a 32-byte topic (last 20 bytes), lowercased 0x-hex."""
    return pl.concat_str([pl.lit("0x"), pl.col(name).str.slice(2 + 24, 40)]).str.to_lowercase()


def norm_token_id_hex(decimal_str: str) -> str:
    """Decimal token id (Gamma clobTokenIds) -> minimal lowercase 0x-hex join key."""
    return hex(int(decimal_str))


def _finish_trades(df: "pl.DataFrame | pl.LazyFrame", exchange: str, generation: int, exchange_address: str):
    return df.with_columns(
        exchange=pl.lit(exchange),
        generation=pl.lit(generation, dtype=pl.Int8),
        price=pl.col("collateral_amount") / pl.col("token_amount"),
        usd_notional=pl.col("collateral_amount") / 1_000_000,
        taker_is_exchange=pl.col("taker") == pl.lit(exchange_address.lower()),
    ).select(TRADE_COLUMNS)


def decode_order_filled_v1(raw: "pl.DataFrame | pl.LazyFrame", exchange: str, exchange_address: str):
    """Works on eager or lazy frames; pass a scan for memory-safe streaming."""
    maker_is_buy = _word_is_zero(0)
    df = raw.select(
        pl.col("block_number"),
        pl.col("block_timestamp"),
        pl.col("tx_hash"),
        pl.col("log_index"),
        order_hash=pl.col("topic1"),
        maker=_topic_addr("topic2"),
        taker=_topic_addr("topic3"),
        side=pl.when(maker_is_buy).then(pl.lit("BUY")).otherwise(pl.lit("SELL")),
        token_id_hex=pl.when(maker_is_buy).then(_word_hex_norm(1)).otherwise(_word_hex_norm(0)),
        collateral_amount=pl.when(maker_is_buy).then(_word_int(2)).otherwise(_word_int(3)),
        token_amount=pl.when(maker_is_buy).then(_word_int(3)).otherwise(_word_int(2)),
        fee=_word_int(4),
    )
    return _finish_trades(df, exchange, 1, exchange_address)


def decode_order_filled_v2(raw: "pl.DataFrame | pl.LazyFrame", exchange: str, exchange_address: str):
    """Works on eager or lazy frames; pass a scan for memory-safe streaming."""
    maker_is_buy = _word_int(0) == 0
    df = raw.select(
        pl.col("block_number"),
        pl.col("block_timestamp"),
        pl.col("tx_hash"),
        pl.col("log_index"),
        order_hash=pl.col("topic1"),
        maker=_topic_addr("topic2"),
        taker=_topic_addr("topic3"),
        side=pl.when(maker_is_buy).then(pl.lit("BUY")).otherwise(pl.lit("SELL")),
        token_id_hex=_word_hex_norm(1),
        collateral_amount=pl.when(maker_is_buy).then(_word_int(2)).otherwise(_word_int(3)),
        token_amount=pl.when(maker_is_buy).then(_word_int(3)).otherwise(_word_int(2)),
        fee=_word_int(4),
    )
    return _finish_trades(df, exchange, 2, exchange_address)


def decode_condition_resolutions(raw: pl.DataFrame) -> pl.DataFrame:
    """ConditionResolution rows -> resolutions table (authoritative on-chain outcome).

    topics: 1=conditionId, 2=oracle, 3=questionId
    data: (uint256 outcomeSlotCount, uint256[] payoutNumerators)
    """
    topic0 = CTF_TOPIC0["ConditionResolution"]
    sub = raw.filter(pl.col("topic0") == topic0)
    rows = []
    for r in sub.iter_rows(named=True):
        slot_count, numerators = abi_decode(
            ["uint256", "uint256[]"], bytes.fromhex(r["data"][2:])
        )
        rows.append(
            {
                "condition_id": r["topic1"].lower(),
                "oracle": "0x" + r["topic2"][2 + 24 :].lower(),
                "question_id": r["topic3"].lower(),
                "outcome_slot_count": int(slot_count),
                "payout_numerators": [int(x) for x in numerators],
                "resolved_block": r["block_number"],
                "resolved_timestamp": r["block_timestamp"],
                "tx_hash": r["tx_hash"],
                "log_index": r["log_index"],
            }
        )
    return pl.DataFrame(
        rows,
        schema={
            "condition_id": pl.Utf8,
            "oracle": pl.Utf8,
            "question_id": pl.Utf8,
            "outcome_slot_count": pl.Int64,
            "payout_numerators": pl.List(pl.Int64),
            "resolved_block": pl.Int64,
            "resolved_timestamp": pl.Int64,
            "tx_hash": pl.Utf8,
            "log_index": pl.Int64,
        },
    )


def decode_condition_preparations(raw: pl.DataFrame) -> pl.DataFrame:
    """ConditionPreparation rows -> condition creation table (leakage-safe start time)."""
    topic0 = CTF_TOPIC0["ConditionPreparation"]
    sub = raw.filter(pl.col("topic0") == topic0)
    rows = []
    for r in sub.iter_rows(named=True):
        (slot_count,) = abi_decode(["uint256"], bytes.fromhex(r["data"][2:]))
        rows.append(
            {
                "condition_id": r["topic1"].lower(),
                "oracle": "0x" + r["topic2"][2 + 24 :].lower(),
                "question_id": r["topic3"].lower(),
                "outcome_slot_count": int(slot_count),
                "prepared_block": r["block_number"],
                "prepared_timestamp": r["block_timestamp"],
            }
        )
    return pl.DataFrame(
        rows,
        schema={
            "condition_id": pl.Utf8,
            "oracle": pl.Utf8,
            "question_id": pl.Utf8,
            "outcome_slot_count": pl.Int64,
            "prepared_block": pl.Int64,
            "prepared_timestamp": pl.Int64,
        },
    )
