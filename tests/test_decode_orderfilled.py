"""Decoder tests: fixtures are canonical eth_abi encodings, so the vectorized
polars decoders are checked against the ABI spec, not against themselves."""

import polars as pl

from conftest import v1_order_filled_row, v2_order_filled_row
from ingest.contracts import CTF_EXCHANGE_V1, CTF_EXCHANGE_V2
from normalize.decode import (
    decode_order_filled_v1,
    decode_order_filled_v2,
    norm_token_id_hex,
)

MAKER = "0x1111111111111111111111111111111111111111"
TAKER = "0x2222222222222222222222222222222222222222"
TOKEN = 0xDEADBEEF_00000000_00000000_00000000_00000000_00000000_00000000_12345678


def test_v1_maker_buy():
    # maker pays 600_000 raw collateral (=$0.60) for 1_000_000 raw tokens (1 share)
    row = v1_order_filled_row(
        maker=MAKER,
        taker=TAKER,
        maker_asset_id=0,
        taker_asset_id=TOKEN,
        maker_amount=600_000,
        taker_amount=1_000_000,
        fee=500,
    )
    out = decode_order_filled_v1(pl.DataFrame([row]), "orderfilled_ctf_v1", CTF_EXCHANGE_V1)
    r = out.to_dicts()[0]
    assert r["side"] == "BUY"
    assert r["token_id_hex"] == hex(TOKEN)
    assert r["collateral_amount"] == 600_000
    assert r["token_amount"] == 1_000_000
    assert abs(r["price"] - 0.6) < 1e-12
    assert abs(r["usd_notional"] - 0.6) < 1e-12
    assert r["fee"] == 500
    assert r["maker"] == MAKER
    assert r["taker"] == TAKER
    assert r["generation"] == 1
    assert r["taker_is_exchange"] is False


def test_v1_maker_sell():
    # maker sells 2_000_000 raw tokens for 500_000 raw collateral => price 0.25
    row = v1_order_filled_row(
        maker=MAKER,
        taker=TAKER,
        maker_asset_id=TOKEN,
        taker_asset_id=0,
        maker_amount=2_000_000,
        taker_amount=500_000,
    )
    out = decode_order_filled_v1(pl.DataFrame([row]), "orderfilled_ctf_v1", CTF_EXCHANGE_V1)
    r = out.to_dicts()[0]
    assert r["side"] == "SELL"
    assert r["token_id_hex"] == hex(TOKEN)
    assert r["collateral_amount"] == 500_000
    assert r["token_amount"] == 2_000_000
    assert abs(r["price"] - 0.25) < 1e-12


def test_v1_taker_aggregate_row_flagged():
    row = v1_order_filled_row(
        maker=MAKER,
        taker=CTF_EXCHANGE_V1,
        maker_asset_id=0,
        taker_asset_id=TOKEN,
        maker_amount=100,
        taker_amount=200,
    )
    out = decode_order_filled_v1(pl.DataFrame([row]), "orderfilled_ctf_v1", CTF_EXCHANGE_V1)
    assert out.to_dicts()[0]["taker_is_exchange"] is True


def test_v2_buy_and_sell():
    buy = v2_order_filled_row(
        maker=MAKER,
        taker=TAKER,
        side=0,
        token_id=TOKEN,
        maker_amount=700_000,
        taker_amount=1_000_000,
        fee=42,
        log_index=1,
    )
    sell = v2_order_filled_row(
        maker=MAKER,
        taker=TAKER,
        side=1,
        token_id=TOKEN,
        maker_amount=1_000_000,
        taker_amount=300_000,
        log_index=2,
    )
    out = decode_order_filled_v2(
        pl.DataFrame([buy, sell]), "orderfilled_ctf_v2", CTF_EXCHANGE_V2
    ).sort("log_index")
    b, s = out.to_dicts()
    assert b["side"] == "BUY" and s["side"] == "SELL"
    assert b["token_id_hex"] == s["token_id_hex"] == hex(TOKEN)
    assert b["collateral_amount"] == 700_000 and b["token_amount"] == 1_000_000
    assert abs(b["price"] - 0.7) < 1e-12
    assert s["collateral_amount"] == 300_000 and s["token_amount"] == 1_000_000
    assert abs(s["price"] - 0.3) < 1e-12
    assert b["generation"] == 2
    assert b["fee"] == 42


def test_token_id_join_key_roundtrip():
    # Gamma serves decimal token-id strings; chain topics give hex words.
    dec = str(TOKEN)
    assert norm_token_id_hex(dec) == hex(TOKEN)
    row = v1_order_filled_row(
        maker=MAKER,
        taker=TAKER,
        maker_asset_id=0,
        taker_asset_id=TOKEN,
        maker_amount=1,
        taker_amount=1,
    )
    out = decode_order_filled_v1(pl.DataFrame([row]), "orderfilled_ctf_v1", CTF_EXCHANGE_V1)
    assert out.to_dicts()[0]["token_id_hex"] == norm_token_id_hex(dec)
