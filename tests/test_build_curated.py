"""End-to-end normalize: dedup, metadata join, politics flag, survivorship cohorts."""

import json

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

from conftest import condition_resolution_row, v1_order_filled_row, write_raw_part
from normalize.build_curated import build, extract_market_row

MAKER = "0x1111111111111111111111111111111111111111"
TAKER = "0x2222222222222222222222222222222222222222"
TOKEN_POLITICS_YES = 111_222_333_444_555_666_777
TOKEN_POLITICS_NO = 111_222_333_444_555_666_778
TOKEN_UNLISTED = 999_888_777_666_555
COND_POLITICS = "0x" + "c1".rjust(64, "0")


def gamma_market(mid: int, condition_id: str, token_ids: list[int], event_id: int, closed: bool) -> dict:
    # Mirrors the live keyset payload: the embedded event has an id but NO tags.
    return {
        "id": mid,
        "question": f"market {mid}?",
        "slug": f"market-{mid}",
        "conditionId": condition_id,
        "createdAt": "2025-01-01T00:00:00Z",
        "closed": closed,
        "clobTokenIds": json.dumps([str(t) for t in token_ids]),
        "outcomes": json.dumps(["Yes", "No"]),
        "events": [{"id": event_id, "slug": f"event-{event_id}"}],
        "negRisk": False,
    }


def gamma_event(eid: int, tags: list[str]) -> dict:
    return {"id": eid, "title": f"event {eid}", "tags": [{"slug": t} for t in tags]}


def _write_snapshot(data_root, endpoint: str, items: list[dict]) -> None:
    from ingest.gamma_markets import PAGE_SCHEMA

    sdir = data_root / "raw" / "gamma" / endpoint / "snapshot=2026-07-01"
    sdir.mkdir(parents=True)
    rows = [
        {
            "market_id": str(m["id"]),
            "raw_json": json.dumps(m),
            "page_index": 0,
            "fetched_at": "2026-07-01T00:00:00Z",
        }
        for m in items
    ]
    pq.write_table(pa.Table.from_pylist(rows, schema=PAGE_SCHEMA), sdir / "page-00000.parquet")
    (sdir / "_COMPLETE").write_text("{}")


def write_gamma_snapshot(data_root, markets: list[dict], events: list[dict]) -> None:
    # both cohorts are mandatory; split fixture markets by their closed flag
    _write_snapshot(data_root, "markets_open", [m for m in markets if not m["closed"]])
    _write_snapshot(data_root, "markets_closed", [m for m in markets if m["closed"]])
    _write_snapshot(data_root, "events", events)


def test_build_dedup_join_cohorts(data_root):
    trade_kwargs = dict(
        maker=MAKER,
        taker=TAKER,
        maker_asset_id=0,
        taker_asset_id=TOKEN_POLITICS_YES,
        maker_amount=600_000,
        taker_amount=1_000_000,
    )
    rows = [
        v1_order_filled_row(**trade_kwargs, log_index=1),
        v1_order_filled_row(**trade_kwargs, log_index=1),  # exact duplicate (re-scan overlap)
        v1_order_filled_row(
            maker=MAKER,
            taker=TAKER,
            maker_asset_id=TOKEN_UNLISTED,
            taker_asset_id=0,
            maker_amount=1_000_000,
            taker_amount=400_000,
            log_index=2,
        ),
    ]
    write_raw_part(data_root, "orderfilled_ctf_v1", rows)
    write_raw_part(
        data_root,
        "ctf_events",
        [
            condition_resolution_row(
                condition_id=COND_POLITICS,
                oracle="0x3333333333333333333333333333333333333333",
                payout_numerators=[1, 0],
            )
        ],
    )
    write_gamma_snapshot(
        data_root,
        markets=[
            gamma_market(1, COND_POLITICS, [TOKEN_POLITICS_YES, TOKEN_POLITICS_NO], event_id=11, closed=True),
            gamma_market(2, "0x" + "c2".rjust(64, "0"), [123, 124], event_id=22, closed=False),
            dict(gamma_market(3, "", [321, 322], event_id=33, closed=True), conditionId=None),
        ],
        events=[
            gamma_event(11, ["us-politics"]),
            gamma_event(22, ["sports"]),
            gamma_event(33, ["crypto"]),
        ],
    )

    stats = build(data_root)
    assert stats["trades"] == 2  # duplicate collapsed
    assert stats["politics_markets"] == 1
    assert stats["chain_only_trades"] == 1
    assert stats["resolutions"] == 1

    trades = pl.read_parquet(data_root / "curated" / "trades.parquet").sort("log_index")
    matched, unlisted = trades.to_dicts()
    assert matched["market_id"] == "1"
    assert matched["is_politics"] is True
    assert matched["cohort"] == "matched"
    assert matched["outcome_label"] == "Yes"
    assert unlisted["market_id"] is None
    assert unlisted["cohort"] == "chain_only"
    assert unlisted["is_politics"] is False

    markets = pl.read_parquet(data_root / "curated" / "markets.parquet").sort("market_id")
    m1, m2, m3 = markets.to_dicts()
    assert m1["cohort"] == "resolved_onchain"  # has on-chain ConditionResolution
    assert m2["cohort"] == "open"  # unresolved & not closed: survivorship cohort kept
    # empty conditionId must never be classified resolved_onchain (regression)
    assert m3["cohort"] == "closed_unresolved"

    res = pl.read_parquet(data_root / "curated" / "resolutions.parquet")
    assert res.to_dicts()[0]["payout_numerators"] == [1, 0]

    bars = pl.read_parquet(data_root / "curated" / "daily_bars.parquet")
    b = bars.filter(pl.col("market_id") == "1").to_dicts()[0]
    assert b["n_fills"] == 1
    assert abs(b["usd_volume"] - 0.6) < 1e-9
    assert abs(b["vwap"] - 0.6) < 1e-9
    assert abs(b["last_price"] - 0.6) < 1e-9
    assert b["is_politics"] is True
    assert b["taker_buy_usd"] == 0.0  # no taker-aggregate rows in fixture


def test_build_without_trades_sink(data_root):
    write_raw_part(
        data_root,
        "orderfilled_ctf_v1",
        [
            v1_order_filled_row(
                maker=MAKER, taker=TAKER, maker_asset_id=0,
                taker_asset_id=TOKEN_POLITICS_YES, maker_amount=500_000,
                taker_amount=1_000_000, log_index=1,
            )
        ],
    )
    write_gamma_snapshot(
        data_root,
        markets=[gamma_market(1, COND_POLITICS, [TOKEN_POLITICS_YES], event_id=11, closed=True)],
        events=[gamma_event(11, ["us-politics"])],
    )
    stats = build(data_root, write_trades=False)
    assert stats["trades"] == 0
    assert stats["daily_bars"] == 1
    assert not (data_root / "curated" / "trades.parquet").exists()
    assert (data_root / "curated" / "daily_bars.parquet").exists()


def test_politics_classification_rule():
    mk = gamma_market(9, "0x" + "c9".rjust(64, "0"), [1, 2], event_id=11, closed=False)
    # tags come from the events snapshot join, not the market payload
    assert extract_market_row(mk, {"11": (None, ["geopolitics"])})["is_politics"] is True
    assert extract_market_row(mk, {"11": (None, ["nba"])})["is_politics"] is False
    assert extract_market_row(mk, {"11": ("US Elections", [])})["is_politics"] is True
    # no events snapshot at all -> not politics, never a crash
    assert extract_market_row(mk, None)["is_politics"] is False


def test_load_markets_falls_back_to_frozen_curated_tables(data_root):
    # Raw Gamma snapshots deleted (2026-10-25): reuse the frozen curated table,
    # politics-only table second, and fail loudly only when neither exists.
    import pytest

    from normalize.build_curated import MARKET_SCHEMA, load_markets

    curated = data_root / "curated"
    curated.mkdir()
    with pytest.raises(FileNotFoundError):
        load_markets(data_root)

    row = {c: None for c in MARKET_SCHEMA}
    row.update(market_id="1", condition_id=COND_POLITICS, is_politics=True,
               clob_token_ids=[str(TOKEN_POLITICS_YES)], tags=["politics"])
    frozen = pl.DataFrame([row], schema=MARKET_SCHEMA).with_columns(cohort=pl.lit("open"))
    frozen.write_parquet(curated / "politics_markets.parquet")
    got = load_markets(data_root)
    assert got.columns == list(MARKET_SCHEMA) and got.height == 1  # cohort dropped

    pl.concat([frozen, frozen.with_columns(market_id=pl.lit("2"), is_politics=pl.lit(False))]
              ).write_parquet(curated / "markets.parquet")
    assert load_markets(data_root).height == 2  # full table preferred


def test_windowed_trades_dedup_and_overlap_fallback(data_root):
    # Duplicates inside one part are collapsed per window; overlapping part
    # ranges (duplicates may span windows) make the windowed path bow out.
    from normalize.build_curated import token_map, write_trades_windowed

    kw = dict(maker=MAKER, taker=TAKER, maker_asset_id=0, taker_asset_id=TOKEN_POLITICS_YES,
              maker_amount=600_000, taker_amount=1_000_000)
    write_raw_part(data_root, "orderfilled_ctf_v1",
                   [v1_order_filled_row(**kw, log_index=1), v1_order_filled_row(**kw, log_index=1)],
                   "part-0000000001-0000000099.parquet")
    write_raw_part(data_root, "orderfilled_ctf_v1",
                   [v1_order_filled_row(**kw, log_index=2, block_number=150, tx_hash="0x" + "ee" * 32)],
                   "part-0000000100-0000000199.parquet")
    write_gamma_snapshot(
        data_root,
        markets=[gamma_market(1, COND_POLITICS, [TOKEN_POLITICS_YES], event_id=11, closed=True)],
        events=[gamma_event(11, ["us-politics"])],
    )
    from normalize.build_curated import load_markets

    tokens = token_map(load_markets(data_root))
    out = data_root / "t.parquet"
    assert write_trades_windowed(data_root, tokens, out) == 2
    got = pl.read_parquet(out)
    assert got["market_id"].to_list() == ["1", "1"] and got["cohort"].unique().to_list() == ["matched"]

    write_raw_part(data_root, "orderfilled_ctf_v1", [v1_order_filled_row(**kw, log_index=3)],
                   "part-0000000150-0000000250.parquet")  # overlaps the previous part
    assert write_trades_windowed(data_root, tokens, data_root / "t2.parquet") is None


def test_windowed_bars_equal_reference_bars(data_root, monkeypatch):
    # Exact equivalence with build_daily_bars when one token-day spans windows
    # and streams (last_price must come from the latest block/log overall).
    import functools

    from normalize import build_curated as bc
    from normalize.compact import iter_windows
    from conftest import v2_order_filled_row

    monkeypatch.setattr(bc, "_iter_windows", functools.partial(iter_windows, window=1))
    day = 1_780_000_000
    v1 = lambda blk, idx, mamt, tamt, taker=TAKER: v1_order_filled_row(
        maker=MAKER, taker=taker, maker_asset_id=0, taker_asset_id=TOKEN_POLITICS_YES,
        maker_amount=mamt, taker_amount=tamt, block_number=blk, block_timestamp=day + blk,
        log_index=idx, tx_hash=f"0x{blk:064x}")
    write_raw_part(data_root, "orderfilled_ctf_v1", [v1(10, 0, 600_000, 1_000_000), v1(10, 1, 300_000, 1_000_000)],
                   "part-0000000001-0000000019.parquet")
    write_raw_part(data_root, "orderfilled_ctf_v1",
                   [v1(30, 0, 700_000, 1_000_000, taker="0x4bfb41d5b3570defd03c39a9a4d8de6bd8b8982e")],
                   "part-0000000020-0000000039.parquet")
    write_raw_part(data_root, "orderfilled_ctf_v2",
                   [v2_order_filled_row(maker=MAKER, taker=TAKER, side=0, token_id=TOKEN_POLITICS_YES,
                                        maker_amount=550_000, taker_amount=1_000_000, block_number=20,
                                        block_timestamp=day + 20, log_index=5)],
                   "part-0000000001-0000000029.parquet")
    write_gamma_snapshot(
        data_root,
        markets=[gamma_market(1, COND_POLITICS, [TOKEN_POLITICS_YES], event_id=11, closed=True)],
        events=[gamma_event(11, ["us-politics"])],
    )
    tokens = bc.token_map(bc.load_markets(data_root))
    got_n = bc.build_daily_bars_windowed(data_root, tokens, data_root / "w.parquet")
    ref_path = data_root / "r.parquet"
    bc.build_daily_bars(bc._join_tokens(bc.scan_all_trades(data_root), tokens), ref_path)
    ref = pl.read_parquet(ref_path).sort("token_id_hex", "date")
    got = pl.read_parquet(data_root / "w.parquet").select(ref.columns).sort("token_id_hex", "date")
    assert got_n == ref.height
    # Float sums differ in the last ulp with summation order (the reference
    # group_by itself is order-nondeterministic), so compare with a tolerance.
    from polars.testing import assert_frame_equal

    assert_frame_equal(got, ref, rel_tol=1e-12)
