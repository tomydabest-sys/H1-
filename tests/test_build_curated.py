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


def gamma_market(mid: int, condition_id: str, token_ids: list[int], tags: list[str], closed: bool) -> dict:
    return {
        "id": mid,
        "question": f"market {mid}?",
        "slug": f"market-{mid}",
        "conditionId": condition_id,
        "createdAt": "2025-01-01T00:00:00Z",
        "closed": closed,
        "clobTokenIds": json.dumps([str(t) for t in token_ids]),
        "outcomes": json.dumps(["Yes", "No"]),
        "events": [{"tags": [{"slug": t} for t in tags]}],
        "negRisk": False,
    }


def write_gamma_snapshot(data_root, markets: list[dict]) -> None:
    sdir = data_root / "raw" / "gamma" / "markets" / "snapshot=2026-07-01"
    sdir.mkdir(parents=True)
    rows = [
        {
            "market_id": str(m["id"]),
            "raw_json": json.dumps(m),
            "page_index": 0,
            "fetched_at": "2026-07-01T00:00:00Z",
        }
        for m in markets
    ]
    pq.write_table(pa.Table.from_pylist(rows), sdir / "page-00000.parquet")
    (sdir / "_COMPLETE").write_text("{}")


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
        [
            gamma_market(1, COND_POLITICS, [TOKEN_POLITICS_YES, TOKEN_POLITICS_NO], ["us-politics"], closed=True),
            gamma_market(2, "0x" + "c2".rjust(64, "0"), [123, 124], ["sports"], closed=False),
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
    m1, m2 = markets.to_dicts()
    assert m1["cohort"] == "resolved_onchain"  # has on-chain ConditionResolution
    assert m2["cohort"] == "open"  # unresolved & not closed: survivorship cohort kept

    res = pl.read_parquet(data_root / "curated" / "resolutions.parquet")
    assert res.to_dicts()[0]["payout_numerators"] == [1, 0]


def test_politics_classification_rule():
    m = extract_market_row(gamma_market(9, "0x" + "c9".rjust(64, "0"), [1, 2], ["geopolitics"], False))
    assert m["is_politics"] is True
    m2 = extract_market_row(gamma_market(9, "0x" + "c9".rjust(64, "0"), [1, 2], ["nba"], False))
    assert m2["is_politics"] is False
    m3 = extract_market_row(
        dict(gamma_market(9, "0x" + "c9".rjust(64, "0"), [1, 2], [], False), category="US Elections")
    )
    assert m3["is_politics"] is True
