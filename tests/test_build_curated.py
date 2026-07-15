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
    pq.write_table(pa.Table.from_pylist(rows), sdir / "page-00000.parquet")
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
        ],
        events=[
            gamma_event(11, ["us-politics"]),
            gamma_event(22, ["sports"]),
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
    mk = gamma_market(9, "0x" + "c9".rjust(64, "0"), [1, 2], event_id=11, closed=False)
    # tags come from the events snapshot join, not the market payload
    assert extract_market_row(mk, {"11": (None, ["geopolitics"])})["is_politics"] is True
    assert extract_market_row(mk, {"11": (None, ["nba"])})["is_politics"] is False
    assert extract_market_row(mk, {"11": ("US Elections", [])})["is_politics"] is True
    # no events snapshot at all -> not politics, never a crash
    assert extract_market_row(mk, None)["is_politics"] is False
