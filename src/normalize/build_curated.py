"""Build analysis-ready curated tables from raw ingested data (rule 7: downstream only).

Inputs (read-only):
    data/raw/hypersync/orderfilled_*/part-*.parquet
    data/raw/hypersync/ctf_events/part-*.parquet
    data/raw/gamma/markets/snapshot=*/page-*.parquet  (latest _COMPLETE snapshot)

Outputs:
    data/curated/trades.parquet       one row per OrderFilled log, deduped on
                                      (tx_hash, log_index), joined to market metadata
    data/curated/markets.parquet      one row per Gamma market with cohort + politics flag
    data/curated/resolutions.parquet  one row per on-chain ConditionResolution
    data/curated/conditions.parquet   one row per on-chain ConditionPreparation

Survivorship (explicit, never silently dropped):
    markets.cohort ∈ {resolved_onchain, closed_unresolved, open}
    trades with no matching Gamma market keep market fields NULL and get
    cohort='chain_only' (delisted/unlisted markets that traded on-chain).

Politics classification (flagged, not silent): a market is is_politics=True if
its Gamma category or any event tag matches POLITICS_PATTERNS. The rule and the
matched-tag inventory are printed by scripts/verify_phase0.py for review.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import polars as pl

from ingest.contracts import STREAM_EXCHANGE_ADDRESS, STREAMS
from ingest.gamma_markets import latest_complete_snapshot
from normalize import decode

POLITICS_PATTERNS = re.compile(r"politic|election|geopolit", re.IGNORECASE)

TRADES_DEDUP_KEY = ["tx_hash", "log_index"]


def _read_stream_raw(data_root: Path, stream: str) -> pl.DataFrame | None:
    d = data_root / "raw" / "hypersync" / stream
    parts = sorted(d.glob("part-*.parquet"))
    if not parts:
        return None
    return pl.read_parquet(parts)


def decode_all_trades(data_root: Path) -> pl.DataFrame:
    frames = []
    for name, spec in STREAMS.items():
        if spec.generation is None:
            continue
        raw = _read_stream_raw(data_root, name)
        if raw is None or raw.is_empty():
            continue
        fn = decode.decode_order_filled_v1 if spec.generation == 1 else decode.decode_order_filled_v2
        frames.append(fn(raw, exchange=name, exchange_address=STREAM_EXCHANGE_ADDRESS[name]))
    if not frames:
        return pl.DataFrame(schema={c: pl.Utf8 for c in decode.TRADE_COLUMNS})
    trades = pl.concat(frames, how="vertical")
    # (tx_hash, log_index) is globally unique on-chain; duplicates can only come
    # from overlapping re-scans and are safe to collapse.
    return trades.unique(subset=TRADES_DEDUP_KEY, keep="first")


def _parse_json_list(v) -> list:
    if v is None:
        return []
    if isinstance(v, list):
        return v
    if isinstance(v, str) and v.strip():
        try:
            parsed = json.loads(v)
            return parsed if isinstance(parsed, list) else []
        except json.JSONDecodeError:
            return []
    return []


def extract_market_row(raw: dict) -> dict:
    tags: set[str] = set()
    for ev in raw.get("events") or []:
        for t in ev.get("tags") or []:
            for k in ("slug", "label"):
                if isinstance(t, dict) and t.get(k):
                    tags.add(str(t[k]).lower())
    for t in raw.get("tags") or []:
        if isinstance(t, dict):
            for k in ("slug", "label"):
                if t.get(k):
                    tags.add(str(t[k]).lower())
        elif isinstance(t, str):
            tags.add(t.lower())

    category = raw.get("category") or ""
    text_pool = [category, *tags]
    is_politics = any(POLITICS_PATTERNS.search(x) for x in text_pool if x)

    clob_ids = [str(x) for x in _parse_json_list(raw.get("clobTokenIds"))]
    outcomes = [str(x) for x in _parse_json_list(raw.get("outcomes"))]

    return {
        "market_id": str(raw.get("id", "")),
        "condition_id": (raw.get("conditionId") or "").lower(),
        "question": raw.get("question"),
        "slug": raw.get("slug"),
        "created_at": raw.get("createdAt"),
        "closed": bool(raw.get("closed")),
        "closed_time": raw.get("closedTime"),
        "end_date": raw.get("endDate"),
        "resolved_at_gamma": raw.get("resolvedAt"),
        "category": category or None,
        "tags": sorted(tags),
        "neg_risk": bool(raw.get("negRisk")),
        "is_politics": is_politics,
        "clob_token_ids": clob_ids,
        "outcomes": outcomes,
    }


def load_markets(data_root: Path) -> pl.DataFrame:
    snap = latest_complete_snapshot(data_root)
    if snap is None:
        raise FileNotFoundError(
            "no complete Gamma snapshot under data/raw/gamma/markets/ — run scripts/backfill_gamma.py first"
        )
    pages = pl.read_parquet(sorted(snap.glob("page-*.parquet")))
    rows = [extract_market_row(json.loads(r)) for r in pages["raw_json"].to_list()]
    df = pl.DataFrame(rows).unique(subset=["market_id"], keep="last")
    return df


def token_map(markets: pl.DataFrame) -> pl.DataFrame:
    """Explode markets to one row per outcome token: token_id_hex -> market fields."""
    exploded = (
        markets.select(
            "market_id",
            "condition_id",
            "is_politics",
            "neg_risk",
            "closed",
            "outcomes",
            "clob_token_ids",
        )
        .filter(pl.col("clob_token_ids").list.len() > 0)
        .with_columns(outcome_index=pl.int_ranges(pl.col("clob_token_ids").list.len()))
        .explode(["clob_token_ids", "outcome_index"], empty_as_null=False)
        .rename({"clob_token_ids": "token_id_dec"})
        .filter(pl.col("token_id_dec").is_not_null() & (pl.col("token_id_dec") != ""))
    )
    exploded = exploded.with_columns(
        token_id_hex=pl.col("token_id_dec").map_elements(
            decode.norm_token_id_hex, return_dtype=pl.Utf8
        ),
        outcome_label=pl.struct(["outcomes", "outcome_index"]).map_elements(
            lambda s: s["outcomes"][s["outcome_index"]]
            if s["outcomes"] and s["outcome_index"] < len(s["outcomes"])
            else None,
            return_dtype=pl.Utf8,
        ),
    )
    # A token id must map to exactly one market; duplicates would corrupt the join.
    dups = exploded.filter(pl.col("token_id_hex").is_duplicated())
    if not dups.is_empty():
        raise ValueError(
            f"{dups.height} duplicate token_id_hex rows in Gamma metadata; "
            f"sample: {dups.head(5).to_dicts()}"
        )
    return exploded.drop("outcomes")


def build(data_root: Path) -> dict:
    curated = data_root / "curated"
    curated.mkdir(parents=True, exist_ok=True)

    trades = decode_all_trades(data_root)
    markets = load_markets(data_root)
    tokens = token_map(markets)

    ctf_raw = _read_stream_raw(data_root, "ctf_events")
    if ctf_raw is None or ctf_raw.is_empty():
        resolutions = pl.DataFrame()
        conditions = pl.DataFrame()
    else:
        resolutions = decode.decode_condition_resolutions(ctf_raw)
        conditions = decode.decode_condition_preparations(ctf_raw)

    resolved_ids = (
        set(resolutions["condition_id"].to_list()) if not resolutions.is_empty() else set()
    )
    markets = markets.with_columns(
        cohort=pl.when(pl.col("condition_id").is_in(list(resolved_ids) or [""]))
        .then(pl.lit("resolved_onchain"))
        .when(pl.col("closed"))
        .then(pl.lit("closed_unresolved"))
        .otherwise(pl.lit("open"))
    )

    trades = trades.join(
        tokens.select("token_id_hex", "market_id", "condition_id", "is_politics", "neg_risk", "outcome_index", "outcome_label"),
        on="token_id_hex",
        how="left",
    ).with_columns(
        cohort=pl.when(pl.col("market_id").is_null())
        .then(pl.lit("chain_only"))
        .otherwise(pl.lit("matched")),
        is_politics=pl.col("is_politics").fill_null(False),
    )

    trades.write_parquet(curated / "trades.parquet")
    markets.write_parquet(curated / "markets.parquet")
    if not resolutions.is_empty():
        resolutions.write_parquet(curated / "resolutions.parquet")
    if not conditions.is_empty():
        conditions.write_parquet(curated / "conditions.parquet")

    return {
        "trades": trades.height,
        "markets": markets.height,
        "resolutions": resolutions.height if not resolutions.is_empty() else 0,
        "conditions": conditions.height if not conditions.is_empty() else 0,
        "politics_markets": int(markets["is_politics"].sum()),
        "chain_only_trades": int((trades["cohort"] == "chain_only").sum()),
    }
