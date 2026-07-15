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


def _scan_stream_raw(data_root: Path, stream: str) -> pl.LazyFrame | None:
    d = data_root / "raw" / "hypersync" / stream
    parts = sorted(d.glob("part-*.parquet"))
    if not parts:
        return None
    return pl.scan_parquet(parts)


def scan_all_trades(data_root: Path) -> pl.LazyFrame | None:
    """Lazy decode of all OrderFilled streams (memory-safe: streams, never
    materializes the full event set)."""
    frames = []
    for name, spec in STREAMS.items():
        if spec.generation is None:
            continue
        raw = _scan_stream_raw(data_root, name)
        if raw is None:
            continue
        fn = decode.decode_order_filled_v1 if spec.generation == 1 else decode.decode_order_filled_v2
        frames.append(fn(raw, exchange=name, exchange_address=STREAM_EXCHANGE_ADDRESS[name]))
    if not frames:
        return None
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


def _collect_tags(raw: dict) -> set[str]:
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
    return tags


def extract_market_row(raw: dict, event_tags: dict[str, tuple[str | None, list[str]]] | None = None) -> dict:
    """Flatten one raw Gamma market. `event_tags` maps event id -> (category, tags)
    from the events snapshot: the keyset /markets payload embeds events WITHOUT
    their tags (live-verified 2026-07-15), so tags/categories must be joined in
    from the /events/keyset snapshot.
    """
    tags = _collect_tags(raw)
    categories = {str(raw.get("category") or "")}
    event_ids = [str(ev.get("id", "")) for ev in raw.get("events") or []]
    for eid in event_ids:
        if event_tags and eid in event_tags:
            ev_cat, ev_tags = event_tags[eid]
            tags.update(ev_tags)
            if ev_cat:
                categories.add(ev_cat)

    text_pool = [*categories, *tags]
    is_politics = any(POLITICS_PATTERNS.search(x) for x in text_pool if x)

    clob_ids = [str(x) for x in _parse_json_list(raw.get("clobTokenIds"))]
    outcomes = [str(x) for x in _parse_json_list(raw.get("outcomes"))]
    category = next((c for c in categories if c), None)

    return {
        "market_id": str(raw.get("id", "")),
        "condition_id": (raw.get("conditionId") or "").lower(),
        "question": raw.get("question"),
        "slug": raw.get("slug"),
        "created_at": raw.get("createdAt"),
        "closed": bool(raw.get("closed")),
        "active": bool(raw.get("active", True)),
        "archived": bool(raw.get("archived")),
        "closed_time": raw.get("closedTime"),
        "end_date": raw.get("endDate"),
        "category": category,
        "tags": sorted(tags),
        "event_ids": event_ids,
        "neg_risk": bool(raw.get("negRisk")),
        "is_politics": is_politics,
        "clob_token_ids": clob_ids,
        "outcomes": outcomes,
    }


def load_event_tags(data_root: Path) -> dict[str, tuple[str | None, list[str]]]:
    """event id -> (category, [tag slugs/labels]) from the latest events snapshot.

    Streams page-by-page: the events snapshot is multi-GB (each event embeds its
    markets), so materializing all pages at once OOMs a 16GB container.
    """
    snap = latest_complete_snapshot(data_root, endpoint="events")
    if snap is None:
        return {}
    out: dict[str, tuple[str | None, list[str]]] = {}
    for page in sorted(snap.glob("page-*.parquet")):
        for raw_str in pl.read_parquet(page, columns=["raw_json"])["raw_json"].to_list():
            raw = json.loads(raw_str)
            tags = sorted(_collect_tags({"tags": raw.get("tags")}))
            out[str(raw.get("id", ""))] = (raw.get("category") or None, tags)
    return out


MARKET_COHORT_ENDPOINTS = ("markets_open", "markets_closed")

MARKET_SCHEMA: dict[str, pl.DataType] = {
    "market_id": pl.Utf8,
    "condition_id": pl.Utf8,
    "question": pl.Utf8,
    "slug": pl.Utf8,
    "created_at": pl.Utf8,
    "closed": pl.Boolean,
    "active": pl.Boolean,
    "archived": pl.Boolean,
    "closed_time": pl.Utf8,
    "end_date": pl.Utf8,
    "category": pl.Utf8,
    "tags": pl.List(pl.Utf8),
    "event_ids": pl.List(pl.Utf8),
    "neg_risk": pl.Boolean,
    "is_politics": pl.Boolean,
    "clob_token_ids": pl.List(pl.Utf8),
    "outcomes": pl.List(pl.Utf8),
}


def load_markets(data_root: Path) -> pl.DataFrame:
    """Union of the open + closed market cohorts (both required: the keyset
    endpoint silently hides closed markets unless closed=true is passed)."""
    snaps = {}
    for ep in MARKET_COHORT_ENDPOINTS:
        snaps[ep] = latest_complete_snapshot(data_root, endpoint=ep)
    missing = [ep for ep, s in snaps.items() if s is None]
    if missing:
        raise FileNotFoundError(
            f"missing complete Gamma snapshot(s) {missing} — run "
            "scripts/backfill_gamma.py --endpoint <name> for each"
        )
    event_tags = load_event_tags(data_root)
    if not event_tags:
        print(
            "WARNING: no events snapshot found — tag enrichment reduced to the "
            "markets' own embedded tags. Run scripts/backfill_gamma.py --endpoint events"
        )
    # Page-at-a-time into compact columnar frames: 1.7M+ markets as Python dicts
    # would eat several GB; explicit schema keeps empty-list columns consistent.
    frames = []
    for snap in snaps.values():
        for page in sorted(snap.glob("page-*.parquet")):
            rows = [
                extract_market_row(json.loads(r), event_tags)
                for r in pl.read_parquet(page, columns=["raw_json"])["raw_json"].to_list()
            ]
            if rows:
                frames.append(pl.DataFrame(rows, schema=MARKET_SCHEMA))
    df = pl.concat(frames, how="vertical").unique(subset=["market_id"], keep="last")
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
    # A token id must map to exactly one market; ambiguous mappings are
    # quarantined (dropped from the join -> those trades become chain_only)
    # and reported loudly rather than crashing a multi-hour build.
    dup_mask = exploded["token_id_hex"].is_duplicated()
    n_dup = int(dup_mask.sum())
    if n_dup:
        sample = exploded.filter(dup_mask).head(4).select("token_id_hex", "market_id").to_dicts()
        print(
            f"WARNING: {n_dup} token rows with ambiguous token->market mapping "
            f"quarantined out of {exploded.height}; sample: {sample}"
        )
        exploded = exploded.filter(~dup_mask)
    return exploded.drop("outcomes")


def build(data_root: Path) -> dict:
    curated = data_root / "curated"
    curated.mkdir(parents=True, exist_ok=True)

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

    trades_path = curated / "trades.parquet"
    trades_lf = scan_all_trades(data_root)
    if trades_lf is None:
        pl.DataFrame(schema={c: pl.Utf8 for c in decode.TRADE_COLUMNS}).write_parquet(trades_path)
        n_trades = 0
        n_chain_only = 0
    else:
        joined = trades_lf.join(
            tokens.select(
                "token_id_hex", "market_id", "condition_id", "is_politics",
                "neg_risk", "outcome_index", "outcome_label",
            ).lazy(),
            on="token_id_hex",
            how="left",
        ).with_columns(
            cohort=pl.when(pl.col("market_id").is_null())
            .then(pl.lit("chain_only"))
            .otherwise(pl.lit("matched")),
            is_politics=pl.col("is_politics").fill_null(False),
        )
        # sink_parquet executes the whole decode+dedup+join as a streaming query
        joined.sink_parquet(trades_path)
        counts = (
            pl.scan_parquet(trades_path)
            .select(
                pl.len().alias("n"),
                (pl.col("cohort") == "chain_only").sum().alias("chain_only"),
            )
            .collect()
        )
        n_trades = int(counts["n"][0])
        n_chain_only = int(counts["chain_only"][0])

    markets.write_parquet(curated / "markets.parquet")
    if not resolutions.is_empty():
        resolutions.write_parquet(curated / "resolutions.parquet")
    if not conditions.is_empty():
        conditions.write_parquet(curated / "conditions.parquet")

    return {
        "trades": n_trades,
        "markets": markets.height,
        "resolutions": resolutions.height if not resolutions.is_empty() else 0,
        "conditions": conditions.height if not conditions.is_empty() else 0,
        "politics_markets": int(markets["is_politics"].sum()),
        "chain_only_trades": n_chain_only,
    }
