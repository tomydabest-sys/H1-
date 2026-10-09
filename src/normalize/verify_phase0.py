"""Phase 0 verification report (operating rule 2): pass/fail, not assertions.

Reports, per the pre-agreed Phase 0 spec:
  - total events ingested per stream
  - date range covered on each side of the 2026-04-28 CLOB V2 migration boundary
  - count of politics-market conditions found
  - sanity: no duplicate (tx_hash, log_index); no non-positive amounts; prices in (0,1)
  - idempotency: per-token trade counts are monotonically non-decreasing vs the
    previous run's snapshot (rule 5)

Completeness is a FAIL, not a warning: a missing stream, a checkpoint short of
the target end block, a gap in a stream's recorded retention coverage, or a
missing curated table all fail the run. An empty dataset therefore fails.

Retention (2026-10-09 amendment): every stream is politics-only over its whole
range; `full` segments are a superset and also count as covered.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import polars as pl

from ingest.contracts import MIGRATION_TS, PINNED_END_BLOCK, STREAMS

COVERING_MODES = {"politics_only", "full"}
REQUIRED_CURATED = ("markets.parquet", "resolutions.parquet", "daily_bars.parquet")


class Report:
    def __init__(self) -> None:
        self.failures: list[str] = []

    def fail(self, msg: str) -> None:
        self.failures.append(msg)
        print(f"  FAIL: {msg}")

    def ok(self, msg: str) -> None:
        print(f"  ok: {msg}")

    def check(self, cond: bool, msg: str) -> None:
        (self.ok if cond else self.fail)(msg)


def _dt(ts: int) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M")


def merge_segments(segments: list[dict]) -> list[tuple[int, int]]:
    """Union of covering [from_block, to_block] ranges, merged where adjacent."""
    spans = sorted(
        (int(s["from_block"]), int(s["to_block"]))
        for s in segments
        if s.get("mode") in COVERING_MODES
    )
    merged: list[tuple[int, int]] = []
    for lo, hi in spans:
        if merged and lo <= merged[-1][1] + 1:
            merged[-1] = (merged[-1][0], max(merged[-1][1], hi))
        else:
            merged.append((lo, hi))
    return merged


def coverage_gaps(segments: list[dict], start: int, end: int) -> list[tuple[int, int]]:
    """Block ranges within [start, end] not covered by any recorded segment."""
    gaps = []
    cursor = start
    for lo, hi in merge_segments(segments):
        if hi < cursor:
            continue
        if lo > end:
            break
        if lo > cursor:
            gaps.append((cursor, lo - 1))
        cursor = max(cursor, hi + 1)
    if cursor <= end:
        gaps.append((cursor, end))
    return gaps


def report_streams(r: Report, data: Path, target_block: int) -> None:
    print("\n== raw streams ==")
    pre_total = post_total = 0
    for name, spec in STREAMS.items():
        sdir = data / "raw" / "hypersync" / name
        parts = sorted(sdir.glob("part-*.parquet"))
        ckpt_f = sdir / "_checkpoint.json"
        ckpt = int(json.loads(ckpt_f.read_text())["last_committed_block"]) if ckpt_f.exists() else None
        if not parts:
            r.fail(f"{name}: no data ingested")
        else:
            df = pl.read_parquet(parts, columns=["block_number", "block_timestamp"])
            ts = df["block_timestamp"]
            pre = df.filter(pl.col("block_timestamp") < MIGRATION_TS).height
            post = df.height - pre
            if spec.generation is not None:
                pre_total += pre
                post_total += post
            print(
                f"[{name}] events={df.height}  blocks {df['block_number'].min()}..{df['block_number'].max()}  "
                f"time {_dt(ts.min())} .. {_dt(ts.max())} UTC  "
                f"pre-migration={pre} post-migration={post}"
            )
            zero_ts = df.filter(pl.col("block_timestamp") <= 0).height
            if zero_ts:
                r.fail(f"{name}: {zero_ts} rows with missing block timestamps")
        if ckpt is None:
            r.fail(f"{name}: no checkpoint")
        else:
            r.check(ckpt >= target_block, f"{name}: checkpoint {ckpt} >= target end block {target_block}")
    r.check(pre_total > 0, f"OrderFilled events before the V2 migration: {pre_total}")
    r.check(post_total > 0, f"OrderFilled events after the V2 migration: {post_total}")


def report_retention(r: Report, data: Path, target_block: int) -> None:
    print(f"\n== raw retention coverage (politics-only expected, through block {target_block}) ==")
    for name, spec in STREAMS.items():
        f = data / "raw" / "hypersync" / name / "_retention.json"
        if not f.exists():
            r.fail(f"{name}: no retention manifest")
            continue
        segs = json.loads(f.read_text())["segments"]
        modes = sorted({s["mode"] for s in segs})
        print(f"[{name}] modes={modes}; covered {merge_segments(segs)}")
        unknown = [m for m in modes if m not in COVERING_MODES]
        if unknown:
            r.fail(f"{name}: unknown retention mode(s) {unknown}")
        gaps = coverage_gaps(segs, spec.from_block, target_block)
        r.check(
            not gaps,
            f"{name}: retention covers {spec.from_block}..{target_block} without gaps"
            + (f" (gaps: {gaps[:5]}{' ...' if len(gaps) > 5 else ''})" if gaps else ""),
        )


def report_bars(r: Report, data: Path) -> None:
    bars = pl.read_parquet(data / "curated" / "daily_bars.parquet")
    print(f"daily bars: {bars.height} (token-days)")
    r.check(bars.height > 0, f"daily bars present: {bars.height}")

    dups = bars.height - bars.unique(subset=["token_id_hex", "date"]).height
    r.check(dups == 0, f"duplicate (token, date) bars: {dups}")

    neg = bars.filter(
        (pl.col("n_fills") < 0) | (pl.col("usd_volume") < 0) | (pl.col("token_volume") < 0)
    ).height
    r.check(neg == 0, f"bars with negative volumes: {neg}")

    with_fills = bars.filter(pl.col("n_fills") > 0)
    bad_price = with_fills.filter(
        (pl.col("vwap") <= 0) | (pl.col("vwap") >= 1)
        | (pl.col("last_price") <= 0) | (pl.col("last_price") >= 1)
    ).height
    frac = bad_price / max(with_fills.height, 1)
    r.check(frac < 0.01, f"bars with vwap/last_price outside (0,1): {bad_price} ({frac:.3%})")

    pol = bars.filter(pl.col("is_politics"))
    n_pol_cond = pol["condition_id"].n_unique()
    print(f"politics token-days: {pol.height}; politics conditions with bars: {n_pol_cond}")
    r.check(n_pol_cond > 0, f"politics conditions with bars: {n_pol_cond}")
    if bars.height:
        print(f"date range: {bars['date'].min()} .. {bars['date'].max()}")
        print(f"total maker-side usd volume: ${bars['usd_volume'].sum():,.0f}")


def report_trades(r: Report, data: Path) -> None:
    trades = pl.read_parquet(data / "curated" / "trades.parquet")
    print(f"trades: {trades.height}")

    dups = trades.height - trades.unique(subset=["tx_hash", "log_index"]).height
    r.check(dups == 0, f"duplicate (tx_hash, log_index) rows: {dups}")

    nulls = trades.filter(
        pl.col("token_amount").is_null() | pl.col("collateral_amount").is_null()
    ).height
    r.check(nulls == 0, f"rows with unparseable amounts: {nulls}")

    nonpos = trades.filter(
        (pl.col("token_amount") <= 0) | (pl.col("collateral_amount") < 0)
    ).height
    r.check(nonpos == 0, f"rows with non-positive amounts: {nonpos}")

    bad_price = trades.filter((pl.col("price") <= 0) | (pl.col("price") >= 1)).height
    frac = bad_price / max(trades.height, 1)
    # A tiny share of fills can print at extreme prices legitimately; a large
    # share means the V2 side-mapping is flipped (see decode.py VERIFY note).
    r.check(frac < 0.01, f"prices outside (0,1): {bad_price} ({frac:.3%}) — flipped side-mapping if large")

    print("\ntrade cohorts:")
    print(trades.group_by("cohort").len().sort("cohort"))
    # Politics-only raw: chain-only (unmatched) fills are filtered out before
    # storage, so this table compares generations within politics only.
    print("\nby exchange, maker/taker aggregate flag (politics subset):")
    print(trades.group_by("exchange", "taker_is_exchange").len().sort("exchange"))


def report_curated(r: Report, data: Path) -> None:
    print("\n== curated ==")
    curated = data / "curated"
    missing = [f for f in REQUIRED_CURATED if not (curated / f).exists()]
    for f in missing:
        r.fail(f"curated/{f} missing (run scripts/build_curated.py)")

    if (curated / "daily_bars.parquet").exists():
        report_bars(r, data)
    if (curated / "trades.parquet").exists():
        report_trades(r, data)

    if (curated / "markets.parquet").exists():
        markets = pl.read_parquet(curated / "markets.parquet")
        n_pol = int(markets["is_politics"].sum()) if markets.height else 0
        print(f"\nmarkets: {markets.height}  politics: {n_pol}")
        r.check(n_pol > 0, f"politics markets in metadata: {n_pol}")
        if markets.height:
            print("market cohorts:")
            print(markets.group_by("cohort", "is_politics").len().sort("cohort"))

    if (curated / "resolutions.parquet").exists():
        res = pl.read_parquet(curated / "resolutions.parquet")
        print(f"on-chain resolutions: {res.height}")
        r.check(res.height > 0, f"on-chain resolutions present: {res.height}")
        if res.height:
            zero_denorm = res.filter(pl.col("payout_numerators").list.sum() <= 0).height
            r.check(zero_denorm == 0, f"resolutions with non-positive payout sum: {zero_denorm}")


def check_idempotency(r: Report, data: Path) -> None:
    print("\n== idempotency (per-token trade counts non-decreasing across runs) ==")
    tpath = data / "curated" / "trades.parquet"
    bpath = data / "curated" / "daily_bars.parquet"
    if tpath.exists():
        counts = (
            pl.read_parquet(tpath, columns=["token_id_hex"])
            .group_by("token_id_hex")
            .len()
            .rename({"len": "n"})
        )
    elif bpath.exists():
        counts = (
            pl.read_parquet(bpath, columns=["token_id_hex", "n_fills"])
            .group_by("token_id_hex")
            .agg(n=pl.col("n_fills").sum())
        )
    else:
        print("skipped (no curated tables)")
        return
    snap_dir = data / "interim" / "verify_snapshots"
    snap_dir.mkdir(parents=True, exist_ok=True)
    prev_path = snap_dir / "token_counts_prev.parquet"
    if prev_path.exists():
        prev = pl.read_parquet(prev_path)
        joined = prev.join(counts, on="token_id_hex", how="left", suffix="_new")
        regressed = joined.filter(pl.col("n_new").is_null() | (pl.col("n_new") < pl.col("n")))
        r.check(regressed.is_empty(), f"tokens whose trade count decreased vs previous run: {regressed.height}")
    else:
        print("NOT YET CHECKED: no previous snapshot — baseline saved; re-run after the next backfill")
    counts.write_parquet(prev_path)


def run(data: Path, target_block: int = PINNED_END_BLOCK) -> int:
    """Print the report; return 0 = PASS, 1 = FAIL."""
    r = Report()
    print(f"data root: {data}\ntarget end block: {target_block}")
    report_streams(r, data, target_block)
    report_retention(r, data, target_block)
    report_curated(r, data)
    check_idempotency(r, data)
    print("\n== verdict ==")
    if r.failures:
        print(f"FAIL ({len(r.failures)} problem(s)):")
        for f in r.failures:
            print(f"  - {f}")
        return 1
    print("PASS")
    return 0
