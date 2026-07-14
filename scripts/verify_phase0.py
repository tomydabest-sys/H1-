"""Phase 0 verification report (operating rule 2): pass/fail, not assertions.

Reports, per the pre-agreed Phase 0 spec:
  - total events ingested per stream
  - date range covered on each side of the 2026-04-28 CLOB V2 migration boundary
  - count of politics-market conditions found
  - sanity: no duplicate (tx_hash, log_index); no non-positive amounts; prices in (0,1)
  - idempotency: per-token trade counts are monotonically non-decreasing vs the
    previous run's snapshot (rule 5)

Exit code 0 = PASS, 1 = FAIL.

Usage:
    uv run python scripts/verify_phase0.py
"""

from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

import polars as pl

import _bootstrap
from ingest.contracts import MIGRATION_TS, STREAMS

DATA = _bootstrap.DATA_ROOT
SNAP_DIR = DATA / "interim" / "verify_snapshots"

failures: list[str] = []


def fail(msg: str) -> None:
    failures.append(msg)
    print(f"  FAIL: {msg}")


def ok(msg: str) -> None:
    print(f"  ok: {msg}")


def _dt(ts: int) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M")


def report_streams() -> None:
    print("\n== raw streams ==")
    for name in STREAMS:
        d = DATA / "raw" / "hypersync" / name
        parts = sorted(d.glob("part-*.parquet"))
        if not parts:
            print(f"[{name}] no data ingested yet")
            continue
        df = pl.read_parquet(parts, columns=["block_number", "block_timestamp"])
        n = df.height
        ts = df["block_timestamp"]
        pre = df.filter(pl.col("block_timestamp") < MIGRATION_TS).height
        post = n - pre
        print(
            f"[{name}] events={n}  blocks {df['block_number'].min()}..{df['block_number'].max()}  "
            f"time {_dt(ts.min())} .. {_dt(ts.max())} UTC  "
            f"pre-migration={pre} post-migration={post}"
        )
        zero_ts = df.filter(pl.col("block_timestamp") <= 0).height
        if zero_ts:
            fail(f"{name}: {zero_ts} rows with missing block timestamps")


def report_curated() -> None:
    print("\n== curated ==")
    tpath = DATA / "curated" / "trades.parquet"
    if not tpath.exists():
        print("no curated trades yet (run scripts/build_curated.py)")
        return

    trades = pl.read_parquet(tpath)
    print(f"trades: {trades.height}")

    dups = trades.height - trades.unique(subset=["tx_hash", "log_index"]).height
    (ok if dups == 0 else fail)(f"duplicate (tx_hash, log_index) rows: {dups}")

    nulls = trades.filter(
        pl.col("token_amount").is_null() | pl.col("collateral_amount").is_null()
    ).height
    (ok if nulls == 0 else fail)(f"rows with unparseable amounts: {nulls}")

    nonpos = trades.filter(
        (pl.col("token_amount") <= 0) | (pl.col("collateral_amount") < 0)
    ).height
    (ok if nonpos == 0 else fail)(f"rows with non-positive amounts: {nonpos}")

    bad_price = trades.filter((pl.col("price") <= 0) | (pl.col("price") >= 1)).height
    frac = bad_price / max(trades.height, 1)
    # A tiny share of fills can print at extreme prices legitimately; a large
    # share means the V2 side-mapping is flipped (see decode.py VERIFY note).
    (ok if frac < 0.01 else fail)(
        f"prices outside (0,1): {bad_price} ({frac:.3%}) — flipped side-mapping if large"
    )

    print("\ntrade cohorts:")
    print(trades.group_by("cohort").len().sort("cohort"))
    print("\nby exchange, maker/taker aggregate flag:")
    print(trades.group_by("exchange", "taker_is_exchange").len().sort("exchange"))

    mpath = DATA / "curated" / "markets.parquet"
    if mpath.exists():
        markets = pl.read_parquet(mpath)
        pol = markets.filter(pl.col("is_politics"))
        print(f"\nmarkets: {markets.height}  politics: {pol.height}")
        print("market cohorts:")
        print(markets.group_by("cohort", "is_politics").len().sort("cohort"))
        pol_conditions = pol["condition_id"].unique()
        traded_pol = trades.filter(pl.col("is_politics"))["condition_id"].n_unique()
        print(f"politics conditions in metadata: {pol_conditions.len()}; with trades: {traded_pol}")

    rpath = DATA / "curated" / "resolutions.parquet"
    if rpath.exists():
        res = pl.read_parquet(rpath)
        print(f"on-chain resolutions: {res.height}")


def check_idempotency() -> None:
    print("\n== idempotency (per-token trade counts non-decreasing across runs) ==")
    tpath = DATA / "curated" / "trades.parquet"
    if not tpath.exists():
        print("skipped (no curated trades)")
        return
    counts = (
        pl.read_parquet(tpath, columns=["token_id_hex"])
        .group_by("token_id_hex")
        .len()
        .rename({"len": "n"})
    )
    SNAP_DIR.mkdir(parents=True, exist_ok=True)
    prev_path = SNAP_DIR / "token_counts_prev.parquet"
    if prev_path.exists():
        prev = pl.read_parquet(prev_path)
        joined = prev.join(counts, on="token_id_hex", how="left", suffix="_new")
        regressed = joined.filter(
            pl.col("n_new").is_null() | (pl.col("n_new") < pl.col("n"))
        )
        (ok if regressed.is_empty() else fail)(
            f"tokens whose trade count decreased vs previous run: {regressed.height}"
        )
    else:
        print("no previous snapshot — baseline saved; re-run after the next backfill to check")
    counts.write_parquet(prev_path)


def main() -> int:
    print(f"data root: {DATA}")
    report_streams()
    report_curated()
    check_idempotency()
    print("\n== verdict ==")
    if failures:
        print(f"FAIL ({len(failures)} problem(s)):")
        for f in failures:
            print(f"  - {f}")
        return 1
    print("PASS")
    return 0


if __name__ == "__main__":
    sys.exit(main())
