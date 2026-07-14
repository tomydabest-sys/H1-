# H1 Polymarket — Stage 1 gate

Quantitative research repo for a pre-registered, falsifiable test (**Stage 1 gate**) of:

> **H1-revised:** long-horizon YES premium in typical Polymarket politics markets — at 1-week-plus horizons before resolution, the NO side carries roughly 5–10 gross percentage points of edge (before costs/fees).

This continues prior work on the Becker archive (Kalshi + Polymarket, ~7.68M markets / ~72.1M trades) that survived an out-of-sample peek on 2026-resolved markets. That dataset lacked the historical trade coverage for a proper gate; this repo backfills trades directly from Polygon chain events (spanning the 2026-04-28 CLOB V2 migration, which broke the public subgraph) and runs the gate against thresholds fixed in `PREREGISTRATION.md` before the test code exists.

## Layout

```
CLAUDE.md            operating rules, commands, kill-criteria pointer
PROPOSALS.md         phased plan + status (Phase 0 → 1 → 2)
PREREGISTRATION.md   hypothesis + falsification thresholds (tagged before Phase 2)
src/
  ingest/            HyperSync OrderFilled backfill (V1 + V2 exchanges), CTF events, Gamma metadata
  normalize/         decode raw events -> labeled trades (price, side, USD notional) -> Parquet
  calibration/       logistic recalibration slope replication
  fairvalue/         calibration-adjusted fair value pipeline
  gate/              Stage 1 test: long-horizon NO-side edge, out-of-sample, cost-adjusted
data/
  raw/               append-only ingested data (never mutated; gitignored)
  interim/           cleaned/joined, not yet analysis-ready (gitignored)
  curated/           analysis-ready Parquet tables (gitignored)
tests/               schema checks, no-leakage checks, gate threshold checks
```

## How to run

Requires [uv](https://docs.astral.sh/uv/).

```sh
uv sync
uv run pytest   # currently only scaffold checks
```

Phase-specific commands (backfill, verification reports) will be documented here as each phase lands.

## Current status

| Phase | State |
|---|---|
| Scaffold | done |
| Phase 0 — data backfill | planning (no implementation yet) |
| Phase 1 — calibration / fair value | not started |
| Phase 2 — Stage 1 gate | not started; blocked on `PREREGISTRATION.md` tag |

## Survivorship-bias note

The metadata ingest will keep created-but-unresolved and delisted markets as an explicit separate cohort rather than silently dropping them. Cohort definitions and counts will be documented here once Phase 0 lands.
