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
| Phase 0 — data backfill | code + offline tests done; live smoke run blocked on network allowlist + HyperSync token |
| Phase 1 — calibration / fair value | not started |
| Phase 2 — Stage 1 gate | not started; blocked on `PREREGISTRATION.md` tag |

## Phase 0 pipeline

Five resumable HyperSync streams (checkpointed part files under `data/raw/hypersync/`):
four `OrderFilled` streams — **V1 CTF, V1 NegRisk, V2 CTF, V2 NegRisk** (the NegRisk
exchanges carry the multi-outcome politics markets; V1 and V2 have different event
signatures) — plus the Conditional Tokens contract for `ConditionPreparation` /
`ConditionResolution` / `PositionSplit` / `PositionsMerge` / `PayoutRedemption`.
Market metadata comes from Gamma's keyset-paginated `/markets/keyset` (dated snapshots
under `data/raw/gamma/`, cursor-stall guard included). `scripts/build_curated.py`
decodes both generations into one labeled-trade schema, dedups on `(tx_hash, log_index)`,
and joins metadata; `scripts/verify_phase0.py` is the phase's pass/fail gate.

### Network + credentials needed for live runs

- Outbound HTTPS to `gamma-api.polymarket.com` and `polygon.hypersync.xyz`
  (in Claude Code web sessions these must be on the environment's network allowlist).
- `HYPERSYNC_BEARER_TOKEN` in `.env` (see `.env.example`) for the chain backfill.

## Survivorship-bias note

The metadata ingest will keep created-but-unresolved and delisted markets as an explicit separate cohort rather than silently dropping them. Cohort definitions and counts will be documented here once Phase 0 lands.
