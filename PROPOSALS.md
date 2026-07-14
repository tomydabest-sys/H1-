# PROPOSALS — phased plan

Living document. Updated as phases are planned, approved, implemented, and verified.

## Background (settled, not re-derived here)

Prior research replicated an academic politics-market calibration slope on the Becker archive (Jon Becker's `prediction-market-analysis` dataset: ~7.68M markets, ~72.1M trades of Kalshi + Polymarket data) and ran a pre-registered out-of-sample peek on markets that resolved in 2026. The surviving hypothesis is:

> **H1-revised:** there is a long-horizon YES premium in typical Polymarket politics markets — at 1-week-plus horizons before resolution, the NO side carries roughly 5–10 gross percentage points of edge (before costs/fees).

The Becker archive lacked sufficient historical trade coverage for a proper Stage 1 gate, and two data obstacles blocked extension: Gamma `/markets`' historical ~10,000 offset ceiling, and the **2026-04-28 CLOB V2 migration**, which deprecated the public subgraph (incomplete/incorrect data post-migration). This repo builds the extended dataset and runs the gate.

## Phase 0 — Data backfill

**Status: implementation landed (plan approved 2026-07-14); offline test suite passes.
Remaining for sign-off: live smoke run on both sides of the migration boundary +
verification output — blocked on (a) network allowlist for `gamma-api.polymarket.com`
and `polygon.hypersync.xyz` in this environment, (b) HyperSync API token.**

Amendments vs. the original spec, surfaced during exploration (not silently resolved):
- **Four exchanges, not two:** added V1 NegRisk (`0xC5d5…f80a`) and V2 NegRisk
  (`0xe2222d…310F59`) — multi-outcome politics markets trade there.
- **Two OrderFilled decoders:** V2's event has a different signature (explicit
  `side` uint8 + single `tokenId` + builder/metadata bytes32s).
- Added `ConditionResolution`/`ConditionPreparation` to the CTF event set —
  the authoritative, leakage-safe resolution record.
- Verify-at-smoke-run items flagged in `src/normalize/decode.py`: V2 side-mapping
  direction (price-range check catches a flip), V2 collateral (pUSD/PMCT) decimals,
  taker-aggregate row pattern per generation.

- Goal: complete, deduplicated, resumable Parquet dataset of Polymarket politics-market trades and resolutions spanning both sides of the 2026-04-28 CLOB V2 migration.
- Source: Envio HyperSync (`hypersync-client-python`, Polygon endpoint `https://polygon.hypersync.xyz`) streaming `OrderFilled` from both exchange generations:
  - V2 CTF Exchange: `0xE111180000d2663C0091e4f400237545B87B996B`
  - V1 CTF Exchange: `0x4bfb41d5b3570defd03c39a9a4d8de6bd8b8982e`
  - CTF (Conditional Tokens) `0x4D97DCd97eC945f40cF65F87097ACe5EA0476045` for `PositionSplit` / `PositionsMerge` / `PayoutRedemption` (continuous across the migration; resolution/outcome data).
- Market metadata via Gamma keyset/cursor pagination (`next_cursor` → `after_cursor`), pulling `createdAt`, `resolvedAt`, `closed`, category/tags. Unresolved/delisted markets kept as an explicit cohort (survivorship bias), documented in README.
- Reference implementation: `warproxxx/poly_data` — adapt, verify its assumptions, note where they no longer hold.
- Rate limiting: exponential backoff with jitter, honor `Retry-After`, headroom below documented limits.
- Verification: script reporting total events ingested, date range covered on each side of the migration boundary, count of politics-market conditions, and an idempotency check (trade counts per market non-negative and monotonically non-decreasing across re-runs).
- **Hard stop:** show verification output and get sign-off before Phase 1.

## Phase 1 — Calibration / fair-value pipeline extension

**Status: not started.**

- Replicate the logistic recalibration slope `b` (`b > 1` = underconfidence / favorite-longshot bias) on the Phase 0 dataset. Resolved markets only; exclude voided/delisted and thin markets (minimum-notional threshold to be flagged for approval, not chosen silently).
- Compute mispricing `δ` = realized outcome − implied probability, and gross excess return by price band, matching the prior Becker-archive methodology.
- Explicitly compute and report maker/taker asymmetry (Becker finding: ~1.12pp gross average excess return in takers' disfavor) on the new dataset — this feeds the Phase 2 cost haircut.
- Verification: calibration slope replication reproduces the qualitative prior result (underconfidence, `b > 1`, politics markets). If not, stop — do not proceed to Phase 2 on a broken premise.

## Phase 2 — Stage 1 gate

**Status: not started. Blocked on `PREREGISTRATION.md` being authored, timestamped, and git-tagged first.**

- Before any test code: write `PREREGISTRATION.md` with the hypothesis, pass condition (NO-side gross edge in the 5–10pp band on the out-of-sample 2026-resolved cohort, t-stat ≥ 2, survives the Phase-1-informed fee/slippage haircut), kill conditions (gross edge < 5pp; sign flip; t-stat < 2; effect disappears after cost haircut; calibration-slope replication failure), and the definition of "inconclusive" (e.g. 3–5pp edge → widen dataset/horizons, no verdict).
- Then: bucket by time-to-resolution (≥7 days primary; shorter horizons for context), compute gross NO-side edge per bucket out-of-sample, apply the cost haircut, report PASS/FAIL/INCONCLUSIVE against the pre-registered thresholds with no post-hoc adjustment.
- Verification: report with gross edge, t-stat, cost-adjusted edge, explicit verdict, and the git commit hash of `PREREGISTRATION.md` proving it predates the test code.
