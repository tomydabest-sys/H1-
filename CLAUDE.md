# CLAUDE.md — H1 Polymarket Research Repo

## Operating rules

1. **Explore → Plan → Implement → Commit.** For each phase below, first explore the relevant APIs/data/code, then produce a written plan (use plan mode) and stop for my approval before writing code that touches more than a handful of files. Do not skip straight to implementation on any phase.
2. **Every phase needs a verification command.** I should be able to run one script or test suite and get a pass/fail, not just your assertion that it worked. Show me the actual command output, not a summary of what you expect it to say.
3. **No look-ahead leakage.** Any feature or label used in the calibration/fair-value/gate code must only use information available strictly before the point in time it's being applied to. Flag anywhere this is even slightly ambiguous rather than resolving it silently.
4. **No post-hoc rationalization of the gate.** The kill-criteria for the Stage 1 gate are fixed in `PREREGISTRATION.md` before you write the test code for Phase 2, and you do not change them after seeing results. If you think a threshold is wrong, tell me — don't quietly adjust it.
5. **Resumable, idempotent ingestion.** All backfill jobs must checkpoint (last block scanned / last cursor) so they can be killed and restarted without re-downloading everything or double-counting.
6. **Package manager: `uv`.** Storage format: Parquet for raw/interim/curated data, queried via DuckDB. Don't introduce a database server or other heavyweight storage without asking first.
7. **Never mutate `data/raw/`.** Raw ingested data is append-only and treated as ground truth; all transformations happen downstream into `data/interim/` and `data/curated/`.
8. **Ask before any live-money or auth-required action.** This is a research/backfill repo. Do not add order-placement, wallet-signing, or live-trading code without explicit instruction.

## Commands

- `uv sync` — install/refresh the environment.
- `uv run pytest` — run the test suite (schema checks, no-leakage checks, gate threshold checks).

### Phase 0 (data backfill)

- `uv run python scripts/backfill_gamma.py` — pull a Gamma market-metadata snapshot (keyset pagination; resumable; no auth).
- `uv run python scripts/backfill_gamma.py --endpoint events` — pull the Gamma events snapshot; REQUIRED for tags/categories (the keyset markets payload does not embed tags — politics classification joins tags from events).
- `uv run python scripts/backfill_chain.py --stream all [--to-block N]` — HyperSync backfill of the four OrderFilled streams + CTF events (resumable; requires `HYPERSYNC_BEARER_TOKEN` in `.env`, see `.env.example`).
- `uv run python scripts/build_curated.py` — decode raw events, dedup, join metadata → `data/curated/`.
- `uv run python scripts/verify_phase0.py` — Phase 0 verification report (pass/fail exit code).

## Kill criteria

The Stage 1 gate's pass/kill/inconclusive thresholds live in `PREREGISTRATION.md`. They are written and git-tagged **before** Phase 2 test code exists, and are never edited after results are seen. See `PROPOSALS.md` for the phased plan and current status.
