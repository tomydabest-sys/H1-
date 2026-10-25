#!/usr/bin/env bash
# Unattended Phase 0 driver: wait for the Gamma snapshots, build curated metadata,
# then run all five chain streams in parallel (politics-only, to PINNED_END_BLOCK)
# while persisting to the phase0-data branch every PERSIST_EVERY seconds.
# Every step is resumable, so the whole script is safe to re-run after a restart.
#
# Usage: nohup scripts/run_phase0_pipeline.sh > <log> 2>&1 &
set -uo pipefail
cd "$(dirname "$0")/.."
END_BLOCK="${END_BLOCK:-95199593}"
PERSIST_EVERY="${PERSIST_EVERY:-1800}"
LOGDIR="${LOGDIR:-data/interim/logs}"
mkdir -p "$LOGDIR"
ts() { date -u +%FT%TZ; }

# Gamma snapshots are only needed until curated/markets.parquet exists (raw
# Gamma was deleted 2026-10-25 for disk; the frozen curated table replaces it).
if [ ! -f data/curated/markets.parquet ]; then
  for ep in markets_open markets_closed events; do
    until ls data/raw/gamma/$ep/snapshot=*/_COMPLETE >/dev/null 2>&1; do sleep 60; done
    echo "$(ts) gamma $ep complete"
  done
fi

if [ ! -f data/curated/markets.parquet ]; then
  echo "$(ts) build_curated (metadata)"
  uv run python scripts/build_curated.py --no-trades || { echo "$(ts) build_curated FAILED"; exit 1; }
fi
scripts/persist_data.sh || echo "$(ts) persist failed (will retry)"

streams=(orderfilled_ctf_v1 orderfilled_negrisk_v1 orderfilled_ctf_v2 orderfilled_negrisk_v2 ctf_events)
pids=()
for s in "${streams[@]}"; do
  uv run python scripts/backfill_chain.py --stream "$s" --to-block "$END_BLOCK" --politics-filter \
    > "$LOGDIR/chain_$s.log" 2>&1 &
  pids+=($!)
  echo "$(ts) started $s (pid $!)"
done

alive() { for p in "${pids[@]}"; do kill -0 "$p" 2>/dev/null && return 0; done; return 1; }
while alive; do
  for _ in $(seq 1 $((PERSIST_EVERY / 30))); do alive || break; sleep 30; done
  echo "$(ts) periodic persist"; scripts/persist_data.sh || echo "$(ts) persist failed (will retry)"
done

for i in "${!pids[@]}"; do
  wait "${pids[$i]}"; rc=$?; echo "$(ts) ${streams[$i]} exited with $rc"
done
scripts/persist_data.sh || echo "$(ts) final persist failed"
echo "$(ts) PIPELINE DONE"
