#!/usr/bin/env bash
# Persist Phase 0 data to the orphan branch `phase0-data` so an idle container
# reclaim can't wipe it (user decision 2026-10-09, option A; no GCS bucket).
#
# Append-only and disk-light: a throwaway git dir fetches only the branch tip's
# commit + trees (no blobs), stages new/changed files straight from data/, and
# commits + pushes in chunks under GitHub's 2GB-per-push limit. No second copy
# of the data is kept. Files >= 95MB are refused (GitHub's limit is 100MB).
#
# Persisted: data/raw/hypersync/** (incl. checkpoints + _retention.json) and
# data/curated/**. Gamma raw snapshots are NOT persisted (~4GB, re-pullable;
# the politics classification used is frozen in curated/markets.parquet).
#
# Usage:   scripts/persist_data.sh            # push whatever is new
# Restore: git fetch origin phase0-data && git --work-tree=data checkout origin/phase0-data -- .
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
DATA="${PERSIST_DATA:-$ROOT/data}"
BRANCH="${PERSIST_BRANCH:-phase0-data}"
CHUNK_BYTES="${PERSIST_CHUNK_BYTES:-1500000000}"
MAX_FILE=$((95 * 1024 * 1024))
REMOTE="${PERSIST_REMOTE:-$(git -C "$ROOT" remote get-url origin)}"
GD="$(mktemp -d)/persist.git"
trap 'rm -rf "$(dirname "$GD")"' EXIT

g() { git --git-dir="$GD" --work-tree="$DATA" "$@"; }

git init -q --bare "$GD"
git --git-dir="$GD" config core.bare false
g remote add origin "$REMOTE"
g config user.name "$(git -C "$ROOT" config user.name || echo h1-persist)"
g config user.email "$(git -C "$ROOT" config user.email || echo h1-persist@localhost)"

if g fetch -q --filter=blob:none --depth=1 origin "$BRANCH" 2>/dev/null; then
  g update-ref "refs/heads/$BRANCH" FETCH_HEAD
  g symbolic-ref HEAD "refs/heads/$BRANCH"
  g read-tree HEAD
  # index from read-tree has no stat info; rehash locally (no blob fetch needed)
  (cd "$DATA" && g update-index -q --refresh >/dev/null || true)
else
  echo "branch $BRANCH not on remote yet — creating it"
  g symbolic-ref HEAD "refs/heads/$BRANCH"
fi

paths=()
for p in raw/hypersync curated; do [ -e "$DATA/$p" ] && paths+=("$p"); done
[ ${#paths[@]} -gt 0 ] || { echo "nothing to persist"; exit 0; }

# New or modified files under the persisted paths (deletions never staged: append-only).
mapfile -t files < <(cd "$DATA" && g ls-files --others --modified --exclude-standard -- "${paths[@]}" \
  | grep -v -e '\.tmp$' -e '/\.gitkeep$' | sort -u)
echo "${#files[@]} new/changed file(s)"
[ ${#files[@]} -gt 0 ] || exit 0

for f in "${files[@]}"; do
  sz=$(stat -c %s "$DATA/$f")
  if [ "$sz" -ge "$MAX_FILE" ]; then
    echo "REFUSING: $f is $sz bytes (>= 95MB GitHub limit) — split it first" >&2
    exit 1
  fi
done

push_chunk() {
  g commit -q -m "phase0 data snapshot $(date -u +%Y-%m-%dT%H:%MZ) ($1 files, $2 bytes)"
  for i in 1 2 3 4 5; do
    if g push -q origin "HEAD:refs/heads/$BRANCH"; then echo "pushed $1 files ($2 bytes)"; return 0; fi
    sleep $((2 ** i))
  done
  echo "push failed after retries" >&2; return 1
}

batch=0; bytes=0
for f in "${files[@]}"; do
  sz=$(stat -c %s "$DATA/$f")
  if [ "$batch" -gt 0 ] && [ $((bytes + sz)) -gt "$CHUNK_BYTES" ]; then
    push_chunk "$batch" "$bytes"; batch=0; bytes=0
  fi
  g add -f -- "$f"
  batch=$((batch + 1)); bytes=$((bytes + sz))
done
[ "$batch" -gt 0 ] && push_chunk "$batch" "$bytes"
echo "done: $(g rev-parse --short HEAD) on $BRANCH"
