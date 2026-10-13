#!/usr/bin/env bash
# Persist Phase 0 data to the orphan branch `phase0-data` so an idle container
# reclaim can't wipe it (user decision 2026-10-09, option A; no GCS bucket).
#
# Append-only and disk-light: a throwaway git dir fetches only the branch tip's
# commit + trees (no blobs), stages new/changed files straight from data/, and
# commits + pushes in chunks under GitHub's 2GB-per-push limit. No second copy
# of the data is kept. Files >= 95MB are skipped with a warning (GitHub's limit is 100MB).
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

export GIT_INDEX_FILE="$GD/persist.index"
HAVE_PARENT=0
if g fetch -q --filter=blob:none --depth=1 origin "$BRANCH" 2>/dev/null; then
  g update-ref "refs/heads/$BRANCH" FETCH_HEAD
  HAVE_PARENT=1
else
  echo "branch $BRANCH not on remote yet — creating it"
fi
# Never lazily download blobs: anything that needs old contents must fail fast
# instead of silently pulling the whole branch into $GD (seen 2026-10-13: 5GB).
g config --unset-all remote.origin.promisor || true
g config --unset-all extensions.partialclone || true

paths=()
for p in raw/hypersync curated; do [ -e "$DATA/$p" ] && paths+=("$p"); done
[ ${#paths[@]} -gt 0 ] || { echo "nothing to persist"; exit 0; }

# path -> blob oid on the branch tip (trees only; no blobs needed)
declare -A remote_oid=()
if [ "$HAVE_PARENT" = 1 ]; then
  while IFS=$'\t' read -r meta path; do remote_oid["$path"]="${meta##* }"; done \
    < <(g ls-tree -r "refs/heads/$BRANCH")
fi

# New files, plus small manifests (*.json) whose content changed. Parquet parts
# are write-once (temp + rename), so an existing path is never re-sent.
files=()
while IFS= read -r f; do
  case "$f" in *.tmp|*/.gitkeep) continue ;; esac
  if [ -z "${remote_oid[$f]:-}" ]; then
    files+=("$f")
  elif [[ "$f" == *.json ]] && [ "$(g hash-object -- "$DATA/$f")" != "${remote_oid[$f]}" ]; then
    files+=("$f")
  fi
done < <(cd "$DATA" && find "${paths[@]}" -type f | sort)
echo "${#files[@]} new/changed file(s)"
[ ${#files[@]} -gt 0 ] || exit 0

keep=()
for f in "${files[@]}"; do
  sz=$(stat -c %s "$DATA/$f")
  if [ "$sz" -ge "$MAX_FILE" ]; then
    echo "SKIPPING: $f is $sz bytes (>= 95MB GitHub limit) — not persisted" >&2
  else
    keep+=("$f")
  fi
done
files=("${keep[@]}")
[ ${#files[@]} -gt 0 ] || exit 0

# Plumbing only (read-tree / update-index / write-tree / commit-tree): none of
# these read existing blobs, unlike porcelain `git commit`.
if [ "$HAVE_PARENT" = 1 ]; then g read-tree "refs/heads/$BRANCH"; else g read-tree --empty; fi

push_chunk() {
  local tree commit
  # --missing-ok: old blobs are deliberately absent (blobless fetch); without it
  # write-tree lazily downloads every blob on the branch (seen: 61k objects, 6.6GB).
  tree=$(g write-tree --missing-ok)
  local msg="phase0 data snapshot $(date -u +%Y-%m-%dT%H:%MZ) ($1 files, $2 bytes)"
  if [ "$HAVE_PARENT" = 1 ]; then
    commit=$(echo "$msg" | g commit-tree "$tree" -p "refs/heads/$BRANCH")
  else
    commit=$(echo "$msg" | g commit-tree "$tree")
  fi
  g update-ref "refs/heads/$BRANCH" "$commit"
  HAVE_PARENT=1
  for i in 1 2 3 4 5; do
    if g push -q origin "refs/heads/$BRANCH:refs/heads/$BRANCH"; then echo "pushed $1 files ($2 bytes)"; return 0; fi
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
  (cd "$DATA" && g update-index --add -- "$f")
  batch=$((batch + 1)); bytes=$((bytes + sz))
done
[ "$batch" -gt 0 ] && push_chunk "$batch" "$bytes"
echo "done: $(g rev-parse --short "refs/heads/$BRANCH") on $BRANCH"
