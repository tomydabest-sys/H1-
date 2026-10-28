#!/usr/bin/env bash
# Persist Phase 0 data to the orphan branch `phase0-data` so an idle container
# reclaim can't wipe it (user decision 2026-10-09, option A; no GCS bucket).
#
# Append-only and disk-light: a throwaway git dir fetches only the branch tip's
# commit + trees (no blobs), stages new/changed files straight from data/, and
# commits + pushes in chunks under GitHub's 2GB-per-push limit. No second copy
# of the data is kept.
#
# Persisted: data/raw/hypersync/** (incl. checkpoints + _retention.json) and
# data/curated/**. Gamma raw snapshots are deleted (2026-10-25 amendment); the
# market metadata is frozen in curated/markets.parquet.
#
# Curated tables are rewritten by every build, so they are re-sent whenever
# their content changes (raw parts are write-once; only new ones are sent).
# Curated files >= 95MB (GitHub's limit is 100MB) are stored as byte-exact
# 90MB pieces <file>.split/000, 001, ... that scripts/restore_data.sh joins.
# Raw files >= 95MB are skipped with a warning (none exist: parts are <2MB).
#
# Usage:   scripts/persist_data.sh            # push whatever is new or changed
# Restore: scripts/restore_data.sh
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
DATA="${PERSIST_DATA:-$ROOT/data}"
BRANCH="${PERSIST_BRANCH:-phase0-data}"
CHUNK_BYTES="${PERSIST_CHUNK_BYTES:-1500000000}"
MAX_FILE=$((${PERSIST_MAX_FILE_MB:-95} * 1024 * 1024))
PIECE_MB="${PERSIST_PIECE_MB:-90}"
PIECE=$((PIECE_MB * 1024 * 1024))
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

split_paths=()
for path in "${!remote_oid[@]}"; do [[ "$path" == *.split/* ]] && split_paths+=("$path"); done

# Blob oid of piece $2 of file $1 (extra args go to hash-object, e.g. -w).
piece_oid() {
  dd if="$DATA/$1" bs=1M skip=$(($2 * PIECE_MB)) count="$PIECE_MB" status=none \
    | g hash-object --stdin "${@:3}"
}

# entries: "file<TAB>path" or "piece<TAB>src<TAB>index<TAB>piece path"
entries=()
removals=()
while IFS= read -r f; do
  case "$f" in *.tmp|*/.gitkeep) continue ;; esac
  sz=$(stat -c %s "$DATA/$f")
  stale=()
  if [[ "$f" == curated/* ]]; then
    for path in "${split_paths[@]}"; do [[ "$path" == "$f.split/"* ]] && stale+=("$path"); done
  fi
  if [ "$sz" -ge "$MAX_FILE" ]; then
    if [[ "$f" != curated/* ]]; then
      echo "SKIPPING: $f is $sz bytes (>= GitHub's 100MB limit) — not persisted" >&2
      continue
    fi
    n=$(((sz + PIECE - 1) / PIECE))
    changed=0
    [ -n "${remote_oid[$f]:-}" ] && { removals+=("$f"); changed=1; }  # was stored whole
    want=()
    for ((i = 0; i < n; i++)); do
      p="$f.split/$(printf %03d "$i")"
      want+=("$p")
      [ "${remote_oid[$p]:-}" = "$(piece_oid "$f" "$i")" ] || changed=1
    done
    for path in "${stale[@]}"; do
      [[ " ${want[*]} " == *" $path "* ]] || { removals+=("$path"); changed=1; }
    done
    if [ "$changed" = 1 ]; then
      for ((i = 0; i < n; i++)); do entries+=("piece"$'\t'"$f"$'\t'"$i"$'\t'"${want[$i]}"); done
    fi
    continue
  fi
  removals+=("${stale[@]}")  # was stored in pieces when it was larger
  if [ -z "${remote_oid[$f]:-}" ]; then
    entries+=("file"$'\t'"$f")
  elif [[ "$f" == *.json || "$f" == curated/* ]] && [ "$(g hash-object -- "$DATA/$f")" != "${remote_oid[$f]}" ]; then
    entries+=("file"$'\t'"$f")
  fi
done < <(cd "$DATA" && find "${paths[@]}" -type f | sort)
echo "${#entries[@]} new/changed file(s)/piece(s), ${#removals[@]} removal(s)"
[ ${#entries[@]} -gt 0 ] || [ ${#removals[@]} -gt 0 ] || exit 0

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

[ ${#removals[@]} -gt 0 ] && g update-index --force-remove -- "${removals[@]}"

batch=0; bytes=0
for e in "${entries[@]}"; do
  IFS=$'\t' read -r kind src idx piece <<<"$e"
  sz=$(stat -c %s "$DATA/$src")
  [ "$kind" = piece ] && sz=$((sz - idx * PIECE < PIECE ? sz - idx * PIECE : PIECE))
  if [ "$batch" -gt 0 ] && [ $((bytes + sz)) -gt "$CHUNK_BYTES" ]; then
    push_chunk "$batch" "$bytes"; batch=0; bytes=0
  fi
  if [ "$kind" = file ]; then
    (cd "$DATA" && g update-index --add -- "$src")
  else
    g update-index --add --cacheinfo "100644,$(piece_oid "$src" "$idx" -w),$piece"
  fi
  batch=$((batch + 1)); bytes=$((bytes + sz))
done
if [ "$batch" -gt 0 ] || [ ${#entries[@]} -eq 0 ]; then push_chunk "$batch" "$bytes"; fi
echo "done: $(g rev-parse --short "refs/heads/$BRANCH") on $BRANCH"
