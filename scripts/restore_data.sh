#!/usr/bin/env bash
# Restore data/ from the orphan branch `phase0-data` (written by persist_data.sh).
#
# A throwaway git dir does the fetch + checkout, so the working branch's index
# and object store are never touched. Curated tables stored as byte-exact
# pieces (<file>.split/000, 001, ...) are joined back into <file>.
#
# Usage: scripts/restore_data.sh
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
DATA="${PERSIST_DATA:-$ROOT/data}"
BRANCH="${PERSIST_BRANCH:-phase0-data}"
REMOTE="${PERSIST_REMOTE:-$(git -C "$ROOT" remote get-url origin)}"
GD="$(mktemp -d)/restore.git"
trap 'rm -rf "$(dirname "$GD")"' EXIT

mkdir -p "$DATA"
git init -q --bare "$GD"
git --git-dir="$GD" fetch -q --depth=1 "$REMOTE" "$BRANCH"
GIT_INDEX_FILE="$GD/restore.index" git --git-dir="$GD" --work-tree="$DATA" checkout -q FETCH_HEAD -- .

while IFS= read -r d; do
  out="${d%.split}"
  n=$(find "$d" -maxdepth 1 -type f -name '[0-9][0-9][0-9]' | wc -l)
  cat "$d"/[0-9][0-9][0-9] >"$out.tmp"
  mv "$out.tmp" "$out"
  rm -rf "$d"
  echo "joined $n piece(s) -> ${out#"$DATA"/}"
done < <(find "$DATA" -type d -name '*.split' | sort)
echo "restored $BRANCH into $DATA"
