"""Politics-only retention filters for the dense 2025-26 era (disk constraint).

The full all-markets raw stream fits on disk only through ~Aug 2025; beyond
that, fill volume reaches 6-9.5M/day and the all-markets dataset would need
~100GB. Politics markets are ~5.6% of fills, and Phases 1-2 are politics-only
analyses by pre-registration, so later ranges retain only fills whose tokenId
belongs to a politics market (per the Gamma snapshots). Rows are filtered by
membership only — kept rows stay byte-identical raw. Non-politics history is
NOT destroyed anywhere: it remains re-pullable from HyperSync; each stream's
`_retention.json` records exactly which block ranges are full vs politics-only.

CTF events: ConditionPreparation / ConditionResolution are always kept (small,
and resolution records are useful metadata for every market). PositionSplit /
PositionsMerge / PayoutRedemption are filtered to politics condition ids.
"""

from __future__ import annotations

from pathlib import Path
from typing import Callable

import polars as pl

from ingest.contracts import CTF_TOPIC0

WORD = 64

KEEP_ALL_CTF_TOPICS = {
    CTF_TOPIC0["ConditionPreparation"],
    CTF_TOPIC0["ConditionResolution"],
}


def _norm_word(word_hex: str) -> str:
    stripped = word_hex.lstrip("0")
    return "0x" + (stripped.lower() if stripped else "0")


def load_politics_sets(markets_parquet: Path) -> tuple[set[str], set[str]]:
    """(politics token_id_hex set, politics condition_id set) from curated markets."""
    m = pl.read_parquet(
        markets_parquet, columns=["is_politics", "condition_id", "clob_token_ids"]
    ).filter(pl.col("is_politics"))
    conditions = {c for c in m["condition_id"].to_list() if c}
    tokens: set[str] = set()
    for ids in m["clob_token_ids"].to_list():
        for dec in ids or []:
            if dec:
                tokens.add(hex(int(dec)))
    return tokens, conditions


def orderfilled_row_filter(generation: int, token_set: set[str]) -> Callable[[dict], bool]:
    """Keep OrderFilled rows whose tokenId is a politics token.

    V1 (no explicit side): tokenId is makerAssetId (word 0) unless the maker
    paid collateral (word 0 == 0), in which case it's takerAssetId (word 1).
    V2: tokenId is always word 1.
    """

    def keep_v1(row: dict) -> bool:
        data = row["data"]
        w0 = data[2 : 2 + WORD]
        token_word = data[2 + WORD : 2 + 2 * WORD] if w0.lstrip("0") == "" else w0
        return _norm_word(token_word) in token_set

    def keep_v2(row: dict) -> bool:
        return _norm_word(row["data"][2 + WORD : 2 + 2 * WORD]) in token_set

    return keep_v1 if generation == 1 else keep_v2


def ctf_row_filter(condition_set: set[str]) -> Callable[[dict], bool]:
    """Keep all preparations/resolutions; filter split/merge/redemption to politics."""
    split_merge = {CTF_TOPIC0["PositionSplit"], CTF_TOPIC0["PositionsMerge"]}
    redemption = CTF_TOPIC0["PayoutRedemption"]

    def keep(row: dict) -> bool:
        t0 = row["topic0"]
        if t0 in KEEP_ALL_CTF_TOPICS:
            return True
        if t0 in split_merge:
            return (row["topic3"] or "").lower() in condition_set
        if t0 == redemption:
            # conditionId is NOT indexed on PayoutRedemption: data word 0
            return ("0x" + row["data"][2 : 2 + WORD].lower()) in condition_set
        return False

    return keep
