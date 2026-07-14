"""Contract addresses, deployment floors, and event signatures for both CLOB generations.

Four fill-emitting exchanges span the 2026-04-28 CLOB V2 migration. Multi-outcome
politics markets trade on the NegRisk exchanges, so omitting them would silently
drop most of the H1 politics cohort. The Conditional Tokens contract is unchanged
across the migration and provides resolution/outcome events.

V1 and V2 OrderFilled have different signatures (different topic0):
  V1 has makerAssetId/takerAssetId and no side field (side is derived from
  makerAssetId == 0, meaning the maker paid collateral, i.e. maker BUY).
  V2 has an explicit uint8 side (BUY=0/SELL=1), a single tokenId, and two
  trailing bytes32 fields (builder, metadata).
"""

from dataclasses import dataclass, field

import hypersync

POLYGON_HYPERSYNC_URL = "https://polygon.hypersync.xyz"

# Polygon reorg headroom: never ingest closer than this to the chain tip.
CONFIRMATIONS = 200

CTF_EXCHANGE_V1 = "0x4bfb41d5b3570defd03c39a9a4d8de6bd8b8982e"
NEGRISK_EXCHANGE_V1 = "0xc5d563a36ae78145c45a50134d48a1215220f80a"
CTF_EXCHANGE_V2 = "0xe111180000d2663c0091e4f400237545b87b996b"
NEGRISK_EXCHANGE_V2 = "0xe2222d279d744050d28e00520010520000310f59"
CONDITIONAL_TOKENS = "0x4d97dcd97ec945f40cf65f87097ace5ea0476045"

SIG_ORDER_FILLED_V1 = (
    "OrderFilled(bytes32,address,address,uint256,uint256,uint256,uint256,uint256)"
)
SIG_ORDER_FILLED_V2 = (
    "OrderFilled(bytes32,address,address,uint8,uint256,uint256,uint256,uint256,bytes32,bytes32)"
)

CTF_EVENT_SIGS = {
    "ConditionPreparation": "ConditionPreparation(bytes32,address,bytes32,uint256)",
    "ConditionResolution": "ConditionResolution(bytes32,address,bytes32,uint256,uint256[])",
    "PositionSplit": "PositionSplit(address,address,bytes32,bytes32,uint256[],uint256)",
    "PositionsMerge": "PositionsMerge(address,address,bytes32,bytes32,uint256[],uint256)",
    "PayoutRedemption": "PayoutRedemption(address,address,bytes32,bytes32,uint256[],uint256)",
}

TOPIC0_ORDER_FILLED_V1 = hypersync.signature_to_topic0(SIG_ORDER_FILLED_V1)
TOPIC0_ORDER_FILLED_V2 = hypersync.signature_to_topic0(SIG_ORDER_FILLED_V2)
CTF_TOPIC0 = {
    name: hypersync.signature_to_topic0(sig) for name, sig in CTF_EVENT_SIGS.items()
}

# Deployment floors are conservative (earlier than actual deployment is harmless:
# HyperSync skips empty address/topic ranges nearly for free; starting late loses
# data). V2 start block is the known deployment block used by reference indexers.
V1_CTF_FROM_BLOCK = 33_000_000  # exchange deployed Sep 2022
V1_NEGRISK_FROM_BLOCK = 33_000_000  # deployed Nov 2023; shared floor for safety
V2_FROM_BLOCK = 84_902_353  # V2 deployment (2026-04-28 migration)
CTF_FROM_BLOCK = 4_000_000  # Conditional Tokens deployed Sep 2020


@dataclass(frozen=True)
class StreamSpec:
    """One resumable HyperSync log stream (address set + topic0 set + block floor)."""

    name: str
    addresses: tuple[str, ...]
    topic0s: tuple[str, ...]
    from_block: int
    generation: int | None = None  # 1/2 for OrderFilled streams, None for CTF events
    extra: dict = field(default_factory=dict)


STREAMS: dict[str, StreamSpec] = {
    "orderfilled_ctf_v1": StreamSpec(
        name="orderfilled_ctf_v1",
        addresses=(CTF_EXCHANGE_V1,),
        topic0s=(TOPIC0_ORDER_FILLED_V1,),
        from_block=V1_CTF_FROM_BLOCK,
        generation=1,
    ),
    "orderfilled_negrisk_v1": StreamSpec(
        name="orderfilled_negrisk_v1",
        addresses=(NEGRISK_EXCHANGE_V1,),
        topic0s=(TOPIC0_ORDER_FILLED_V1,),
        from_block=V1_NEGRISK_FROM_BLOCK,
        generation=1,
    ),
    "orderfilled_ctf_v2": StreamSpec(
        name="orderfilled_ctf_v2",
        addresses=(CTF_EXCHANGE_V2,),
        topic0s=(TOPIC0_ORDER_FILLED_V2,),
        from_block=V2_FROM_BLOCK,
        generation=2,
    ),
    "orderfilled_negrisk_v2": StreamSpec(
        name="orderfilled_negrisk_v2",
        addresses=(NEGRISK_EXCHANGE_V2,),
        topic0s=(TOPIC0_ORDER_FILLED_V2,),
        from_block=V2_FROM_BLOCK,
        generation=2,
    ),
    "ctf_events": StreamSpec(
        name="ctf_events",
        addresses=(CONDITIONAL_TOKENS,),
        topic0s=tuple(CTF_TOPIC0.values()),
        from_block=CTF_FROM_BLOCK,
        generation=None,
    ),
}

# Exchange address per stream, for flagging rows where the recorded taker is the
# exchange itself (the per-match taker-order aggregate row; see decode notes).
STREAM_EXCHANGE_ADDRESS = {
    "orderfilled_ctf_v1": CTF_EXCHANGE_V1,
    "orderfilled_negrisk_v1": NEGRISK_EXCHANGE_V1,
    "orderfilled_ctf_v2": CTF_EXCHANGE_V2,
    "orderfilled_negrisk_v2": NEGRISK_EXCHANGE_V2,
}

# CLOB V2 migration cutover (2026-04-28 ~11:00 UTC), unix seconds.
MIGRATION_TS = 1_777_374_000
