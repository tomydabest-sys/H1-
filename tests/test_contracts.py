"""Topic0 constants: the V1 hash is pinned to the independently documented value."""

from ingest import contracts

# Widely documented topic0 for the V1 CTF Exchange OrderFilled event.
KNOWN_V1_ORDER_FILLED_TOPIC0 = (
    "0xd0a08e8c493f9c94f29311604c9de1b4e8c8d4c06bd0c789af57f2d65bfec0f6"
)


def test_v1_orderfilled_topic0_matches_documented_value():
    assert contracts.TOPIC0_ORDER_FILLED_V1 == KNOWN_V1_ORDER_FILLED_TOPIC0


def test_all_topic0s_are_32_byte_hex():
    topics = [
        contracts.TOPIC0_ORDER_FILLED_V1,
        contracts.TOPIC0_ORDER_FILLED_V2,
        *contracts.CTF_TOPIC0.values(),
    ]
    for t in topics:
        assert t.startswith("0x") and len(t) == 66, t


def test_v1_and_v2_signatures_differ():
    assert contracts.TOPIC0_ORDER_FILLED_V1 != contracts.TOPIC0_ORDER_FILLED_V2


def test_stream_registry_covers_all_four_exchanges_plus_ctf():
    assert set(contracts.STREAMS) == {
        "orderfilled_ctf_v1",
        "orderfilled_negrisk_v1",
        "orderfilled_ctf_v2",
        "orderfilled_negrisk_v2",
        "ctf_events",
    }
    gens = {s.name: s.generation for s in contracts.STREAMS.values()}
    assert gens["orderfilled_ctf_v1"] == gens["orderfilled_negrisk_v1"] == 1
    assert gens["orderfilled_ctf_v2"] == gens["orderfilled_negrisk_v2"] == 2
    assert gens["ctf_events"] is None
