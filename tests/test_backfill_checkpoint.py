"""Resumability/idempotency of the HyperSync runner (rule 5), with a scripted fake client."""

import asyncio
from types import SimpleNamespace

import polars as pl
import pytest

from ingest.contracts import StreamSpec
from ingest.hypersync_backfill import load_checkpoint, run_stream, stream_dir

SPEC = StreamSpec(
    name="teststream",
    addresses=("0x" + "11" * 20,),
    topic0s=("0x" + "22" * 32,),
    from_block=100,
    generation=1,
)


def fake_log(block: int, idx: int) -> SimpleNamespace:
    return SimpleNamespace(
        block_number=block,
        log_index=idx,
        transaction_hash=f"0x{block:064x}",
        address=SPEC.addresses[0],
        topics=[SPEC.topic0s[0], "0x" + "33" * 32, "0x" + "44" * 32, "0x" + "55" * 32],
        data="0x" + "00" * 32,
    )


def fake_block(number: int) -> SimpleNamespace:
    return SimpleNamespace(number=number, timestamp=hex(1_700_000_000 + number))


class FakeClient:
    """Serves scripted responses keyed by query.from_block; optionally crashes."""

    def __init__(self, responses: dict[int, SimpleNamespace], height: int, crash_at_call: int | None = None):
        self.responses = responses
        self.height = height
        self.crash_at_call = crash_at_call
        self.calls = 0

    async def get_height(self) -> int:
        return self.height

    async def get(self, query):
        self.calls += 1
        if self.crash_at_call is not None and self.calls >= self.crash_at_call:
            raise ConnectionError("simulated crash")
        return self.responses[query.from_block]


def resp(from_block: int, next_block: int, blocks_with_logs: list[int]) -> SimpleNamespace:
    logs = [fake_log(b, i) for b in blocks_with_logs for i in range(2)]
    return SimpleNamespace(
        next_block=next_block,
        archive_height=10_000,
        data=SimpleNamespace(logs=logs, blocks=[fake_block(b) for b in blocks_with_logs]),
    )


RESPONSES = {
    100: resp(100, 150, [110, 120]),
    150: resp(150, 200, [160]),
    200: resp(200, 301, [250, 300]),
}


def _read_all(data_root):
    parts = sorted(stream_dir(data_root, SPEC.name).glob("part-*.parquet"))
    return pl.read_parquet(parts) if parts else pl.DataFrame()


def test_full_run_writes_parts_and_checkpoint(data_root):
    client = FakeClient(RESPONSES, height=10_000)
    result = asyncio.run(
        run_stream(SPEC, data_root=data_root, client=client, to_block=300, log_every=0)
    )
    assert result.events == 10  # 5 blocks x 2 logs
    assert result.parts_written == 3
    assert load_checkpoint(stream_dir(data_root, SPEC.name)) == 300
    df = _read_all(data_root)
    assert df.height == 10
    assert df["block_timestamp"].min() == 1_700_000_000 + 110  # hex timestamps normalized


def test_crash_then_resume_no_duplicates(data_root):
    crashing = FakeClient(RESPONSES, height=10_000, crash_at_call=2)
    with pytest.raises(ConnectionError):
        asyncio.run(
            run_stream(SPEC, data_root=data_root, client=crashing, to_block=300, log_every=0)
        )
    sdir = stream_dir(data_root, SPEC.name)
    assert load_checkpoint(sdir) == 149  # first batch committed before the crash
    assert _read_all(data_root).height == 4

    healthy = FakeClient(RESPONSES, height=10_000)
    result = asyncio.run(
        run_stream(SPEC, data_root=data_root, client=healthy, to_block=300, log_every=0)
    )
    assert result.from_block == 150  # resumed from checkpoint, not from spec floor
    df = _read_all(data_root)
    assert df.height == 10
    assert df.unique(subset=["tx_hash", "log_index"]).height == 10
    assert load_checkpoint(sdir) == 300


def test_rerun_when_up_to_date_is_noop(data_root):
    client = FakeClient(RESPONSES, height=10_000)
    asyncio.run(run_stream(SPEC, data_root=data_root, client=client, to_block=300, log_every=0))
    before = _read_all(data_root)

    again = FakeClient({}, height=10_000)  # would KeyError if any query were issued
    result = asyncio.run(
        run_stream(SPEC, data_root=data_root, client=again, to_block=300, log_every=0)
    )
    assert result.events == 0 and result.parts_written == 0
    assert again.calls == 0
    assert _read_all(data_root).equals(before)


def test_respects_confirmation_headroom(data_root):
    # to_block beyond safe tip must be clamped: height 400 - 200 confirmations = 200
    responses = {100: resp(100, 201, [150])}
    client = FakeClient(responses, height=400)
    result = asyncio.run(
        run_stream(SPEC, data_root=data_root, client=client, to_block=10_000, log_every=0)
    )
    assert result.last_committed_block == 200
