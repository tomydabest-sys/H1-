"""The runner must stop cleanly (checkpoint intact) when disk headroom vanishes."""

import asyncio

import pytest

from ingest import hypersync_backfill as hb
from test_backfill_checkpoint import RESPONSES, SPEC, FakeClient


def test_disk_guard_aborts_resumably(data_root, monkeypatch):
    class FakeUsage:
        free = 1 * 1024**3  # below MIN_FREE_BYTES

    monkeypatch.setattr(hb.shutil, "disk_usage", lambda _: FakeUsage)
    client = FakeClient(RESPONSES, height=10_000)
    with pytest.raises(hb.DiskSpaceError):
        asyncio.run(hb.run_stream(SPEC, data_root=data_root, client=client, to_block=300, log_every=0))
    # nothing ingested, no checkpoint corruption — a later run starts fresh
    assert hb.load_checkpoint(hb.stream_dir(data_root, SPEC.name)) is None
