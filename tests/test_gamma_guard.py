"""Gamma keyset ingest: pagination, resume, completion marker, and the cursor-stall guard."""

import httpx
import pytest

from ingest.gamma_markets import (
    GammaCursorStallError,
    latest_complete_snapshot,
    run_snapshot,
    snapshot_dir,
)


def market(i: int) -> dict:
    return {"id": i, "question": f"q{i}", "conditionId": f"0x{i:064x}"}


PAGES = {
    None: {"data": [market(1), market(2)], "next_cursor": "c1"},
    "c1": {"data": [market(3), market(4)], "next_cursor": "c2"},
    "c2": {"data": [market(5)]},  # no next_cursor => last page
}


def paged_handler(request: httpx.Request) -> httpx.Response:
    cursor = request.url.params.get("after_cursor")
    return httpx.Response(200, json=PAGES[cursor])


def make_client(handler) -> httpx.Client:
    return httpx.Client(transport=httpx.MockTransport(handler))


def test_full_snapshot(data_root):
    stats = run_snapshot(data_root, snapshot="t1", client=make_client(paged_handler))
    assert stats["status"] == "complete"
    assert stats["pages"] == 3
    assert stats["markets"] == 5
    sdir = snapshot_dir(data_root, "t1")
    assert (sdir / "_COMPLETE").exists()
    assert len(list(sdir.glob("page-*.parquet"))) == 3
    assert latest_complete_snapshot(data_root) == sdir


def test_rerun_complete_snapshot_is_noop(data_root):
    run_snapshot(data_root, snapshot="t1", client=make_client(paged_handler))
    stats = run_snapshot(data_root, snapshot="t1", client=make_client(paged_handler))
    assert stats["status"] == "already_complete"


def test_resume_after_pause(data_root):
    stats1 = run_snapshot(
        data_root, snapshot="t2", client=make_client(paged_handler), max_pages=1
    )
    assert stats1["status"] == "paused_max_pages"
    stats2 = run_snapshot(data_root, snapshot="t2", client=make_client(paged_handler))
    assert stats2["status"] == "complete"
    sdir = snapshot_dir(data_root, "t2")
    files = sorted(p.name for p in sdir.glob("page-*.parquet"))
    assert files == ["page-00000.parquet", "page-00001.parquet", "page-00002.parquet"]


def test_cursor_stall_aborts_loudly(data_root):
    def stalled(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200, json={"data": [market(1), market(2)], "next_cursor": "c1"}
        )

    with pytest.raises(GammaCursorStallError):
        run_snapshot(data_root, snapshot="t3", client=make_client(stalled))
    assert not (snapshot_dir(data_root, "t3") / "_COMPLETE").exists()


def test_server_errors_are_retried(data_root):
    calls = {"n": 0}

    def flaky(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        if calls["n"] == 1:
            return httpx.Response(500, text="boom")
        return paged_handler(request)

    stats = run_snapshot(data_root, snapshot="t4", client=make_client(flaky))
    assert stats["status"] == "complete"
    assert calls["n"] == 4  # one retry + three pages
