"""Events by-id fallback: resumes after the keyset tail, bisects 500s, records failures."""

import json

import httpx
import pytest
from tenacity import wait_none

from ingest import gamma_markets
from ingest.gamma_markets import _write_page, run_events_by_id, snapshot_dir


@pytest.fixture(autouse=True)
def no_retry_wait(monkeypatch):
    monkeypatch.setattr(gamma_markets._fetch_events_by_id.retry, "wait", wait_none())

EXISTING = {1, 2, 3, 10, 12, 15, 20, 21}  # ids served by the fake /events
BROKEN = 12  # any request containing this id returns 500


def handler(request: httpx.Request) -> httpx.Response:
    ids = [int(v) for k, v in request.url.params.multi_items() if k == "id"]
    if BROKEN in ids:
        return httpx.Response(500)
    return httpx.Response(200, json=[{"id": str(i), "tags": []} for i in ids if i in EXISTING])


def client():
    return httpx.Client(transport=httpx.MockTransport(handler))


def _seed_keyset_pages(data_root):
    sdir = snapshot_dir(data_root, "t", endpoint="events")
    sdir.mkdir(parents=True)
    _write_page(sdir, 0, [{"id": "1"}, {"id": "2"}, {"id": "3"}])
    (sdir / "_checkpoint.json").write_text(json.dumps({"next_page_index": 1}))
    return sdir


def _all_ids(sdir):
    import polars as pl

    return sorted(int(i) for i in pl.read_parquet(sorted(sdir.glob("page-*.parquet")))["market_id"])


def test_byid_completes_tail_and_records_failures(data_root):
    sdir = _seed_keyset_pages(data_root)
    stats = run_events_by_id(data_root, "t", client=client(), batch=4, stop_after_empty_ids=8)
    assert stats["status"] == "complete"
    assert _all_ids(sdir) == [1, 2, 3, 10, 15, 20, 21]  # 12 is broken, nothing duplicated
    done = json.loads((sdir / "_COMPLETE").read_text())
    assert done["failed_ids"] == [BROKEN]
    assert done["byid_id_range"] == [4, 21]


def test_byid_resumes_from_checkpoint(data_root):
    sdir = _seed_keyset_pages(data_root)
    run_events_by_id(data_root, "t", client=client(), batch=4, stop_after_empty_ids=8, max_batches=2)
    assert not (sdir / "_COMPLETE").exists()
    run_events_by_id(data_root, "t", client=client(), batch=4, stop_after_empty_ids=8)
    assert _all_ids(sdir) == [1, 2, 3, 10, 15, 20, 21]
