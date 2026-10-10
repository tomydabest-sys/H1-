"""Gamma market-metadata ingest via keyset/cursor pagination (rule 5: resumable).

Legacy offset-paginated ``/markets`` was sunset 2026-05-01; the replacement is
``/markets/keyset`` with ``after_cursor``/``limit<=100`` and a ``next_cursor``
in each response. A bug reported in April 2026 (Polymarket/agents#227) had the
server ignoring the cursor and returning the first page forever — so this
ingester keeps a stall guard: if a page's first market id repeats the previous
page's, it aborts loudly instead of looping.

Each snapshot is a dated directory of append-only page Parquet files storing
the *full* raw JSON per market (no field selection at ingest time — rule 7:
raw is ground truth). A ``_COMPLETE`` marker closes a snapshot; re-running a
complete snapshot is a no-op, re-running an interrupted one resumes from the
checkpointed cursor.

Survivorship: no ``closed``/``active`` filter is applied — created-but-unresolved
and delisted markets are kept and cohorted downstream.
"""

from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
from tenacity import (
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential_jitter,
)

GAMMA_BASE = "https://gamma-api.polymarket.com"
# Live-verified 2026-07-15 (empirically — the OpenAPI spec claims no default
# filter, but reality disagrees):
#   * /markets/keyset silently excludes closed markets unless closed=true is
#     passed (default listing starts at id 540817; closed=true starts at id 12).
#     A resolution study needs the closed cohort above all, so markets are
#     pulled as two EXPLICIT cohorts, never via the implicit default.
#   * include_tag=true embeds top-level tags on each market; without it the
#     payload carries none. Kept on as belt-and-suspenders alongside the
#     events-snapshot tag join.
#   * /events/keyset appears unfiltered by default (closed 2021 events on
#     page 1) and events carry tags + category.
ENDPOINTS: dict[str, tuple[str, dict[str, str]]] = {
    "markets_open": (f"{GAMMA_BASE}/markets/keyset", {"closed": "false", "include_tag": "true"}),
    "markets_closed": (f"{GAMMA_BASE}/markets/keyset", {"closed": "true", "include_tag": "true"}),
    "events": (f"{GAMMA_BASE}/events/keyset", {}),
}
PAGE_LIMIT = 100

# Column is named market_id for historical reasons; for the events endpoint it
# holds the event id. Readers should key off raw_json.
PAGE_SCHEMA = pa.schema(
    [
        ("market_id", pa.string()),
        ("raw_json", pa.string()),
        ("page_index", pa.int64()),
        ("fetched_at", pa.string()),
    ]
)


class GammaCursorStallError(RuntimeError):
    """Server returned the same page again — cursor is being ignored (agents#227)."""


class GammaShapeError(RuntimeError):
    """Response JSON didn't match any known keyset shape."""


def _utcnow() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def snapshot_dir(data_root: Path, snapshot: str | None = None, endpoint: str = "markets_open") -> Path:
    snap = snapshot or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    return data_root / "raw" / "gamma" / endpoint / f"snapshot={snap}"


def _retryable(exc: BaseException) -> bool:
    if isinstance(exc, httpx.TransportError):
        return True
    if isinstance(exc, httpx.HTTPStatusError):
        code = exc.response.status_code
        return code == 429 or code >= 500
    return False


def _respect_retry_after(exc: BaseException) -> None:
    """Honor Retry-After when present (plus headroom) before tenacity's own wait."""
    if isinstance(exc, httpx.HTTPStatusError):
        ra = exc.response.headers.get("Retry-After")
        if ra:
            try:
                time.sleep(min(float(ra) + 1.0, 120.0))
            except ValueError:
                pass


@retry(
    retry=retry_if_exception(_retryable),
    wait=wait_exponential_jitter(initial=1, max=60),
    stop=stop_after_attempt(8),
    after=lambda rs: _respect_retry_after(rs.outcome.exception())
    if rs.outcome and rs.outcome.exception()
    else None,
    reraise=True,
)
def _fetch_page(
    client: httpx.Client, url: str, after_cursor: str | None, extra_params: dict[str, str] | None = None
) -> dict:
    params: dict[str, Any] = {**(extra_params or {}), "limit": PAGE_LIMIT}
    if after_cursor:
        params["after_cursor"] = after_cursor
    resp = client.get(url, params=params, timeout=30.0)
    resp.raise_for_status()
    return {"json": resp.json()}


def parse_page(js: Any) -> tuple[list[dict], str | None]:
    """Extract (markets, next_cursor) from the known keyset response shapes."""
    if isinstance(js, list):
        return js, None  # bare list: single page, no cursor
    if isinstance(js, dict):
        items = None
        for key in ("data", "markets", "events", "results"):
            if isinstance(js.get(key), list):
                items = js[key]
                break
        if items is None:
            raise GammaShapeError(f"no item list under data/markets/events/results: keys={list(js)[:10]}")
        cursor = js.get("next_cursor") or js.get("nextCursor") or None
        return items, cursor
    raise GammaShapeError(f"unexpected JSON type {type(js).__name__}")


def _load_checkpoint(sdir: Path) -> dict | None:
    f = sdir / "_checkpoint.json"
    return json.loads(f.read_text()) if f.exists() else None


def _save_checkpoint(sdir: Path, after_cursor: str | None, page_index: int, first_id: str | None) -> None:
    f = sdir / "_checkpoint.json"
    tmp = f.with_suffix(".json.tmp")
    tmp.write_text(
        json.dumps(
            {
                "after_cursor": after_cursor,
                "next_page_index": page_index,
                "last_first_id": first_id,
                "updated_at": _utcnow(),
            }
        )
    )
    os.replace(tmp, f)


def _write_page(sdir: Path, page_index: int, markets: list[dict]) -> Path:
    part = sdir / f"page-{page_index:05d}.parquet"
    tmp = part.with_name(part.name + ".tmp")
    now = _utcnow()
    rows = [
        {
            "market_id": str(m.get("id", "")),
            "raw_json": json.dumps(m, separators=(",", ":")),
            "page_index": page_index,
            "fetched_at": now,
        }
        for m in markets
    ]
    pq.write_table(pa.Table.from_pylist(rows, schema=PAGE_SCHEMA), tmp)
    os.replace(tmp, part)
    return part


def run_snapshot(
    data_root: Path,
    snapshot: str | None = None,
    client: httpx.Client | None = None,
    max_pages: int | None = None,
    endpoint: str = "markets_open",
) -> dict:
    """Pull (or resume) one full metadata snapshot. Returns summary stats."""
    if endpoint not in ENDPOINTS:
        raise ValueError(f"endpoint must be one of {sorted(ENDPOINTS)}")
    url, extra_params = ENDPOINTS[endpoint]
    own_client = client is None
    client = client or httpx.Client(headers={"User-Agent": "h1-polymarket-research/0.1"})
    sdir = snapshot_dir(data_root, snapshot, endpoint=endpoint)
    sdir.mkdir(parents=True, exist_ok=True)

    complete_marker = sdir / "_COMPLETE"
    if complete_marker.exists():
        return {"snapshot_dir": str(sdir), "status": "already_complete", "pages": 0, "markets": 0}

    ckpt = _load_checkpoint(sdir) or {}
    after_cursor: str | None = ckpt.get("after_cursor")
    page_index: int = int(ckpt.get("next_page_index", 0))
    last_first_id: str | None = ckpt.get("last_first_id")

    pages = 0
    markets_written = 0
    try:
        while True:
            js = _fetch_page(client, url, after_cursor, extra_params)["json"]
            items, next_cursor = parse_page(js)
            if not items:
                break
            first_id = str(items[0].get("id", ""))
            if last_first_id is not None and first_id == last_first_id:
                raise GammaCursorStallError(
                    f"page {page_index} repeats first market id {first_id!r} — "
                    "server is ignoring after_cursor (known bug, agents#227); aborting"
                )
            _write_page(sdir, page_index, items)
            markets_written += len(items)
            pages += 1
            last_first_id = first_id
            page_index += 1
            after_cursor = next_cursor
            _save_checkpoint(sdir, after_cursor, page_index, last_first_id)
            if next_cursor is None:
                break
            if max_pages is not None and pages >= max_pages:
                return {
                    "snapshot_dir": str(sdir),
                    "status": "paused_max_pages",
                    "pages": pages,
                    "markets": markets_written,
                }
        complete_marker.write_text(json.dumps({"finished_at": _utcnow(), "pages": page_index}))
        return {
            "snapshot_dir": str(sdir),
            "status": "complete",
            "pages": pages,
            "markets": markets_written,
        }
    finally:
        if own_client:
            client.close()


def latest_complete_snapshot(data_root: Path, endpoint: str = "markets_open") -> Path | None:
    root = data_root / "raw" / "gamma" / endpoint
    if not root.exists():
        return None
    snaps = sorted(
        (d for d in root.iterdir() if d.is_dir() and (d / "_COMPLETE").exists()),
        key=lambda d: d.name,
    )
    return snaps[-1] if snaps else None


# --- events by-id fallback -------------------------------------------------
# Live finding 2026-10-10: /events/keyset returns a persistent 500 for the
# cursor after event id 692222 (even at limit=1), so keyset paging cannot pass
# it. The public /events endpoint accepts repeated `id=` params and returns the
# same event payloads (tags/category identical; it omits only
# `requiresTranslation`). The tail of the events snapshot is completed by
# walking ascending id ranges. A batch that errors is bisected down to single
# ids; ids that still fail are recorded, never silently dropped.
EVENTS_BY_ID_URL = f"{GAMMA_BASE}/events"
BYID_CHECKPOINT = "_byid_checkpoint.json"


@retry(
    retry=retry_if_exception(_retryable),
    wait=wait_exponential_jitter(initial=1, max=30),
    stop=stop_after_attempt(4),
    after=lambda rs: _respect_retry_after(rs.outcome.exception())
    if rs.outcome and rs.outcome.exception()
    else None,
    reraise=True,
)
def _fetch_events_by_id(client: httpx.Client, ids: list[int]) -> list[dict]:
    params = [("id", str(i)) for i in ids] + [("limit", str(len(ids)))]
    resp = client.get(EVENTS_BY_ID_URL, params=params, timeout=30.0)
    resp.raise_for_status()
    js = resp.json()
    if not isinstance(js, list):
        raise GammaShapeError(f"/events?id= returned {type(js).__name__}, expected list")
    return js


def _fetch_ids_bisecting(client: httpx.Client, ids: list[int], failed: list[int]) -> list[dict]:
    try:
        return _fetch_events_by_id(client, ids)
    except httpx.HTTPStatusError as exc:
        if exc.response.status_code < 500:
            raise
        if len(ids) == 1:
            failed.append(ids[0])
            return []
        mid = len(ids) // 2
        return _fetch_ids_bisecting(client, ids[:mid], failed) + _fetch_ids_bisecting(client, ids[mid:], failed)


def max_written_id(sdir: Path) -> int:
    """Highest numeric id across a snapshot's pages (0 if none)."""
    import polars as pl

    pages = sorted(sdir.glob("page-*.parquet"))
    if not pages:
        return 0
    ids = pl.read_parquet(pages[-50:], columns=["market_id"])["market_id"]
    return int(ids.cast(pl.Int64, strict=False).max() or 0)


def run_events_by_id(
    data_root: Path,
    snapshot: str,
    client: httpx.Client | None = None,
    batch: int = PAGE_LIMIT,
    stop_after_empty_ids: int = 50_000,
    max_batches: int | None = None,
) -> dict:
    """Finish an interrupted events snapshot by ascending id ranges (resumable).

    Starts after the highest id already written (keyset pages are id-ordered);
    ends after `stop_after_empty_ids` consecutive ids return nothing.
    """
    own_client = client is None
    client = client or httpx.Client(headers={"User-Agent": "h1-polymarket-research/0.1"})
    sdir = snapshot_dir(data_root, snapshot, endpoint="events")
    if (sdir / "_COMPLETE").exists():
        return {"snapshot_dir": str(sdir), "status": "already_complete", "events": 0}
    ck_path = sdir / BYID_CHECKPOINT
    if ck_path.exists():
        ck = json.loads(ck_path.read_text())
    else:
        main = _load_checkpoint(sdir) or {}
        start = max_written_id(sdir) + 1
        ck = {
            "start_id": start,
            "next_id": start,
            "next_page_index": int(main.get("next_page_index", 0)),
            "last_found_id": start - 1,
            "failed_ids": [],
        }
    written = 0
    batches = 0
    try:
        while ck["next_id"] - ck["last_found_id"] <= stop_after_empty_ids:
            ids = list(range(ck["next_id"], ck["next_id"] + batch))
            failed: list[int] = []
            events = _fetch_ids_bisecting(client, ids, failed)
            events.sort(key=lambda e: int(e.get("id", 0)))
            if events:
                _write_page(sdir, ck["next_page_index"], events)
                ck["next_page_index"] += 1
                ck["last_found_id"] = max(ck["last_found_id"], int(events[-1]["id"]))
                written += len(events)
            ck["failed_ids"].extend(failed)
            ck["next_id"] = ids[-1] + 1
            ck["updated_at"] = _utcnow()
            tmp = ck_path.with_suffix(".json.tmp")
            tmp.write_text(json.dumps(ck))
            os.replace(tmp, ck_path)
            batches += 1
            if max_batches is not None and batches >= max_batches:
                return {"snapshot_dir": str(sdir), "status": "paused_max_batches", "events": written}
        (sdir / "_COMPLETE").write_text(
            json.dumps(
                {
                    "finished_at": _utcnow(),
                    "pages": ck["next_page_index"],
                    "tail_source": "events?id= by-id fallback (keyset 500 after id "
                    f"{ck['start_id'] - 1})",
                    "byid_id_range": [ck["start_id"], ck["last_found_id"]],
                    "failed_ids": ck["failed_ids"],
                }
            )
        )
        return {
            "snapshot_dir": str(sdir),
            "status": "complete",
            "events": written,
            "failed_ids": len(ck["failed_ids"]),
        }
    finally:
        if own_client:
            client.close()
