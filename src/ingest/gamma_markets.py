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

GAMMA_KEYSET_URL = "https://gamma-api.polymarket.com/markets/keyset"
PAGE_LIMIT = 100

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


def snapshot_dir(data_root: Path, snapshot: str | None = None) -> Path:
    snap = snapshot or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    return data_root / "raw" / "gamma" / "markets" / f"snapshot={snap}"


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
def _fetch_page(client: httpx.Client, after_cursor: str | None) -> dict:
    params: dict[str, Any] = {"limit": PAGE_LIMIT}
    if after_cursor:
        params["after_cursor"] = after_cursor
    resp = client.get(GAMMA_KEYSET_URL, params=params, timeout=30.0)
    resp.raise_for_status()
    return {"json": resp.json()}


def parse_page(js: Any) -> tuple[list[dict], str | None]:
    """Extract (markets, next_cursor) from the known keyset response shapes."""
    if isinstance(js, list):
        return js, None  # bare list: single page, no cursor
    if isinstance(js, dict):
        items = None
        for key in ("data", "markets", "results"):
            if isinstance(js.get(key), list):
                items = js[key]
                break
        if items is None:
            raise GammaShapeError(f"no market list under data/markets/results: keys={list(js)[:10]}")
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
) -> dict:
    """Pull (or resume) one full market-metadata snapshot. Returns summary stats."""
    own_client = client is None
    client = client or httpx.Client(headers={"User-Agent": "h1-polymarket-research/0.1"})
    sdir = snapshot_dir(data_root, snapshot)
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
            js = _fetch_page(client, after_cursor)["json"]
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


def latest_complete_snapshot(data_root: Path) -> Path | None:
    root = data_root / "raw" / "gamma" / "markets"
    if not root.exists():
        return None
    snaps = sorted(
        (d for d in root.iterdir() if d.is_dir() and (d / "_COMPLETE").exists()),
        key=lambda d: d.name,
    )
    return snaps[-1] if snaps else None
