"""Generic, resumable HyperSync log backfill (operating rule 5).

Each stream writes append-only Parquet part files under
``data/raw/hypersync/<stream>/part-<from>-<to>.parquet`` plus a JSON checkpoint
recording the last committed block. Part files are written to a temp name,
fsynced, and renamed *before* the checkpoint advances, so a killed run resumes
at ``checkpoint + 1`` without rewriting or double-counting anything. Dedup on
``(tx_hash, log_index)`` is additionally enforced downstream, so even a crash
between rename and checkpoint write (which would re-scan one batch) cannot
double-count.

Raw rows are stored undecoded (hex topics + data): ``data/raw`` is ground truth
(rule 7); decoding happens in ``normalize``.
"""

from __future__ import annotations

import asyncio
import json
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from ingest.contracts import CONFIRMATIONS, POLYGON_HYPERSYNC_URL, STREAMS, StreamSpec

RAW_SCHEMA = pa.schema(
    [
        ("block_number", pa.int64()),
        ("block_timestamp", pa.int64()),  # unix seconds
        ("tx_hash", pa.string()),
        ("log_index", pa.int64()),
        ("address", pa.string()),
        ("topic0", pa.string()),
        ("topic1", pa.string()),
        ("topic2", pa.string()),
        ("topic3", pa.string()),
        ("data", pa.string()),
    ]
)


class TokenMissingError(RuntimeError):
    pass


def _to_int(v: Any) -> int:
    if isinstance(v, str):
        return int(v, 16) if v.startswith("0x") else int(v)
    return int(v)


def _norm_hex(v: Any) -> str | None:
    return v.lower() if isinstance(v, str) else v


def stream_dir(data_root: Path, stream_name: str) -> Path:
    return data_root / "raw" / "hypersync" / stream_name


def load_checkpoint(sdir: Path) -> int | None:
    f = sdir / "_checkpoint.json"
    if not f.exists():
        return None
    return int(json.loads(f.read_text())["last_committed_block"])


def save_checkpoint(sdir: Path, last_committed_block: int) -> None:
    f = sdir / "_checkpoint.json"
    tmp = f.with_suffix(".json.tmp")
    payload = {
        "last_committed_block": last_committed_block,
        "updated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }
    tmp.write_text(json.dumps(payload))
    os.replace(tmp, f)


def block_ts_map(blocks: list[Any]) -> dict[int, int]:
    out = {}
    for b in blocks:
        num = getattr(b, "number", None)
        ts = getattr(b, "timestamp", None)
        if num is None or ts is None:
            continue
        out[_to_int(num)] = _to_int(ts)
    return out


def logs_to_rows(logs: list[Any], ts_by_block: dict[int, int]) -> list[dict]:
    rows = []
    for lg in logs:
        # The Python client exposes either topic0..topic3 attributes or a
        # topics list depending on version; handle both.
        topics = getattr(lg, "topics", None)
        if topics is not None:
            t = list(topics) + [None] * (4 - len(topics))
        else:
            t = [getattr(lg, f"topic{i}", None) for i in range(4)]
        bn = _to_int(lg.block_number)
        rows.append(
            {
                "block_number": bn,
                "block_timestamp": ts_by_block.get(bn, 0),
                "tx_hash": _norm_hex(lg.transaction_hash),
                "log_index": _to_int(lg.log_index),
                "address": _norm_hex(lg.address),
                "topic0": _norm_hex(t[0]),
                "topic1": _norm_hex(t[1]),
                "topic2": _norm_hex(t[2]),
                "topic3": _norm_hex(t[3]),
                "data": _norm_hex(lg.data),
            }
        )
    return rows


def write_part(sdir: Path, from_block: int, to_block: int, rows: list[dict]) -> Path:
    part = sdir / f"part-{from_block:010d}-{to_block:010d}.parquet"
    tmp = part.with_name(part.name + ".tmp")
    table = pa.Table.from_pylist(rows, schema=RAW_SCHEMA)
    pq.write_table(table, tmp)
    os.replace(tmp, part)
    return part


def _build_query(spec: StreamSpec, from_block: int, to_block_exclusive: int):
    import hypersync
    from hypersync import BlockField, FieldSelection, LogField, LogSelection, Query

    return Query(
        from_block=from_block,
        to_block=to_block_exclusive,
        logs=[
            LogSelection(
                address=list(spec.addresses),
                topics=[list(spec.topic0s)],
            )
        ],
        field_selection=FieldSelection(
            log=[
                LogField.BLOCK_NUMBER,
                LogField.LOG_INDEX,
                LogField.TRANSACTION_HASH,
                LogField.ADDRESS,
                LogField.TOPIC0,
                LogField.TOPIC1,
                LogField.TOPIC2,
                LogField.TOPIC3,
                LogField.DATA,
            ],
            block=[BlockField.NUMBER, BlockField.TIMESTAMP],
        ),
    )


def make_client(api_token: str | None = None):
    import hypersync

    token = api_token or os.environ.get("HYPERSYNC_BEARER_TOKEN")
    if not token:
        raise TokenMissingError(
            "HYPERSYNC_BEARER_TOKEN is not set. Get a token at "
            "https://envio.dev/app/api-tokens and put it in .env"
        )
    return hypersync.HypersyncClient(
        hypersync.ClientConfig(url=POLYGON_HYPERSYNC_URL, bearer_token=token)
    )


@dataclass
class RunResult:
    stream: str
    from_block: int
    last_committed_block: int
    events: int
    parts_written: int


async def run_stream(
    spec: StreamSpec,
    data_root: Path,
    client: Any = None,
    to_block: int | None = None,
    max_batches: int | None = None,
    log_every: int = 1,
) -> RunResult:
    """Backfill one stream from its checkpoint (or deployment floor) to `to_block`
    (inclusive; default = chain tip minus CONFIRMATIONS). Injectable `client` for tests.
    """
    if client is None:
        client = make_client()
    sdir = stream_dir(data_root, spec.name)
    sdir.mkdir(parents=True, exist_ok=True)

    ckpt = load_checkpoint(sdir)
    start_block = (ckpt + 1) if ckpt is not None else spec.from_block

    height = await client.get_height()
    safe_tip = height - CONFIRMATIONS
    end_inclusive = min(to_block, safe_tip) if to_block is not None else safe_tip

    result = RunResult(
        stream=spec.name,
        from_block=start_block,
        last_committed_block=ckpt if ckpt is not None else start_block - 1,
        events=0,
        parts_written=0,
    )
    if start_block > end_inclusive:
        return result  # already up to date — idempotent no-op

    query = _build_query(spec, start_block, end_inclusive + 1)
    batch = 0
    while True:
        res = await client.get(query)
        next_block = res.next_block  # first block NOT covered by this response
        covered_to = next_block - 1
        rows = logs_to_rows(res.data.logs, block_ts_map(res.data.blocks))
        if rows:
            write_part(sdir, query.from_block, covered_to, rows)
            result.parts_written += 1
            result.events += len(rows)
        save_checkpoint(sdir, covered_to)
        result.last_committed_block = covered_to
        batch += 1
        if log_every and batch % log_every == 0:
            print(
                f"[{spec.name}] blocks {query.from_block}..{covered_to} "
                f"(+{len(rows)} events, total {result.events})",
                flush=True,
            )
        if covered_to >= end_inclusive:
            break
        if max_batches is not None and batch >= max_batches:
            break
        query.from_block = next_block
    return result


async def run_streams(
    names: list[str],
    data_root: Path,
    to_block: int | None = None,
    client: Any = None,
) -> list[RunResult]:
    results = []
    for name in names:
        spec = STREAMS[name]
        results.append(
            await run_stream(spec, data_root=data_root, client=client, to_block=to_block)
        )
    return results
