"""Read-only JSONL source.

Layout: a directory containing one file per partition, ``p<id>.jsonl``.
Within a file every line is one JSON object and its zero-based *line number*
is the partition offset, so offsets are continuous from 0 by construction.

Integrity rules (a source that violates them is never reported as safely
finished):

* no blank / missing lines  -> a gap would break offset continuity
* every line must parse as a JSON object
* if a record carries an explicit ``offset`` field it must equal the line no.
* if ``manifest.json`` exists, per-partition line counts must match
"""

from __future__ import annotations

import json
import os
import re
from typing import Iterator, Optional

_FILE_RE = re.compile(r"^p(\d+)\.jsonl$")


class SourceError(Exception):
    """Raised on discontinuous or corrupt source data."""


def source_path(source_dir: str, partition: int) -> str:
    return os.path.join(source_dir, f"p{partition}.jsonl")


def list_partitions(source_dir: str) -> list[int]:
    if not os.path.isdir(source_dir):
        raise SourceError(f"source directory not found: {source_dir}")
    parts = []
    for name in os.listdir(source_dir):
        m = _FILE_RE.match(name)
        if m:
            parts.append(int(m.group(1)))
    return sorted(parts)


def _iter_lines(source_dir: str, partition: int) -> Iterator[tuple[int, str]]:
    path = source_path(source_dir, partition)
    with open(path, "r", encoding="utf-8") as fh:
        for offset, raw in enumerate(fh):
            line = raw.rstrip("\n")
            if line.strip() == "":
                raise SourceError(
                    f"partition {partition}: blank line at offset {offset} "
                    "(offset discontinuity)"
                )
            yield offset, line


def iter_records(source_dir: str, partition: int) -> Iterator[tuple[int, dict]]:
    for offset, line in _iter_lines(source_dir, partition):
        try:
            obj = json.loads(line)
        except json.JSONDecodeError as exc:
            raise SourceError(
                f"partition {partition}: invalid JSON at offset {offset}: {exc}"
            ) from exc
        if not isinstance(obj, dict):
            raise SourceError(
                f"partition {partition}: record at offset {offset} is not an object"
            )
        if "offset" in obj and obj["offset"] != offset:
            raise SourceError(
                f"partition {partition}: embedded offset {obj['offset']} != "
                f"line offset {offset} (discontinuous source)"
            )
        yield offset, obj


def count_lines(source_dir: str, partition: int) -> int:
    n = 0
    for _ in _iter_lines(source_dir, partition):
        n += 1
    return n


def read_batch(
    source_dir: str, partition: int, start_offset: int, max_batch: int
) -> list[tuple[int, dict, str]]:
    """Read up to *max_batch* records beginning at *start_offset*.

    Returns ``(offset, parsed, raw_line)`` tuples.  Raising on a gap or bad
    JSON means a corrupt source cannot be silently treated as completion.
    """
    out: list[tuple[int, dict, str]] = []
    for offset, line in _iter_lines(source_dir, partition):
        if offset < start_offset:
            continue
        if len(out) >= max_batch:
            break
        try:
            obj = json.loads(line)
        except json.JSONDecodeError as exc:
            raise SourceError(
                f"partition {partition}: invalid JSON at offset {offset}: {exc}"
            ) from exc
        if not isinstance(obj, dict):
            raise SourceError(
                f"partition {partition}: record at offset {offset} is not an object"
            )
        if "offset" in obj and obj["offset"] != offset:
            raise SourceError(
                f"partition {partition}: embedded offset {obj['offset']} != "
                f"line offset {offset} (discontinuous source)"
            )
        out.append((offset, obj, line))
    if out and out[0][0] != start_offset:
        raise SourceError(
            f"partition {partition}: requested offset {start_offset} is missing "
            "(discontinuous source)"
        )
    return out


def validate_source(source_dir: str) -> list[str]:
    """Full validation; returns a list of human-readable problems (empty = OK)."""
    problems: list[str] = []
    try:
        parts = list_partitions(source_dir)
    except SourceError as exc:
        return [str(exc)]
    manifest: Optional[dict] = None
    mpath = os.path.join(source_dir, "manifest.json")
    if os.path.exists(mpath):
        try:
            with open(mpath, encoding="utf-8") as fh:
                manifest = json.load(fh)
        except (OSError, json.JSONDecodeError) as exc:
            problems.append(f"manifest unreadable: {exc}")
    for p in parts:
        count = 0
        try:
            for offset, _ in iter_records(source_dir, p):
                count = offset + 1
        except SourceError as exc:
            problems.append(str(exc))
            continue
        if manifest is not None:
            expected = manifest.get("counts", {}).get(str(p))
            if expected is not None and expected != count:
                problems.append(
                    f"partition {p}: manifest expects {expected} records, "
                    f"file has {count} (truncated/extended source)"
                )
    if manifest is not None:
        for k in manifest.get("counts", {}):
            if int(k) not in parts:
                problems.append(f"partition {k} listed in manifest is missing")
    return problems
