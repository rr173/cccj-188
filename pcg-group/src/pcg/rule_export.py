"""Official versioned read entry point.

An export takes ONE WAL-consistent snapshot (a single deferred read
transaction) in which it fixes:

* the active version and its publish sequence;
* the committed-offset vector.

It then streams every source position of that section in source order,
emitting either the full versioned result or the explicit rejection
diagnostic. A concurrent publish cannot split one export across two versions
or drop a position included in the section: the snapshot stays valid for the
whole iteration.
"""

from __future__ import annotations

import json
import sqlite3
from typing import Iterator

from .rule_review import snapshot_vector
from .rule_store import get_active


def export_rows(conn: sqlite3.Connection,
                version_id: str | None = None) -> Iterator[dict]:
    # one read transaction = one section for the entire export
    conn.execute("BEGIN")
    try:
        state = get_active(conn)
        if version_id is None:
            version_id = state["active_version"]
        vector = snapshot_vector(conn)
        for pid, last_off in vector.items():
            for offset in range(last_off + 1):
                row = conn.execute(
                    "SELECT * FROM rule_results WHERE version_id=? "
                    "AND partition_id=? AND offset=?",
                    (version_id, pid, offset),
                ).fetchone()
                if row is None:
                    # invariant violation for an active version; never invent
                    # a default value and never skip silently
                    raise RuntimeError(
                        f"active version {version_id!r} has no result at "
                        f"p{pid} offset {offset} inside its committed section")
                if row["status"] == "ok":
                    yield {
                        "version_id": version_id,
                        "publish_seq": state["publish_seq"],
                        "partition_id": pid,
                        "offset": offset,
                        "status": "ok",
                        "result": json.loads(row["result_json"]),
                    }
                else:
                    yield {
                        "version_id": version_id,
                        "publish_seq": state["publish_seq"],
                        "partition_id": pid,
                        "offset": offset,
                        "status": "rejected",
                        "error": json.loads(row["error_json"]),
                    }
    finally:
        # read-only; rollback releases the snapshot without ever writing
        conn.execute("ROLLBACK")


def export_section(conn: sqlite3.Connection,
                   version_id: str | None = None) -> dict:
    """Return the header describing the section an export would use."""
    conn.execute("BEGIN")
    try:
        state = get_active(conn)
        vid = version_id or state["active_version"]
        vector = snapshot_vector(conn)
        conn.execute("ROLLBACK")
        return {"version_id": vid, "publish_seq": state["publish_seq"],
                "offset_vector": vector,
                "positions": sum(v + 1 for v in vector.values())}
    except Exception:
        conn.execute("ROLLBACK")
        raise
