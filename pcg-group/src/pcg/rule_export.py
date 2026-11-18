"""Official versioned read entry point.

An export takes ONE WAL-consistent snapshot (a single deferred read
transaction) in which it fixes:

* the active version and its publish sequence;
* the committed-offset vector;
* the source-correction sequence and every position's current revision.

It then streams every source position of that section in source order,
emitting either the full versioned result or the explicit rejection
diagnostic, each tagged with the source revision it was computed from. A
concurrent publish or correction approval cannot split one export across two
versions, pair a new source revision with stale cleaned values, or drop a
position included in the section: the snapshot stays valid for the whole
iteration, and for the active version every row's ``source_revision`` must
equal the position's current revision inside that same snapshot (a violation
is an invariant error, never silently skipped).
"""

from __future__ import annotations

import json
import sqlite3
from typing import Iterator

from .correct_store import correction_seq, revision_map
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
        is_active = version_id == state["active_version"]
        vector = snapshot_vector(conn)
        revisions = revision_map(conn)
        corr_seq = correction_seq(conn)
        for pid, last_off in vector.items():
            for offset in range(last_off + 1):
                row = conn.execute(
                    "SELECT * FROM rule_results WHERE version_id=? "
                    "AND partition_id=? AND offset=?",
                    (version_id, pid, offset),
                ).fetchone()
                if row is None:
                    # invariant violation for a covered version; never invent
                    # a default value and never skip silently
                    raise RuntimeError(
                        f"version {version_id!r} has no result at "
                        f"p{pid} offset {offset} inside its committed section")
                cur_rev = revisions.get((pid, offset), 0)
                if is_active and row["source_revision"] != cur_rev:
                    # the approval transaction keeps the active version's
                    # results current atomically; a mismatch can only mean
                    # corruption, which must be loud rather than exported
                    raise RuntimeError(
                        f"active version {version_id!r} result at p{pid} "
                        f"offset {offset} is stale inside the export snapshot "
                        f"(source revision {row['source_revision']} != "
                        f"current {cur_rev})")
                out = {
                    "version_id": version_id,
                    "publish_seq": state["publish_seq"],
                    "correction_seq": corr_seq,
                    "partition_id": pid,
                    "offset": offset,
                    "source_revision": row["source_revision"],
                    "status": row["status"],
                }
                if row["source_revision"] < cur_rev:
                    # non-active versions may legitimately lag corrections;
                    # say so explicitly instead of presenting a stale value
                    # as current
                    out["stale"] = True
                    out["current_revision"] = cur_rev
                if row["status"] == "ok":
                    out["result"] = json.loads(row["result_json"])
                else:
                    out["error"] = json.loads(row["error_json"])
                yield out
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
        corr_seq = correction_seq(conn)
        conn.execute("ROLLBACK")
        return {"version_id": vid, "publish_seq": state["publish_seq"],
                "correction_seq": corr_seq,
                "offset_vector": vector,
                "positions": sum(v + 1 for v in vector.values())}
    except Exception:
        conn.execute("ROLLBACK")
        raise
