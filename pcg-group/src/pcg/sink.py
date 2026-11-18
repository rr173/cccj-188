"""Sink writes with the transaction-bound write fence.

A batch commit must pass *all* of the following checks inside the very same
``BEGIN IMMEDIATE`` transaction that inserts the rows and advances progress:

1. the assignment identified by ``(partition, gen)`` exists, belongs to this
   worker, and is currently ``active`` -- a revoked/fenced generation can
   never commit, no matter how long ago it computed the batch;
2. the owning member still holds a valid lease at commit time (not merely at
   compute time) -- checking only ``worker_id`` or checking once up-front is
   explicitly insufficient;
3. the batch starts exactly at ``progress.committed_offset + 1`` (or 0 for a
   fresh partition), so progress can never jump or go backwards;
4. none of the batch offsets already exist as business rows -- overlap cannot
   be hidden behind an idempotent UPSERT (we INSERT plain and treat a
   constraint hit as a rejected late commit);
5. the batch offsets themselves are contiguous.

A failure records an audit row in ``rejected_commits`` *before* rollback and
raises :class:`CommitRejected`.  Therefore a late whole-batch from a worker
that lost ownership is refused wholesale; already-committed source offsets are
delivered exactly once, while uncommitted computation may be redone by the
next owner.
"""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from typing import Any

from .db import tx
from .rule_store import build_outcome, get_active, load_version, parse_source


class CommitRejected(Exception):
    def __init__(self, reason: str):
        super().__init__(reason)
        self.reason = reason


@dataclass
class BatchItem:
    offset: int
    source_raw: str
    clean_value: dict[str, Any]


def _reject(conn: sqlite3.Connection, *, at_ms: int, partition: int, gen: int,
            plan_id: int, worker_id: str, start: int, end: int, reason: str) -> None:
    """Persist the rejection audit row.

    This MUST survive the rollback of the surrounding business transaction,
    otherwise a refused late commit would leave no evidence at all. It is
    written in its own immediate transaction after the caller rolls back.
    """
    conn.execute(
        "INSERT INTO rejected_commits(at_ms,partition_id,gen,plan_id,worker_id,"
        "start_offset,end_offset,reason) VALUES(?,?,?,?,?,?,?,?)",
        (at_ms, partition, gen, plan_id, worker_id, start, end, reason),
    )


def commit_batch(conn: sqlite3.Connection, clock, *, partition: int, gen: int,
                 worker_id: str, items: list[BatchItem]) -> int:
    """Fenced commit. Returns the new committed offset (or unchanged progress)."""
    if not items:
        row = conn.execute(
            "SELECT committed_offset FROM progress WHERE partition_id=?", (partition,)
        ).fetchone()
        return row["committed_offset"] if row else -1

    start = items[0].offset
    end = items[-1].offset
    now = clock.now_ms()

    def fail(reason: str, plan_id: int = -1) -> None:
        # roll back the (empty, row-wise) business transaction first...
        conn.execute("ROLLBACK")
        # ...then persist the audit trail in its OWN transaction so it survives
        conn.execute("BEGIN IMMEDIATE")
        _reject(conn, at_ms=now, partition=partition, gen=gen, plan_id=plan_id,
                worker_id=worker_id, start=start, end=end, reason=reason)
        conn.execute("COMMIT")
        raise CommitRejected(reason)

    conn.execute("BEGIN IMMEDIATE")
    try:
        asg = conn.execute(
            "SELECT * FROM assignments WHERE partition_id=? AND gen=?",
            (partition, gen),
        ).fetchone()
        if asg is None:
            fail("generation unknown (already superseded)")
        plan_id = asg["plan_id"]
        if asg["worker_id"] != worker_id:
            fail(f"owned by {asg['worker_id']}, not {worker_id}", plan_id)
        if asg["state"] != "active":
            fail(f"assignment state is {asg['state']}, not active", plan_id)

        member = conn.execute(
            "SELECT * FROM members WHERE worker_id=?", (worker_id,)
        ).fetchone()
        if member is None or member["status"] == "dead":
            fail("member is dead", plan_id)
        if member["status"] == "alive" and now - member["last_beat"] > member["lease_ms"]:
            fail(f"lease expired ({now - member['last_beat']}ms > "
                 f"{member['lease_ms']}ms)", plan_id)
        if member["status"] not in ("alive", "leaving"):
            fail(f"member status {member['status']} cannot commit", plan_id)

        prog = conn.execute(
            "SELECT * FROM progress WHERE partition_id=?", (partition,)
        ).fetchone()
        committed = prog["committed_offset"] if prog else -1
        expected = committed + 1
        if start != expected:
            fail(f"non-contiguous batch: starts at {start}, expected {expected} "
                 f"(committed={committed})", plan_id)

        offsets = [it.offset for it in items]
        if any(offsets[i] + 1 != offsets[i + 1] for i in range(len(offsets) - 1)):
            fail("batch offsets are not contiguous", plan_id)

        overlap = conn.execute(
            "SELECT offset FROM records WHERE partition_id=? AND offset BETWEEN ? AND ? LIMIT 1",
            (partition, start, end),
        ).fetchone()
        if overlap is not None:
            fail(f"offset {overlap['offset']} already committed (late duplicate batch)",
                 plan_id)

        # The active rule version is selected *inside* this fence transaction,
        # so a batch that was computed before a switch but commits afterwards
        # can never masquerade old-calibre values under the new version: its
        # rule outcome is (re)computed here against whatever version is active
        # at commit time. Source rows + progress + versioned outcomes therefore
        # land atomically; a rule rejection keeps the raw row and progress and
        # stores an explicit diagnostic instead of silently dropping the row.
        # Newly committed offsets are always at source revision 0: corrections
        # can only target already-committed positions, so no correction can
        # exist for this batch's offsets.
        rule_state = get_active(conn)
        active_version_id = rule_state["active_version"]
        active_version = load_version(conn, active_version_id)
        rule_outcomes = []
        for it in items:
            src_obj = parse_source(it.source_raw)
            status, payload = build_outcome(active_version, src_obj)
            rule_outcomes.append((it.offset, status, payload))

        for it in items:
            conn.execute(
                "INSERT INTO records(partition_id,offset,gen,worker_id,source_json,"
                "clean_json,committed_at) VALUES(?,?,?,?,?,?,?)",
                (partition, it.offset, gen, worker_id, it.source_raw,
                 json.dumps(it.clean_value, sort_keys=True, separators=(",", ":"),
                            ensure_ascii=False), now),
            )
        # normalise the delivery payload to canonical JSON strings (build_outcome
        # returns a JSON string for 'ok' and a dict for 'rejected').
        delivery_outcomes = []
        for offset, status, payload in rule_outcomes:
            if status == "ok":
                payload_json = payload
            else:
                payload_json = json.dumps(payload, sort_keys=True,
                                          separators=(",", ":"), ensure_ascii=False)
            delivery_outcomes.append((offset, status, payload_json))
            if status == "ok":
                conn.execute(
                    "INSERT INTO rule_results(version_id,partition_id,offset,"
                    "status,result_json,error_json,computed_at,source_revision) "
                    "VALUES(?,?,?,'ok',?,NULL,?,0)",
                    (active_version_id, partition, offset, payload, now),
                )
            else:
                conn.execute(
                    "INSERT INTO rule_results(version_id,partition_id,offset,"
                    "status,result_json,error_json,computed_at,source_revision) "
                    "VALUES(?,?,?,'rejected',NULL,?,?,0)",
                    (active_version_id, partition, offset, payload_json, now),
                )
        # the backfill watermark of the active version advances with progress,
        # so a later recompute resumes exactly after these committed rows
        conn.execute(
            "INSERT OR REPLACE INTO rule_recompute_progress(version_id,partition_id,"
            "done_offset,updated_at) VALUES(?,?,?,?)",
            (active_version_id, partition, end, now),
        )
        conn.execute(
            "INSERT INTO progress(partition_id,committed_offset,gen,worker_id,updated_at)"
            " VALUES(?,?,?,?,?) ON CONFLICT(partition_id) DO UPDATE SET "
            "committed_offset=excluded.committed_offset, gen=excluded.gen, "
            "worker_id=excluded.worker_id, updated_at=excluded.updated_at",
            (partition, end, gen, worker_id, now),
        )
        # delivery events for the new official positions land in this SAME
        # transaction: the business rows and their change-log entries can never
        # commit independently, so no committed change is ever undeliverable.
        from .delivery_store import emit_ingest_events
        emit_ingest_events(
            conn, partition=partition, items=items,
            rule_outcomes=delivery_outcomes, active_version=active_version_id,
            publish_seq=rule_state["publish_seq"], worker_id=worker_id,
            now_ms=now)
        conn.execute("COMMIT")
        return end
    except CommitRejected:
        raise
    except Exception:
        conn.execute("ROLLBACK")
        raise
