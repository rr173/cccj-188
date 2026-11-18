"""Review snapshots, diffs and atomic activation.

A review fixes an explicit data section:

* the vector of per-partition committed offsets (empty partition => -1);
* the currently active version and the monotonic publish sequence;
* the candidate rule's content hash;
* the global correction sequence, which pins the exact set of effected
  source-record corrections (revisions are immutable and append-only, so one
  monotonic number identifies the whole source-revision state).

All per-position findings are computed inside one read-only WAL snapshot and
materialised through a separate writer in bounded, short transactions, so
ingestion, recomputation and corrections that happen afterwards cannot
silently alter what the review represented (and the scan never blocks
writers). Activation re-validates the persisted token against the *current*
pointer, sequence, offset vector, correction sequence and candidate coverage
inside another single atomic transaction: a correction that lands after the
review - even one later undone back to the original value - moves the
correction sequence and therefore kills the token.
"""

from __future__ import annotations

import json
import secrets
import sqlite3

from .correct_store import correction_seq, revision_map
from .rule_engine import json_type_name
from .rule_store import get_active, load_version
from .rule_recompute import _partitions


class ReviewError(Exception):
    """Review cannot be created (unknown version, ...)."""


class ActivationError(Exception):
    """Token/section mismatch or incomplete candidate (exit code 6)."""


# ----------------------------------------------------------------------- snapshot

def snapshot_vector(conn: sqlite3.Connection) -> dict[int, int]:
    """Committed-offset vector; partitions with no progress read as -1."""
    vector: dict[int, int] = {}
    for pid in _partitions(conn):
        row = conn.execute(
            "SELECT committed_offset FROM progress WHERE partition_id=?", (pid,)
        ).fetchone()
        vector[pid] = row["committed_offset"] if row else -1
    return dict(sorted(vector.items()))


def _result_row(conn: sqlite3.Connection, version_id: str,
                pid: int, offset: int) -> sqlite3.Row | None:
    return conn.execute(
        "SELECT * FROM rule_results WHERE version_id=? AND partition_id=? "
        "AND offset=?", (version_id, pid, offset),
    ).fetchone()


def _field_diffs(base_obj: dict, cand_obj: dict) -> list[dict]:
    diffs = []
    for field in sorted(set(base_obj) | set(cand_obj)):
        b_present = field in base_obj
        c_present = field in cand_obj
        entry: dict = {"field": field}
        if not b_present:
            entry["base"] = {"present": False}
        else:
            entry["base"] = {"present": True, "type": json_type_name(base_obj[field]),
                             "value": base_obj[field]}
        if not c_present:
            entry["candidate"] = {"present": False}
        else:
            entry["candidate"] = {"present": True,
                                  "type": json_type_name(cand_obj[field]),
                                  "value": cand_obj[field]}
        same = (b_present and c_present
                and json_type_name(base_obj[field]) == json_type_name(cand_obj[field])
                and base_obj[field] == cand_obj[field])
        if not same:
            diffs.append(entry)
    return diffs


# ------------------------------------------------------------------------- review

REVIEW_PAGE = 500


def _classify_position(conn, candidate_id: str, base_id: str,
                       pid: int, offset: int,
                       revisions: dict[tuple[int, int], int]):
    """Return ``(kind, detail_dict_or_None)`` for one section position."""
    crow = _result_row(conn, candidate_id, pid, offset)
    brow = _result_row(conn, base_id, pid, offset)
    cur_rev = revisions.get((pid, offset), 0)
    if crow is None:
        return "uncovered", {"reason": "candidate has no result at this position"}
    if crow["source_revision"] < cur_rev:
        return "stale", {
            "reason": "candidate result predates the current source "
                      "correction; run recompute",
            "result_revision": crow["source_revision"],
            "current_revision": cur_rev,
        }
    if crow["status"] == "rejected":
        detail = {"candidate_error": json.loads(crow["error_json"])}
        if brow is not None:
            detail["base_status"] = brow["status"]
        return "candidate_rejected", detail
    if brow is not None and brow["status"] == "rejected":
        return ("base_rejected_candidate_ok",
                {"base_error": json.loads(brow["error_json"])})
    cand_obj = json.loads(crow["result_json"])
    base_obj = json.loads(brow["result_json"]) if brow is not None else None
    if base_obj is None:
        return "changed", {"fields": _field_diffs({}, cand_obj)}
    diffs = _field_diffs(base_obj, cand_obj)
    if diffs:
        return "changed", {"fields": diffs}
    return "same", None


def create_review(conn: sqlite3.Connection, candidate_id: str,
                  base_id: str | None, now_ms: int,
                  db_path: str | None = None) -> dict:
    """Create a fixed-section review and persist its findings.

    The section and every finding are computed inside ONE deferred read
    transaction (a WAL snapshot), so concurrent ingestion/recomputation cannot
    change what the review represents while it is being built. When
    *db_path* is given, persistence uses a *separate* writer connection in
    bounded, short write transactions, keeping the scan off the write lock and
    process memory bounded. Without it (in-process callers), the same
    connection is reused.
    """
    candidate = load_version(conn, candidate_id)
    state = get_active(conn)
    if base_id is None:
        base_id = state["active_version"]
    load_version(conn, base_id)

    own_writer = db_path is not None
    if db_path is not None:
        from .db import connect
        writer_conn = connect(db_path)
    else:
        writer_conn = conn

    token = secrets.token_hex(16)
    stats = {"same": 0, "changed": 0, "candidate_rejected": 0,
             "uncovered": 0, "base_rejected_candidate_ok": 0, "stale": 0,
             "positions": 0}

    # ---- read-only snapshot section: no write lock is held for the scan ----
    conn.execute("BEGIN")
    try:
        vector = snapshot_vector(conn)
        revisions = revision_map(conn)
        corr_seq = correction_seq(conn)
        header_written = False
        pending: list[tuple] = []

        def flush(force_header: bool = False, final_stats: str | None = None) -> None:
            nonlocal header_written
            if not force_header and not pending and final_stats is None:
                return
            if own_writer:
                writer_conn.execute("BEGIN IMMEDIATE")
            else:
                # single-connection fallback: end the read snapshot and take
                # one write transaction for the whole review
                conn.execute("COMMIT")
                conn.execute("BEGIN IMMEDIATE")
            try:
                if not header_written:
                    writer_conn.execute(
                        "INSERT INTO rule_reviews(token,version_id,base_version,"
                        "spec_sha256,offset_vector_json,publish_seq,stats_json,"
                        "created_at,correction_seq) VALUES(?,?,?,?,?,?,?,?,?)",
                        (token, candidate_id, base_id, candidate["spec_sha256"],
                         json.dumps(vector, sort_keys=True,
                                    separators=(",", ":")),
                         state["publish_seq"],
                         json.dumps(stats, sort_keys=True,
                                    separators=(",", ":")), now_ms, corr_seq))
                    header_written = True
                for p_pid, p_offset, kind, detail in pending:
                    writer_conn.execute(
                        "INSERT INTO rule_review_items(token,partition_id,offset,"
                        "kind,detail_json) VALUES(?,?,?,?,?)",
                        (token, p_pid, p_offset, kind, detail))
                if final_stats is not None:
                    # the header may have been written by an early page flush
                    # with only partial counters: persist the FINAL stats so
                    # the saved token always describes the complete section
                    writer_conn.execute(
                        "UPDATE rule_reviews SET stats_json=? WHERE token=?",
                        (final_stats, token))
                writer_conn.execute("COMMIT")
            except Exception:
                writer_conn.execute("ROLLBACK")
                raise
            pending.clear()
            if not own_writer:
                # cannot re-enter the historical snapshot; the fallback writes
                # everything in this one transaction, so it is only reached once
                conn.execute("BEGIN")

        for pid, last_off in vector.items():
            for offset in range(last_off + 1):
                stats["positions"] += 1
                kind, detail = _classify_position(
                    conn, candidate_id, base_id, pid, offset, revisions)
                stats[kind] += 1
                if kind == "same":
                    continue  # counted, but not materialised as a finding
                pending.append((pid, offset, kind, json.dumps(
                    detail, sort_keys=True, separators=(",", ":"),
                    ensure_ascii=False)))
                if own_writer and len(pending) >= REVIEW_PAGE:
                    flush()
        # always write the header even when there are zero findings, and
        # persist the final stats with the last page
        flush(force_header=not header_written,
              final_stats=json.dumps(stats, sort_keys=True,
                                     separators=(",", ":")))
        conn.execute("ROLLBACK")  # read-only snapshot release
    except Exception:
        conn.execute("ROLLBACK")
        if own_writer:
            # remove any pages already committed for this token so a failed
            # review never leaves a half-materialised, unusable token
            try:
                writer_conn.execute(
                    "DELETE FROM rule_review_items WHERE token=?", (token,))
                writer_conn.execute(
                    "DELETE FROM rule_reviews WHERE token=?", (token,))
                writer_conn.commit()
            except sqlite3.Error:
                pass
            writer_conn.close()
        raise
    if own_writer:
        writer_conn.close()

    return {"token": token, "candidate": candidate_id, "base": base_id,
            "vector": vector, "publish_seq": state["publish_seq"],
            "correction_seq": corr_seq,
            "spec_sha256": candidate["spec_sha256"], "stats": stats}


def get_review(conn: sqlite3.Connection, token: str) -> sqlite3.Row:
    row = conn.execute(
        "SELECT * FROM rule_reviews WHERE token=?", (token,)).fetchone()
    if row is None:
        raise ActivationError(f"unknown review token {token!r}")
    return row


def iter_review_items(conn: sqlite3.Connection, token: str):
    """Stream findings in source-position order (partition, offset)."""
    for row in conn.execute(
        "SELECT * FROM rule_review_items WHERE token=? "
        "ORDER BY partition_id, offset", (token,),
    ):
        yield row


# ---------------------------------------------------------------------- activation

def _candidate_covers_section(conn: sqlite3.Connection, candidate_id: str,
                           vector: dict[int, int]) -> tuple[bool, str | None]:
    """Every section position must have a CURRENT 'ok' candidate result.

    A result whose ``source_revision`` predates the position's current
    correction revision is stale and counts as not covered: the candidate
    must be recomputed over the corrected objects before it can publish.
    """
    for pid, last_off in vector.items():
        if last_off < 0:
            continue
        bad = conn.execute(
            "SELECT offset,status,error_json FROM rule_results "
            "WHERE version_id=? AND partition_id=? AND offset<=? "
            "AND status!='ok' LIMIT 1",
            (candidate_id, pid, last_off),
        ).fetchone()
        if bad is not None:
            return False, (f"candidate {candidate_id} rejects p{pid} offset "
                           f"{bad['offset']}: {bad['error_json']}")
        n = conn.execute(
            "SELECT COUNT(*) c FROM rule_results WHERE version_id=? "
            "AND partition_id=? AND offset BETWEEN 0 AND ? AND status='ok'",
            (candidate_id, pid, last_off),
        ).fetchone()["c"]
        if n != last_off + 1:
            return False, (f"candidate {candidate_id} has only {n}/{last_off + 1} "
                           f"successful results on partition {pid} (holes present)")
        stale = conn.execute(
            "SELECT cs.offset, cs.revision, rr.source_revision FROM "
            "correction_state cs JOIN rule_results rr ON rr.version_id=? "
            "AND rr.partition_id=cs.partition_id AND rr.offset=cs.offset "
            "WHERE cs.partition_id=? AND cs.offset<=? "
            "AND rr.source_revision < cs.revision LIMIT 1",
            (candidate_id, pid, last_off),
        ).fetchone()
        if stale is not None:
            return False, (
                f"candidate {candidate_id} result at p{pid} offset "
                f"{stale['offset']} is stale (source revision "
                f"{stale['source_revision']} < current {stale['revision']}); "
                "run recompute to catch up with source corrections")
    return True, None


def activate(conn: sqlite3.Connection, token: str, now_ms: int,
             reason: str = "manual") -> dict:
    """Atomically validate the token and section, then move the pointer."""
    conn.execute("BEGIN IMMEDIATE")
    try:
        review = get_review(conn, token)
        state = get_active(conn)
        candidate_id = review["version_id"]
        candidate = load_version(conn, candidate_id)
        vector = {int(k): v for k, v in
                  json.loads(review["offset_vector_json"]).items()}

        failures = []
        if review["base_version"] != state["active_version"]:
            failures.append(
                f"review base version {review['base_version']!r} is no longer "
                f"active (current {state['active_version']!r}); re-run review")
        if review["publish_seq"] != state["publish_seq"]:
            failures.append(
                f"review publish_seq {review['publish_seq']} is stale "
                f"(current {state['publish_seq']}); a publish happened after "
                "the review, re-run review")
        if review["spec_sha256"] != candidate["spec_sha256"]:
            failures.append("candidate rule content changed after the review")
        current_vector = snapshot_vector(conn)
        if current_vector != vector:
            failures.append(
                f"committed offset vector changed after the review: "
                f"review={vector} current={current_vector}; re-run review")
        current_corr_seq = correction_seq(conn)
        if review["correction_seq"] != current_corr_seq:
            failures.append(
                f"source corrections changed after the review: review "
                f"correction_seq={review['correction_seq']} current "
                f"{current_corr_seq}; a correction took effect (possibly "
                "since undone) - re-run review")

        # already-active candidate: report that no switch is needed. This never
        # "restores" the token -- nothing is written, the pointer/sequence do
        # not move, and the same token presented for any other purpose remains
        # bound to its original (now historical) section.
        if state["active_version"] == candidate_id:
            conn.execute("ROLLBACK")
            return {"action": "noop", "version_id": candidate_id,
                    "publish_seq": state["publish_seq"],
                    "warnings": failures,
                    "message": "version already active; no switch performed "
                               "(old review tokens are not restored)"}

        ok, why = _candidate_covers_section(conn, candidate_id, vector)
        if not ok:
            failures.append(why)

        if failures:
            conn.execute("ROLLBACK")
            raise ActivationError("; ".join(failures))

        new_seq = state["publish_seq"] + 1
        conn.execute(
            "UPDATE rule_state SET active_version=?, publish_seq=?, updated_at=? "
            "WHERE id=1", (candidate_id, new_seq, now_ms))
        conn.execute(
            "INSERT INTO rule_publishes(seq,version_id,at_ms,reason) "
            "VALUES(?,?,?,?)", (new_seq, candidate_id, now_ms, reason))
        # the whole historical recompute set becomes visible as ONE logical
        # publish batch in the delivery stream, inside this same transaction.
        # Candidate rows prepared by earlier recomputes stay invisible until
        # this commit; a crash before it leaves both the pointer and the
        # delivery log untouched, so a partial "new version" can never be read.
        from .delivery_store import emit_publish_events
        emitted = emit_publish_events(
            conn, candidate_id=candidate_id, vector=vector, new_seq=new_seq,
            review_token=token, now_ms=now_ms)
        conn.execute("COMMIT")
        return {"action": "switched", "version_id": candidate_id,
                "publish_seq": new_seq, "delivery_events": emitted}
    except ActivationError:
        raise
    except Exception:
        conn.execute("ROLLBACK")
        raise
