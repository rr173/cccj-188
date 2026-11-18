"""Correction workflow for already-committed source records.

A correction never rewrites collection facts: ``records.source_json`` /
``records.clean_json``, the original JSONL files, ``progress`` and other
versions' recompute results are all untouched.  Instead the workflow is:

    propose  -> a *proposal* row (invisible to every official read)
    approve  -> ONE ``BEGIN IMMEDIATE`` transaction that
                  * re-checks the expected per-position revision number,
                  * reads the current active rule version + publish sequence,
                  * appends an immutable revision to ``source_corrections``,
                  * moves the per-position pointer in ``correction_state``,
                  * reprocesses the new complete object under that version and
                    lands the outcome (result OR rejection diagnostic) as the
                    active version's official row for the position,
                  * bumps the global correction sequence exactly once;
                any failure rolls the whole transaction back, so a failed
                approval never leaves half a revision or changes official
                results.
    reject   -> operator decision on a pending proposal (no revision).

Undo is a normal correction whose replacement object is copied from an older
revision: it appends a NEW revision and never deletes or rewrites history.

Revision numbers are per-position, strictly increasing from 1; revision 0 is
defined as the originally collected ``records.source_json``.  A proposal and
its approval both carry the revision number the operator saw; a mismatch is a
stale-proposal conflict that returns the ACTUAL current revision.

The caller-provided ``correction_id`` is an idempotency key: retrying a
proposal with byte-identical content returns the existing row; reusing the id
with different content is rejected.
"""

from __future__ import annotations

import hashlib
import json
import re
import sqlite3

from .rule_engine import result_to_json
from .rule_store import build_outcome, get_active, load_version

#: upper bound for free-text fields, so diagnostics stay readable
MAX_REASON_LEN = 500
MAX_ID_LEN = 200
_CONTROL_RE = re.compile(r"[\x00-\x1f\x7f]")


class CorrectError(Exception):
    """Base class for correction diagnostics; ``exit_code`` maps to the CLI."""
    exit_code = 2


class UnknownCorrection(CorrectError):
    """No proposal exists for the given correction id."""
    exit_code = 9


class UnknownPosition(CorrectError):
    """The (partition, offset) coordinates name no committed record."""
    exit_code = 10


class InvalidCorrection(CorrectError):
    """Payload problem: non-object JSON, illegal reason, no-change, ..."""
    exit_code = 11


class CorrectionIdConflict(CorrectError):
    """The correction id already exists with DIFFERENT content."""
    exit_code = 12


class RevisionConflict(CorrectError):
    """Stale expected revision or an already-decided proposal."""

    def __init__(self, msg: str, actual_revision: int | None = None):
        super().__init__(msg)
        self.actual_revision = actual_revision
        self.exit_code = 13


# ---------------------------------------------------------------------- migrate

def correct_migrate(conn: sqlite3.Connection, now_ms: int) -> None:
    """Idempotent upgrade for the correction workflow.

    Adds the ``source_revision`` column to existing rule tables and seeds the
    global correction sequence.  Existing rows are never rewritten: SQLite
    fills constant column defaults at read time, so historical results simply
    read as revision 0 ("computed from the original source object"), which is
    exactly their meaning.  Safe to run repeatedly on empty and populated
    databases.
    """
    for table, column in (("rule_results", "source_revision"),
                          ("rule_reviews", "correction_seq")):
        cols = {r["name"] for r in conn.execute(f"PRAGMA table_info({table})")}
        if column not in cols:
            try:
                conn.execute(
                    f"ALTER TABLE {table} ADD COLUMN {column} "
                    f"INTEGER NOT NULL DEFAULT 0")
            except sqlite3.OperationalError as exc:
                # two processes upgrading the same database concurrently:
                # the other one won; the column now exists either way
                if "duplicate column" not in str(exc).lower():
                    raise
    conn.execute(
        "INSERT OR IGNORE INTO meta(key,value) VALUES('correction_seq','0')")


# ---------------------------------------------------------------------- helpers

def correction_seq(conn: sqlite3.Connection) -> int:
    row = conn.execute(
        "SELECT value FROM meta WHERE key='correction_seq'").fetchone()
    return int(row[0]) if row is not None else 0


def _bump_correction_seq(conn: sqlite3.Connection) -> int:
    seq = correction_seq(conn) + 1
    conn.execute(
        "INSERT INTO meta(key,value) VALUES('correction_seq',?) "
        "ON CONFLICT(key) DO UPDATE SET value=excluded.value", (str(seq),))
    return seq


def current_revision(conn: sqlite3.Connection, partition: int, offset: int) -> int:
    row = conn.execute(
        "SELECT revision FROM correction_state WHERE partition_id=? AND offset=?",
        (partition, offset)).fetchone()
    return row["revision"] if row is not None else 0


def _source_json(conn: sqlite3.Connection, partition: int, offset: int) -> str:
    row = conn.execute(
        "SELECT source_json FROM records WHERE partition_id=? AND offset=?",
        (partition, offset)).fetchone()
    if row is None:
        raise UnknownPosition(
            f"no committed record at partition {partition} offset {offset}; "
            "corrections can only target already-committed positions")
    return row["source_json"]


def effective_at(conn: sqlite3.Connection, partition: int, offset: int,
                 revision: int) -> dict:
    """The complete effective object of one position at a given revision."""
    if revision < 0:
        raise InvalidCorrection(f"revision must be >= 0, got {revision}")
    if revision == 0:
        return json.loads(_source_json(conn, partition, offset))
    row = conn.execute(
        "SELECT new_json FROM source_corrections WHERE partition_id=? "
        "AND offset=? AND revision=?", (partition, offset, revision)).fetchone()
    if row is None:
        cur = current_revision(conn, partition, offset)
        raise UnknownCorrection(
            f"partition {partition} offset {offset} has no revision "
            f"{revision} (current revision is {cur})")
    return json.loads(row["new_json"])


def effective_object(conn: sqlite3.Connection, partition: int,
                     offset: int) -> tuple[int, dict]:
    """``(revision, object)`` currently in force at one committed position."""
    rev = current_revision(conn, partition, offset)
    return rev, effective_at(conn, partition, offset, rev)


def collected_object(conn: sqlite3.Connection, partition: int,
                     offset: int) -> dict:
    """The originally collected object (validates the coordinates)."""
    return json.loads(_source_json(conn, partition, offset))


def revision_map(conn: sqlite3.Connection) -> dict[tuple[int, int], int]:
    """Current revision of every corrected position (small: corrections only)."""
    return {(r["partition_id"], r["offset"]): r["revision"]
            for r in conn.execute(
                "SELECT partition_id, offset, revision FROM correction_state")}


def _canonical(obj: dict) -> str:
    return result_to_json(obj)


def _content_hash(partition: int, offset: int, expected_revision: int,
                  new_canon: str, reason: str) -> str:
    blob = json.dumps({
        "partition_id": partition, "offset": offset,
        "expected_revision": expected_revision,
        "new_json": new_canon, "reason": reason,
    }, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def _validate_reason(reason) -> str:
    if not isinstance(reason, str):
        raise InvalidCorrection("reason must be a string")
    if reason.strip() == "":
        raise InvalidCorrection("reason must be a non-empty string")
    if len(reason) > MAX_REASON_LEN:
        raise InvalidCorrection(
            f"reason is {len(reason)} chars; maximum is {MAX_REASON_LEN}")
    if _CONTROL_RE.search(reason):
        raise InvalidCorrection("reason must not contain control characters")
    return reason


def _validate_id(correction_id) -> str:
    if not isinstance(correction_id, str) or correction_id.strip() == "":
        raise InvalidCorrection("correction id must be a non-empty string")
    if len(correction_id) > MAX_ID_LEN:
        raise InvalidCorrection(
            f"correction id is {len(correction_id)} chars; maximum "
            f"is {MAX_ID_LEN}")
    if _CONTROL_RE.search(correction_id):
        raise InvalidCorrection(
            "correction id must not contain control characters")
    return correction_id


def _validate_replacement(new_obj) -> dict:
    if not isinstance(new_obj, dict):
        raise InvalidCorrection(
            "replacement record must be a JSON object; got JSON "
            + ("array" if isinstance(new_obj, list)
               else type(new_obj).__name__))
    return new_obj


# ---------------------------------------------------------------------- propose

def propose(conn: sqlite3.Connection, partition: int, offset: int, *,
            correction_id: str, expected_revision: int, new_obj: dict,
            reason: str, now_ms: int) -> dict:
    """Create a correction proposal (or return the identical existing one).

    A proposal is inert: it changes no official read, recompute, review or
    export until it is approved.
    """
    _validate_id(correction_id)
    _validate_reason(reason)
    _validate_replacement(new_obj)
    if not isinstance(expected_revision, int) or expected_revision < 0:
        raise InvalidCorrection(
            f"expected revision must be an integer >= 0, got "
            f"{expected_revision!r}")

    # coordinates must name a committed record (also loads the original)
    _source_json(conn, partition, offset)
    cur_rev, cur_obj = effective_object(conn, partition, offset)
    new_canon = _canonical(new_obj)
    digest = _content_hash(partition, offset, expected_revision,
                           new_canon, reason)

    existing = conn.execute(
        "SELECT * FROM correction_proposals WHERE correction_id=?",
        (correction_id,)).fetchone()
    if existing is not None:
        if existing["content_sha256"] == digest:
            # idempotent retry of the exact same proposal content
            return {"created": False, "proposal": existing}
        raise CorrectionIdConflict(
            f"correction id {correction_id!r} already exists with different "
            f"content (submitted sha256={digest}, recorded "
            f"sha256={existing['content_sha256']}); use a new correction id")

    if expected_revision != cur_rev:
        raise RevisionConflict(
            f"stale proposal for p{partition} offset {offset}: expected "
            f"revision {expected_revision} but the current revision is "
            f"{cur_rev}; re-read the position and propose again",
            actual_revision=cur_rev)
    if new_canon == _canonical(cur_obj):
        raise InvalidCorrection(
            f"replacement object is identical to the current effective "
            f"object at p{partition} offset {offset} (revision {cur_rev}); "
            "a correction must change the record")

    try:
        conn.execute(
            "INSERT INTO correction_proposals(correction_id,partition_id,"
            "offset,expected_revision,prev_json,new_json,reason,"
            "content_sha256,state,created_at) "
            "VALUES(?,?,?,?,?,?,?,?,'pending',?)",
            (correction_id, partition, offset, expected_revision,
             _canonical(cur_obj), new_canon, reason, digest, now_ms))
    except sqlite3.IntegrityError:
        # a concurrent proposer won the id race between our check and insert
        winner = conn.execute(
            "SELECT * FROM correction_proposals WHERE correction_id=?",
            (correction_id,)).fetchone()
        if winner is not None and winner["content_sha256"] == digest:
            return {"created": False, "proposal": winner}
        raise CorrectionIdConflict(
            f"correction id {correction_id!r} already exists with different "
            "content (concurrent proposal); use a new correction id")
    row = conn.execute(
        "SELECT * FROM correction_proposals WHERE correction_id=?",
        (correction_id,)).fetchone()
    return {"created": True, "proposal": row}


def propose_restore(conn: sqlite3.Connection, partition: int, offset: int, *,
                    to_revision: int, correction_id: str,
                    expected_revision: int, reason: str,
                    now_ms: int) -> dict:
    """Undo: propose a replacement equal to an older revision's object.

    Implemented as an ordinary proposal; approving it appends a NEW revision
    (history is never deleted or rewritten).
    """
    if not isinstance(to_revision, int) or to_revision < 0:
        raise InvalidCorrection(
            f"target revision must be an integer >= 0, got {to_revision!r}")
    cur = current_revision(conn, partition, offset)
    if to_revision >= cur:
        raise InvalidCorrection(
            f"cannot restore revision {to_revision}: current revision of "
            f"p{partition} offset {offset} is {cur}; restore targets an "
            "older revision")
    target = effective_at(conn, partition, offset, to_revision)
    return propose(conn, partition, offset, correction_id=correction_id,
                   expected_revision=expected_revision, new_obj=target,
                   reason=reason, now_ms=now_ms)


# ---------------------------------------------------------------------- approve

def approve(conn: sqlite3.Connection, correction_id: str,
            now_ms: int) -> dict:
    """Effect one pending proposal inside a single transaction boundary.

    Losers of a concurrent approval race (and stale proposals) raise
    :class:`RevisionConflict`; the winner's revision is the only change.
    """
    conn.execute("BEGIN IMMEDIATE")
    try:
        prop = conn.execute(
            "SELECT * FROM correction_proposals WHERE correction_id=?",
            (correction_id,)).fetchone()
        if prop is None:
            conn.execute("ROLLBACK")
            raise UnknownCorrection(
                f"no correction proposal with id {correction_id!r}")
        if prop["state"] != "pending":
            note = prop["decided_note"] or ""
            conn.execute("ROLLBACK")
            raise RevisionConflict(
                f"correction {correction_id!r} is already {prop['state']}"
                + (f" ({note})" if note else "")
                + "; a proposal can be approved exactly once")

        pid, offset = prop["partition_id"], prop["offset"]
        # the position must still be a committed record (collection facts are
        # append-only, so this is a defensive re-check of the propose-time
        # validation)
        _source_json(conn, pid, offset)
        cur_rev = current_revision(conn, pid, offset)
        if cur_rev != prop["expected_revision"]:
            # someone else's correction took effect meanwhile: refuse, mark
            # the stale proposal rejected (a proposal-state change only; no
            # revision, no official result) and report the ACTUAL revision.
            conn.execute(
                "UPDATE correction_proposals SET state='rejected', "
                "decided_at=?, decided_note=? WHERE correction_id=?",
                (now_ms,
                 f"stale at approval: expected revision "
                 f"{prop['expected_revision']}, actual {cur_rev}",
                 correction_id))
            conn.execute("COMMIT")
            raise RevisionConflict(
                f"stale proposal for p{pid} offset {offset}: expected "
                f"revision {prop['expected_revision']} but the current "
                f"revision is {cur_rev}; the proposal was marked rejected - "
                "re-read the position and propose with a new correction id",
                actual_revision=cur_rev)

        # pin the official cleaning version inside the same transaction
        state = get_active(conn)
        version = load_version(conn, state["active_version"])
        new_obj = json.loads(prop["new_json"])
        status, payload = build_outcome(version, new_obj)
        new_rev = cur_rev + 1

        conn.execute(
            "INSERT INTO source_corrections(partition_id,offset,revision,"
            "correction_id,prev_json,new_json,reason,rule_version,"
            "publish_seq,created_at) VALUES(?,?,?,?,?,?,?,?,?,?)",
            (pid, offset, new_rev, correction_id, prop["prev_json"],
             prop["new_json"], prop["reason"], state["active_version"],
             state["publish_seq"], now_ms))
        conn.execute(
            "INSERT INTO correction_state(partition_id,offset,revision,"
            "updated_at) VALUES(?,?,?,?) ON CONFLICT(partition_id,offset) "
            "DO UPDATE SET revision=excluded.revision, "
            "updated_at=excluded.updated_at",
            (pid, offset, new_rev, now_ms))
        # the corrected object, processed under the CURRENT official version,
        # becomes that version's official row for the position - including an
        # explicit rejection diagnostic when the rule refuses the object.
        if status == "ok":
            conn.execute(
                "INSERT INTO rule_results(version_id,partition_id,offset,"
                "status,result_json,error_json,computed_at,source_revision) "
                "VALUES(?,?,?,'ok',?,NULL,?,?) "
                "ON CONFLICT(version_id,partition_id,offset) DO UPDATE SET "
                "status='ok',result_json=excluded.result_json,error_json=NULL,"
                "computed_at=excluded.computed_at,"
                "source_revision=excluded.source_revision",
                (state["active_version"], pid, offset, payload, now_ms,
                 new_rev))
        else:
            conn.execute(
                "INSERT INTO rule_results(version_id,partition_id,offset,"
                "status,result_json,error_json,computed_at,source_revision) "
                "VALUES(?,?,?,'rejected',NULL,?,?,?) "
                "ON CONFLICT(version_id,partition_id,offset) DO UPDATE SET "
                "status='rejected',result_json=NULL,"
                "error_json=excluded.error_json,"
                "computed_at=excluded.computed_at,"
                "source_revision=excluded.source_revision",
                (state["active_version"], pid, offset,
                 json.dumps(payload, sort_keys=True, separators=(",", ":"),
                            ensure_ascii=False),
                 now_ms, new_rev))
        seq = _bump_correction_seq(conn)
        conn.execute(
            "UPDATE correction_proposals SET state='applied', decided_at=?, "
            "decided_note=? WHERE correction_id=?",
            (now_ms, f"applied as revision {new_rev}", correction_id))
        conn.execute("COMMIT")
        return {
            "correction_id": correction_id,
            "partition_id": pid, "offset": offset,
            "revision": new_rev,
            "rule_version": state["active_version"],
            "publish_seq": state["publish_seq"],
            "correction_seq": seq,
            "outcome": status,
        }
    except CorrectError:
        raise
    except Exception:
        conn.execute("ROLLBACK")
        raise


def reject(conn: sqlite3.Connection, correction_id: str, note: str,
           now_ms: int) -> dict:
    """Operator rejection of a pending proposal (no revision is appended)."""
    if note is not None and len(note) > MAX_REASON_LEN:
        raise InvalidCorrection(
            f"note is {len(note)} chars; maximum is {MAX_REASON_LEN}")
    conn.execute("BEGIN IMMEDIATE")
    try:
        prop = conn.execute(
            "SELECT * FROM correction_proposals WHERE correction_id=?",
            (correction_id,)).fetchone()
        if prop is None:
            conn.execute("ROLLBACK")
            raise UnknownCorrection(
                f"no correction proposal with id {correction_id!r}")
        if prop["state"] != "pending":
            prev = prop["decided_note"] or ""
            conn.execute("ROLLBACK")
            raise RevisionConflict(
                f"correction {correction_id!r} is already {prop['state']}"
                + (f" ({prev})" if prev else ""))
        conn.execute(
            "UPDATE correction_proposals SET state='rejected', decided_at=?, "
            "decided_note=? WHERE correction_id=?",
            (now_ms, note or "rejected by operator", correction_id))
        conn.execute("COMMIT")
        return {"correction_id": correction_id, "state": "rejected"}
    except CorrectError:
        raise
    except Exception:
        conn.execute("ROLLBACK")
        raise


# ----------------------------------------------------------------------- reads

def get_proposal(conn: sqlite3.Connection, correction_id: str) -> sqlite3.Row:
    row = conn.execute(
        "SELECT * FROM correction_proposals WHERE correction_id=?",
        (correction_id,)).fetchone()
    if row is None:
        raise UnknownCorrection(
            f"no correction proposal with id {correction_id!r}")
    return row


def iter_proposals(conn: sqlite3.Connection, state: str | None = None):
    sql = "SELECT * FROM correction_proposals"
    params: tuple = ()
    if state is not None:
        sql += " WHERE state=?"
        params = (state,)
    sql += " ORDER BY created_at, correction_id"
    yield from conn.execute(sql, params)


def iter_history(conn: sqlite3.Connection, partition: int, offset: int):
    """Stream the immutable revision chain of one position, oldest first."""
    yield from conn.execute(
        "SELECT * FROM source_corrections WHERE partition_id=? AND offset=? "
        "ORDER BY revision", (partition, offset))


def show_position(conn: sqlite3.Connection, partition: int, offset: int) -> dict:
    """Original vs current effective view of one committed position."""
    source_json = _source_json(conn, partition, offset)
    rev = current_revision(conn, partition, offset)
    effective = effective_at(conn, partition, offset, rev)
    state = get_active(conn)
    result = conn.execute(
        "SELECT status, result_json, error_json, source_revision FROM "
        "rule_results WHERE version_id=? AND partition_id=? AND offset=?",
        (state["active_version"], partition, offset)).fetchone()
    out = {
        "partition_id": partition,
        "offset": offset,
        "current_revision": rev,
        "correction_seq": correction_seq(conn),
        "original": json.loads(source_json),
        "effective": effective,
        "active_version": state["active_version"],
        "publish_seq": state["publish_seq"],
    }
    if result is not None:
        out["active_result"] = {
            "status": result["status"],
            "source_revision": result["source_revision"],
        }
        if result["status"] == "ok":
            out["active_result"]["result"] = json.loads(result["result_json"])
        else:
            out["active_result"]["error"] = json.loads(result["error_json"])
    return out
