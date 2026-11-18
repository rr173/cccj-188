"""Durable change-delivery log and named consumer group (persistent CDC).

This module is the storage half of the change-delivery feature.  It owns:

* an append-only, gap-free event stream (``delivery_events``) grouped into
  atomic logical batches (``delivery_batches``);
* producer hooks called *inside the same transaction* as the business change,
  so a committed ingest/correction/publish can never exist without its
  delivery events and vice versa;
* named consumers with a pinned initial-snapshot boundary, at-most-one
  outstanding page, opaque cursors and strict, audited ack validation.

Delivery contract (at-least-once):

* every event carries the stable source position ``(partition_id, offset)``
  plus the source revision, rule version and official result/rejection
  diagnostic that were in force when it was emitted;
* a later state of the SAME position is a NEW event (never a new position):
  consumers order by ``seq`` and overwrite the old state keyed by position;
* the whole historical recompute set of one rule activation is ONE
  ``publish`` batch. Physical read pages are strictly bounded by the
  requested page size and never mix batches, so a large batch arrives as
  several chunks of that one batch; only the final chunk is marked
  ``batch_complete`` and a consumer can never confirm past a batch while part
  of it is undelivered;
* candidate recompute results and unapproved correction proposals are never
  emitted - only the activation/approval transaction makes them visible;
* one outstanding page per consumer is pinned (cursor + content digest), so a
  re-read before ack is byte-identical and a content-changing retry is
  rejected; stale, skip-ahead, cross-consumer and non-matching acks fail with
  an explicit diagnostic and a durable audit row.
"""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
import sqlite3
import time

# --------------------------------------------------------------------------- errors


class DeliveryUpgradeLock:
    """Exclusive flock serialising the first delivery upgrade per database.

    Lives in a sidecar file like the coordinator/recompute locks, so it
    survives WAL checkpoints and is released by the kernel if the upgrader is
    SIGKILLed. It is only held during the (paged) bootstrap; steady-state
    producers take no extra lock.
    """

    def __init__(self, db_path: str):
        self._path = db_path + ".deliverylock"
        self._fh = None

    def acquire(self, timeout: float = 30.0) -> bool:
        self._fh = open(self._path, "w")
        deadline = time.monotonic() + timeout
        while True:
            try:
                fcntl.flock(self._fh.fileno(),
                            fcntl.LOCK_EX | fcntl.LOCK_NB)
                self._fh.write(str(os.getpid()))
                self._fh.flush()
                return True
            except BlockingIOError:
                if time.monotonic() >= deadline:
                    self._fh.close()
                    self._fh = None
                    return False
                time.sleep(0.05)

    def release(self) -> None:
        if self._fh is not None:
            fcntl.flock(self._fh.fileno(), fcntl.LOCK_UN)
            self._fh.close()
            self._fh = None


class DeliveryError(Exception):
    """Base class for delivery diagnostics; ``exit_code`` maps to the CLI."""

    exit_code = 20


class UnknownConsumer(DeliveryError):
    exit_code = 21


class ConsumerExists(DeliveryError):
    exit_code = 22


class ConsumerNotReady(DeliveryError):
    """Read/ack before the initial sync was started, or after it finished."""

    exit_code = 23


class NoOutstandingPage(DeliveryError):
    """Replay/ack requested while the consumer has no unacknowledged page."""

    exit_code = 24


class AckRejected(DeliveryError):
    """Stale/skip/cross/non-matching ack; the audit table records the reason."""

    exit_code = 25


# ---------------------------------------------------------------------- migration

MIGRATION_MARKER = "delivery_migration_bootstrap_v1"


def delivery_migrate(conn: sqlite3.Connection, now_ms: int,
                     db_path: str | None = None) -> None:
    """Idempotent upgrade; safe on empty and populated databases.

    On the first upgrade of a database that already contains official
    results, one immutable ``bootstrap`` batch seeds the delivery stream with
    the *current* official state of every committed position.  A consumer
    created afterwards therefore starts from exactly the same snapshot a
    full export would produce, without the pre-upgrade history having been
    logged event by event.

    The backfill is serialised across processes by a sidecar flock and runs
    in short, resumable transactions: a crashed upgrade reuses its existing
    bootstrap batch and only inserts positions still absent from the event
    stream, so it can never duplicate events or interleave with producer
    emissions. Repeating the upgrade changes nothing.
    """
    already = conn.execute(
        "SELECT 1 FROM meta WHERE key=?", (MIGRATION_MARKER,)).fetchone()
    if already is not None:
        return
    if db_path is None:
        # in-process callers without a path keep the old best-effort path;
        # every CLI/worker entry point passes the path and gets the flock.
        _delivery_migrate_locked(conn, now_ms)
        return
    lock = DeliveryUpgradeLock(db_path)
    if not lock.acquire(timeout=30.0):
        raise DeliveryError("could not acquire the delivery upgrade lock")
    try:
        # another process may have completed the upgrade while we waited
        if conn.execute(
                "SELECT 1 FROM meta WHERE key=?",
                (MIGRATION_MARKER,)).fetchone() is not None:
            return
        _delivery_migrate_locked(conn, now_ms)
    finally:
        lock.release()


def _delivery_migrate_locked(conn: sqlite3.Connection, now_ms: int) -> None:
    # nothing official yet: any future change will be logged from its own
    # producer transaction, so the marker alone is sufficient.
    have_rows = conn.execute(
        "SELECT 1 FROM rule_results LIMIT 1").fetchone()
    if have_rows is None:
        conn.execute(
            "INSERT OR IGNORE INTO meta(key,value) VALUES(?,?)",
            (MIGRATION_MARKER, str(now_ms)))
        return

    state = conn.execute("SELECT * FROM rule_state WHERE id=1").fetchone()
    active_version = state["active_version"]
    publish_seq = state["publish_seq"]
    # resume after a crash: reuse an unfinished bootstrap batch instead of
    # opening a second one
    existing = conn.execute(
        "SELECT batch_id FROM delivery_batches WHERE kind='bootstrap' "
        "ORDER BY batch_id LIMIT 1").fetchone()
    if existing is not None:
        batch_id = existing["batch_id"]
    else:
        batch_id = conn.execute(
            "INSERT INTO delivery_batches(kind,producer_ref,rule_version,"
            "publish_seq,created_at,event_count) "
            "VALUES('bootstrap',?,?,?,?,0)",
            ("migration", active_version, publish_seq, now_ms)).lastrowid

    page = 500
    total = conn.execute(
        "SELECT COUNT(*) c FROM delivery_events WHERE batch_id=?",
        (batch_id,)).fetchone()["c"]
    while True:
        # a position already present ANYWHERE in the stream (a resumed
        # bootstrap, or any producer row) is never inserted again
        rows = conn.execute(
            "SELECT rr.partition_id, rr.offset, rr.status, rr.result_json, "
            "rr.error_json, rr.source_revision FROM rule_results rr "
            "WHERE rr.version_id=? AND NOT EXISTS ("
            "  SELECT 1 FROM delivery_events de WHERE de.partition_id=rr.partition_id "
            "  AND de.offset=rr.offset) "
            "ORDER BY rr.partition_id, rr.offset LIMIT ?",
            (active_version, page)).fetchall()
        if not rows:
            break
        conn.execute("BEGIN IMMEDIATE")
        try:
            for r in rows:
                conn.execute(
                    "INSERT INTO delivery_events(batch_id,partition_id,offset,"
                    "source_revision,rule_version,status,result_json,error_json,"
                    "created_at) VALUES(?,?,?,?,?,?,?,?,?)",
                    (batch_id, r["partition_id"], r["offset"],
                     r["source_revision"], active_version, r["status"],
                     r["result_json"], r["error_json"], now_ms))
            total += len(rows)
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise

    conn.execute("BEGIN IMMEDIATE")
    try:
        conn.execute(
            "UPDATE delivery_batches SET event_count=? WHERE batch_id=?",
            (total, batch_id))
        conn.execute(
            "INSERT INTO meta(key,value) VALUES(?,?)",
            (MIGRATION_MARKER, str(now_ms)))
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise


# ------------------------------------------------------------------------ helpers

def max_event_seq(conn: sqlite3.Connection) -> int:
    row = conn.execute("SELECT MAX(seq) m FROM delivery_events").fetchone()
    return row["m"] if row is not None and row["m"] is not None else 0


def _open_batch(conn: sqlite3.Connection, kind: str, producer_ref: str,
                rule_version: str, publish_seq: int, now_ms: int) -> int:
    cur = conn.execute(
        "INSERT INTO delivery_batches(kind,producer_ref,rule_version,"
        "publish_seq,created_at,event_count) VALUES(?,?,?,?,?,0)",
        (kind, producer_ref, rule_version, publish_seq, now_ms))
    return cur.lastrowid


def _close_batch(conn: sqlite3.Connection, batch_id: int, count: int) -> None:
    conn.execute(
        "UPDATE delivery_batches SET event_count=? WHERE batch_id=?",
        (count, batch_id))


def _insert_event(conn: sqlite3.Connection, *, batch_id: int, partition: int,
                  offset: int, source_revision: int, rule_version: str,
                  status: str, result_json: str | None,
                  error_json: str | None, now_ms: int) -> None:
    conn.execute(
        "INSERT INTO delivery_events(batch_id,partition_id,offset,"
        "source_revision,rule_version,status,result_json,error_json,created_at)"
        " VALUES(?,?,?,?,?,?,?,?,?)",
        (batch_id, partition, offset, source_revision, rule_version, status,
         result_json, error_json, now_ms))


# ---------------------------------------------------------------- producer hooks
# Every helper below assumes the caller already holds the producer's
# BEGIN IMMEDIATE transaction; it only adds rows to it, so business state and
# delivery events commit or roll back together.


def emit_ingest_events(conn: sqlite3.Connection, *, partition: int,
                       items, rule_outcomes, active_version: str,
                       publish_seq: int, worker_id: str,
                       now_ms: int) -> None:
    """Emit one ``ingest`` event per newly committed source position.

    ``items`` are :class:`~pcg.sink.BatchItem` rows and ``rule_outcomes`` the
    parallel ``(offset, status, payload_json)`` list built inside the same
    fenced commit; new positions are always at source revision 0.
    """
    if not items:
        return
    batch_id = _open_batch(conn, "ingest", worker_id, active_version,
                           publish_seq, now_ms)
    n = 0
    for it, (offset, status, payload) in zip(items, rule_outcomes):
        if status == "ok":
            result_json, error_json = payload, None
        else:
            result_json, error_json = None, payload
        _insert_event(conn, batch_id=batch_id, partition=partition,
                      offset=offset, source_revision=0,
                      rule_version=active_version, status=status,
                      result_json=result_json, error_json=error_json,
                      now_ms=now_ms)
        n += 1
    _close_batch(conn, batch_id, n)


def emit_correction_event(conn: sqlite3.Connection, *, partition: int,
                          offset: int, new_revision: int, status: str,
                          payload: str, active_version: str,
                          publish_seq: int, correction_id: str,
                          now_ms: int) -> None:
    """Emit the single ``correction`` event of an approval transaction."""
    batch_id = _open_batch(conn, "correction", correction_id, active_version,
                           publish_seq, now_ms)
    if status == "ok":
        result_json, error_json = payload, None
    else:
        result_json, error_json = None, payload
    _insert_event(conn, batch_id=batch_id, partition=partition, offset=offset,
                  source_revision=new_revision, rule_version=active_version,
                  status=status, result_json=result_json,
                  error_json=error_json, now_ms=now_ms)
    _close_batch(conn, batch_id, 1)


def emit_publish_events(conn: sqlite3.Connection, *, candidate_id: str,
                        vector: dict[int, int], new_seq: int,
                        review_token: str, now_ms: int,
                        page_size: int = 500) -> int:
    """Emit the complete ``publish`` batch of a rule activation.

    Every position covered by the published section gets exactly one event
    carrying the candidate's official row (result or rejection diagnostic) and
    the source revision that row was computed from.  The rows are inserted in
    bounded keyset pages while staying inside the caller's activation
    transaction; the batch row is created first and finalised last, so any
    reader - including a process that crashes mid-way and reopens the database
    - sees the events only together with the committed pointer switch and
    never as a partial "new version".

    Returns the number of emitted events.
    """
    batch_id = _open_batch(conn, "publish", review_token, candidate_id,
                           new_seq, now_ms)
    total = 0
    # deterministic section order: partition id then offset, matching exports
    for pid in sorted(vector):
        last_off = vector[pid]
        if last_off < 0:
            continue
        after = -1
        while after < last_off:
            rows = conn.execute(
                "SELECT offset, status, result_json, error_json, "
                "source_revision FROM rule_results WHERE version_id=? "
                "AND partition_id=? AND offset>? ORDER BY offset LIMIT ?",
                (candidate_id, pid, after, page_size)).fetchall()
            if not rows:
                # activation already proved full coverage; a missing row here
                # is corruption and must abort the whole publish transaction
                raise RuntimeError(
                    f"publish of {candidate_id!r}: missing rule_results row "
                    f"at p{pid} offset > {after} (section ends {last_off})")
            for r in rows:
                _insert_event(
                    conn, batch_id=batch_id, partition=pid, offset=r["offset"],
                    source_revision=r["source_revision"],
                    rule_version=candidate_id, status=r["status"],
                    result_json=r["result_json"], error_json=r["error_json"],
                    now_ms=now_ms)
                total += 1
            after = rows[-1]["offset"]
    _close_batch(conn, batch_id, total)
    return total


# ------------------------------------------------------------- consumer cursors

def _make_cursor(consumer_id: str, from_seq: int, to_seq: int) -> str:
    raw = f"{consumer_id}|{from_seq}|{to_seq}"
    digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:32]
    return f"{from_seq}:{to_seq}:{digest}"


def _parse_cursor(consumer_id: str, cursor: str) -> tuple[int, int]:
    parts = cursor.split(":")
    if len(parts) != 3:
        raise AckRejected(f"malformed cursor {cursor!r}")
    try:
        from_seq, to_seq = int(parts[0]), int(parts[1])
    except ValueError:
        raise AckRejected(f"malformed cursor {cursor!r}")
    if _make_cursor(consumer_id, from_seq, to_seq) != cursor:
        raise AckRejected("cursor token does not match its bounds/consumer")
    return from_seq, to_seq


def _page_digest(events: list[sqlite3.Row]) -> str:
    h = hashlib.sha256()
    for e in events:
        h.update(json.dumps({
            "seq": e["seq"], "batch_id": e["batch_id"],
            "partition_id": e["partition_id"], "offset": e["offset"],
            "source_revision": e["source_revision"],
            "rule_version": e["rule_version"], "status": e["status"],
            "result_json": e["result_json"], "error_json": e["error_json"],
        }, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode())
    h.update(b"|page-boundary|")
    return h.hexdigest()


def _event_payload(e: sqlite3.Row) -> dict:
    out = {
        "seq": e["seq"],
        "batch_id": e["batch_id"],
        "partition_id": e["partition_id"],
        "offset": e["offset"],
        "source_revision": e["source_revision"],
        "rule_version": e["rule_version"],
        "publish_seq": e["publish_seq"] if "publish_seq" in e.keys() else None,
        "status": e["status"],
    }
    if e["status"] == "ok":
        out["result"] = json.loads(e["result_json"])
    else:
        out["error"] = json.loads(e["error_json"])
    return out


# ------------------------------------------------------------ consumer lifecycle

def create_consumer(conn: sqlite3.Connection, consumer_id: str,
                    now_ms: int) -> dict:
    """Register a named consumer. No snapshot boundary is pinned yet."""
    existing = conn.execute(
        "SELECT * FROM delivery_consumers WHERE consumer_id=?",
        (consumer_id,)).fetchone()
    if existing is not None:
        raise ConsumerExists(f"consumer {consumer_id!r} already exists")
    conn.execute(
        "INSERT INTO delivery_consumers(consumer_id,state,created_at,acked_seq)"
        " VALUES(?,'registered',?,0)", (consumer_id, now_ms))
    return {"consumer_id": consumer_id, "state": "registered",
            "boundary_seq": None, "acked_seq": 0}


def get_consumer(conn: sqlite3.Connection, consumer_id: str) -> sqlite3.Row:
    row = conn.execute(
        "SELECT * FROM delivery_consumers WHERE consumer_id=?",
        (consumer_id,)).fetchone()
    if row is None:
        raise UnknownConsumer(f"unknown consumer {consumer_id!r}")
    return row


def list_consumers(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    return list(conn.execute(
        "SELECT * FROM delivery_consumers ORDER BY consumer_id"))


def start_sync(conn: sqlite3.Connection, consumer_id: str,
               now_ms: int) -> dict:
    """Pin the consistent initial-snapshot boundary and enter initial sync.

    The boundary is the current maximum durable event seq.  Because every
    producer commits its events together with its business change inside one
    transaction, all events ``seq <= boundary`` already represent complete
    official states and every later change is a separate event strictly after
    the boundary - there is no gap between the snapshot and the live feed.
    Idempotent: re-starting an already-started consumer returns its existing
    boundary instead of moving it.
    """
    conn.execute("BEGIN IMMEDIATE")
    try:
        row = get_consumer(conn, consumer_id)
        if row["state"] != "registered":
            _rollback_quietly(conn)
            return {
                "consumer_id": consumer_id, "state": row["state"],
                "boundary_seq": row["boundary_seq"],
                "acked_seq": row["acked_seq"],
                "already_started": True,
            }
        boundary = max_event_seq(conn)
        # an empty snapshot (no official state yet) needs no paging: the
        # consumer is immediately live and receives every change from seq 1.
        new_state = "active" if boundary == 0 else "syncing"
        conn.execute(
            "UPDATE delivery_consumers SET state=?, boundary_seq=? "
            "WHERE consumer_id=?", (new_state, boundary, consumer_id))
        conn.execute("COMMIT")
        return {"consumer_id": consumer_id, "state": new_state,
                "boundary_seq": boundary, "acked_seq": 0,
                "already_started": False}
    except Exception:
        _rollback_quietly(conn)
        raise


def _batch_span(conn: sqlite3.Connection, seq: int) -> tuple[int, int, int]:
    """Return ``(batch_id, first_seq, last_seq)`` of the event at ``seq``."""
    row = conn.execute(
        "SELECT e.batch_id, b.event_count FROM delivery_events e "
        "JOIN delivery_batches b ON b.batch_id=e.batch_id WHERE e.seq=?",
        (seq,)).fetchone()
    if row is None:
        raise DeliveryError(f"no event at seq {seq}")
    span = conn.execute(
        "SELECT MIN(seq) lo, MAX(seq) hi FROM delivery_events WHERE batch_id=?",
        (row["batch_id"],)).fetchone()
    return row["batch_id"], span["lo"], span["hi"]


def _rollback_quietly(conn: sqlite3.Connection) -> None:
    """Roll back any open write transaction; tolerate 'no transaction'."""
    try:
        conn.execute("ROLLBACK")
    except sqlite3.OperationalError:
        pass


def _record_ack_rejection(conn: sqlite3.Connection, now_ms: int,
                          consumer_id: str, cursor: str | None,
                          reason: str) -> None:
    conn.execute(
        "INSERT INTO delivery_ack_rejects(at_ms,consumer_id,cursor,reason)"
        " VALUES(?,?,?,?)", (now_ms, consumer_id, cursor, reason))


def read_page(conn: sqlite3.Connection, consumer_id: str, page_size: int,
              now_ms: int) -> dict:
    """Return the next page (or the pinned outstanding page) for a consumer.

    Streaming/keyset semantics: at most ``page_size`` candidate events are
    loaded, but a page that would end inside a publish batch is extended to
    the batch boundary so batches are delivered whole.  The first read after
    ``start`` serves the snapshot events ``1..boundary_seq``; afterwards the
    same mechanism serves live events with no discontinuity.
    """
    if page_size < 1:
        raise DeliveryError("page_size must be >= 1")
    conn.execute("BEGIN IMMEDIATE")
    try:
        consumer = get_consumer(conn, consumer_id)
        if consumer["state"] == "registered":
            _rollback_quietly(conn)
            raise ConsumerNotReady(
                f"consumer {consumer_id!r} has not started its initial sync; "
                "run 'delivery start' first")

        # an unacknowledged page is immutable: re-read returns the exact same
        # records and cursor (at-least-once replay after a crash).
        if consumer["page_cursor"] is not None:
            events = conn.execute(
                "SELECT e.*, b.publish_seq FROM delivery_events e "
                "JOIN delivery_batches b ON b.batch_id=e.batch_id "
                "WHERE e.seq BETWEEN ? AND ? ORDER BY e.seq",
                (consumer["page_from_seq"], consumer["page_to_seq"])).fetchall()
            digest = _page_digest(events)
            if digest != consumer["page_digest"]:
                # the log is append-only/immutable, so this indicates tampering
                _rollback_quietly(conn)
                raise DeliveryError(
                    "outstanding page content changed underneath the consumer; "
                    "delivery log integrity violated")
            return _page_response(conn, consumer, events, replayed=True)

        from_seq = consumer["acked_seq"] + 1
        head = max_event_seq(conn)
        if from_seq > head:
            _rollback_quietly(conn)
            return {"consumer_id": consumer_id, "cursor": None,
                    "from_seq": consumer["acked_seq"], "to_seq": consumer["acked_seq"],
                    "events": [], "replayed": False,
                    "state": consumer["state"], "boundary_seq": consumer["boundary_seq"],
                    "head_seq": head, "caught_up": True,
                    "batch_id": None, "batch_complete": True}

        # physical pages are strictly bounded by page_size and never mix
        # batches. The page is the next up to page_size events of the single
        # batch containing from_seq (a large batch is read as several
        # page-sized chunks). Its logical identity (batch_id and the
        # batch_complete flag on the final chunk) stays indivisible, so a
        # consumer can never treat a partially delivered batch as finished.
        first_batch, _batch_lo, batch_hi = _batch_span(conn, from_seq)

        in_initial_sync = (consumer["state"] == "syncing"
                           and consumer["boundary_seq"] is not None)
        cap = batch_hi
        if in_initial_sync and cap > consumer["boundary_seq"]:
            # snapshot pages stay inside 1..boundary; the boundary is itself a
            # batch boundary, so this only shortens the final snapshot page
            if from_seq > consumer["boundary_seq"]:
                _rollback_quietly(conn)
                raise DeliveryError(
                    "initial-sync read passed its snapshot boundary")
            cap = consumer["boundary_seq"]

        events = conn.execute(
            "SELECT e.*, b.publish_seq FROM delivery_events e "
            "JOIN delivery_batches b ON b.batch_id=e.batch_id "
            "WHERE e.seq BETWEEN ? AND ? ORDER BY e.seq LIMIT ?",
            (from_seq, cap, page_size)).fetchall()
        if not events:
            _rollback_quietly(conn)
            raise DeliveryError(
                f"no events found for expected page starting at seq {from_seq}")
        to_seq = events[-1]["seq"]
        batch_complete = to_seq == cap and cap == batch_hi
        cursor = _make_cursor(consumer_id, from_seq, to_seq)
        digest = _page_digest(events)
        conn.execute(
            "UPDATE delivery_consumers SET page_from_seq=?, page_to_seq=?, "
            "page_cursor=?, page_digest=?, page_delivered_at=? "
            "WHERE consumer_id=?",
            (from_seq, to_seq, cursor, digest, now_ms, consumer_id))
        conn.execute("COMMIT")
        return _page_response(
            conn, consumer, events, replayed=False,
            override_cursor=cursor, override_to=to_seq, override_from=from_seq,
            override_batch=first_batch, override_batch_complete=batch_complete)
    except Exception:
        _rollback_quietly(conn)
        raise


def _page_response(conn: sqlite3.Connection, consumer: sqlite3.Row,
                   events: list[sqlite3.Row], *,
                   replayed: bool, override_cursor: str | None = None,
                   override_from: int | None = None,
                   override_to: int | None = None,
                   override_batch: int | None = None,
                   override_batch_complete: bool | None = None) -> dict:
    from_seq = override_from if override_from is not None \
        else consumer["page_from_seq"]
    to_seq = override_to if override_to is not None \
        else consumer["page_to_seq"]
    cursor = override_cursor if override_cursor is not None \
        else consumer["page_cursor"]
    boundary = consumer["boundary_seq"]
    state = consumer["state"]
    in_sync = state == "syncing" and boundary is not None
    boundary_page = in_sync and from_seq <= boundary <= to_seq
    # every physical page holds events of exactly one batch
    batch_ids = {e["batch_id"] for e in events}
    if len(batch_ids) != 1:
        raise DeliveryError("a delivery page mixed two batches")
    batch_id = next(iter(batch_ids))
    if override_batch is not None and batch_id != override_batch:
        raise DeliveryError("page batch id disagrees with its cursor page")
    _, _, batch_hi = _batch_span(conn, events[-1]["seq"])
    batch_complete = (override_batch_complete if override_batch_complete
                      is not None else to_seq == batch_hi)
    return {
        "consumer_id": consumer["consumer_id"],
        "state": state,
        "boundary_seq": boundary,
        "from_seq": from_seq,
        "to_seq": to_seq,
        "cursor": cursor,
        "replayed": replayed,
        "snapshot_page": in_sync and to_seq <= boundary,
        "boundary_page": boundary_page,
        "batch_id": batch_id,
        "batch_complete": batch_complete,
        "events": [_event_payload(e) for e in events],
        "head_seq": to_seq,
    }


def ack_page(conn: sqlite3.Connection, consumer_id: str, cursor: str,
             now_ms: int) -> dict:
    """Validate and commit an acknowledgement; promote state at the boundary.

    The ack is accepted only when it names the consumer's actual outstanding
    page: exact consumer, exact ``from = acked+1`` continuity, exact
    ``to`` and a cursor token whose digest still matches the stored page.
    Everything else (stale cursor, skip-ahead, cross-consumer token,
    changed-content retry) is rejected with an audit row - never silently
    advanced.
    """
    conn.execute("BEGIN IMMEDIATE")
    try:
        try:
            consumer = get_consumer(conn, consumer_id)
        except UnknownConsumer as exc:
            _record_ack_rejection(conn, now_ms, consumer_id, cursor, str(exc))
            conn.execute("COMMIT")
            raise

        if consumer["page_cursor"] is None:
            reason = "no outstanding page: nothing to acknowledge (replay first)"
            _record_ack_rejection(conn, now_ms, consumer_id, cursor, reason)
            conn.execute("COMMIT")
            raise NoOutstandingPage(reason)

        try:
            from_seq, to_seq = _parse_cursor(consumer_id, cursor)
        except AckRejected as exc:
            _record_ack_rejection(conn, now_ms, consumer_id, cursor, str(exc))
            conn.execute("COMMIT")
            raise

        reasons = []
        if from_seq != consumer["page_from_seq"] or to_seq != consumer["page_to_seq"]:
            reasons.append(
                f"cursor bounds {from_seq}..{to_seq} do not match the "
                f"outstanding page {consumer['page_from_seq']}.."
                f"{consumer['page_to_seq']}")
        if from_seq != consumer["acked_seq"] + 1:
            reasons.append(
                f"non-contiguous ack: page starts at {from_seq}, expected "
                f"{consumer['acked_seq'] + 1}")
        if cursor != consumer["page_cursor"]:
            reasons.append("cursor token does not match the delivered page token")
        # re-read the page and prove its content is still exactly what was
        # delivered; a content-changing retry cannot be acknowledged.
        events = conn.execute(
            "SELECT * FROM delivery_events WHERE seq BETWEEN ? AND ? ORDER BY seq",
            (consumer["page_from_seq"], consumer["page_to_seq"])).fetchall()
        if _page_digest(events) != consumer["page_digest"]:
            reasons.append("page content differs from the delivered page")

        # batch invariant: one physical page is fully contained in exactly one
        # logical batch; it may be a middle chunk of a large batch, but it can
        # never cross from one batch into another. An ack therefore cannot
        # confirm the next batch while any part of the current one is
        # undelivered. Reads expose batch_complete so downstream knows when the
        # whole logical batch has been applied.
        if not reasons:
            lo_batch, _, _ = _batch_span(conn, from_seq)
            hi_batch, _, hi_hi = _batch_span(conn, to_seq)
            if lo_batch != hi_batch:
                reasons.append(
                    f"page crosses batches {lo_batch} and {hi_batch}: a page "
                    "must stay inside one logical batch")
            if hi_hi < to_seq:
                reasons.append(
                    f"page ends at seq {to_seq}, past batch {hi_batch}'s "
                    f"end at {hi_hi}")

        if reasons:
            reason = "; ".join(reasons)
            _record_ack_rejection(conn, now_ms, consumer_id, cursor, reason)
            conn.execute("COMMIT")
            raise AckRejected(reason)

        new_state = consumer["state"]
        if consumer["state"] == "syncing":
            # acking the page that reaches the pinned boundary completes the
            # initial sync; live events (if any were attached) are included in
            # the same whole-batch pages, so no gap is possible.
            if to_seq >= consumer["boundary_seq"]:
                new_state = "active"
        conn.execute(
            "UPDATE delivery_consumers SET acked_seq=?, page_from_seq=NULL, "
            "page_to_seq=NULL, page_cursor=NULL, page_digest=NULL, "
            "page_delivered_at=NULL, state=? WHERE consumer_id=?",
            (to_seq, new_state, consumer_id))
        conn.execute("COMMIT")
        return {"consumer_id": consumer_id, "acked_seq": to_seq,
                "state": new_state, "boundary_seq": consumer["boundary_seq"],
                "head_seq": max_event_seq(conn)}
    except DeliveryError:
        raise
    except Exception:
        _rollback_quietly(conn)
        raise


def position(conn: sqlite3.Connection, consumer_id: str) -> dict:
    """Read-only view of a consumer's current delivery position."""
    consumer = get_consumer(conn, consumer_id)
    head = max_event_seq(conn)
    out = {
        "consumer_id": consumer_id,
        "state": consumer["state"],
        "boundary_seq": consumer["boundary_seq"],
        "acked_seq": consumer["acked_seq"],
        "head_seq": head,
        "pending_events": max(0, head - consumer["acked_seq"]),
        "outstanding_page": None,
    }
    if consumer["page_cursor"] is not None:
        out["outstanding_page"] = {
            "from_seq": consumer["page_from_seq"],
            "to_seq": consumer["page_to_seq"],
            "cursor": consumer["page_cursor"],
            "delivered_at": consumer["page_delivered_at"],
        }
    return out
