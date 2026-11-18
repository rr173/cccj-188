"""Persistence and upgrade logic for versioned cleaning rules.

Responsibilities:

* idempotent upgrade of an *empty or populated* database (runs from every CLI
  entry point via :func:`pcg.db.init_db`);
* rule registration with content-address identity (canonical JSON + SHA-256);
* building one outcome -- a full result or an explicit rejection -- for any
  registered version against one source object;
* reading the active-version pointer / publish sequence.

The fixed cleaner remains available under the reserved, non-overwritable
built-in version :data:`~pcg.rule_engine.LEGACY_VERSION`. On first upgrade it
becomes the active version and every historical ``records.clean_json`` is
copied verbatim into the versioned result table, so legacy reads are
byte-identical to the pre-upgrade values.
"""

from __future__ import annotations

import json
import sqlite3

from .clean import clean_record
from .rule_engine import (
    LEGACY_SPEC,
    LEGACY_VERSION,
    RuleReject,
    RuleSpecError,
    canonical_spec,
    evaluate,
    result_to_json,
    validate_spec,
)

BACKFILL_PAGE = 500


class RuleConflict(RuleSpecError):
    """Same version id registered with different content (exit code 4)."""


class UnknownVersion(Exception):
    def __init__(self, version_id: str):
        super().__init__(f"unknown rule version: {version_id!r}")
        self.version_id = version_id


def rule_migrate(conn: sqlite3.Connection, now_ms: int) -> None:
    """Idempotent upgrade; safe on empty and populated databases, repeatable.

    The historical backfill of the built-in version runs exactly once per
    database (gated by a durable migration marker). Later ``init_db`` calls
    from worker processes must NOT backfill an inactive version behind the
    active pointer: rows received while a version is inactive become covered
    for that version only through an explicit recompute/catch-up.
    """
    # 1) partition catalog: every partition seen in records/progress, plus
    #    coordinator-registered ones (added elsewhere). Empty partitions may
    #    legitimately be absent until the coordinator first observes them.
    conn.execute(
        "INSERT OR IGNORE INTO partitions(partition_id, first_seen_at) "
        "SELECT partition_id, ? FROM ("
        "  SELECT DISTINCT partition_id FROM records "
        "  UNION SELECT DISTINCT partition_id FROM progress)",
        (now_ms,),
    )

    # 2) reserved built-in version: cannot be overwritten by registration.
    legacy_canon, legacy_hash = canonical_spec(LEGACY_SPEC)
    conn.execute(
        "INSERT OR IGNORE INTO rule_versions(version_id,spec_json,spec_sha256,"
        "builtin,created_at) VALUES(?,?,?,1,?)",
        (LEGACY_VERSION, legacy_canon, legacy_hash, now_ms),
    )

    # 3) singleton pointer: first upgrade selects the built-in version.
    conn.execute(
        "INSERT OR IGNORE INTO rule_state(id,active_version,publish_seq,updated_at)"
        " VALUES(1, ?, 0, ?)",
        (LEGACY_VERSION, now_ms),
    )

    already_migrated = conn.execute(
        "SELECT 1 FROM meta WHERE key='rule_migration_legacy_v1'"
    ).fetchone() is not None

    if not already_migrated:
        # 4) backfill legacy results from existing rows in SHORT transactions,
        #    so upgrading a large database never holds one history-spanning
        #    write transaction. Resumable: only positions lacking a result are
        #    touched; the marker is written only after the full pass so a
        #    crashed upgrade resumes instead of skipping the backfill.
        while True:
            page = conn.execute(
                "SELECT r.partition_id, r.offset, r.clean_json FROM records r "
                "WHERE NOT EXISTS (SELECT 1 FROM rule_results rr WHERE "
                "rr.version_id=? AND rr.partition_id=r.partition_id "
                "AND rr.offset=r.offset) ORDER BY r.partition_id, r.offset LIMIT ?",
                (LEGACY_VERSION, BACKFILL_PAGE),
            ).fetchall()
            if not page:
                break
            conn.execute("BEGIN IMMEDIATE")
            try:
                for row in page:
                    conn.execute(
                        "INSERT OR IGNORE INTO rule_results(version_id,partition_id,"
                        "offset,status,result_json,error_json,computed_at) "
                        "VALUES(?,?,?,'ok',?,NULL,?)",
                        (LEGACY_VERSION, row["partition_id"], row["offset"],
                         row["clean_json"], now_ms),
                    )
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise

        # 5) durable backfill watermarks for legacy: up to each partition's
        #    committed offset at upgrade time.
        conn.execute("BEGIN IMMEDIATE")
        try:
            for p in conn.execute(
                "SELECT DISTINCT partition_id FROM partitions"
            ).fetchall():
                pid = p["partition_id"]
                prog = conn.execute(
                    "SELECT committed_offset FROM progress WHERE partition_id=?",
                    (pid,),
                ).fetchone()
                done = prog["committed_offset"] if prog else -1
                conn.execute(
                    "INSERT OR REPLACE INTO rule_recompute_progress(version_id,"
                    "partition_id,done_offset,updated_at) VALUES(?,?,?,?)",
                    (LEGACY_VERSION, pid, done, now_ms),
                )
            conn.execute(
                "INSERT INTO meta(key,value) VALUES('rule_migration_legacy_v1',?)",
                (str(now_ms),),
            )
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise


def register_rule(conn: sqlite3.Connection, version_id: str, spec: dict,
                  now_ms: int) -> tuple[bool, str]:
    """Register a rule. Returns ``(created, spec_sha256)``.

    Retrying the same id with the same canonical content succeeds. The same id
    with different content raises :class:`RuleConflict`; the built-in id is
    reserved.
    """
    if version_id == LEGACY_VERSION:
        raise RuleConflict(
            f"version id {LEGACY_VERSION!r} is reserved for the built-in "
            "fixed cleaner and cannot be registered or overwritten")
    validate_spec(spec)
    canon, digest = canonical_spec(spec)
    existing = conn.execute(
        "SELECT spec_sha256 FROM rule_versions WHERE version_id=?",
        (version_id,),
    ).fetchone()
    if existing is not None:
        if existing["spec_sha256"] == digest:
            return False, digest
        raise RuleConflict(
            f"version {version_id!r} already exists with different content "
            f"(registered sha256={existing['spec_sha256']}, "
            f"submitted sha256={digest})")
    conn.execute(
        "INSERT INTO rule_versions(version_id,spec_json,spec_sha256,builtin,"
        "created_at) VALUES(?,?,?,0,?)",
        (version_id, canon, digest, now_ms),
    )
    return True, digest


def get_active(conn: sqlite3.Connection) -> sqlite3.Row:
    row = conn.execute("SELECT * FROM rule_state WHERE id=1").fetchone()
    if row is None:  # pragma: no cover - init_db always migrates
        raise RuntimeError("rule state not initialised; run database upgrade")
    return row


def load_version(conn: sqlite3.Connection, version_id: str) -> sqlite3.Row:
    row = conn.execute(
        "SELECT * FROM rule_versions WHERE version_id=?", (version_id,)
    ).fetchone()
    if row is None:
        raise UnknownVersion(version_id)
    return row


def list_versions(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    return list(conn.execute(
        "SELECT v.*, CASE WHEN v.version_id=s.active_version THEN 1 ELSE 0 END "
        "AS is_active FROM rule_versions v, rule_state s WHERE s.id=1 "
        "ORDER BY v.builtin DESC, v.version_id"))


def build_outcome(version: sqlite3.Row, source_obj: dict):
    """Return ``('ok', result_json)`` or ``('rejected', error_dict)``."""
    vid = version["version_id"]
    if version["builtin"]:
        # the fixed cleaner is total (never rejects); serialise exactly like
        # the sink so built-in output matches historical clean_json byte-wise
        return "ok", result_to_json(clean_record(source_obj))
    spec = json.loads(version["spec_json"])
    try:
        cleaned = evaluate(spec, source_obj)
    except RuleReject as exc:
        return "rejected", {
            "type": "rule_reject",
            "version_id": vid,
            "field": exc.field,
            "op": exc.op,
            "reason": exc.reason,
        }
    return "ok", result_to_json(cleaned)


def parse_source(source_json: str) -> dict:
    return json.loads(source_json)
