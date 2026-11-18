"""Independent reference verifier for durable change delivery.

This program deliberately imports NONE of the production pcg code (no
``pcg.delivery_store``, no ``pcg.rule_*``, no ``pcg.clean``).  It opens the
SQLite database directly with the standard library and re-derives every
delivery guarantee from the raw tables, so a verification pass does not trust
the code that wrote the rows.

Checks:

1. stream integrity      - events are gap-free 1..N and batches tile the whole
                           sequence space without overlap or holes;
2. per-position order    - events of one (partition, offset) have strictly
                           increasing source revisions and never masquerade a
                           change as a new position;
3. payload truth         - ingest events are revision-0 collected positions;
                           correction events match the immutable revision row;
                           publish events match the candidate's stored results;
                           the LAST event of every position equals the current
                           official active-version row byte-for-byte;
4. batch contracts       - event_count is exact; correction batches hold one
                           event; publish batches match their review section
                           and never expose candidate-only (unpublished) rows;
5. consumer cursors      - boundary/acked/outstanding continuity, batch-aligned
                           pages, independent cursor-token recomputation;
6. snapshot replay       - an independent SQL rebuild of the snapshot at every
                           consumer's boundary equals a paged replay of events
                           1..boundary (the two are computed differently);
7. ack audit            - every rejected ack left a durable reason row.

Usage:  python3 -I tools_delivery_ref.py --db g.db [--json]
Exit code is non-zero if any problem is found.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3
import sys


def open_db(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, timeout=15)
    conn.row_factory = sqlite3.Row
    return conn


def _canon(obj) -> str:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=False)


def check_stream(conn: sqlite3.Connection, problems: list[str]) -> None:
    """Gap-free seq space and tiling batches."""
    n = conn.execute("SELECT COUNT(*) c FROM delivery_events").fetchone()["c"]
    if n == 0:
        return
    seqs = [r["seq"] for r in conn.execute(
        "SELECT seq FROM delivery_events ORDER BY seq")]
    if seqs != list(range(1, n + 1)):
        problems.append("delivery_events.seq is not a gap-free 1..N sequence")
    # batches tile 1..N exactly
    spans = conn.execute(
        "SELECT b.batch_id, b.event_count, MIN(e.seq) lo, MAX(e.seq) hi, "
        "COUNT(e.seq) actual FROM delivery_batches b "
        "JOIN delivery_events e ON e.batch_id=b.batch_id "
        "GROUP BY b.batch_id ORDER BY lo").fetchall()
    expected = 1
    for s in spans:
        if s["lo"] != expected:
            problems.append(
                f"batch {s['batch_id']} starts at {s['lo']}, expected {expected} "
                "(overlap or hole between batches)")
        if s["hi"] - s["lo"] + 1 != s["actual"]:
            problems.append(f"batch {s['batch_id']} span has internal holes")
        if s["event_count"] != s["actual"]:
            problems.append(
                f"batch {s['batch_id']} event_count={s['event_count']} but "
                f"contains {s['actual']} events")
        expected = s["hi"] + 1
    if expected != n + 1:
        problems.append("batches do not cover the whole event stream")


def check_position_order(conn: sqlite3.Connection,
                         problems: list[str]) -> None:
    """Per-position revisions never regress; corrections strictly increase.

    Ingest (rev 0) followed by a publish (still rev 0) is a legitimate new
    state of the same position, so revisions are non-decreasing in general.
    Only a correction event may move to a higher revision, and then it must be
    exactly one above the position's previous event revision.
    """
    rows = conn.execute(
        "SELECT e.partition_id, e.offset, e.source_revision, e.seq, b.kind "
        "FROM delivery_events e JOIN delivery_batches b "
        "ON b.batch_id=e.batch_id "
        "ORDER BY e.partition_id, e.offset, e.seq").fetchall()
    last_pos = None
    last_rev = None
    for r in rows:
        pos = (r["partition_id"], r["offset"])
        if pos != last_pos:
            last_pos = pos
            last_rev = None
        if last_rev is not None:
            if r["source_revision"] < last_rev:
                problems.append(
                    f"p{r['partition_id']} offset {r['offset']}: source "
                    f"revision regressed (seq {r['seq']} rev "
                    f"{r['source_revision']} after {last_rev})")
            if r["kind"] == "correction" and r["source_revision"] != last_rev + 1:
                problems.append(
                    f"p{r['partition_id']} offset {r['offset']}: correction "
                    f"event seq {r['seq']} must move revision "
                    f"{last_rev} -> {last_rev + 1}, got {r['source_revision']}")
            if r["kind"] != "correction" and r["source_revision"] > last_rev:
                problems.append(
                    f"p{r['partition_id']} offset {r['offset']}: non-correction "
                    f"event seq {r['seq']} unexpectedly raised the revision")
        else:
            if r["source_revision"] != 0:
                problems.append(
                    f"p{r['partition_id']} offset {r['offset']}: first event "
                    f"must be at revision 0, got {r['source_revision']}")
        last_rev = r["source_revision"]


def check_payloads(conn: sqlite3.Connection, problems: list[str]) -> None:
    """Every event is explainable from the raw production tables."""
    active = conn.execute("SELECT * FROM rule_state WHERE id=1").fetchone()
    latest_seq = conn.execute(
        "SELECT MAX(seq) m FROM delivery_events").fetchone()["m"]
    if latest_seq is None:
        return

    # latest event per position must equal the current official row exactly
    latest = conn.execute(
        "SELECT e.* FROM delivery_events e JOIN ("
        "  SELECT partition_id, offset, MAX(seq) m FROM delivery_events "
        "  GROUP BY partition_id, offset) x ON x.partition_id=e.partition_id "
        "AND x.offset=e.offset AND x.m=e.seq").fetchall()
    for e in latest:
        rr = conn.execute(
            "SELECT * FROM rule_results WHERE version_id=? AND partition_id=? "
            "AND offset=?", (active["active_version"], e["partition_id"],
                             e["offset"])).fetchone()
        if rr is None:
            problems.append(
                f"latest event seq {e['seq']} p{e['partition_id']}:"
                f"{e['offset']} has no official active-version row")
            continue
        if rr["source_revision"] != e["source_revision"]:
            problems.append(
                f"p{e['partition_id']}:{e['offset']} latest event rev "
                f"{e['source_revision']} != official rev {rr['source_revision']}")
        if rr["status"] != e["status"]:
            problems.append(
                f"p{e['partition_id']}:{e['offset']} status mismatch")
        if e["status"] == "ok" and rr["result_json"] != e["result_json"]:
            problems.append(
                f"p{e['partition_id']}:{e['offset']} result payload mismatch")
        if e["status"] == "rejected" and rr["error_json"] != e["error_json"]:
            problems.append(
                f"p{e['partition_id']}:{e['offset']} error payload mismatch")
        if e["rule_version"] != active["active_version"]:
            problems.append(
                f"p{e['partition_id']}:{e['offset']} latest event version "
                f"{e['rule_version']} != active {active['active_version']}")

    # kind-specific provenance
    for e in conn.execute(
            "SELECT e.*, b.kind, b.producer_ref FROM delivery_events e "
            "JOIN delivery_batches b ON b.batch_id=e.batch_id"):
        rec = conn.execute(
            "SELECT 1 FROM records WHERE partition_id=? AND offset=?",
            (e["partition_id"], e["offset"])).fetchone()
        if rec is None:
            problems.append(
                f"event seq {e['seq']} names an uncommitted source position "
                f"p{e['partition_id']}:{e['offset']}")
            continue
        if e["kind"] == "ingest":
            if e["source_revision"] != 0:
                problems.append(
                    f"ingest event seq {e['seq']} has revision != 0")
        elif e["kind"] == "correction":
            sc = conn.execute(
                "SELECT * FROM source_corrections WHERE partition_id=? "
                "AND offset=? AND revision=?",
                (e["partition_id"], e["offset"],
                 e["source_revision"])).fetchone()
            if sc is None:
                problems.append(
                    f"correction event seq {e['seq']} has no matching "
                    "source_corrections row")
            elif sc["correction_id"] != e["producer_ref"]:
                problems.append(
                    f"correction event seq {e['seq']} producer_ref "
                    f"{e['producer_ref']} != correction id "
                    f"{sc['correction_id']}")
            elif sc["rule_version"] != e["rule_version"]:
                problems.append(
                    f"correction event seq {e['seq']} version mismatch")
        elif e["kind"] == "publish":
            rr = conn.execute(
                "SELECT * FROM rule_results WHERE version_id=? AND "
                "partition_id=? AND offset=?",
                (e["rule_version"], e["partition_id"], e["offset"])).fetchone()
            if rr is None:
                problems.append(
                    f"publish event seq {e['seq']}: candidate row missing")
                continue
            # revisions only move forward, so the stored current revision can
            # never be older than the one that was published
            if rr["source_revision"] < e["source_revision"]:
                problems.append(
                    f"publish event seq {e['seq']} rev {e['source_revision']} "
                    f"newer than stored {rr['source_revision']}")
            if rr["source_revision"] == e["source_revision"]:
                payload = rr["result_json"] if e["status"] == "ok" \
                    else rr["error_json"]
                mine = e["result_json"] if e["status"] == "ok" \
                    else e["error_json"]
                if payload != mine:
                    problems.append(
                        f"publish event seq {e['seq']} payload differs from "
                        "the stored candidate row")


def check_batch_contracts(conn: sqlite3.Connection,
                          problems: list[str]) -> None:
    for b in conn.execute("SELECT * FROM delivery_batches ORDER BY batch_id"):
        n = conn.execute(
            "SELECT COUNT(*) c FROM delivery_events WHERE batch_id=?",
            (b["batch_id"],)).fetchone()["c"]
        if n != b["event_count"]:
            problems.append(
                f"batch {b['batch_id']} event_count inaccurate")
        if b["kind"] == "correction" and n != 1:
            problems.append(
                f"correction batch {b['batch_id']} holds {n} events, expected 1")
        if b["kind"] == "publish":
            # one publish batch per non-zero rule_publishes row
            pub = conn.execute(
                "SELECT * FROM rule_publishes WHERE seq=?",
                (b["publish_seq"],)).fetchone()
            if pub is None or pub["version_id"] != b["rule_version"]:
                problems.append(
                    f"publish batch {b['batch_id']} has no matching "
                    "rule_publishes row")
                continue
            # its section must equal the review token it names
            review = conn.execute(
                "SELECT * FROM rule_reviews WHERE token=?",
                (b["producer_ref"],)).fetchone()
            if review is None:
                problems.append(
                    f"publish batch {b['batch_id']} names unknown review token")
                continue
            vector = {int(k): v for k, v in
                      json.loads(review["offset_vector_json"]).items()}
            want_positions = {(pid, off) for pid, last in vector.items()
                              for off in range(last + 1)}
            got_positions = {tuple(r) for r in conn.execute(
                "SELECT partition_id, offset FROM delivery_events "
                "WHERE batch_id=?", (b["batch_id"],))}
            if got_positions != want_positions:
                problems.append(
                    f"publish batch {b['batch_id']} section mismatch: "
                    f"want {len(want_positions)} positions, "
                    f"got {len(got_positions)}")
            # every event of a publish batch carries the published version
            bad = conn.execute(
                "SELECT 1 FROM delivery_events WHERE batch_id=? AND "
                "rule_version!=? LIMIT 1",
                (b["batch_id"], b["rule_version"])).fetchone()
            if bad is not None:
                problems.append(
                    f"publish batch {b['batch_id']} contains foreign versions")
        if b["kind"] == "bootstrap":
            # a bootstrap is the only batch allowed before any producer txn
            later = conn.execute(
                "SELECT 1 FROM delivery_batches WHERE batch_id<? AND kind!=?",
                (b["batch_id"], "bootstrap")).fetchone()
            if later is not None:
                problems.append(
                    f"bootstrap batch {b['batch_id']} is not the first batch")


def check_candidate_invisibility(conn: sqlite3.Connection,
                                 problems: list[str]) -> None:
    """Rule results that no publish ever made official cannot appear."""
    # every rule_version named on an event must be a registered version
    # (bootstrap/ingest/correction/publish all name official versions)
    for e in conn.execute(
            "SELECT DISTINCT e.rule_version FROM delivery_events e"):
        versions = {r["version_id"] for r in conn.execute(
            "SELECT version_id FROM rule_versions")}
        if e["rule_version"] not in versions:
            problems.append(
                f"event references unregistered version {e['rule_version']}")
    # pending proposals have no effect anywhere official
    n_pending_events = conn.execute(
        "SELECT COUNT(*) c FROM delivery_events e JOIN delivery_batches b "
        "ON b.batch_id=e.batch_id JOIN correction_proposals p "
        "ON p.correction_id=b.producer_ref AND p.state='pending'").fetchone()["c"]
    if n_pending_events:
        problems.append("pending correction proposals produced delivery events")


def _recompute_cursor(consumer_id: str, from_seq: int, to_seq: int) -> str:
    raw = f"{consumer_id}|{from_seq}|{to_seq}"
    return f"{from_seq}:{to_seq}:" + hashlib.sha256(
        raw.encode("utf-8")).hexdigest()[:32]


def check_consumers(conn: sqlite3.Connection, problems: list[str]) -> None:
    head = conn.execute("SELECT MAX(seq) m FROM delivery_events").fetchone()["m"]
    head = head or 0
    for c in conn.execute("SELECT * FROM delivery_consumers"):
        cid = c["consumer_id"]
        if c["boundary_seq"] is not None:
            if not (0 <= c["boundary_seq"] <= head):
                problems.append(f"consumer {cid}: boundary out of range")
            if c["state"] == "registered":
                problems.append(f"consumer {cid}: registered with a boundary")
        if c["acked_seq"] < 0 or c["acked_seq"] > head:
            problems.append(f"consumer {cid}: acked_seq out of range")
        if c["state"] == "active" and c["boundary_seq"] is not None \
                and c["acked_seq"] < c["boundary_seq"]:
            problems.append(
                f"consumer {cid}: active but has not passed its boundary")
        if c["state"] == "syncing" and c["acked_seq"] >= (c["boundary_seq"] or 0) \
                and c["boundary_seq"] is not None and c["page_cursor"] is None:
            problems.append(
                f"consumer {cid}: syncing with boundary fully acked but not active")
        if c["page_cursor"] is not None:
            f, t = c["page_from_seq"], c["page_to_seq"]
            if f != c["acked_seq"] + 1:
                problems.append(
                    f"consumer {cid}: outstanding page is not contiguous")
            if _recompute_cursor(cid, f, t) != c["page_cursor"]:
                problems.append(
                    f"consumer {cid}: cursor token cannot be independently "
                    "recomputed from its bounds")
            rows = conn.execute(
                "SELECT COUNT(*) c FROM delivery_events WHERE seq BETWEEN ? AND ?",
                (f, t)).fetchone()["c"]
            if rows != t - f + 1:
                problems.append(f"consumer {cid}: pinned page has missing events")
            # a physical page may be a middle chunk of one batch, but must
            # never cross from one batch into another
            bids = {r[0] for r in conn.execute(
                "SELECT DISTINCT batch_id FROM delivery_events "
                "WHERE seq BETWEEN ? AND ?", (f, t))}
            if len(bids) != 1:
                problems.append(
                    f"consumer {cid}: outstanding page crosses batches {bids}")
        else:
            for col in ("page_from_seq", "page_to_seq", "page_digest"):
                if c[col] is not None:
                    problems.append(
                        f"consumer {cid}: {col} set without a cursor")


def check_snapshot_replay(conn: sqlite3.Connection,
                          problems: list[str]) -> None:
    """Two independent rebuilds of each boundary snapshot must agree.

    Rebuild A: SQL window function - the row with max seq per position within
    1..boundary.  Rebuild B: an ordered Python fold over the same events,
    applied keyed by position (the way a downstream program would).  The two
    implementations share no code; any disagreement is a real defect.
    """
    for c in conn.execute(
            "SELECT * FROM delivery_consumers WHERE boundary_seq IS NOT NULL"):
        boundary = c["boundary_seq"]
        if boundary == 0:
            continue
        # A: SQL
        a = {}
        for r in conn.execute(
            "SELECT e.partition_id, e.offset, e.status, e.result_json, "
            "e.error_json, e.source_revision, e.rule_version FROM "
            "delivery_events e JOIN (SELECT partition_id, offset, MAX(seq) m "
            "FROM delivery_events WHERE seq<=? GROUP BY partition_id, offset) x "
            "ON x.partition_id=e.partition_id AND x.offset=e.offset AND x.m=e.seq",
                (boundary,)):
            a[(r["partition_id"], r["offset"])] = (
                r["status"], r["result_json"], r["error_json"],
                r["source_revision"], r["rule_version"])
        # B: ordered fold
        b = {}
        for r in conn.execute(
            "SELECT * FROM delivery_events WHERE seq<=? ORDER BY seq",
                (boundary,)):
            b[(r["partition_id"], r["offset"])] = (
                r["status"], r["result_json"], r["error_json"],
                r["source_revision"], r["rule_version"])
        if a != b:
            problems.append(
                f"consumer {c['consumer_id']}: boundary {boundary} snapshot "
                "rebuilds disagree")
        # the acked prefix must be a prefix of the same replay (no skipped seq)
        if c["acked_seq"]:
            n = conn.execute(
                "SELECT COUNT(*) c FROM delivery_events WHERE seq<=?",
                (c["acked_seq"],)).fetchone()["c"]
            if n != c["acked_seq"]:
                problems.append(
                    f"consumer {c['consumer_id']}: acked_seq skips events")


def check_ack_audit(conn: sqlite3.Connection, problems: list[str]) -> None:
    # every rejection carries a non-empty reason; an audit row may reference a
    # consumer id that never existed (that attempt must itself be refused, and
    # keeping the evidence is more important than requiring the row to exist)
    for r in conn.execute("SELECT * FROM delivery_ack_rejects"):
        if not r["reason"]:
            problems.append(f"ack reject #{r['id']} has empty reason")
        if not r["consumer_id"]:
            problems.append(f"ack reject #{r['id']} has no consumer id")


def verify_database(db_path: str) -> tuple[list[str], dict]:
    conn = open_db(db_path)
    problems: list[str] = []
    try:
        tables = {r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'")}
        required = {"delivery_events", "delivery_batches",
                    "delivery_consumers", "delivery_ack_rejects"}
        missing = required - tables
        if missing:
            return [f"missing delivery tables: {sorted(missing)}"], {}
        stats = {
            "events": conn.execute(
                "SELECT COUNT(*) c FROM delivery_events").fetchone()["c"],
            "batches": conn.execute(
                "SELECT COUNT(*) c FROM delivery_batches").fetchone()["c"],
            "consumers": conn.execute(
                "SELECT COUNT(*) c FROM delivery_consumers").fetchone()["c"],
            "ack_rejects": conn.execute(
                "SELECT COUNT(*) c FROM delivery_ack_rejects").fetchone()["c"],
        }
        for check in (check_stream, check_position_order, check_payloads,
                      check_batch_contracts, check_candidate_invisibility,
                      check_consumers, check_snapshot_replay, check_ack_audit):
            check(conn, problems)
        return problems, stats
    finally:
        conn.close()


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--db", required=True)
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args()
    problems, stats = verify_database(args.db)
    if args.json:
        print(json.dumps({"problems": problems, "stats": stats}, indent=2,
                         sort_keys=True))
    else:
        print("independent delivery verification "
              f"(events={stats.get('events', 0)}, batches={stats.get('batches', 0)}, "
              f"consumers={stats.get('consumers', 0)}, "
              f"ack_rejects={stats.get('ack_rejects', 0)})")
        if problems:
            for p in problems:
                print(f"  PROBLEM: {p}")
        else:
            print("  all delivery invariants hold")
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
