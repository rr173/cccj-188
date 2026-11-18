"""End-to-end acceptance tests for durable change delivery.

Every scenario runs REAL OS processes (coordinator, workers, management and
``pcg delivery`` commands) that communicate only through the shared SQLite
database.  An independent reference verifier
(:mod:`tests.tools_delivery_ref`) imports none of the production code and
re-derives every invariant directly from the tables.

Coverage (mapped to the delivery contract):

* migration            - empty/populated databases, repeatable, bootstrap
                         snapshot equals the official export, no mutation of
                         existing rows, progress, rules or corrections;
* producer atomicity   - ingest/correction/publish txns and their delivery
                         events commit together; pending proposals and
                         candidate recomputes are invisible;
* initial sync         - pinned boundary, consistent paged snapshot, no gap
                         when writes continue after the consumer is created;
* replay/crash         - same page and cursor on re-read; process exits
                         before and after ack resume correctly;
* ack validation       - stale, skip-ahead, cross-consumer, bogus-token,
                         double-ack and empty-ack are all refused + audited;
* independent consumers- two consumers create/read/pause/ack without moving
                         each other's position;
* publish batching     - one logical batch per activation, never split across
                         pages, activation interrupted before commit leaves no
                         partial new-version stream;
* concurrency          - collection, correction approval, publish, reads and
                         acks interleaved across separate processes;
* bounded memory       - skewed long history initial sync and catch-up inside
                         a tight RSS budget, streaming pages only;
* independent reference- snapshot, order, revisions, payloads and cursors
                         verified from raw tables.
"""

from __future__ import annotations

import json
import os
import resource
import sqlite3
import subprocess
import sys
import tempfile
import time
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SRC = os.path.join(ROOT, "src")
TESTS = os.path.dirname(os.path.abspath(__file__))
ENV = dict(os.environ, PYTHONPATH=SRC)
sys.path.insert(0, SRC)
sys.path.insert(0, TESTS)

from harness import ProcGroup, make_source  # noqa: E402
from tools_delivery_ref import verify_database  # noqa: E402


def pcg(db_path: str, *args, check: bool = True):
    p = subprocess.run(
        [sys.executable, "-m", "pcg", "--db", db_path, "--clock", "real",
         *args],
        env=ENV, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    if check and p.returncode != 0:
        raise AssertionError(f"pcg {' '.join(args)} rc={p.returncode}: {p.stdout}")
    return p


def delivery(db_path: str, *args, check: bool = True):
    return pcg(db_path, "delivery", *args, check=check)


def jload(p: subprocess.CompletedProcess):
    return json.loads(p.stdout)


def open_db(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, timeout=15)
    conn.row_factory = sqlite3.Row
    return conn


def wait_count(db_path: str, sql: str, want: int, timeout: float = 40.0):
    deadline = time.monotonic() + timeout
    last = -1
    while time.monotonic() < deadline:
        try:
            with open_db(db_path) as c:
                last = c.execute(sql).fetchone()[0]
            if last >= want:
                return last
        except sqlite3.OperationalError:
            pass
        time.sleep(0.1)
    raise AssertionError(f"timeout waiting {want} for {sql}; last={last}")


def wait_records(db_path: str, want: int):
    return wait_count(db_path, "SELECT COUNT(*) FROM records", want)


def write_rule(base: str, name: str, spec: dict) -> str:
    path = os.path.join(base, f"{name}.json")
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(spec, fh)
    return path


def start_group(tmp: str, counts: list[int], seed: int = 11) -> tuple[ProcGroup, str]:
    src = make_source(tmp, counts, seed=seed)
    db = os.path.join(tmp, "g.db")
    pg = ProcGroup(db, src, lease_ms=6000, ack_timeout_ms=6600,
                   tick_ms=40, clock="manual")
    pg.start_coordinator()
    return pg, db


def drain_workers(pg: ProcGroup, wids: list[str], total: int):
    for wid in wids:
        pg.admin("add", wid, "--lease-ms", "6000")
        pg.start_worker(wid, batch_size=25)
    wait_records(pg.db, total)


def assert_clean(db_path: str):
    problems, stats = verify_database(db_path)
    assert not problems, problems
    return stats


def consume_all(db_path: str, consumer: str, page_size: int = 50,
                stop_at: int | None = None) -> list[dict]:
    """Read+ack every available page; returns all seen events in seq order."""
    seen = []
    while True:
        page = jload(delivery(db_path, "read", consumer,
                              "--page-size", str(page_size), "--json"))
        if not page["events"]:
            return seen
        seen.extend(page["events"])
        if stop_at is not None and page["to_seq"] >= stop_at:
            return seen
        delivery(db_path, "ack", consumer, page["cursor"])


V1_SPEC = {"fields": {
    "name": {"missing": "preserve", "null": "null",
             "steps": [{"op": "nfkc"}, {"op": "strip"},
                       {"op": "collapse_ws"}, {"op": "upper"}]},
    "city": {"missing": "preserve", "null": "preserve",
             "steps": [{"op": "strip"}, {"op": "collapse_ws"},
                       {"op": "title"}]}}}
V2_SPEC = {"fields": {
    "name": {"missing": "preserve", "null": "null",
             "steps": [{"op": "nfkc"}, {"op": "strip"},
                       {"op": "collapse_ws"}, {"op": "lower"}]}}}


# ====================================================================== migration

def simulate_pre_delivery_database(db_path: str) -> None:
    """Remove every delivery object so the next init_db performs a real
    upgrade of a populated pre-delivery database (bootstrap path)."""
    with open_db(db_path) as c:
        c.execute("DROP TABLE IF EXISTS delivery_events")
        c.execute("DROP TABLE IF EXISTS delivery_batches")
        c.execute("DROP TABLE IF EXISTS delivery_consumers")
        c.execute("DROP TABLE IF EXISTS delivery_ack_rejects")
        c.execute("DELETE FROM meta WHERE key='delivery_migration_bootstrap_v1'")
        c.commit()


class TestMigration(unittest.TestCase):
    def test_bootstrap_on_populated_database_matches_export(self):
        tmp = tempfile.mkdtemp(prefix="pcgdel-")
        pg, db = start_group(tmp, [12, 4])
        try:
            drain_workers(pg, ["w1"], 16)
            time.sleep(0.3)
        finally:
            pg.shutdown_all()
        # snapshot the official state, then make the database look pre-delivery
        with open_db(db) as c:
            active = c.execute(
                "SELECT active_version FROM rule_state WHERE id=1").fetchone()[0]
            official = [tuple(r) for r in c.execute(
                "SELECT partition_id, offset, status, result_json, error_json, "
                "source_revision FROM rule_results WHERE version_id=? "
                "ORDER BY partition_id, offset", (active,))]
            record_count = c.execute("SELECT COUNT(*) FROM records").fetchone()[0]
        simulate_pre_delivery_database(db)
        # the upgrade entry point runs in a separate real process
        r = subprocess.run(
            [sys.executable, "-c",
             "import sys; sys.path.insert(0, %r); "
             "from pcg.db import init_db; init_db(%r)" % (SRC, db)],
            env=ENV, capture_output=True, text=True)
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)

        with open_db(db) as c:
            boot = c.execute(
                "SELECT * FROM delivery_batches WHERE kind='bootstrap'").fetchone()
            self.assertIsNotNone(boot, "no bootstrap batch on populated upgrade")
            n_ev = c.execute(
                "SELECT COUNT(*) FROM delivery_events WHERE batch_id=?",
                (boot["batch_id"],)).fetchone()[0]
            self.assertEqual(n_ev, 16)
            self.assertEqual(boot["event_count"], 16)
            # bootstrap payloads equal the official active-version rows exactly
            booted = [tuple(r) for r in c.execute(
                "SELECT partition_id, offset, status, result_json, error_json, "
                "source_revision FROM delivery_events WHERE batch_id=? "
                "ORDER BY partition_id, offset", (boot["batch_id"],))]
            self.assertEqual(booted, official)
            # one event per committed position
            positions = c.execute(
                "SELECT COUNT(*) c FROM (SELECT DISTINCT partition_id, offset "
                "FROM delivery_events)").fetchone()["c"]
            self.assertEqual(positions, 16)
            # existing facts and progress are untouched
            self.assertEqual(
                c.execute("SELECT COUNT(*) FROM records").fetchone()[0],
                record_count)
            self.assertEqual(
                c.execute("SELECT SUM(committed_offset+1) FROM progress")
                .fetchone()[0], 16)
        # a consumer starting now gets the bootstrap as its complete snapshot
        delivery(db, "create", "c1")
        out = jload(delivery(db, "start", "c1", "--json"))
        self.assertEqual(out["boundary_seq"], 16)
        events = consume_all(db, "c1")
        self.assertEqual(len(events), 16)
        pos = jload(delivery(db, "position", "c1", "--json"))
        self.assertEqual(pos["state"], "active")
        assert_clean(db)

    def test_repeat_migration_is_noop(self):
        tmp = tempfile.mkdtemp(prefix="pcgdel-")
        pg, db = start_group(tmp, [5])
        try:
            drain_workers(pg, ["w1"], 5)
        finally:
            pg.shutdown_all()
        with open_db(db) as c:
            before = [tuple(r) for r in c.execute(
                "SELECT seq,partition_id,offset,source_revision FROM "
                "delivery_events ORDER BY seq")]
            progress = [tuple(r) for r in c.execute(
                "SELECT * FROM progress ORDER BY partition_id")]
        # run the upgrade entry point twice more in separate processes
        for _ in range(2):
            r = subprocess.run(
                [sys.executable, "-c",
                 "import sys; sys.path.insert(0, %r); "
                 "from pcg.db import init_db; init_db(%r)" % (SRC, db)],
                env=ENV, capture_output=True, text=True)
            self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
        with open_db(db) as c:
            after = [tuple(r) for r in c.execute(
                "SELECT seq,partition_id,offset,source_revision FROM "
                "delivery_events ORDER BY seq")]
            progress2 = [tuple(r) for r in c.execute(
                "SELECT * FROM progress ORDER BY partition_id")]
        self.assertEqual(before, after)
        self.assertEqual(progress, progress2)
        assert_clean(db)

    def test_bootstrap_resumes_after_a_crash_without_duplicating(self):
        tmp = tempfile.mkdtemp(prefix="pcgdel-")
        pg, db = start_group(tmp, [9, 7])
        try:
            drain_workers(pg, ["w1"], 16)
        finally:
            pg.shutdown_all()
        with open_db(db) as c:
            active = c.execute(
                "SELECT active_version FROM rule_state WHERE id=1").fetchone()[0]
            n_positions = c.execute(
                "SELECT COUNT(*) FROM rule_results WHERE version_id=?",
                (active,)).fetchone()[0]
        simulate_pre_delivery_database(db)
        init = (
            "import sys; sys.path.insert(0, %r); "
            "from pcg.db import init_db; init_db(%r)" % (SRC, db))
        subprocess.run([sys.executable, "-c", init], env=ENV, check=True,
                       capture_output=True)
        with open_db(db) as c:
            boot = c.execute(
                "SELECT batch_id FROM delivery_batches WHERE kind='bootstrap'"
            ).fetchone()["batch_id"]
            written = c.execute(
                "SELECT COUNT(*) FROM delivery_events WHERE batch_id=?",
                (boot,)).fetchone()[0]
        # hard-crash scenario A: pages flushed, finalisation marker never
        # written. The next upgrade must reuse the batch and add nothing.
        with open_db(db) as c:
            c.execute("DELETE FROM meta WHERE key='delivery_migration_bootstrap_v1'")
            c.commit()
        subprocess.run([sys.executable, "-c", init], env=ENV, check=True,
                       capture_output=True)
        with open_db(db) as c:
            self.assertEqual(c.execute(
                "SELECT COUNT(*) FROM delivery_batches WHERE kind='bootstrap'"
            ).fetchone()[0], 1)
            n_events = c.execute(
                "SELECT COUNT(*) FROM delivery_events").fetchone()[0]
            distinct_positions = c.execute(
                "SELECT COUNT(*) c FROM (SELECT DISTINCT partition_id, offset "
                "FROM delivery_events)").fetchone()["c"]
            self.assertEqual(n_events, written)
            self.assertEqual(distinct_positions, n_positions)
            self.assertIsNotNone(c.execute(
                "SELECT 1 FROM meta WHERE key='delivery_migration_bootstrap_v1'"
            ).fetchone())

        # hard-crash scenario B: the whole partial batch is lost. A later
        # upgrade rebuilds it from scratch with a gap-free event stream.
        with open_db(db) as c:
            c.execute("DELETE FROM delivery_events")
            c.execute("DELETE FROM delivery_batches WHERE kind='bootstrap'")
            c.execute("DELETE FROM meta WHERE key='delivery_migration_bootstrap_v1'")
            c.commit()
        subprocess.run([sys.executable, "-c", init], env=ENV, check=True,
                       capture_output=True)
        with open_db(db) as c:
            self.assertEqual(c.execute(
                "SELECT COUNT(*) FROM delivery_batches WHERE kind='bootstrap'"
            ).fetchone()[0], 1)
            n_events = c.execute(
                "SELECT COUNT(*) FROM delivery_events").fetchone()[0]
            distinct_positions = c.execute(
                "SELECT COUNT(*) c FROM (SELECT DISTINCT partition_id, offset "
                "FROM delivery_events)").fetchone()["c"]
            seqs = [r[0] for r in c.execute(
                "SELECT seq FROM delivery_events ORDER BY seq")]
            self.assertEqual(seqs, list(range(1, n_events + 1)))
            self.assertEqual(n_events, n_positions)
            self.assertEqual(distinct_positions, n_positions)
        # and the rebuilt snapshot is fully consumable
        delivery(db, "create", "c1")
        jload(delivery(db, "start", "c1", "--json"))
        events = consume_all(db, "c1")
        self.assertEqual(len(events), n_positions)
        assert_clean(db)

    def test_concurrent_upgrades_produce_one_bootstrap(self):
        tmp = tempfile.mkdtemp(prefix="pcgdel-")
        pg, db = start_group(tmp, [8, 8])
        try:
            drain_workers(pg, ["w1"], 16)
        finally:
            pg.shutdown_all()
        with open_db(db) as c:
            active = c.execute(
                "SELECT active_version FROM rule_state WHERE id=1").fetchone()[0]
            n_positions = c.execute(
                "SELECT COUNT(*) FROM rule_results WHERE version_id=?",
                (active,)).fetchone()[0]
        simulate_pre_delivery_database(db)
        init = (
            "import sys; sys.path.insert(0, %r); "
            "from pcg.db import init_db; init_db(%r)" % (SRC, db))
        procs = [subprocess.Popen([sys.executable, "-c", init], env=ENV,
                                  stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                  text=True) for _ in range(4)]
        outputs = [p.wait() for p in procs]
        self.assertEqual(outputs, [0, 0, 0, 0],
                         [p.stdout.read() for p in procs])
        with open_db(db) as c:
            self.assertEqual(c.execute(
                "SELECT COUNT(*) FROM delivery_batches WHERE kind='bootstrap'"
            ).fetchone()[0], 1)
            n_events = c.execute(
                "SELECT COUNT(*) FROM delivery_events").fetchone()[0]
            distinct = c.execute(
                "SELECT COUNT(*) c FROM (SELECT DISTINCT partition_id, offset "
                "FROM delivery_events)").fetchone()["c"]
            self.assertEqual(n_events, n_positions)
            self.assertEqual(distinct, n_positions)
        assert_clean(db)

    def test_empty_database_no_bootstrap(self):
        tmp = tempfile.mkdtemp(prefix="pcgdel-")
        db = os.path.join(tmp, "g.db")
        subprocess.run([sys.executable, "-c",
                        "import sys; sys.path.insert(0, %r); "
                        "from pcg.db import init_db; init_db(%r)" % (SRC, db)],
                       env=ENV, check=True, capture_output=True)
        with open_db(db) as c:
            self.assertEqual(
                c.execute("SELECT COUNT(*) FROM delivery_batches").fetchone()[0], 0)
            self.assertEqual(
                c.execute("SELECT COUNT(*) FROM delivery_events").fetchone()[0], 0)
        delivery(db, "create", "c1")
        out = jload(delivery(db, "start", "c1", "--json"))
        self.assertEqual(out["boundary_seq"], 0)
        assert_clean(db)


# ============================================================== producer wiring

class TestProducerAtomicity(unittest.TestCase):
    def test_ingest_events_match_commits_and_revisions(self):
        tmp = tempfile.mkdtemp(prefix="pcgdel-")
        pg, db = start_group(tmp, [10, 7])
        try:
            drain_workers(pg, ["w1", "w2"], 17)
            time.sleep(0.2)
            with open_db(db) as c:
                # exactly one ingest event per committed position
                n = c.execute(
                    "SELECT COUNT(*) FROM delivery_events e JOIN "
                    "delivery_batches b ON b.batch_id=e.batch_id "
                    "WHERE b.kind='ingest'").fetchone()[0]
                self.assertEqual(n, 17)
                revs = {r[0] for r in c.execute(
                    "SELECT DISTINCT source_revision FROM delivery_events")}
                self.assertEqual(revs, {0})
                # gap-free
                seqs = [r[0] for r in c.execute(
                    "SELECT seq FROM delivery_events ORDER BY seq")]
                self.assertEqual(seqs, list(range(1, 18)))
        finally:
            pg.shutdown_all()
        assert_clean(db)

    def test_pending_proposal_invisible_approval_emits_one(self):
        tmp = tempfile.mkdtemp(prefix="pcgdel-")
        pg, db = start_group(tmp, [6])
        try:
            drain_workers(pg, ["w1"], 6)
            rec = {"offset": 3, "name": "Corrected Person", "city": "Berlin"}
            pcg(db, "correct", "propose", "0", "3", "--correction-id", "FIX-1",
                "--expected-revision", "0", "--reason", "city fix",
                "--record", json.dumps(rec))
            with open_db(db) as c:
                head_before = c.execute(
                    "SELECT MAX(seq) m FROM delivery_events").fetchone()["m"]
            # proposal pending: reads and the delivery stream do not move
            delivery(db, "create", "c1")
            jload(delivery(db, "start", "c1", "--json"))
            with open_db(db) as c:
                self.assertEqual(
                    c.execute("SELECT MAX(seq) m FROM delivery_events")
                    .fetchone()["m"], head_before)
            pcg(db, "correct", "approve", "FIX-1")
            with open_db(db) as c:
                ev = c.execute(
                    "SELECT e.*, b.kind FROM delivery_events e JOIN "
                    "delivery_batches b ON b.batch_id=e.batch_id "
                    "WHERE b.kind='correction'").fetchone()
                self.assertEqual(ev["partition_id"], 0)
                self.assertEqual(ev["offset"], 3)
                self.assertEqual(ev["source_revision"], 1)
            # the consumer sees the correction as a new event, same position
            events = consume_all(db, "c1")
            last = [e for e in events if e["partition_id"] == 0
                    and e["offset"] == 3][-1]
            self.assertEqual(last["source_revision"], 1)
            self.assertEqual(last["seq"], head_before + 1)
        finally:
            pg.shutdown_all()
        assert_clean(db)

    def test_publish_is_one_complete_batch_candidate_invisible(self):
        tmp = tempfile.mkdtemp(prefix="pcgdel-")
        pg, db = start_group(tmp, [8, 5])
        try:
            drain_workers(pg, ["w1"], 13)
            pcg(db, "rule", "register", "v1", write_rule(tmp, "v1", V1_SPEC))
            pcg(db, "rule", "recompute", "v1", "--page-size", "4")
            with open_db(db) as c:
                # candidate rows exist...
                self.assertEqual(
                    c.execute("SELECT COUNT(*) FROM rule_results WHERE version_id='v1'")
                    .fetchone()[0], 13)
                # ...but no publish batch exists yet
                self.assertEqual(
                    c.execute("SELECT COUNT(*) FROM delivery_batches WHERE kind='publish'")
                    .fetchone()[0], 0)
            token = jload(pcg(db, "rule", "review", "v1", "--json"))["token"]
            pcg(db, "rule", "activate", token)
            with open_db(db) as c:
                batch = c.execute(
                    "SELECT * FROM delivery_batches WHERE kind='publish'").fetchone()
                self.assertEqual(batch["event_count"], 13)
                vers = {r[0] for r in c.execute(
                    "SELECT DISTINCT rule_version FROM delivery_events WHERE batch_id=?",
                    (batch["batch_id"],))}
                self.assertEqual(vers, {"v1"})
        finally:
            pg.shutdown_all()
        assert_clean(db)

    def test_rejected_outcome_is_delivered_as_diagnostic(self):
        tmp = tempfile.mkdtemp(prefix="pcgdel-")
        # custom source with string amounts the strict decimal rule accepts
        src = os.path.join(tmp, "custom-src")
        os.makedirs(src)
        with open(os.path.join(src, "p0.jsonl"), "w") as fh:
            for off, amt in enumerate(["12.5", "8.0", "1.005"]):
                fh.write(json.dumps({"offset": off, "amount": amt}) + "\n")
        json.dump({"partitions": 1, "counts": {"0": 3}, "immutable": True},
                  open(os.path.join(src, "manifest.json"), "w"))
        pg = ProcGroup(os.path.join(tmp, "g.db"), src, lease_ms=6000,
                       ack_timeout_ms=6600, clock="manual")
        pg.start_coordinator()
        db = pg.db
        try:
            drain_workers(pg, ["w1"], 3)
            strict = {"fields": {"amount": {
                "missing": "reject", "null": "reject",
                "steps": [{"op": "decimal", "scale": 2,
                           "rounding": "HALF_UP"}]}}}
            pcg(db, "rule", "register", "strict", write_rule(tmp, "strict", strict))
            pcg(db, "rule", "recompute", "strict", "--page-size", "2")
            # the strict rule covers the section successfully; activation works
            token = jload(pcg(db, "rule", "review", "strict", "--json"))["token"]
            pcg(db, "rule", "activate", token)
            # now a correction replaces offset 1 with a non-decimal amount:
            # its approval stores an explicit rejection as the official row and
            # must deliver that rejection diagnostic through the change stream
            bad = {"offset": 1, "amount": "not-a-number"}
            pcg(db, "correct", "propose", "0", "1", "--correction-id", "FIX-BAD",
                "--expected-revision", "0", "--reason", "upstream typo",
                "--record", json.dumps(bad))
            approved = jload(pcg(db, "correct", "approve", "FIX-BAD", "--json"))
            self.assertEqual(approved["outcome"], "rejected")
            delivery(db, "create", "c1")
            jload(delivery(db, "start", "c1", "--json"))
            events = consume_all(db, "c1")
            rej = [e for e in events if e["status"] == "rejected"]
            self.assertEqual(len(rej), 1)
            ev = rej[0]
            self.assertEqual(ev["partition_id"], 0)
            self.assertEqual(ev["offset"], 1)
            self.assertEqual(ev["source_revision"], 1)
            self.assertEqual(ev["error"]["field"], "amount")
            # latest event of that position is the rejection diagnostic
            p1 = [e for e in events if e["partition_id"] == 0
                  and e["offset"] == 1]
            self.assertEqual(p1[-1]["seq"], ev["seq"])
        finally:
            pg.shutdown_all()
        assert_clean(db)


# =============================================================== initial sync

class TestInitialSync(unittest.TestCase):
    def test_snapshot_then_live_feed_no_gap_with_live_writes(self):
        tmp = tempfile.mkdtemp(prefix="pcgdel-")
        pg, db = start_group(tmp, [40])
        pg.admin("add", "w1", "--lease-ms", "6000")
        pg.start_worker("w1", batch_size=5)
        try:
            wait_records(db, 15)
            # create + start while the worker is still committing the tail
            delivery(db, "create", "c1")
            started = jload(delivery(db, "start", "c1", "--json"))
            boundary = started["boundary_seq"]
            self.assertGreaterEqual(boundary, 15)
            # drain the snapshot pages up to the boundary, paging slowly
            snapshot_events = []
            while True:
                page = jload(delivery(db, "read", "c1",
                                      "--page-size", "7", "--json"))
                self.assertTrue(page["events"])
                snapshot_events.extend(page["events"])
                delivery(db, "ack", "c1", page["cursor"])
                if page["to_seq"] >= boundary:
                    break
            self.assertEqual(max(e["seq"] for e in snapshot_events), boundary)
            # meanwhile ingestion continued: live events exist past boundary
            wait_records(db, 40)
            with open_db(db) as c:
                head = c.execute("SELECT MAX(seq) m FROM delivery_events").fetchone()["m"]
            self.assertGreater(head, boundary)
            live = consume_all(db, "c1")
            self.assertTrue(live)
            self.assertEqual(min(e["seq"] for e in live), boundary + 1)
            # full replay covers every seq exactly once
            all_events = snapshot_events + live
            seqs = sorted(e["seq"] for e in all_events)
            self.assertEqual(seqs, list(range(1, head + 1)))
            pos = jload(delivery(db, "position", "c1", "--json"))
            self.assertEqual(pos["state"], "active")
            self.assertEqual(pos["acked_seq"], head)
        finally:
            pg.shutdown_all()
        assert_clean(db)

    def test_changes_during_initial_paging_stay_outside_the_snapshot(self):
        """A correction and a publish that land while the consumer is still
        inside initial sync cannot enter snapshot pages or open a gap: they
        appear only after the pinned boundary, in the live feed."""
        tmp = tempfile.mkdtemp(prefix="pcgdel-")
        # small worker batches force several ingest batches so the snapshot
        # spans multiple pages and the consumer stays in 'syncing' for a while
        pg, db = start_group(tmp, [24])
        try:
            pg.admin("add", "w1", "--lease-ms", "6000")
            pg.start_worker("w1", batch_size=6)
            wait_records(db, 24)
            delivery(db, "create", "c1")
            started = jload(delivery(db, "start", "c1", "--json"))
            boundary = started["boundary_seq"]
            # read and ack just the first snapshot page, keeping the consumer
            # inside its initial sync while official changes keep happening
            first = jload(delivery(db, "read", "c1", "--page-size", "4",
                                   "--json"))
            self.assertLessEqual(first["to_seq"], boundary)
            jload(delivery(db, "ack", "c1", first["cursor"], "--json"))

            rec = {"offset": 7, "name": "Mid Sync", "city": "Oslo"}
            pcg(db, "correct", "propose", "0", "7", "--correction-id", "MID-1",
                "--expected-revision", "0", "--reason", "during sync",
                "--record", json.dumps(rec))
            pcg(db, "correct", "approve", "MID-1")
            pcg(db, "rule", "register", "v1", write_rule(tmp, "v1", V1_SPEC))
            pcg(db, "rule", "recompute", "v1", "--page-size", "8")
            tok = jload(pcg(db, "rule", "review", "v1", "--json"))["token"]
            pcg(db, "rule", "activate", tok)
            with open_db(db) as c:
                new_head = c.execute(
                    "SELECT MAX(seq) m FROM delivery_events").fetchone()["m"]
                later_batches = {r[0] for r in c.execute(
                    "SELECT DISTINCT b.batch_id FROM delivery_batches b "
                    "JOIN delivery_events e ON e.batch_id=b.batch_id "
                    "WHERE e.seq>?", (boundary,))}
                snapshot_batches = {r[0] for r in c.execute(
                    "SELECT DISTINCT b.batch_id FROM delivery_batches b "
                    "JOIN delivery_events e ON e.batch_id=b.batch_id "
                    "WHERE e.seq<=?", (boundary,))}
            # nothing created during/after the boundary can belong to it
            self.assertTrue(snapshot_batches.isdisjoint(later_batches))

            # full ordered replay (including the page already acked): snapshot
            # part stays within 1..boundary and the live part starts exactly
            # at boundary+1 (no gap, no overlap)
            all_events = list(first["events"]) + consume_all(db, "c1")
            self.assertEqual(sorted(e["seq"] for e in all_events),
                             list(range(1, new_head + 1)))
            snap = [e for e in all_events if e["seq"] <= boundary]
            live = [e for e in all_events if e["seq"] > boundary]
            self.assertTrue(snap and live)
            self.assertEqual(max(e["seq"] for e in snap), boundary)
            self.assertEqual(min(e["seq"] for e in live), boundary + 1)
            self.assertTrue({e["batch_id"] for e in live}
                            .issubset(later_batches))
            pos = jload(delivery(db, "position", "c1", "--json"))
            self.assertEqual(pos["state"], "active")
            self.assertEqual(pos["acked_seq"], new_head)
            # the correction's later state of position (0,7) is delivered
            # beyond the boundary at the SAME stable position (never a new
            # position): at least the correction event, and any publish event
            # of the recomputed section, each a distinct ordered seq, all rev 1
            p7 = [e for e in live if e["partition_id"] == 0
                  and e["offset"] == 7 and e["source_revision"] == 1]
            self.assertGreaterEqual(len(p7), 1)
            self.assertEqual(len({e["seq"] for e in p7}), len(p7))
            # no event for this position masquerades under a different position
            self.assertTrue(all(e["partition_id"] == 0 and e["offset"] == 7
                                for e in p7))
        finally:
            pg.shutdown_all()
        assert_clean(db)

    def test_start_is_idempotent_boundary_never_moves(self):
        tmp = tempfile.mkdtemp(prefix="pcgdel-")
        pg, db = start_group(tmp, [10])
        try:
            drain_workers(pg, ["w1"], 10)
            delivery(db, "create", "c1")
            first = jload(delivery(db, "start", "c1", "--json"))
            # more changes happen...
            rec = {"offset": 1, "name": "Later Change"}
            pcg(db, "correct", "propose", "0", "1", "--correction-id", "LATE",
                "--expected-revision", "0", "--reason", "x",
                "--record", json.dumps(rec))
            pcg(db, "correct", "approve", "LATE")
            second = jload(delivery(db, "start", "c1", "--json"))
            self.assertTrue(second["already_started"])
            self.assertEqual(second["boundary_seq"], first["boundary_seq"])
        finally:
            pg.shutdown_all()
        assert_clean(db)

    def test_read_before_start_rejected(self):
        tmp = tempfile.mkdtemp(prefix="pcgdel-")
        pg, db = start_group(tmp, [3])
        try:
            drain_workers(pg, ["w1"], 3)
            delivery(db, "create", "c1")
            r = delivery(db, "read", "c1", check=False)
            self.assertEqual(r.returncode, 23)
        finally:
            pg.shutdown_all()


# ================================================================== replay/crash

class TestReplayAndCrash(unittest.TestCase):
    def test_reread_returns_identical_page_and_cursor(self):
        tmp = tempfile.mkdtemp(prefix="pcgdel-")
        pg, db = start_group(tmp, [12])
        try:
            drain_workers(pg, ["w1"], 12)
            delivery(db, "create", "c1")
            jload(delivery(db, "start", "c1", "--json"))
            p1 = jload(delivery(db, "read", "c1", "--page-size", "4", "--json"))
            p2 = jload(delivery(db, "read", "c1", "--page-size", "4", "--json"))
            self.assertTrue(p2["replayed"])
            self.assertEqual(p1["cursor"], p2["cursor"])
            self.assertEqual(
                json.dumps(p1["events"], sort_keys=True),
                json.dumps(p2["events"], sort_keys=True))
            # a different requested page size must not change the pinned page
            p3 = jload(delivery(db, "read", "c1", "--page-size", "99", "--json"))
            self.assertEqual(p3["events"], p1["events"])
        finally:
            pg.shutdown_all()
        assert_clean(db)

    def test_process_exit_before_ack_replays_page(self):
        tmp = tempfile.mkdtemp(prefix="pcgdel-")
        pg, db = start_group(tmp, [10])
        try:
            drain_workers(pg, ["w1"], 10)
            delivery(db, "create", "c1")
            jload(delivery(db, "start", "c1", "--json"))
            page = jload(delivery(db, "read", "c1", "--page-size", "3", "--json"))
            cursor = page["cursor"]
            # simulate the downstream process dying: a brand-new process
            # resumes from the persisted position and replays the same page
            pos = jload(delivery(db, "position", "c1", "--json"))
            self.assertEqual(pos["outstanding_page"]["cursor"], cursor)
            replay = jload(delivery(db, "replay", "c1", "--json"))
            self.assertEqual(replay["cursor"], cursor)
            self.assertEqual(
                [e["seq"] for e in replay["events"]],
                [e["seq"] for e in page["events"]])
            # acking from the new process works and advances exactly once
            jload(delivery(db, "ack", "c1", cursor, "--json"))
            r = delivery(db, "ack", "c1", cursor, check=False)
            self.assertEqual(r.returncode, 24)  # no outstanding page now
        finally:
            pg.shutdown_all()
        assert_clean(db)

    def test_exit_after_ack_continues_at_next_page(self):
        tmp = tempfile.mkdtemp(prefix="pcgdel-")
        # enough rows that collection spans more than one ingest batch
        pg, db = start_group(tmp, [14])
        try:
            drain_workers(pg, ["w1"], 14)
            delivery(db, "create", "c1")
            jload(delivery(db, "start", "c1", "--json"))
            first = jload(delivery(db, "read", "c1", "--page-size", "3",
                                   "--json"))
            self.assertTrue(first["events"])
            jload(delivery(db, "ack", "c1", first["cursor"], "--json"))
            # new process: position persisted past the first whole-batch page
            pos = jload(delivery(db, "position", "c1", "--json"))
            self.assertIsNone(pos["outstanding_page"])
            self.assertEqual(pos["acked_seq"], first["to_seq"])
            if first["to_seq"] < pos["head_seq"]:
                second = jload(delivery(db, "read", "c1", "--page-size", "3",
                                        "--json"))
                self.assertEqual(second["from_seq"], first["to_seq"] + 1)
                self.assertFalse(second["replayed"])
            else:
                # one batch contained the whole history: a fresh read reports
                # caught-up instead of fabricating an empty page
                second = jload(delivery(db, "read", "c1", "--page-size", "3",
                                        "--json"))
                self.assertEqual(second["events"], [])
                self.assertTrue(second["caught_up"])
        finally:
            pg.shutdown_all()
        assert_clean(db)


# ============================================================== ack validation

class TestAckValidation(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="pcgdel-")
        self.pg, self.db = start_group(self.tmp, [12])
        drain_workers(self.pg, ["w1"], 12)
        delivery(self.db, "create", "c1")
        jload(delivery(self.db, "start", "c1", "--json"))
        self.page = jload(delivery(self.db, "read", "c1",
                                   "--page-size", "4", "--json"))

    def tearDown(self):
        self.pg.shutdown_all()
        assert_clean(self.db)

    def test_double_ack_refused(self):
        jload(delivery(self.db, "ack", "c1", self.page["cursor"], "--json"))
        r = delivery(self.db, "ack", "c1", self.page["cursor"], check=False)
        self.assertEqual(r.returncode, 24)

    def test_bogus_cursor_token_refused(self):
        f, t = self.page["from_seq"], self.page["to_seq"]
        r = delivery(self.db, "ack", "c1", f"{f}:{t}:{'0'*32}", check=False)
        self.assertEqual(r.returncode, 25)

    def test_skip_ahead_cursor_refused(self):
        t = self.page["to_seq"]
        r = delivery(self.db, "ack", "c1", f"1:{t + 1}:{'0'*32}", check=False)
        self.assertEqual(r.returncode, 25)

    def test_stale_bounds_refused(self):
        r = delivery(self.db, "ack", "c1", f"1:{self.page['to_seq'] - 1}:{'0'*32}",
                     check=False)
        self.assertEqual(r.returncode, 25)

    def test_unknown_consumer_refused(self):
        r = delivery(self.db, "ack", "ghost", self.page["cursor"], check=False)
        self.assertEqual(r.returncode, 21)

    def test_rejections_are_audited(self):
        for cursor in (f"1:{self.page['to_seq'] + 5}:{'0'*32}",
                       f"{self.page['from_seq']}:{self.page['to_seq']}:{'0'*32}"):
            delivery(self.db, "ack", "c1", cursor, check=False)
        rej = pcg(self.db, "delivery", "rejects", "--json")
        rows = jload(rej)
        self.assertGreaterEqual(len(rows), 2)
        self.assertTrue(all(r["reason"] for r in rows))
        # failed acks never moved the position
        pos = jload(delivery(self.db, "position", "c1", "--json"))
        self.assertEqual(pos["acked_seq"], 0)
        self.assertIsNotNone(pos["outstanding_page"])


# ========================================================== consumer independence

class TestIndependentConsumers(unittest.TestCase):
    def test_two_consumers_advance_independently(self):
        tmp = tempfile.mkdtemp(prefix="pcgdel-")
        pg, db = start_group(tmp, [20])
        try:
            # small worker batches produce several independent ingest batches,
            # so one page ack cannot accidentally consume the whole history
            pg.admin("add", "w1", "--lease-ms", "6000")
            pg.start_worker("w1", batch_size=4)
            wait_records(db, 20)
            for cid in ("slow", "fast"):
                delivery(db, "create", cid)
                jload(delivery(db, "start", cid, "--json"))
            # fast drains everything; slow opens one page and keeps it
            # unacknowledged while its rejection/independence checks run.
            fast_seen = consume_all(db, "fast", page_size=4)
            self.assertTrue(fast_seen)
            slow_page = jload(delivery(db, "read", "slow",
                                       "--page-size", "4", "--json"))
            self.assertTrue(slow_page["events"])
            f, t = (slow_page["from_seq"], slow_page["to_seq"])
            # cross-consumer cursor: same numeric bounds, hash bound to fast's
            # id -> explicit refusal (25), slow's page stays unacknowledged
            cross = f"{f}:{t}:" + self._fast_hash(f, t)
            r = delivery(db, "ack", "slow", cross, check=False)
            self.assertEqual(r.returncode, 25)
            # tampering with slow's own page token is also refused (25)
            r2 = delivery(db, "ack", "slow", f"{f}:{t}:{'0' * 32}",
                          check=False)
            self.assertEqual(r2.returncode, 25)
            # nothing moved: the same page is still the outstanding one
            pos_slow = jload(delivery(db, "position", "slow", "--json"))
            self.assertEqual(pos_slow["outstanding_page"]["cursor"],
                             slow_page["cursor"])
            self.assertEqual(pos_slow["acked_seq"], f - 1)
            # now the genuine ack succeeds
            jload(delivery(db, "ack", "slow", slow_page["cursor"], "--json"))
            pos_fast = jload(delivery(db, "position", "fast", "--json"))
            pos_slow = jload(delivery(db, "position", "slow", "--json"))
            # independent positions: fast reached the head, slow only acked
            # its single first page and therefore lags behind
            self.assertEqual(pos_fast["pending_events"], 0)
            self.assertEqual(pos_fast["acked_seq"], pos_fast["head_seq"])
            self.assertLess(pos_slow["acked_seq"], pos_fast["acked_seq"])
            # slow can still replay/continue and eventually finish
            rest = consume_all(db, "slow")
            self.assertTrue(rest)
            head = pos_fast["head_seq"]
            final = jload(delivery(db, "position", "slow", "--json"))
            self.assertEqual(final["acked_seq"], head)
            self.assertEqual(final["state"], "active")
            # listing shows both, independent positions recorded
            listed = {r["consumer_id"]: r for r in
                      jload(pcg(db, "delivery", "list", "--json"))}
            self.assertEqual(set(listed), {"slow", "fast"})
        finally:
            pg.shutdown_all()
        assert_clean(db)

    @staticmethod
    def _fast_hash(from_seq: int, to_seq: int) -> str:
        """The token digest slow's bounds would have under fast's consumer id."""
        import hashlib
        raw = f"fast|{from_seq}|{to_seq}"
        return hashlib.sha256(raw.encode()).hexdigest()[:32]

    def test_consumer_created_after_changes_starts_from_current_snapshot(self):
        tmp = tempfile.mkdtemp(prefix="pcgdel-")
        pg, db = start_group(tmp, [8])
        try:
            drain_workers(pg, ["w1"], 8)
            # early consumer pins boundary at 8
            delivery(db, "create", "early")
            early = jload(delivery(db, "start", "early", "--json"))
            rec = {"offset": 0, "name": "After Early"}
            pcg(db, "correct", "propose", "0", "0", "--correction-id", "MOVE",
                "--expected-revision", "0", "--reason", "x",
                "--record", json.dumps(rec))
            pcg(db, "correct", "approve", "MOVE")
            delivery(db, "create", "late")
            late = jload(delivery(db, "start", "late", "--json"))
            self.assertEqual(late["boundary_seq"], early["boundary_seq"] + 1)
            # late's snapshot already contains the corrected state as the
            # effective per-position state (last event wins)
            events = consume_all(db, "late")
            p0 = [e for e in events if e["partition_id"] == 0
                  and e["offset"] == 0]
            self.assertEqual(p0[-1]["source_revision"], 1)
            # early must still receive the correction through the live feed
            early_events = consume_all(db, "early")
            self.assertTrue(any(e["source_revision"] == 1 for e in early_events))
        finally:
            pg.shutdown_all()
        assert_clean(db)


# ============================================================ publish batching

class TestPublishBatching(unittest.TestCase):
    def test_publish_batch_reads_as_bounded_chunks_and_completes_atomically(self):
        """A 30-event publish is read in page-sized chunks (bounded memory),
        every chunk stays inside the one logical batch, and only the last
        chunk is marked batch_complete - so a consumer cannot regard the
        publish as applied until all its chunks are processed."""
        tmp = tempfile.mkdtemp(prefix="pcgdel-")
        pg, db = start_group(tmp, [30])
        try:
            drain_workers(pg, ["w1"], 30)
            delivery(db, "create", "c1")
            jload(delivery(db, "start", "c1", "--json"))
            consume_all(db, "c1")
            pcg(db, "rule", "register", "v1", write_rule(tmp, "v1", V1_SPEC))
            pcg(db, "rule", "recompute", "v1", "--page-size", "7")
            token = jload(pcg(db, "rule", "review", "v1", "--json"))["token"]
            pcg(db, "rule", "activate", token)
            with open_db(db) as c:
                batch = c.execute(
                    "SELECT * FROM delivery_batches WHERE kind='publish'").fetchone()
                span = c.execute(
                    "SELECT MIN(seq) lo, MAX(seq) hi FROM delivery_events "
                    "WHERE batch_id=?", (batch["batch_id"],)).fetchone()
            chunks, completed = [], 0
            while True:
                page = jload(delivery(db, "read", "c1",
                                      "--page-size", "3", "--json"))
                if not page["events"]:
                    break
                # every chunk is bounded by the page size and stays in batch
                self.assertLessEqual(len(page["events"]), 3)
                self.assertTrue(all(e["batch_id"] == batch["batch_id"]
                                    for e in page["events"]))
                if page["batch_complete"]:
                    completed += 1
                chunks.append(page)
                jload(delivery(db, "ack", "c1", page["cursor"], "--json"))
                if page["to_seq"] == span["hi"]:
                    break
            # 30 events at page size 3 -> multiple chunks, exactly one complete
            self.assertGreater(len(chunks), 1)
            self.assertEqual(completed, 1)
            self.assertTrue(chunks[-1]["batch_complete"])
            self.assertFalse(any(p["batch_complete"] for p in chunks[:-1]))
            # chunks tile the full batch without gaps or a foreign batch
            self.assertEqual(chunks[0]["from_seq"], span["lo"])
            self.assertEqual(chunks[-1]["to_seq"], span["hi"])
            total = sum(len(p["events"]) for p in chunks)
            self.assertEqual(total, 30)
            pos = jload(delivery(db, "position", "c1", "--json"))
            self.assertEqual(pos["acked_seq"], span["hi"])
        finally:
            pg.shutdown_all()
        assert_clean(db)

    def test_interrupted_activation_leaves_no_partial_publish(self):
        tmp = tempfile.mkdtemp(prefix="pcgdel-")
        pg, db = start_group(tmp, [20])
        try:
            drain_workers(pg, ["w1"], 20)
            pcg(db, "rule", "register", "v1", write_rule(tmp, "v1", V1_SPEC))
            pcg(db, "rule", "recompute", "v1", "--page-size", "5")
            token = jload(pcg(db, "rule", "review", "v1", "--json"))["token"]
            # kill the activation process at the OS level; SQLite either
            # commits the whole activation transaction or none of it
            proc = subprocess.Popen(
                [sys.executable, "-m", "pcg", "--db", db, "--clock", "real",
                 "rule", "activate", token],
                env=ENV, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            time.sleep(0.05)
            proc.kill()
            proc.wait()
            with open_db(db) as c:
                active = c.execute(
                    "SELECT active_version FROM rule_state WHERE id=1").fetchone()[0]
                n_pub = c.execute(
                    "SELECT COUNT(*) FROM delivery_batches WHERE kind='publish'"
                ).fetchone()[0]
            if active == "v1":
                # commit won the race: the full batch must be present
                self.assertEqual(n_pub, 1)
                with open_db(db) as c:
                    batch = c.execute(
                        "SELECT * FROM delivery_batches WHERE kind='publish'").fetchone()
                    n = c.execute(
                        "SELECT COUNT(*) FROM delivery_events WHERE batch_id=?",
                        (batch["batch_id"],)).fetchone()[0]
                self.assertEqual(n, batch["event_count"])
            else:
                # rollback won: no publish events at all, retry succeeds
                self.assertEqual(n_pub, 0)
                pcg(db, "rule", "activate", token)
                with open_db(db) as c:
                    self.assertEqual(c.execute(
                        "SELECT COUNT(*) FROM delivery_batches WHERE kind='publish'"
                    ).fetchone()[0], 1)
        finally:
            pg.shutdown_all()
        assert_clean(db)

    def test_small_batch_before_publish_is_not_merged_into_one_page(self):
        """A one-event correction immediately followed by a large publish must
        remain two batch-aligned pages, even with a page size spanning both."""
        tmp = tempfile.mkdtemp(prefix="pcgdel-")
        pg, db = start_group(tmp, [12])
        try:
            drain_workers(pg, ["w1"], 12)
            delivery(db, "create", "c1")
            jload(delivery(db, "start", "c1", "--json"))
            consume_all(db, "c1")  # past the snapshot
            # correction -> one-event batch, then a publish -> 12-event batch
            rec = {"offset": 4, "name": "Between Batches", "city": "Lima"}
            pcg(db, "correct", "propose", "0", "4", "--correction-id", "BB-1",
                "--expected-revision", "0", "--reason", "x",
                "--record", json.dumps(rec))
            pcg(db, "correct", "approve", "BB-1")
            pcg(db, "rule", "register", "v1", write_rule(tmp, "v1", V1_SPEC))
            pcg(db, "rule", "recompute", "v1", "--page-size", "6")
            tok = jload(pcg(db, "rule", "review", "v1", "--json"))["token"]
            pcg(db, "rule", "activate", tok)
            with open_db(db) as c:
                corr = c.execute(
                    "SELECT b.batch_id, MIN(e.seq) lo, MAX(e.seq) hi "
                    "FROM delivery_batches b JOIN delivery_events e "
                    "ON e.batch_id=b.batch_id WHERE b.kind='correction' "
                    "GROUP BY b.batch_id").fetchone()
                pub = c.execute(
                    "SELECT b.batch_id, MIN(e.seq) lo, MAX(e.seq) hi "
                    "FROM delivery_batches b JOIN delivery_events e "
                    "ON e.batch_id=b.batch_id WHERE b.kind='publish' "
                    "GROUP BY b.batch_id").fetchone()
            # page size 5 spans the boundary: first page is the correction only
            p1 = jload(delivery(db, "read", "c1", "--page-size", "5",
                                "--json"))
            self.assertEqual(len(p1["events"]), 1)
            self.assertEqual(p1["from_seq"], corr["lo"])
            self.assertEqual(p1["to_seq"], corr["hi"])
            self.assertEqual(p1["batch_id"], corr["batch_id"])
            self.assertTrue(p1["batch_complete"])
            jload(delivery(db, "ack", "c1", p1["cursor"], "--json"))
            # the next page belongs only to the publish batch (never merged)
            p2 = jload(delivery(db, "read", "c1", "--page-size", "5",
                                "--json"))
            self.assertEqual(p2["from_seq"], pub["lo"])
            self.assertEqual(p2["batch_id"], pub["batch_id"])
            self.assertLessEqual(len(p2["events"]), 5)
            self.assertEqual({e["batch_id"] for e in p2["events"]},
                             {pub["batch_id"]})
        finally:
            pg.shutdown_all()
        assert_clean(db)

    def test_two_publishes_are_distinct_ordered_batches(self):
        tmp = tempfile.mkdtemp(prefix="pcgdel-")
        pg, db = start_group(tmp, [10])
        try:
            drain_workers(pg, ["w1"], 10)
            delivery(db, "create", "c1")
            jload(delivery(db, "start", "c1", "--json"))
            consume_all(db, "c1")
            for vid, spec in (("v1", V1_SPEC), ("v2", V2_SPEC)):
                pcg(db, "rule", "register", vid, write_rule(tmp, vid, spec))
                pcg(db, "rule", "recompute", vid, "--page-size", "6")
                tok = jload(pcg(db, "rule", "review", vid, "--json"))["token"]
                pcg(db, "rule", "activate", tok)
            with open_db(db) as c:
                pubs = [r["batch_id"] for r in c.execute(
                    "SELECT batch_id FROM delivery_batches WHERE kind='publish' "
                    "ORDER BY batch_id")]
                self.assertEqual(len(pubs), 2)
            pages = []
            while True:
                page = jload(delivery(db, "read", "c1",
                                      "--page-size", "4", "--json"))
                if not page["events"]:
                    break
                bids = {e["batch_id"] for e in page["events"]}
                self.assertEqual(len(bids), 1)  # never mixes two batches
                self.assertLessEqual(len(page["events"]), 4)  # bounded page
                pages.append(page)
                delivery(db, "ack", "c1", page["cursor"])
            # both publish batches were covered and each has one completing page
            seen_batches = {p["batch_id"] for p in pages}
            self.assertEqual(seen_batches, set(pubs))
            for bid in pubs:
                completing = [p for p in pages if p["batch_id"] == bid
                              and p["batch_complete"]]
                self.assertEqual(len(completing), 1)
        finally:
            pg.shutdown_all()
        assert_clean(db)


# ================================================================ concurrency

class TestConcurrentWritersAndConsumers(unittest.TestCase):
    def test_interleaved_ingest_correction_publish_read_ack(self):
        tmp = tempfile.mkdtemp(prefix="pcgdel-")
        pg, db = start_group(tmp, [60])
        pg.admin("add", "w1", "--lease-ms", "6000")
        pg.start_worker("w1", batch_size=7)
        try:
            wait_records(db, 20)
            for cid in ("a", "b"):
                delivery(db, "create", cid)
                jload(delivery(db, "start", cid, "--json"))

            # background consumers: read, sleep, ack at different speeds
            def consumer_loop(cid: str, page_size: int, stop_file: str):
                while not os.path.exists(stop_file):
                    p = subprocess.run(
                        [sys.executable, "-m", "pcg", "--db", db,
                         "--clock", "real", "delivery", "read", cid,
                         "--page-size", str(page_size), "--json"],
                        env=ENV, capture_output=True, text=True)
                    if p.returncode:
                        time.sleep(0.05)
                        continue
                    page = json.loads(p.stdout)
                    if not page["events"]:
                        time.sleep(0.05)
                        continue
                    time.sleep(0.02 if cid == "a" else 0.09)
                    subprocess.run(
                        [sys.executable, "-m", "pcg", "--db", db,
                         "--clock", "real", "delivery", "ack", cid,
                         page["cursor"]], env=ENV, check=True,
                        capture_output=True)

            stop = os.path.join(tmp, "stop")
            import threading
            ta = threading.Thread(target=consumer_loop, args=("a", 5, stop),
                                  daemon=True)
            tb = threading.Thread(target=consumer_loop, args=("b", 9, stop),
                                  daemon=True)
            ta.start(); tb.start()

            # writer-side activity interleaving with the consumer loops
            wait_records(db, 40)
            rec = {"offset": 5, "name": "Concurrent Fix", "city": "Rome"}
            pcg(db, "correct", "propose", "0", "5", "--correction-id", "C1",
                "--expected-revision", "0", "--reason", "race",
                "--record", json.dumps(rec))
            pcg(db, "correct", "approve", "C1")
            pcg(db, "rule", "register", "v1", write_rule(tmp, "v1", V1_SPEC))
            pcg(db, "rule", "recompute", "v1", "--page-size", "11")
            token = jload(pcg(db, "rule", "review", "v1", "--json"))["token"]
            pcg(db, "rule", "activate", token)
            wait_records(db, 60)
            time.sleep(2.0)  # let both consumers drain the live tail
            open(stop, "w").close()
            ta.join(timeout=10); tb.join(timeout=10)

            with open_db(db) as c:
                head = c.execute(
                    "SELECT MAX(seq) m FROM delivery_events").fetchone()["m"]
            for cid in ("a", "b"):
                # any still-outstanding page gets acked through a fresh process
                pos = jload(delivery(db, "position", cid, "--json"))
                if pos["outstanding_page"]:
                    page = jload(delivery(db, "replay", cid, "--json"))
                    delivery(db, "ack", cid, page["cursor"])
                consume_all(db, cid)
                final = jload(delivery(db, "position", cid, "--json"))
                self.assertEqual(final["acked_seq"], head, cid)
                self.assertEqual(final["pending_events"], 0, cid)
            # independent verification of the whole interleaved history
            problems, _ = verify_database(db)
            self.assertEqual(problems, [])
        finally:
            pg.shutdown_all()


# ============================================================== bounded memory

class TestBoundedMemory(unittest.TestCase):
    def test_skewed_long_history_initial_sync_and_catchup(self):
        tmp = tempfile.mkdtemp(prefix="pcgbound-")
        # heavy skew: empty, huge and medium partitions
        pg, db = start_group(tmp, [0, 3000, 400], seed=33)
        try:
            drain_workers(pg, ["w1", "w2"], 3400)
            delivery(db, "create", "c1")
            jload(delivery(db, "start", "c1", "--json"))
            # a second publish makes the live tail large as well
            pcg(db, "rule", "register", "v1", write_rule(tmp, "v1", V1_SPEC))
            pcg(db, "rule", "recompute", "v1", "--page-size", "200")
            token = jload(pcg(db, "rule", "review", "v1", "--json"))["token"]
            pcg(db, "rule", "activate", token)

            script = (
                "import resource, subprocess, sys\n"
                "resource.setrlimit(resource.RLIMIT_AS, "
                "(180*1024*1024, 180*1024*1024))\n"
                f"db={db!r}; env={ENV!r}\n"
                "pages = 0; events = 0\n"
                "while True:\n"
                "    p = subprocess.run([sys.executable,'-m','pcg','--db',db,"
                "'--clock','real','delivery','read','c1','--page-size','50',"
                "'--json'], env=env, capture_output=True, text=True)\n"
                "    if p.returncode: sys.stderr.write(p.stdout); sys.exit(1)\n"
                "    page = __import__('json').loads(p.stdout)\n"
                "    if not page['events']: break\n"
                "    events += len(page['events']); pages += 1\n"
                # physical pages are strictly bounded by the requested size;
                # batch identity is preserved via batch_id/batch_complete
                "    assert len(page['events']) <= 50, page\n"
                "    assert len({e['batch_id'] for e in page['events']}) == 1\n"
                "    subprocess.run([sys.executable,'-m','pcg','--db',db,"
                "'--clock','real','delivery','ack','c1',page['cursor']], "
                "env=env, check=True, capture_output=True)\n"
                "print(f'pages={pages} events={events}')\n"
            )
            r = subprocess.run([sys.executable, "-I", "-c", script],
                               env=ENV, capture_output=True, text=True)
            self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
            self.assertIn("events=6800", r.stdout)  # 3400 ingest + 3400 publish
            pos = jload(delivery(db, "position", "c1", "--json"))
            self.assertEqual(pos["pending_events"], 0)
            self.assertEqual(pos["state"], "active")
        finally:
            pg.shutdown_all()
        assert_clean(db)


if __name__ == "__main__":
    unittest.main(verbosity=2)
