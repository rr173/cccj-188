"""End-to-end safety tests using real processes and a controllable clock.

Scenarios (mapped to the task requirements):

 1. barrier handoff     - worker computes offset 0, blocks before commit,
                           partition is transferred; old whole batch is
                           rejected, the successor commits and the barrier
                           only then releases the old worker; new values are
                           never overwritten;
 2. graceful revoke     - SIGTERM draining with a normal ack;
 3. kill mid-revoke     - SIGKILL while a handoff is pending; lease fence;
 4. coordinator restart - old unresolved generation is uniformly sealed;
 5. late stale ack      - an ack from the old plan cannot roll back the new
                           allocation (lands in stale_acks);
 6. two member changes  - back-to-back add/remove rounds;
 7. empty + skew        - empty partitions and heavily skewed counts;
 8. full reconcile      - per-offset value + progress continuity check.
"""

from __future__ import annotations

import os
import signal
import time
import unittest

from harness import PcgTestCase, ProcGroup, make_source


def barrier_name(wid: str, part: int, start: int) -> str:
    return f"{wid}:p{part}:o{start}"


class TestBarrierHandoff(PcgTestCase):
    """The headline scenario: safe handoff of an in-flight partition."""

    def test_computed_uncommitted_batch_is_rejected_and_never_overwrites(self):
        # 2 partitions, 2 workers. Deterministic naming gives aa->p0, zz->p1.
        # The barrier parks zz AFTER it has read+cleaned p1 offset 0..4 but
        # BEFORE the commit transaction; its heartbeat thread keeps the lease.
        pg = self.new_group([12, 12], lease_ms=6000, ack_timeout_ms=3000)
        # register BOTH members before any worker starts so the first plan is
        # already the balanced target (aa->p0, zz->p1), no startup rebalance
        pg.admin("add", "aa", "--lease-ms", "6000")
        pg.admin("add", "zz", "--lease-ms", "6000")
        pg.start_worker("aa", batch_size=5)
        pg.wait_registered("aa")
        pg.start_worker("zz", batch_size=5, barrier="zz:1:0")
        pg.wait_registered("zz")
        bname = barrier_name("zz", 1, 0)
        pg.wait_barrier_reached(bname)
        # nothing from the parked partition is committed yet
        self.assertIsNone(pg.sql_one(
            "SELECT committed_offset FROM progress WHERE partition_id=1"))

        old = pg.sql_one(
            "SELECT gen, worker_id FROM assignments WHERE partition_id=1 "
            "ORDER BY gen DESC LIMIT 1")
        self.assertEqual(old["worker_id"], "zz")
        old_gen = old["gen"]

        # one membership change: aa leaves, so zz must take over p1.
        pg.wait_heartbeat_advancing("aa")
        pg.admin("remove", "aa")
        pg.wait_member_status("aa", "leaving")
        self.assertEqual(pg.graceful_stop("aa"), 0)

        # the revocation for zz's old generation is published and the term is
        # not yet fenced (revoking/waiting), so a late commit will be refused
        pg.wait_for(lambda: pg.sql_one(
            "SELECT 1 FROM assignments WHERE partition_id=1 AND gen=? "
            "AND state IN ('revoking','revoked')",
            (old_gen,)), timeout=15)

        # release the old worker BEFORE the ack deadline: it wakes holding a
        # generation that is already 'revoking', so its whole pre-computed
        # batch is rejected by the transaction-bound fence (not UPSERTed).
        pg.release_barrier(bname)
        rej = pg.wait_for(lambda: pg.sql_one(
            "SELECT * FROM rejected_commits WHERE partition_id=1 AND gen=? "
            "ORDER BY id LIMIT 1", (old_gen,)), timeout=15)
        self.assertEqual((rej["start_offset"], rej["end_offset"]), (0, 4))

        # the old term is then fenced and the new generation activates; the
        # successor (zz, new gen) drains BOTH partitions from durable progress
        pg.wait_for(lambda: pg.sql_one(
            "SELECT state FROM assignments WHERE partition_id=1 AND gen=?",
            (old_gen,))["state"] in ("fenced", "revoked"), timeout=20)
        pg.wait_committed(0, 11, timeout=25)
        pg.wait_committed(1, 11, timeout=25)

        # full independent reconciliation: every offset/value + continuity
        problems, total = self.reconcile()
        self.assertEqual(problems, 0)
        self.assertEqual(total, 24)

        # the rejected generation never wrote a row; progress belongs to new gen
        gens = {r["gen"] for r in pg.sql(
            "SELECT DISTINCT gen FROM records WHERE partition_id=1")}
        self.assertNotIn(old_gen, gens)
        row = pg.sql_one("SELECT gen, worker_id FROM progress WHERE partition_id=1")
        self.assertNotEqual(row["gen"], old_gen)
        self.assertEqual(row["worker_id"], "zz")


class TestGracefulRevoke(PcgTestCase):
    def test_sigterm_drains_and_acks(self):
        pg = self.new_group([10, 10])
        pg.admin("add", "w1")
        pg.admin("add", "w2")
        pg.start_worker("w1", batch_size=3)
        pg.wait_registered("w1")
        pg.start_worker("w2", batch_size=3)
        pg.wait_registered("w2")
        pg.wait_committed(0, 5, timeout=15)
        # ask w1 to leave while a plan may be in flight
        pg.wait_heartbeat_advancing("w1")
        pg.admin("remove", "w1")
        pg.wait_member_status("w1", "leaving")
        code = pg.graceful_stop("w1")
        self.assertEqual(code, 0)
        # all partitions eventually consumed and reconciled
        pg.wait_committed(0, 9, timeout=20)
        pg.wait_committed(1, 9, timeout=20)
        problems, total = self.reconcile()
        self.assertEqual(problems, 0)
        self.assertEqual(total, 20)
        # at least one ack must have been produced for the handoff
        self.assertTrue(pg.sql("SELECT 1 FROM revocations LIMIT 1"))


class TestKillMidRevoke(PcgTestCase):
    def test_sigkill_fences_old_generation(self):
        pg = self.new_group([8, 8, 8], lease_ms=700, ack_timeout_ms=900)
        pg.start_worker("w1", batch_size=3)
        pg.wait_committed(0, 2)
        pg.admin("add", "w2", "--lease-ms", "700")
        pg.start_worker("w2", batch_size=3)
        pg.wait_for(lambda: any(
            r["state"] == "revoking"
            for r in pg.sql("SELECT state FROM assignments")))
        # hard kill: no ack will ever arrive from w1
        pg.sigkill("w1")
        pg.wait_exit("w1")
        # coordinator must fence rather than wait forever
        pg.wait_for(lambda: any(
            r["state"] == "fenced" for r in pg.sql(
                "SELECT state FROM revocations")))
        pg.wait_committed(0, 7, timeout=20)
        pg.wait_committed(1, 7, timeout=20)
        pg.wait_committed(2, 7, timeout=20)
        problems, total = self.reconcile()
        self.assertEqual(problems, 0)
        self.assertEqual(total, 24)


class TestCoordinatorRestart(PcgTestCase):
    def test_restart_seals_old_generation_and_resumes(self):
        # two stable members so we can create an in-flight handoff to survive
        pg = self.new_group([6, 6], lease_ms=4000, ack_timeout_ms=8000)
        pg.admin("add", "w1", "--lease-ms", "4000")
        pg.admin("add", "w2", "--lease-ms", "4000")
        pg.start_worker("w1", batch_size=2)
        pg.start_worker("w2", batch_size=2)
        pg.wait_registered("w1")
        pg.wait_registered("w2")
        pg.wait_committed(0, 3)

        # remove w2 so a revocation for its partition is left waiting
        w2_part = pg.sql_one(
            "SELECT partition_id p FROM assignments WHERE worker_id='w2' LIMIT 1")["p"]
        old_gen = pg.sql_one(
            "SELECT gen FROM assignments WHERE partition_id=? ORDER BY gen DESC LIMIT 1",
            (w2_part,))["gen"]
        pg.admin("remove", "w2")
        pg.sigkill("w2")
        pg.wait_exit("w2")
        pg.wait_for(lambda: pg.sql_one(
            "SELECT 1 FROM revocations WHERE partition_id=? AND gen=? AND state='waiting'",
            (w2_part, old_gen)))

        # kill the coordinator mid-handoff; its lease proof dies with it
        pg.stop_coordinator(sig=signal.SIGKILL)
        pg.wait_exit("__coord__")
        epoch_before = int(pg.sql_one(
            "SELECT value v FROM meta WHERE key='epoch'")["v"])

        # relaunch: bump epoch and uniformly fence the unresolved old generation
        pg.start_coordinator()
        pg.wait_for(lambda: int(pg.sql_one(
            "SELECT value v FROM meta WHERE key='epoch'")["v"]) > epoch_before)
        pg.wait_for(lambda: pg.sql_one(
            "SELECT state FROM assignments WHERE partition_id=? AND gen=?",
            (w2_part, old_gen))["state"] == "fenced")

        # processing completes and reconciles under the new incarnation
        pg.wait_committed(0, 5, timeout=25)
        pg.wait_committed(1, 5, timeout=25)
        problems, _ = self.reconcile()
        self.assertEqual(problems, 0)
        # the meta epoch was bumped exactly once by the restart (the durable
        # incarnation marker); plans only carry an epoch when membership work
        # happens, so we assert the marker rather than a plan row
        self.assertEqual(
            int(pg.sql_one("SELECT value v FROM meta WHERE key='epoch'")["v"]),
            epoch_before + 1)


class TestLateStaleAck(PcgTestCase):
    def test_old_plan_ack_is_quarantined_not_applied(self):
        # pre-register two members so the first plan is already stable:
        # w1->p0, w2->p1. Hard-kill w2 FIRST (before any membership change)
        # so the dead incarnation can never write a graceful ack; removing it
        # afterwards forces the coordinator to fence the dead term. (Doing it
        # in the other order races with w2's own graceful-drain exit, which
        # legitimately acks before the SIGKILL lands.)
        pg = self.new_group([6, 6], lease_ms=900, ack_timeout_ms=1000)
        pg.admin("add", "w1", "--lease-ms", "900")
        pg.admin("add", "w2", "--lease-ms", "900")
        pg.start_worker("w1", batch_size=2)
        pg.start_worker("w2", batch_size=2)
        pg.wait_registered("w1")
        pg.wait_registered("w2")
        pg.wait_committed(1, 1)

        old = pg.sql_one(
            "SELECT * FROM assignments WHERE partition_id=1 ORDER BY gen DESC LIMIT 1")
        self.assertEqual(old["worker_id"], "w2")
        pg.sigkill("w2")
        pg.wait_exit("w2")
        pg.admin("remove", "w2")
        # wait for the single transfer revocation to be published (it may
        # already have progressed past 'waiting' under load)
        pg.wait_for(lambda: pg.sql_one(
            "SELECT 1 FROM revocations WHERE partition_id=1 AND gen=?",
            (old["gen"],)))
        # coordinator fences the old generation instead of waiting forever
        pg.wait_for(lambda: pg.sql_one(
            "SELECT state FROM assignments WHERE partition_id=1 AND gen=?",
            (old["gen"],))["state"] == "fenced")
        new_plan = pg.sql_one("SELECT MAX(plan_id) p FROM plans")["p"]
        pg.wait_plan_finalized(new_plan)

        # a delayed ack from the dead incarnation, bound to the OLD plan/gen
        import sqlite3
        c = sqlite3.connect(pg.db, timeout=15)
        old_plan = pg.sql_one(
            "SELECT plan_id FROM revocations WHERE partition_id=1 AND gen=?",
            (old["gen"],))["plan_id"]
        c.execute(
            "INSERT OR REPLACE INTO rev_acks(partition_id,gen,plan_id,worker_id,"
            "acked_at,drained) VALUES(?,?,?,?,?,?)",
            (1, old["gen"], old_plan, "w2",
             pg.sql_one("SELECT value v FROM meta WHERE key='now_ms'")["v"], 0))
        c.commit()
        c.close()
        # coordinator tick must quarantine it, never roll back the new plan
        pg.wait_for(lambda: pg.sql_one(
            "SELECT 1 FROM stale_acks WHERE partition_id=1 AND gen=?",
            (old["gen"],)))
        pg.wait_for(lambda: pg.sql_one(
            "SELECT state FROM assignments WHERE partition_id=1 ORDER BY gen DESC LIMIT 1"
            )["state"] == "active", timeout=15)
        cur = pg.sql_one(
            "SELECT * FROM assignments WHERE partition_id=1 ORDER BY gen DESC LIMIT 1")
        self.assertEqual(cur["state"], "active")
        self.assertEqual(cur["worker_id"], "w1")
        pg.wait_committed(1, 5, timeout=20)
        problems, _ = self.reconcile()
        self.assertEqual(problems, 0)


class TestTwoMembershipRounds(PcgTestCase):
    def test_back_to_back_changes_converge_balanced(self):
        pg = self.new_group([5, 5, 5, 5])
        pg.admin("add", "w1")
        pg.start_worker("w1", batch_size=2)
        pg.wait_registered("w1")
        pg.wait_committed(0, 1)
        # round 1: grow to three members
        for wid in ("w2", "w3"):
            pg.admin("add", wid)
            pg.start_worker(wid, batch_size=2)
            pg.wait_registered(wid)
        pg.wait_for(lambda: pg.sql_one(
            "SELECT COUNT(*) c FROM plans WHERE finalized=1")["c"] >= 1, timeout=20)
        # round 2: shrink back to two while plans may still be settling
        pg.wait_heartbeat_advancing("w3")  # ensure its loop/handler are live first
        pg.admin("remove", "w3")
        pg.wait_member_status("w3", "leaving")
        self.assertEqual(pg.graceful_stop("w3"), 0)
        # final ownership: all 4 partitions on alive members, diff <= 1
        def balanced():
            latest = pg.sql_one("SELECT MAX(plan_id) p FROM plans")["p"]
            rows = pg.sql(
                "SELECT worker_id, COUNT(*) c FROM assignments WHERE plan_id=? "
                "AND state='active' GROUP BY worker_id", (latest,))
            if sum(r["c"] for r in rows) != 4:
                return None
            counts = [r["c"] for r in rows]
            return rows if max(counts) - min(counts) <= 1 else None
        owners = pg.wait_for(balanced, timeout=20)
        alive = {r["worker_id"] for r in pg.sql(
            "SELECT worker_id FROM members WHERE status IN ('alive','leaving')")}
        self.assertTrue({r["worker_id"] for r in owners}.issubset(alive))
        for p in range(4):
            pg.wait_committed(p, 4, timeout=25)
        problems, total = self.reconcile()
        self.assertEqual(problems, 0)
        self.assertEqual(total, 20)


class TestEmptyAndSkew(PcgTestCase):
    def test_empty_partition_and_skewed_source(self):
        pg = self.new_group([0, 30, 0, 3, 0])
        # pre-register so the first plan already balances 5 partitions / 2 workers
        pg.admin("add", "w1")
        pg.admin("add", "w2")
        pg.start_worker("w1", batch_size=7)
        pg.start_worker("w2", batch_size=7)
        pg.wait_registered("w1")
        pg.wait_registered("w2")
        # empty partitions still get assigned (balanced plan) but produce no rows
        pg.wait_committed(1, 29, timeout=25)
        pg.wait_committed(3, 2, timeout=25)
        problems, total = self.reconcile()
        self.assertEqual(problems, 0)
        self.assertEqual(total, 33)
        latest = pg.sql_one("SELECT MAX(plan_id) p FROM plans")["p"]
        owners = pg.sql(
            "SELECT worker_id, COUNT(*) c FROM assignments WHERE plan_id=? "
            "AND state='active' GROUP BY worker_id",
            (latest,))
        counts = sorted(r["c"] for r in owners)
        self.assertEqual(sum(counts), 5)          # every partition assigned once
        self.assertEqual(counts[-1] - counts[0], 1)  # balanced 3 vs 2
        # the three empty partitions have no progress rows and no records
        for empty_p in (0, 2, 4):
            self.assertIsNone(pg.sql_one(
                "SELECT 1 FROM progress WHERE partition_id=?", (empty_p,)))
            self.assertEqual(pg.sql_one(
                "SELECT COUNT(*) c FROM records WHERE partition_id=?",
                (empty_p,))["c"], 0)


class TestDeterministicPlanner(unittest.TestCase):
    def test_balance_retention_and_tiebreak(self):
        from pcg.planner import plan_assignment
        parts = list(range(6))
        # equal members: contiguous low-id blocks
        m = plan_assignment(parts, ["b", "a"], {})
        self.assertEqual(m, {0: "a", 1: "a", 2: "a", 3: "b", 4: "b", 5: "b"})
        # growth retains maximum ownership under capacity
        prev = m
        m2 = plan_assignment(parts, ["a", "b", "c"], prev)
        self.assertEqual(m2[0], "a")
        self.assertEqual(m2[3], "b")
        retained = sum(1 for p in parts if m2[p] == prev[p])
        self.assertEqual(retained, 4)  # capacities 2,2,2 -> a keeps 2, b keeps 2
        # shrinking keeps the lowest ids deterministically
        m3 = plan_assignment(parts, ["a", "b"], m2)
        self.assertEqual(m3[0], "a")
        # no members -> empty plan, no crash
        self.assertEqual(plan_assignment(parts, [], prev), {})
        # more members than partitions: some idle, diff <= 1
        m4 = plan_assignment([0, 1], ["a", "b", "c"], {})
        counts = {w: list(m4.values()).count(w) for w in ("a", "b", "c")}
        self.assertEqual(max(counts.values()) - min(counts.values()), 1)
        self.assertEqual(counts["c"], 0)


class TestSourceIntegrity(unittest.TestCase):
    def test_discontinuous_source_is_rejected(self):
        import tempfile
        from pcg.source import validate_source, SourceError, iter_records
        d = tempfile.mkdtemp(prefix="pcgbad-")
        with open(os.path.join(d, "p0.jsonl"), "w") as fh:
            fh.write('{"offset":0,"name":"x"}\n')
            fh.write("\n")  # blank line -> discontinuity
            fh.write('{"offset":2,"name":"y"}\n')
        problems = validate_source(d)
        self.assertTrue(problems)
        with self.assertRaises(SourceError):
            list(iter_records(d, 0))


if __name__ == "__main__":
    unittest.main(verbosity=2)
