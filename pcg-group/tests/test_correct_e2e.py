"""End-to-end acceptance tests for the source-record correction workflow.

Every scenario uses REAL OS processes (coordinator, workers and the
``pcg correct`` / ``pcg rule`` management commands), coordinated only through
the shared SQLite database.  Value/diagnostic verification goes through
:mod:`tests.tools_rule_ref`, an independent re-implementation that imports
none of the production cleaning code.

Coverage (mapped to the task requirements):

* two consecutive corrections on one offset + restore of an old value keep
  every revision; original record, clean values and progress never change;
* stale proposals (propose-time and approve-time) are refused with the actual
  revision; correction ids are idempotent for identical content and rejected
  for different content;
* two admin processes approving the same proposal: exactly one wins;
* a pending proposal changes no export, review, diff or recompute result;
* after approval: original fields, revision history, active-version results,
  rejection diagnostics and collection progress are all verified;
* review tokens die when any correction takes effect, even if later undone;
* an inactive version must catch up (recompute over effective objects) and
  re-review before it can be republished after corrections;
* exports interleaved with approvals/publishes are single-snapshot consistent;
* a barrier-parked, already-computed worker batch cannot overwrite an
  approved correction nor resurrect the old value as a new revision;
* skewed long history: revision queries and rule catch-up stay bounded-memory;
* old-database upgrade is non-destructive and repeatable.
"""

from __future__ import annotations

import json
import os
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
from tools_rule_ref import (  # noqa: E402
    ref_clean_fixed,
    ref_outcome,
    snapshot_immutables,
    verify_database,
)


# --------------------------------------------------------------------------- helpers

def open_db(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, timeout=15)
    conn.row_factory = sqlite3.Row
    return conn


def pcg(db_path: str, *args, check: bool = True):
    p = subprocess.run(
        [sys.executable, "-m", "pcg", "--db", db_path, "--clock", "real",
         *args],
        env=ENV, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    if check and p.returncode != 0:
        raise AssertionError(f"pcg {' '.join(args)} rc={p.returncode}: {p.stdout}")
    return p


def correct(db_path: str, *args, check: bool = True):
    return pcg(db_path, "correct", *args, check=check)


def rule(db_path: str, *args, check: bool = True):
    return pcg(db_path, "rule", *args, check=check)


def write_rule(base: str, name: str, spec: dict) -> str:
    path = os.path.join(base, f"{name}.json")
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(spec, fh)
    return path


def start_group(tmp: str, source: str, lease_ms=6000, tick_ms=50) -> ProcGroup:
    db = os.path.join(tmp, "g.db")
    pg = ProcGroup(db, source, lease_ms=lease_ms, ack_timeout_ms=lease_ms + 600,
                   tick_ms=tick_ms, clock="manual")
    pg.start_coordinator()
    return pg


def drain(pg, expected_total: int, timeout: float = 60.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        with open_db(pg.db) as c:
            n = c.execute("SELECT COUNT(*) FROM records").fetchone()[0]
        if n >= expected_total:
            return
        time.sleep(0.1)
    raise AssertionError("workers did not drain the source in time")


def export_rows(db_path: str):
    lines = rule(db_path, "export", "--json").stdout.splitlines()
    return json.loads(lines[0])["section"], [json.loads(l) for l in lines[1:]]


def show(db_path: str, pid: int, offset: int) -> dict:
    return json.loads(correct(db_path, "show", str(pid), str(offset),
                              "--json").stdout)


def propose(db_path: str, pid: int, offset: int, cid: str, expected: int,
            record: dict, reason: str = "correction", check: bool = True):
    return correct(db_path, "propose", str(pid), str(offset),
                   "--correction-id", cid,
                   "--expected-revision", str(expected),
                   "--reason", reason,
                   "--record", json.dumps(record, ensure_ascii=False),
                   check=check)


def approve(db_path: str, cid: str, check: bool = True):
    return correct(db_path, "approve", cid, check=check)


def history(db_path: str, pid: int, offset: int) -> list[dict]:
    out = correct(db_path, "history", str(pid), str(offset), "--json").stdout
    return [json.loads(l) for l in out.splitlines()]


def source_record(db_path: str, pid: int, offset: int) -> dict:
    with open_db(db_path) as c:
        row = c.execute(
            "SELECT source_json FROM records WHERE partition_id=? AND offset=?",
            (pid, offset)).fetchone()
    return json.loads(row["source_json"])


def progress_of(db_path: str, pid: int) -> int:
    with open_db(db_path) as c:
        row = c.execute(
            "SELECT committed_offset FROM progress WHERE partition_id=?",
            (pid,)).fetchone()
    return row["committed_offset"] if row else -1


V1_SPEC = {"fields": {
    "name": {"missing": "preserve", "null": "null",
             "steps": [{"op": "nfkc"}, {"op": "strip"},
                       {"op": "collapse_ws"}, {"op": "upper"}]},
    "city": {"missing": "preserve", "null": "preserve",
             "steps": [{"op": "strip"}, {"op": "collapse_ws"},
                       {"op": "title"}]}}}

STRICT_SPEC = {"fields": {
    "amount": {"missing": "reject", "null": "reject",
               "steps": [{"op": "decimal", "scale": 2,
                          "rounding": "HALF_UP"}]}}}


class CorrectionCase(unittest.TestCase):
    """Base: a drained group with two partitions and stopped processes."""

    COUNTS = [8, 6]

    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="pcgcorrect-")
        self.db = os.path.join(self.tmp, "g.db")
        self.src = make_source(self.tmp, self.COUNTS, seed=17)
        self.pg = start_group(self.tmp, self.src)
        self.pg.admin("add", "w1")
        self.pg.start_worker("w1", batch_size=4)
        drain(self.pg, sum(self.COUNTS))
        self.pg.graceful_stop("w1")
        self.pg.stop_coordinator()
        self.pg.procs["__coord__"].wait(timeout=15)
        self.immutables = snapshot_immutables(self.db)

    def tearDown(self):
        self.pg.shutdown_all()

    def assert_collection_untouched(self):
        now = snapshot_immutables(self.db)
        self.assertEqual(now["records"], self.immutables["records"])
        self.assertEqual(now["progress"], self.immutables["progress"])

    def corrected(self, pid: int, offset: int, **changes) -> dict:
        obj = source_record(self.db, pid, offset)
        obj.update(changes)
        return obj


# --------------------------------------------------------------------- basics

class TestCorrectionBasics(CorrectionCase):
    def test_two_corrections_then_restore_keeps_all_revisions(self):
        # two consecutive corrections on the same offset
        r1 = self.corrected(0, 2, city="Berlin")
        propose(self.db, 0, 2, "c-1", 0, r1, reason="city was wrong")
        approve(self.db, "c-1")
        r2 = self.corrected(0, 2, city="Berlin", email="new@example.com")
        propose(self.db, 0, 2, "c-2", 1, r2, reason="email was wrong too")
        approve(self.db, "c-2")
        # undo back to the originally collected object (revision 0)
        correct(self.db, "restore", "0", "2", "--to-revision", "0",
                "--correction-id", "c-3", "--expected-revision", "2",
                "--reason", "upstream says original was right after all")
        approve(self.db, "c-3")

        # every revision is retained, in order, with linked prev/new objects
        hist = history(self.db, 0, 2)
        self.assertEqual([h["kind"] for h in hist],
                         ["collected", "correction", "correction", "correction"])
        self.assertEqual([h["revision"] for h in hist], [0, 1, 2, 3])
        original = hist[0]["object"]
        self.assertEqual(hist[1]["prev"], original)
        self.assertEqual(hist[1]["new"], r1)
        self.assertEqual(hist[2]["prev"], r1)
        self.assertEqual(hist[2]["new"], r2)
        self.assertEqual(hist[3]["prev"], r2)
        self.assertEqual(hist[3]["new"], original)  # restored old value
        self.assertEqual([h["correction_id"] for h in hist[1:]],
                         ["c-1", "c-2", "c-3"])
        for h in hist[1:]:
            self.assertTrue(h["reason"])
            self.assertEqual(h["rule_version"], "legacy")

        # current effective record is the restored original at revision 3
        cur = show(self.db, 0, 2)
        self.assertEqual(cur["current_revision"], 3)
        self.assertEqual(cur["effective"], original)
        self.assertEqual(cur["original"], original)

        # the official read shows the restored value processed by the active
        # (legacy) cleaner, tagged with source revision 3
        section, rows = export_rows(self.db)
        row = next(r for r in rows if r["partition_id"] == 0 and r["offset"] == 2)
        self.assertEqual(row["source_revision"], 3)
        self.assertEqual(row["result"], ref_clean_fixed(original))
        self.assertEqual(section["correction_seq"], 3)

        # collection facts never moved
        self.assert_collection_untouched()
        problems, _ = verify_database(self.db)
        self.assertEqual(problems, [])

    def test_restore_to_intermediate_revision(self):
        r1 = self.corrected(1, 1, city="Alpha")
        r2 = self.corrected(1, 1, city="Beta")
        propose(self.db, 1, 1, "r-1", 0, r1)
        approve(self.db, "r-1")
        propose(self.db, 1, 1, "r-2", 1, r2)
        approve(self.db, "r-2")
        # restore revision 1 (not the original)
        correct(self.db, "restore", "1", "1", "--to-revision", "1",
                "--correction-id", "r-3", "--expected-revision", "2",
                "--reason", "second correction was itself wrong")
        approve(self.db, "r-3")
        cur = show(self.db, 1, 1)
        self.assertEqual(cur["current_revision"], 3)
        self.assertEqual(cur["effective"], r1)
        self.assertEqual(len(history(self.db, 1, 1)), 4)
        problems, _ = verify_database(self.db)
        self.assertEqual(problems, [])

    def test_diagnostics_and_exit_codes(self):
        # unknown coordinates (nothing committed there)
        r = propose(self.db, 0, 999, "x-1", 0, {"a": 1}, check=False)
        self.assertEqual(r.returncode, 10, r.stdout)
        r = correct(self.db, "show", "7", "0", check=False)
        self.assertEqual(r.returncode, 10)
        # non-object replacement JSON
        for bad in ("[1,2]", '"text"', "42", "null", "true"):
            r = propose(self.db, 0, 1, "x-2", 0, bad, check=False)
            self.assertEqual(r.returncode, 11, (bad, r.stdout))
        # illegal reasons
        r = propose(self.db, 0, 1, "x-3", 0, {"a": 1}, reason="", check=False)
        self.assertEqual(r.returncode, 11)
        r = propose(self.db, 0, 1, "x-4", 0, {"a": 1},
                    reason="x" * 501, check=False)
        self.assertEqual(r.returncode, 11)
        # no-change replacement is refused
        same = source_record(self.db, 0, 1)
        r = propose(self.db, 0, 1, "x-5", 0, same, check=False)
        self.assertEqual(r.returncode, 11, r.stdout)
        self.assertIn("identical", r.stdout)
        # unknown correction id
        r = approve(self.db, "no-such-id", check=False)
        self.assertEqual(r.returncode, 9)
        r = correct(self.db, "get", "no-such-id", check=False)
        self.assertEqual(r.returncode, 9)
        r = correct(self.db, "reject", "no-such-id", check=False)
        self.assertEqual(r.returncode, 9)
        # nothing was written by the failures
        with open_db(self.db) as c:
            n = c.execute("SELECT COUNT(*) FROM correction_proposals"
                          ).fetchone()[0]
            m = c.execute("SELECT COUNT(*) FROM source_corrections"
                          ).fetchone()[0]
        self.assertEqual((n, m), (0, 0))
        problems, _ = verify_database(self.db)
        self.assertEqual(problems, [])


# ------------------------------------------------- ids, staleness, concurrency

class TestIdsAndConcurrency(CorrectionCase):
    def test_idempotent_retry_and_conflicting_id(self):
        rec = self.corrected(0, 3, city="Lyon")
        propose(self.db, 0, 3, "id-1", 0, rec)
        # exact retry: same id, same content (even with reordered JSON keys)
        reordered = json.loads(json.dumps(rec, sort_keys=True))
        out = json.loads(correct(
            self.db, "propose", "0", "3", "--correction-id", "id-1",
            "--expected-revision", "0", "--reason", "correction",
            "--record", json.dumps(reordered), "--json").stdout)
        self.assertFalse(out["created"])  # idempotent: no second proposal
        with open_db(self.db) as c:
            n = c.execute("SELECT COUNT(*) FROM correction_proposals"
                          ).fetchone()[0]
        self.assertEqual(n, 1)
        # same id, DIFFERENT content -> conflict (exit 12)
        r = propose(self.db, 0, 3, "id-1", 0,
                    self.corrected(0, 3, city="Nice"), check=False)
        self.assertEqual(r.returncode, 12, r.stdout)
        r = propose(self.db, 0, 4, "id-1", 0,
                    self.corrected(0, 4, city="Nice"), check=False)
        self.assertEqual(r.returncode, 12, r.stdout)
        r = propose(self.db, 0, 3, "id-1", 0, rec,
                    reason="different reason", check=False)
        self.assertEqual(r.returncode, 12, r.stdout)
        # the single proposal is still pending and approvable exactly once
        approve(self.db, "id-1")
        self.assertEqual(show(self.db, 0, 3)["current_revision"], 1)

    def test_stale_expected_revision_at_propose_and_approve(self):
        # propose-time staleness: expected revision is already wrong
        rec = self.corrected(0, 1, city="Turin")
        propose(self.db, 0, 1, "s-1", 0, rec)
        approve(self.db, "s-1")
        r = propose(self.db, 0, 1, "s-2", 0,
                    self.corrected(0, 1, city="Milan"), check=False)
        self.assertEqual(r.returncode, 13, r.stdout)
        self.assertIn("actual_revision=1", r.stdout)
        # approve-time staleness: valid when proposed, overtaken meanwhile
        propose(self.db, 0, 1, "s-3", 1, self.corrected(0, 1, city="Genoa"))
        propose(self.db, 0, 1, "s-4", 1, self.corrected(0, 1, city="Parma"))
        approve(self.db, "s-4")  # wins -> revision 2
        r = approve(self.db, "s-3", check=False)
        self.assertEqual(r.returncode, 13, r.stdout)
        self.assertIn("actual_revision=2", r.stdout)
        # the loser is marked rejected with the actual revision recorded
        loser = json.loads(correct(self.db, "get", "s-3", "--json").stdout)
        self.assertEqual(loser["state"], "rejected")
        self.assertIn("actual 2", loser["decided_note"])
        # exactly the winner's revision exists; effective value is the winner's
        self.assertEqual(show(self.db, 0, 1)["current_revision"], 2)
        self.assertEqual(show(self.db, 0, 1)["effective"]["city"], "Parma")
        self.assertEqual(len(history(self.db, 0, 1)), 3)  # 0 + 2 revisions
        problems, _ = verify_database(self.db)
        self.assertEqual(problems, [])

    def test_two_admin_processes_race_one_winner(self):
        rec = self.corrected(1, 2, city="Porto")
        propose(self.db, 1, 2, "race-1", 0, rec)
        # two independent management processes approve the same proposal
        procs = [subprocess.Popen(
            [sys.executable, "-m", "pcg", "--db", self.db, "--clock", "real",
             "correct", "approve", "race-1"],
            env=ENV, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True) for _ in range(2)]
        results = [p.communicate(timeout=60) for p in procs]
        rcs = sorted(p.returncode for p in procs)
        self.assertEqual(rcs, [0, 13],
                         f"expected one winner one conflict: {results}")
        loser_out = next(out for (out, _), p in zip(results, procs)
                         if p.returncode == 13)
        self.assertIn("already applied", loser_out)
        # exactly one revision, exactly one sequence bump
        self.assertEqual(len(history(self.db, 1, 2)), 2)
        with open_db(self.db) as c:
            seq = c.execute("SELECT value FROM meta WHERE key='correction_seq'"
                            ).fetchone()[0]
        self.assertEqual(int(seq), 1)
        self.assertEqual(show(self.db, 1, 2)["effective"]["city"], "Porto")
        problems, _ = verify_database(self.db)
        self.assertEqual(problems, [])

    def test_reject_flow_and_decided_proposal_conflicts(self):
        rec = self.corrected(0, 5, city="Gent")
        propose(self.db, 0, 5, "j-1", 0, rec)
        correct(self.db, "reject", "j-1", "--note", "upstream dispute")
        prop = json.loads(correct(self.db, "get", "j-1", "--json").stdout)
        self.assertEqual(prop["state"], "rejected")
        self.assertEqual(prop["decided_note"], "upstream dispute")
        # a rejected proposal can no longer be approved or re-rejected
        r = approve(self.db, "j-1", check=False)
        self.assertEqual(r.returncode, 13)
        r = correct(self.db, "reject", "j-1", check=False)
        self.assertEqual(r.returncode, 13)
        # and an applied one cannot be rejected either
        propose(self.db, 0, 5, "j-2", 0, rec)
        approve(self.db, "j-2")
        r = correct(self.db, "reject", "j-2", check=False)
        self.assertEqual(r.returncode, 13)
        r = approve(self.db, "j-2", check=False)
        self.assertEqual(r.returncode, 13)
        self.assertIn("already applied", r.stdout)
        # proposals list shows both states
        lsting = correct(self.db, "list", "--json").stdout.splitlines()
        states = {json.loads(l)["correction_id"]: json.loads(l)["state"]
                  for l in lsting}
        self.assertEqual(states, {"j-1": "rejected", "j-2": "applied"})
        pend = correct(self.db, "list", "--state", "pending",
                       "--json").stdout.splitlines()
        self.assertEqual(pend, [])
        problems, _ = verify_database(self.db)
        self.assertEqual(problems, [])


# ------------------------------------------------- proposals are pre-review only

class TestProposalInvisibility(CorrectionCase):
    def test_pending_proposal_changes_no_official_read(self):
        section_before, rows_before = export_rows(self.db)
        review_before = json.loads(
            rule(self.db, "review", "legacy", "--json").stdout)
        recompute_before = rule(self.db, "recompute", "legacy").stdout

        rec = self.corrected(0, 0, city="Invisible")
        propose(self.db, 0, 0, "p-1", 0, rec)

        # export is byte-identical while the proposal awaits review
        section_after, rows_after = export_rows(self.db)
        self.assertEqual(section_before, section_after)
        self.assertEqual(rows_before, rows_after)
        # a fresh review sees the same stats (the proposal is not a source)
        review_after = json.loads(
            rule(self.db, "review", "legacy", "--json").stdout)
        self.assertEqual(review_before["stats"], review_after["stats"])
        self.assertEqual(review_before["correction_seq"],
                         review_after["correction_seq"])
        # recompute has nothing new to do
        self.assertEqual(recompute_before, rule(self.db, "recompute", "legacy").stdout)
        # the proposal itself is visible through the correction views only
        self.assertEqual(json.loads(correct(self.db, "get", "p-1",
                                            "--json").stdout)["state"], "pending")
        self.assertEqual(show(self.db, 0, 0)["current_revision"], 0)

        # after approval the export DOES change at exactly that position
        # (ignoring the correction_seq tag, which moves for every row)
        approve(self.db, "p-1")
        _, rows_final = export_rows(self.db)

        def strip(rows):
            return [{k: v for k, v in r.items() if k != "correction_seq"}
                    for r in rows]
        diffs = [(r["partition_id"], r["offset"]) for r, b in
                 zip(strip(rows_final), strip(rows_before)) if r != b]
        self.assertEqual(diffs, [(0, 0)])
        problems, _ = verify_database(self.db)
        self.assertEqual(problems, [])


# ------------------------------------------------------- effect verification

class TestEffectVerification(CorrectionCase):
    def test_effect_lands_and_collection_facts_stay(self):
        before = snapshot_immutables(self.db)
        rec = self.corrected(1, 4, city="Quito", amount="999.95")
        propose(self.db, 1, 4, "e-1", 0, rec, reason="city+amount fix")
        out = json.loads(correct(self.db, "approve", "e-1", "--json").stdout)
        self.assertEqual(out["outcome"], "ok")
        self.assertEqual(out["rule_version"], "legacy")
        self.assertEqual(out["revision"], 1)

        # original fields and clean_json untouched; progress untouched
        self.assertEqual(snapshot_immutables(self.db)["records"],
                         before["records"])
        self.assertEqual(snapshot_immutables(self.db)["progress"],
                         before["progress"])
        # revision history
        hist = history(self.db, 1, 4)
        self.assertEqual(len(hist), 2)
        self.assertEqual(hist[1]["new"], rec)
        # active rule result equals the independent fixed-cleaner reference
        cur = show(self.db, 1, 4)
        self.assertEqual(cur["active_result"]["status"], "ok")
        self.assertEqual(cur["active_result"]["result"], ref_clean_fixed(rec))
        self.assertEqual(cur["active_result"]["source_revision"], 1)
        # export row agrees
        _, rows = export_rows(self.db)
        row = next(r for r in rows
                   if r["partition_id"] == 1 and r["offset"] == 4)
        self.assertEqual(row["result"], ref_clean_fixed(rec))
        problems, _ = verify_database(self.db)
        self.assertEqual(problems, [])

    def test_rejection_diagnostic_lands_as_official_result(self):
        # hand-written source whose rows all pass the strict rule, so the
        # rule can be activated before the correction introduces a violation
        self.pg.shutdown_all()  # replace the generated-source group
        tmp = tempfile.mkdtemp(prefix="pcgrejcorr-")
        self.db = os.path.join(tmp, "g.db")
        rows = [
            {"name": "ok one", "amount": "12.50"},
            {"name": "ok two", "amount": "1.25"},
            {"name": "ok three", "amount": "7.255"},
        ]
        src = os.path.join(tmp, "src")
        os.makedirs(src)
        with open(os.path.join(src, "p0.jsonl"), "w") as fh:
            for off, obj in enumerate(rows):
                fh.write(json.dumps(dict(obj, offset=off)) + "\n")
        with open(os.path.join(src, "manifest.json"), "w") as fh:
            json.dump({"partitions": 1, "counts": {"0": 3},
                       "immutable": True}, fh)
        pg = ProcGroup(self.db, src, lease_ms=6000, ack_timeout_ms=6600,
                       tick_ms=50, clock="manual")
        pg.start_coordinator()
        self.pg = pg
        pg.admin("add", "w1")
        pg.start_worker("w1", batch_size=2)
        drain(pg, 3)
        pg.graceful_stop("w1")
        pg.stop_coordinator()
        pg.procs["__coord__"].wait(timeout=15)
        self.immutables = snapshot_immutables(self.db)
        self.COUNTS = [3]

        # publish a strict rule first, then correct a record into a violation
        rule(self.db, "register", "strict",
             write_rule(self.tmp, "strict", STRICT_SPEC))
        rule(self.db, "recompute", "strict", "--page-size", "5")
        token = json.loads(rule(self.db, "review", "strict",
                                "--json").stdout)["token"]
        rule(self.db, "activate", token)

        rec = self.corrected(0, 1, amount="not-a-number")
        propose(self.db, 0, 1, "rej-1", 0, rec, reason="bad amount upstream")
        out = json.loads(correct(self.db, "approve", "rej-1", "--json").stdout)
        self.assertEqual(out["outcome"], "rejected")

        # the official read shows the explicit rejection at that position
        _, rows = export_rows(self.db)
        row = next(r for r in rows
                   if r["partition_id"] == 0 and r["offset"] == 1)
        self.assertEqual(row["status"], "rejected")
        self.assertEqual(row["source_revision"], 1)
        self.assertEqual(row["error"]["type"], "rule_reject")
        self.assertEqual(row["error"]["field"], "amount")
        self.assertEqual(row["error"]["op"], "decimal")
        self.assertEqual(row["error"]["version_id"], "strict")
        # collection progress and the raw record are unchanged
        self.assert_collection_untouched()
        self.assertEqual(progress_of(self.db, 0), self.COUNTS[0] - 1)
        # correction history is intact and the audit agrees
        self.assertEqual(len(history(self.db, 0, 1)), 2)
        problems, stats = verify_database(self.db)
        self.assertEqual(problems, [])
        self.assertEqual(stats["strict"]["rejected"], 1)

        # correcting it again to a valid value clears the rejection
        rec2 = self.corrected(0, 1, amount="12.5")
        propose(self.db, 0, 1, "rej-2", 1, rec2, reason="fixed the amount")
        approve(self.db, "rej-2")
        _, rows = export_rows(self.db)
        row = next(r for r in rows
                   if r["partition_id"] == 0 and r["offset"] == 1)
        self.assertEqual(row["status"], "ok")
        self.assertEqual(row["result"]["amount"], "12.50")
        problems, _ = verify_database(self.db)
        self.assertEqual(problems, [])


# ------------------------------------------------------- rule-flow interaction

class TestRuleInteraction(CorrectionCase):
    def _publish(self, version: str, spec: dict):
        rule(self.db, "register", version,
             write_rule(self.tmp, version, spec))
        rule(self.db, "recompute", version, "--page-size", "5")
        token = json.loads(rule(self.db, "review", version,
                                "--json").stdout)["token"]
        rule(self.db, "activate", token)

    def test_token_dies_after_correction_even_if_undone(self):
        rule(self.db, "register", "v1", write_rule(self.tmp, "v1", V1_SPEC))
        rule(self.db, "recompute", "v1", "--page-size", "5")
        token = json.loads(rule(self.db, "review", "v1",
                                "--json").stdout)["token"]
        # a correction takes effect after the review...
        rec = self.corrected(0, 0, city="Tmp")
        propose(self.db, 0, 0, "t-1", 0, rec)
        approve(self.db, "t-1")
        # ...and is then undone back to the original value
        correct(self.db, "restore", "0", "0", "--to-revision", "0",
                "--correction-id", "t-2", "--expected-revision", "1",
                "--reason", "undo")
        approve(self.db, "t-2")
        # the effective objects are exactly what the review saw, yet the
        # token is dead: the correction sequence moved twice
        self.assertEqual(show(self.db, 0, 0)["effective"],
                         source_record(self.db, 0, 0))
        r = rule(self.db, "activate", token, check=False)
        self.assertEqual(r.returncode, 6, r.stdout)
        self.assertIn("correction", r.stdout)
        # the candidate must catch up with the corrected positions, then a
        # fresh review + activate works
        rule(self.db, "recompute", "v1", "--page-size", "5")
        token2 = json.loads(rule(self.db, "review", "v1",
                                 "--json").stdout)["token"]
        rule(self.db, "activate", token2)
        problems, _ = verify_database(self.db)
        self.assertEqual(problems, [])

    def test_inactive_version_must_catchup_after_correction(self):
        self._publish("v1", V1_SPEC)
        # switch to a second calibre; v1 becomes inactive
        v2 = {"fields": {"name": {"missing": "preserve", "null": "null",
              "steps": [{"op": "lower"}]}}}
        self._publish("v2", v2)
        # a correction lands while v1 is inactive (only v2 is kept current)
        rec = self.corrected(1, 0, name="  corrected   name ", city="Lille")
        propose(self.db, 1, 0, "i-1", 0, rec)
        approve(self.db, "i-1")
        # v1's stored result at that position is now stale: a stale review
        # token for v1 cannot be activated, and a fresh review marks the
        # position stale
        review = json.loads(rule(self.db, "review", "v1", "--json").stdout)
        self.assertGreaterEqual(review["stats"]["stale"], 1)
        r = rule(self.db, "activate", review["token"], check=False)
        self.assertEqual(r.returncode, 6, r.stdout)
        self.assertIn("stale", r.stdout)
        # catch-up recomputes exactly the corrected position(s) from the
        # CURRENT effective objects, then review+activate succeeds
        out = rule(self.db, "recompute", "v1", "--page-size", "4").stdout
        self.assertIn("refreshed=1", out)
        token = json.loads(rule(self.db, "review", "v1",
                                "--json").stdout)["token"]
        rule(self.db, "activate", token)
        # the rolled-back version now serves the corrected record
        _, rows = export_rows(self.db)
        row = next(r for r in rows
                   if r["partition_id"] == 1 and r["offset"] == 0)
        self.assertEqual(row["version_id"], "v1")
        self.assertEqual(row["source_revision"], 1)
        spec = V1_SPEC
        status, payload = ref_outcome(spec, rec)
        self.assertEqual(status, "ok")
        self.assertEqual(json.loads(payload), row["result"])
        problems, _ = verify_database(self.db)
        self.assertEqual(problems, [])

    def test_candidate_recompute_uses_effective_objects(self):
        # corrections exist BEFORE the candidate is even registered
        rec = self.corrected(0, 3, name="corrected person", city="Nice")
        propose(self.db, 0, 3, "u-1", 0, rec)
        approve(self.db, "u-1")
        rule(self.db, "register", "v1", write_rule(self.tmp, "v1", V1_SPEC))
        rule(self.db, "recompute", "v1", "--page-size", "4")
        # the candidate's result at the corrected position comes from the
        # corrected object, not the original source_json
        with open_db(self.db) as c:
            row = c.execute(
                "SELECT result_json, source_revision FROM rule_results "
                "WHERE version_id='v1' AND partition_id=0 AND offset=3"
            ).fetchone()
        self.assertEqual(row["source_revision"], 1)
        status, payload = ref_outcome(V1_SPEC, rec)
        self.assertEqual(status, "ok")
        self.assertEqual(row["result_json"], payload)
        # publish it and confirm the official read agrees
        token = json.loads(rule(self.db, "review", "v1",
                                "--json").stdout)["token"]
        rule(self.db, "activate", token)
        _, rows = export_rows(self.db)
        erow = next(r for r in rows
                    if r["partition_id"] == 0 and r["offset"] == 3)
        self.assertEqual(erow["result"], json.loads(payload))
        problems, _ = verify_database(self.db)
        self.assertEqual(problems, [])


# ------------------------------------------------------- export consistency

class TestExportConsistency(CorrectionCase):
    def test_export_snapshot_held_across_approval(self):
        # deterministic in-process proof: an export snapshot opened before an
        # approval keeps reading the pre-approval revision set
        from pcg.clock import Clock
        from pcg.correct_store import approve as store_approve
        from pcg.correct_store import propose as store_propose
        from pcg.db import connect
        from pcg.rule_export import export_rows as gen_rows

        conn = connect(self.db)
        gen = gen_rows(conn)
        first = next(gen)
        self.assertEqual(first["correction_seq"], 0)

        admin = connect(self.db)
        clock = Clock(admin, "real")
        rec = self.corrected(0, 0, city="MidExport")
        store_propose(admin, 0, 0, correction_id="m-1", expected_revision=0,
                      new_obj=rec, reason="mid-export correction",
                      now_ms=clock.now_ms())
        admin.commit()
        out = store_approve(admin, "m-1", clock.now_ms())
        self.assertEqual(out["correction_seq"], 1)
        admin.close()

        rest = list(gen)
        conn.close()
        all_rows = [first] + rest
        # the whole export stayed in the pre-approval snapshot
        self.assertEqual({r["correction_seq"] for r in all_rows}, {0})
        self.assertEqual({r["source_revision"] for r in all_rows}, {0})
        row = next(r for r in all_rows
                   if r["partition_id"] == 0 and r["offset"] == 0)
        self.assertEqual(row["result"],
                         ref_clean_fixed(source_record(self.db, 0, 0)))
        # a NEW export sees the correction
        _, rows2 = export_rows(self.db)
        row2 = next(r for r in rows2
                    if r["partition_id"] == 0 and r["offset"] == 0)
        self.assertEqual(row2["source_revision"], 1)
        problems, _ = verify_database(self.db)
        self.assertEqual(problems, [])

    def test_exports_during_concurrent_approvals_and_publishes(self):
        rule(self.db, "register", "v1", write_rule(self.tmp, "v1", V1_SPEC))
        v2 = {"fields": {"name": {"missing": "preserve", "null": "null",
              "steps": [{"op": "lower"}]}}}
        rule(self.db, "register", "v2", write_rule(self.tmp, "v2", v2))
        rule(self.db, "recompute", "v1", "--page-size", "10")
        rule(self.db, "recompute", "v2", "--page-size", "10")

        stop = os.path.join(self.tmp, "stop")
        # publisher process: alternates versions via fresh reviews
        publisher = os.path.join(self.tmp, "publisher.py")
        with open(publisher, "w") as fh:
            fh.write(
                "import json,os,subprocess,sys,time\n"
                f"DB={self.db!r};ENV=dict(os.environ,PYTHONPATH={SRC!r})\n"
                f"STOP={stop!r}\n"
                "def pcg(*a):\n"
                "    return subprocess.run([sys.executable,'-m','pcg','--db',"
                "DB,'--clock','real','rule',*a],env=ENV,capture_output=True,"
                "text=True)\n"
                "i=0\n"
                "while not os.path.exists(STOP):\n"
                "    vid=['v1','v2','legacy'][i%3];i+=1\n"
                "    r=pcg('review',vid,'--json')\n"
                "    if r.returncode: time.sleep(0.02); continue\n"
                "    pcg('activate',json.loads(r.stdout)['token'])\n"
                "    time.sleep(0.03)\n")
        # corrector process: effects a bounded series of corrections
        corrector = os.path.join(self.tmp, "corrector.py")
        positions = [(0, 0), (1, 1), (0, 3), (1, 4), (0, 5), (1, 0)]
        with open(corrector, "w") as fh:
            fh.write(
                "import json,os,subprocess,sys,time\n"
                f"DB={self.db!r};ENV=dict(os.environ,PYTHONPATH={SRC!r})\n"
                f"POS={positions!r}\n"
                "def pcg(*a):\n"
                "    return subprocess.run([sys.executable,'-m','pcg','--db',"
                "DB,'--clock','real','correct',*a],env=ENV,"
                "capture_output=True,text=True)\n"
                "n=0\n"
                "for i,(p,o) in enumerate(POS):\n"
                "    s=json.loads(pcg('show',str(p),str(o),'--json').stdout)\n"
                "    rec=dict(s['effective']);rec['city']=f'City{i}'\n"
                "    cid=f'cc-{i}'\n"
                "    r=pcg('propose',str(p),str(o),'--correction-id',cid,\n"
                "          '--expected-revision',str(s['current_revision']),\n"
                "          '--reason','bulk','--record',json.dumps(rec))\n"
                "    if r.returncode: continue\n"
                "    a=pcg('approve',cid)\n"
                "    n+= a.returncode==0\n"
                "print(f'effected={n}')\n")
        pub = subprocess.Popen([sys.executable, "-I", publisher], env=ENV)
        cor = subprocess.run([sys.executable, "-I", corrector], env=ENV,
                             capture_output=True, text=True, timeout=300)
        self.assertEqual(cor.returncode, 0, cor.stderr + cor.stdout)
        self.assertIn("effected=6", cor.stdout)
        try:
            for round_no in range(6):
                out = os.path.join(self.tmp, f"exp-{round_no}.jsonl")
                with open(out, "w") as fh:
                    exp = subprocess.run(
                        [sys.executable, "-m", "pcg", "--db", self.db,
                         "--clock", "real", "rule", "export", "--json"],
                        env=ENV, stdout=fh, stderr=subprocess.PIPE, text=True)
                self.assertEqual(exp.returncode, 0, exp.stderr)
                with open(out) as fh:
                    lines = fh.read().splitlines()
                section = json.loads(lines[0])["section"]
                rows = [json.loads(l) for l in lines[1:]]
                # one version, one publish seq, one correction seq per export
                self.assertEqual({r["version_id"] for r in rows},
                                 {section["version_id"]})
                self.assertEqual({r["publish_seq"] for r in rows},
                                 {section["publish_seq"]})
                self.assertEqual({r["correction_seq"] for r in rows},
                                 {section["correction_seq"]})
                self.assertEqual(len(rows), section["positions"])
                pos = [(r["partition_id"], r["offset"]) for r in rows]
                self.assertEqual(pos, sorted(pos))
                self.assertEqual(len(set(pos)), len(pos))  # no duplicates
                # the active version never exports a stale row
                self.assertTrue(all("stale" not in r for r in rows))
        finally:
            open(stop, "w").close()
            pub.wait(timeout=30)
        problems, _ = verify_database(self.db)
        self.assertEqual(problems, [])


# ------------------------------------------------- worker barrier interaction

class TestBarrierCorrection(unittest.TestCase):
    def test_stale_batch_cannot_overwrite_correction(self):
        tmp = tempfile.mkdtemp(prefix="pcgbarrier-")
        db = os.path.join(tmp, "g.db")
        src = make_source(tmp, [8, 8], seed=23)
        pg = ProcGroup(db, src, lease_ms=6000, ack_timeout_ms=3000,
                       tick_ms=50, clock="manual")
        pg.start_coordinator()
        try:
            pg.admin("add", "aa", "--lease-ms", "6000")
            pg.admin("add", "zz", "--lease-ms", "6000")
            pg.start_worker("aa", batch_size=4)
            pg.wait_registered("aa")
            # zz parks after computing p1 offsets 0..3, before the commit
            pg.start_worker("zz", batch_size=4, barrier="zz:1:0")
            pg.wait_registered("zz")
            pg.wait_barrier_reached("zz:p1:o0")
            self.assertIsNone(pg.sql_one(
                "SELECT committed_offset FROM progress WHERE partition_id=1"))
            old = pg.sql_one(
                "SELECT gen FROM assignments WHERE partition_id=1 "
                "ORDER BY gen DESC LIMIT 1")

            # ownership of p1 moves to aa; zz is parked and cannot ack, so
            # its generation is fenced and aa commits p1 from the source
            pg.admin("remove", "zz")
            pg.wait_for(lambda: pg.sql_one(
                "SELECT 1 FROM assignments WHERE partition_id=1 AND gen=? "
                "AND state='fenced'", (old["gen"],)), timeout=20)
            pg.wait_committed(1, 7, timeout=30)
            owner = pg.sql_one(
                "SELECT worker_id FROM progress WHERE partition_id=1")
            self.assertEqual(owner["worker_id"], "aa")

            # approve a correction at p1 offset 2 - inside the range of zz's
            # parked, already-computed batch - while zz is still parked
            original = json.loads(pg.sql_one(
                "SELECT source_json FROM records WHERE partition_id=1 "
                "AND offset=2")["source_json"])
            rec = dict(original, city="Correctedville")
            correct(db, "propose", "1", "2", "--correction-id", "b-1",
                    "--expected-revision", "0", "--reason", "city fix",
                    "--record", json.dumps(rec))
            correct(db, "approve", "b-1")
            self.assertEqual(show(db, 1, 2)["current_revision"], 1)

            # release the parked worker: its stale batch must be rejected by
            # the fence and must not disturb the correction
            pg.release_barrier("zz:p1:o0")
            rej = pg.wait_for(lambda: pg.sql_one(
                "SELECT * FROM rejected_commits WHERE partition_id=1 "
                "AND gen=?", (old["gen"],)), timeout=20)
            self.assertEqual((rej["start_offset"], rej["end_offset"]), (0, 3))
            pg.graceful_stop("zz")
            pg.wait_committed(0, 7, timeout=30)

            # the correction survived: still revision 1, no new revision with
            # the old value, effective object still the corrected one
            hist = history(db, 1, 2)
            self.assertEqual(len(hist), 2)  # collected + exactly 1 revision
            self.assertEqual(hist[1]["new"], rec)
            cur = show(db, 1, 2)
            self.assertEqual(cur["current_revision"], 1)
            self.assertEqual(cur["effective"], rec)
            self.assertEqual(cur["original"], original)
            # the official read at that position is the corrected outcome
            _, rows = export_rows(db)
            row = next(r for r in rows
                       if r["partition_id"] == 1 and r["offset"] == 2)
            self.assertEqual(row["source_revision"], 1)
            self.assertEqual(row["result"], ref_clean_fixed(rec))
            # collection facts are exactly what aa committed
            self.assertEqual(progress_of(db, 1), 7)
            gens = {r["gen"] for r in pg.sql(
                "SELECT DISTINCT gen FROM records WHERE partition_id=1")}
            self.assertNotIn(old["gen"], gens)
            problems, _ = verify_database(db)
            self.assertEqual(problems, [])
        finally:
            pg.shutdown_all()


# ------------------------------------------------- skewed history, bounded mem

class TestRecomputePauseCorrection(unittest.TestCase):
    def test_correction_during_pause_is_not_born_stale(self):
        # a recompute page that was COMPUTED before a correction must not be
        # flushed with the pre-correction revision: the flush revalidates
        tmp = tempfile.mkdtemp(prefix="pcgpausecorr-")
        db = os.path.join(tmp, "g.db")
        src = make_source(tmp, [30], seed=33)
        pg = start_group(tmp, src)
        try:
            pg.admin("add", "w1")
            pg.start_worker("w1", batch_size=10)
            drain(pg, 30)
            rule(db, "register", "v1", write_rule(tmp, "v1", V1_SPEC))
            pause = os.path.join(tmp, "PAUSE")
            open(pause, "w").close()
            parked = subprocess.Popen(
                [sys.executable, "-m", "pcg", "--db", db, "--clock", "real",
                 "rule", "recompute", "v1", "--page-size", "10",
                 "--pause-file", pause, "--pause-wait-s", "60"],
                env=ENV, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                text=True)
            # wait until the first page is computed but provably not flushed
            deadline = time.monotonic() + 30
            while time.monotonic() < deadline:
                with open_db(db) as c:
                    n = c.execute("SELECT COUNT(*) FROM rule_results "
                                  "WHERE version_id='v1'").fetchone()[0]
                if parked.poll() is None and n == 0:
                    time.sleep(0.3)
                    with open_db(db) as c:
                        still = c.execute(
                            "SELECT COUNT(*) FROM rule_results "
                            "WHERE version_id='v1'").fetchone()[0]
                    if parked.poll() is None and still == 0:
                        break
                time.sleep(0.1)
            self.assertIsNone(parked.poll(), "recompute should be parked")
            # correct a position inside the parked page's range (0..9)
            cur = show(db, 0, 3)
            rec = dict(cur["effective"], city="Pauseville")
            propose(db, 0, 3, "pp-1", 0, rec)
            approve(db, "pp-1")
            os.remove(pause)
            out, _ = parked.communicate(timeout=60)
            self.assertEqual(parked.returncode, 0, out)
            self.assertIn("revalidated=1", out)
            # the flushed page carries the correction: no stale row was born
            with open_db(db) as c:
                row = c.execute(
                    "SELECT result_json, source_revision FROM rule_results "
                    "WHERE version_id='v1' AND partition_id=0 AND offset=3"
                ).fetchone()
            self.assertEqual(row["source_revision"], 1)
            status, payload = ref_outcome(V1_SPEC, rec)
            self.assertEqual(status, "ok")
            self.assertEqual(row["result_json"], payload)
            problems, _ = verify_database(db)
            self.assertEqual(problems, [])
        finally:
            pg.shutdown_all()


class TestSkewedHistoryBounded(unittest.TestCase):
    def test_revision_queries_and_catchup_bounded(self):
        tmp = tempfile.mkdtemp(prefix="pcgskew-")
        db = os.path.join(tmp, "g.db")
        src = make_source(tmp, [0, 4000, 0, 600], seed=71)
        pg = start_group(tmp, src)
        try:
            pg.admin("add", "w1")
            pg.admin("add", "w2")
            pg.start_worker("w1", batch_size=200)
            pg.start_worker("w2", batch_size=200)
            drain(pg, 4600)
            pg.graceful_stop("w1")
            pg.graceful_stop("w2")
            pg.stop_coordinator()
            pg.procs["__coord__"].wait(timeout=15)

            # sprinkle corrections: repeated ones on hot positions, undos,
            # and positions across the skewed partitions
            cid = 0
            for pid, off in [(1, 0), (1, 1), (1, 3999), (3, 0), (3, 599),
                             (1, 1000), (1, 2000), (3, 300)]:
                cur = show(db, pid, off)
                rec = dict(cur["effective"], city=f"Skew{cid}")
                cid += 1
                propose(db, pid, off, f"k-{cid}", cur["current_revision"], rec)
                approve(db, f"k-{cid}")
            # a second correction + an undo on one hot position
            cur = show(db, 1, 0)
            rec = dict(cur["effective"], city="SkewAgain")
            propose(db, 1, 0, "k-x", cur["current_revision"], rec)
            approve(db, "k-x")
            correct(db, "restore", "1", "0", "--to-revision", "0",
                    "--correction-id", "k-y", "--expected-revision", "2",
                    "--reason", "undo")
            approve(db, "k-y")
            self.assertEqual(show(db, 1, 0)["current_revision"], 3)
            self.assertEqual(len(history(db, 1, 0)), 4)

            # candidate recompute over the skewed, corrected history under a
            # tight address-space cap: bounded memory, paged catch-up
            rule(db, "register", "v1", write_rule(tmp, "v1", V1_SPEC))
            runner = f"""
import resource, subprocess, sys
def pre():
    resource.setrlimit(resource.RLIMIT_AS, (160*1024*1024, 160*1024*1024))
p = subprocess.Popen([sys.executable,'-m','pcg','--db',{db!r},'--clock','real',
    'rule','recompute','v1','--page-size','50'],
    env={ENV!r}, preexec_fn=pre, stdout=subprocess.PIPE, text=True)
out,_ = p.communicate()
print(out)
sys.exit(p.returncode)
"""
            r = subprocess.run([sys.executable, "-I", "-c", runner],
                               capture_output=True, text=True)
            self.assertEqual(r.returncode, 0, r.stderr + r.stdout)
            self.assertIn("complete=yes", r.stdout)
            self.assertIn("refreshed=0", r.stdout)  # v1 is new: phase A only
            with open_db(db) as c:
                self.assertEqual(c.execute(
                    "SELECT COUNT(*) FROM rule_results WHERE version_id='v1'"
                ).fetchone()[0], 4600)
                corrected = c.execute(
                    "SELECT COUNT(*) FROM rule_results WHERE version_id='v1' "
                    "AND source_revision > 0").fetchone()[0]
                hot_rev = c.execute(
                    "SELECT source_revision FROM rule_results WHERE "
                    "version_id='v1' AND partition_id=1 AND offset=0"
                ).fetchone()[0]
            self.assertEqual(corrected, 8)  # one result row per position
            self.assertEqual(hot_rev, 3)    # head revision of the hot spot
            # legacy (active) was kept current by the approvals themselves
            problems, _ = verify_database(db)
            self.assertEqual(problems, [])
            # publish v1 to prove the full catch-up contract on skewed data
            token = json.loads(rule(db, "review", "v1",
                                    "--json").stdout)["token"]
            rule(db, "activate", token)
            _, rows = export_rows(db)
            self.assertEqual(len(rows), 4600)
            hot = next(r for r in rows
                       if r["partition_id"] == 1 and r["offset"] == 0)
            self.assertEqual(hot["source_revision"], 3)
            self.assertEqual(hot["result"],
                             ref_outcome(V1_SPEC, show(db, 1, 0)["effective"])[1]
                             and json.loads(ref_outcome(
                                 V1_SPEC, show(db, 1, 0)["effective"])[1]))
            problems, _ = verify_database(db)
            self.assertEqual(problems, [])
        finally:
            pg.shutdown_all()


# ------------------------------------------------------------------- upgrade

class TestUpgrade(unittest.TestCase):
    def test_old_database_upgrade_is_safe_and_repeatable(self):
        # simulate a pre-correction database: old schema (no correction
        # tables, no source_revision / correction_seq columns) with committed
        # rows, then run the normal CLI upgrade path over it
        tmp = tempfile.mkdtemp(prefix="pcgupgrade-")
        db = os.path.join(tmp, "g.db")
        conn = sqlite3.connect(db)
        conn.executescript("""
        PRAGMA journal_mode=WAL;
        CREATE TABLE meta(key TEXT PRIMARY KEY, value TEXT NOT NULL);
        CREATE TABLE records(
          partition_id INTEGER NOT NULL, offset INTEGER NOT NULL,
          gen INTEGER NOT NULL, worker_id TEXT NOT NULL,
          source_json TEXT NOT NULL, clean_json TEXT NOT NULL,
          committed_at INTEGER NOT NULL,
          PRIMARY KEY(partition_id, offset)) WITHOUT ROWID;
        CREATE TABLE progress(
          partition_id INTEGER PRIMARY KEY, committed_offset INTEGER NOT NULL,
          gen INTEGER NOT NULL, worker_id TEXT NOT NULL,
          updated_at INTEGER NOT NULL);
        CREATE TABLE partitions(
          partition_id INTEGER PRIMARY KEY, first_seen_at INTEGER NOT NULL);
        CREATE TABLE rule_versions(
          version_id TEXT PRIMARY KEY, spec_json TEXT NOT NULL,
          spec_sha256 TEXT NOT NULL, builtin INTEGER NOT NULL DEFAULT 0,
          created_at INTEGER NOT NULL);
        CREATE TABLE rule_state(
          id INTEGER PRIMARY KEY CHECK(id = 1),
          active_version TEXT NOT NULL, publish_seq INTEGER NOT NULL,
          updated_at INTEGER NOT NULL);
        CREATE TABLE rule_publishes(
          seq INTEGER PRIMARY KEY, version_id TEXT NOT NULL,
          at_ms INTEGER NOT NULL, reason TEXT NOT NULL);
        CREATE TABLE rule_results(
          version_id TEXT NOT NULL, partition_id INTEGER NOT NULL,
          offset INTEGER NOT NULL,
          status TEXT NOT NULL CHECK(status IN ('ok','rejected')),
          result_json TEXT, error_json TEXT, computed_at INTEGER NOT NULL,
          PRIMARY KEY(version_id, partition_id, offset)) WITHOUT ROWID;
        CREATE TABLE rule_recompute_progress(
          version_id TEXT NOT NULL, partition_id INTEGER NOT NULL,
          done_offset INTEGER NOT NULL, updated_at INTEGER NOT NULL,
          PRIMARY KEY(version_id, partition_id)) WITHOUT ROWID;
        CREATE TABLE rule_reviews(
          token TEXT PRIMARY KEY, version_id TEXT NOT NULL,
          base_version TEXT NOT NULL, spec_sha256 TEXT NOT NULL,
          offset_vector_json TEXT NOT NULL, publish_seq INTEGER NOT NULL,
          stats_json TEXT NOT NULL, created_at INTEGER NOT NULL);
        CREATE TABLE rule_review_items(
          token TEXT NOT NULL, partition_id INTEGER NOT NULL,
          offset INTEGER NOT NULL, kind TEXT NOT NULL,
          detail_json TEXT NOT NULL,
          PRIMARY KEY(token, partition_id, offset)) WITHOUT ROWID;
        """)
        # historical rows as an old database would store them
        rows = [
            (0, 0, '{"name":" alice ","city":"PARIS","amount":" 10.5 "}'),
            (0, 1, '{"name":"bob","city":"new_york","amount":"bad"}'),
            (1, 0, '{"name":"cid","email":"A@B.CD"}'),
        ]
        for pid, off, src_json in rows:
            obj = json.loads(src_json)
            conn.execute(
                "INSERT INTO records VALUES(?,?,?,?,?,?,?)",
                (pid, off, 1, "w-old", src_json,
                 json.dumps(ref_clean_fixed(obj), sort_keys=True,
                            separators=(",", ":"), ensure_ascii=False), 1000))
        conn.execute("INSERT INTO progress VALUES(0,1,1,'w-old',1000)")
        conn.execute("INSERT INTO progress VALUES(1,0,1,'w-old',1000)")
        conn.commit()
        conn.close()

        def dump():
            with open_db(db) as c:
                recs = [tuple(r) for r in c.execute(
                    "SELECT * FROM records ORDER BY partition_id, offset")]
                res = [tuple(r) for r in c.execute(
                    "SELECT * FROM rule_results ORDER BY version_id,"
                    "partition_id, offset")]
            return recs, res

        # first command runs the upgrade; repeat it twice more
        for _ in range(3):
            rule(db, "status")
        recs, res = dump()
        with open_db(db) as c:
            cols_results = {r["name"] for r in c.execute(
                "PRAGMA table_info(rule_results)")}
            cols_reviews = {r["name"] for r in c.execute(
                "PRAGMA table_info(rule_reviews)")}
            seq = c.execute(
                "SELECT value FROM meta WHERE key='correction_seq'"
            ).fetchone()[0]
            n_corr = c.execute(
                "SELECT COUNT(*) FROM source_corrections").fetchone()[0]
        self.assertIn("source_revision", cols_results)
        self.assertIn("correction_seq", cols_reviews)
        self.assertEqual(int(seq), 0)
        self.assertEqual(n_corr, 0)
        # historical raw and cleaned values are byte-identical after upgrade
        self.assertEqual([r[4] for r in recs], [r[2] for r in rows])
        legacy = [r for r in res if r[0] == "legacy"]
        self.assertEqual(len(legacy), 3)
        for r in legacy:
            self.assertEqual(r[-1], 0)  # source_revision defaults to 0
        # repeating the upgrade changed nothing
        rule(db, "status")
        self.assertEqual(dump(), (recs, res))
        # the upgraded database supports the full correction workflow
        propose(db, 0, 0, "up-1", 0,
                {"name": " alice ", "city": "Lyon", "amount": " 10.5 "})
        approve(db, "up-1")
        self.assertEqual(show(db, 0, 0)["current_revision"], 1)
        problems, _ = verify_database(db)
        self.assertEqual(problems, [])


if __name__ == "__main__":
    unittest.main(verbosity=2)
