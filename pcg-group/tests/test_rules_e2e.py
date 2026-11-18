"""End-to-end acceptance tests for the cleaning-rule publishing workflow.

Every multi-process scenario uses REAL OS processes (coordinator, workers and
the ``pcg rule`` management commands), coordinated only through the shared
SQLite database.  Value/diagnostic verification goes through
:mod:`tests.tools_rule_ref`, an independent re-implementation that imports
none of the production cleaning code.

Coverage (mapped to the task requirements):

* contract semantics  - hand-computed text/decimal cases, JSON types,
                        missing/null policies, validation, identity hashing,
                        precision-independence, source-dir independence;
* upgrade             - empty and populated databases, repeatable, legacy
                        values byte-identical to historical clean_json;
* publish gate        - rejections and uncovered positions block activation;
* pause + ingestion   - recompute parked after one computed, unflushed page
                        while workers keep committing and reads keep working;
* SIGKILL resume      - real kill of the recompute process, per-position
                        independent verification, no rescanning of history;
* review tokens       - new commits invalidate a token even after catch-up;
                        switch-away-and-back invalidates the old token;
* in-flight switch    - a batch computed before the switch commits under the
                        new version (fence-time version selection);
* rollback catch-up   - legacy recomputed over rows received while inactive;
* export consistency  - concurrent real-process publish never mixes versions
                        inside one export; concurrent activations serialise;
* active rejection    - raw row + progress + diagnostic land together and the
                        official read shows an explicit REJECTED row;
* long skewed history - paged catch-up with bounded memory;
* review immutability - later ingestion/recompute cannot change a review.
"""

from __future__ import annotations

import json
import os
import signal
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
from tools_rule_ref import ref_outcome, verify_database, snapshot_immutables  # noqa: E402


# --------------------------------------------------------------------------- helpers

def write_rule(base: str, name: str, spec: dict) -> str:
    path = os.path.join(base, f"{name}.json")
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(spec, fh)
    return path


def open_db(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, timeout=15)
    conn.row_factory = sqlite3.Row
    return conn


def rule_cmd(db_path: str, *args, check: bool = True):
    p = subprocess.run(
        [sys.executable, "-m", "pcg", "--db", db_path, "--clock", "real",
         "rule", *args],
        env=ENV, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    if check and p.returncode != 0:
        raise AssertionError(f"rule {' '.join(args)} rc={p.returncode}: {p.stdout}")
    return p


def custom_source(base: str, records_by_partition: dict[int, list[dict]]) -> str:
    src = os.path.join(base, "custom-src")
    os.makedirs(src, exist_ok=True)
    counts = {}
    for pid, rows in records_by_partition.items():
        with open(os.path.join(src, f"p{pid}.jsonl"), "w", encoding="utf-8") as fh:
            for off, obj in enumerate(rows):
                doc = dict(obj)
                doc.setdefault("offset", off)
                fh.write(json.dumps(doc, ensure_ascii=False) + "\n")
        counts[str(pid)] = len(rows)
    with open(os.path.join(src, "manifest.json"), "w", encoding="utf-8") as fh:
        json.dump({"partitions": len(counts), "counts": counts,
                   "immutable": True}, fh)
    return src


def start_group(tmp: str, source: str, counts_note=None, lease_ms=6000,
                tick_ms=50) -> ProcGroup:
    db = os.path.join(tmp, "g.db")
    pg = ProcGroup(db, source, lease_ms=lease_ms, ack_timeout_ms=lease_ms + 600,
                   tick_ms=tick_ms, clock="manual")
    pg.start_coordinator()
    return pg


def drain(pg, expected_total: int):
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        with open_db(pg.db) as c:
            n = c.execute("SELECT COUNT(*) FROM records").fetchone()[0]
        if n >= expected_total:
            return
        time.sleep(0.1)
    raise AssertionError("workers did not drain the source in time")


def active_version(db_path: str) -> str:
    with open_db(db_path) as c:
        return c.execute(
            "SELECT active_version FROM rule_state WHERE id=1").fetchone()[0]


def publish_seq(db_path: str) -> int:
    with open_db(db_path) as c:
        return c.execute(
            "SELECT publish_seq FROM rule_state WHERE id=1").fetchone()[0]


# ------------------------------------------------------------ contract semantics

class TestContractSemantics(unittest.TestCase):
    def setUp(self):
        from pcg.rule_engine import validate_spec, evaluate, RuleReject
        self.validate = validate_spec
        self.evaluate = evaluate
        self.Reject = RuleReject

    def test_spec_text_example_and_decimal_rounding(self):
        ev = self.evaluate
        spec = {"fields": {"name": {"missing": "preserve", "null": "preserve",
            "steps": [{"op": "nfkc"}, {"op": "collapse_ws"},
                      {"op": "upper"}]}}}
        self.assertEqual(
            ev(spec, {"name": "  Ａlice   Smith  "})["name"], "ALICE SMITH")
        for mode, want in (("HALF_EVEN", "1.00"), ("HALF_UP", "1.01")):
            s = {"fields": {"amount": {"missing": "reject", "null": "reject",
                "steps": [{"op": "decimal", "scale": 2, "rounding": mode}]}}}
            self.assertEqual(ev(s, {"amount": "1.005"})["amount"], want)

    def test_decimal_formatting_and_grammar(self):
        ev = self.evaluate
        s0 = {"fields": {"a": {"missing": "reject", "null": "reject",
            "steps": [{"op": "decimal", "scale": 0, "rounding": "HALF_UP"}]}}}
        self.assertEqual(ev(s0, {"a": "  2.5 "})["a"], "3")
        self.assertEqual(ev(s0, {"a": "1e3"})["a"], "1000")
        self.assertEqual(ev(s0, {"a": ".5"})["a"], "1")
        self.assertEqual(ev(s0, {"a": "1."})["a"], "1")
        s2 = {"fields": {"a": {"missing": "reject", "null": "reject",
            "steps": [{"op": "decimal", "scale": 2, "rounding": "HALF_EVEN"}]}}}
        self.assertEqual(ev(s2, {"a": "-0"})["a"], "0.00")
        self.assertEqual(ev(s2, {"a": "-0.000"})["a"], "0.00")
        self.assertEqual(ev(s2, {"a": "123456.789"})["a"], "123456.79")
        for bad in [1.005, True, False, "NaN", "Infinity", "-Infinity",
                    "1,000", "1_000", "", "   ", "0x10", "1 2",
                    "1e999999999", "9" * 5000, "1e-999999999"]:
            with self.assertRaises(self.Reject) as ctx:
                ev(s2, {"a": bad})
            self.assertEqual(ctx.exception.field, "a")
            self.assertEqual(ctx.exception.op, "decimal")

    def test_decimal_independent_of_process_context(self):
        import decimal
        # poison the process-wide decimal context: low precision, different
        # rounding mode. The engine must ignore it entirely.
        old_prec, old_round = decimal.getcontext().prec, decimal.getcontext().rounding
        decimal.getcontext().prec = 2
        decimal.getcontext().rounding = decimal.ROUND_DOWN
        try:
            s = {"fields": {"a": {"missing": "reject", "null": "reject",
                "steps": [{"op": "decimal", "scale": 2,
                           "rounding": "HALF_UP"}]}}}
            # 10 integer digits need more than the poisoned precision of 2;
            # HALF_UP on ...995 must produce the ...00 carry.
            self.assertEqual(self.evaluate(s, {"a": "9999999999.995"})["a"],
                             "10000000000.00")
            self.assertEqual(self.evaluate(s, {"a": "1.005"})["a"], "1.01")
        finally:
            decimal.getcontext().prec = old_prec
            decimal.getcontext().rounding = old_round

    def test_missing_and_null_policies(self):
        ev = self.evaluate
        for policy, missing_expect, null_expect in [
            ("preserve", "absent", None),
            ("null", None, None),
        ]:
            spec = {"fields": {"a": {"missing": policy, "null": policy,
                "steps": [{"op": "upper"}]}}}
            out_m = ev(spec, {})
            if missing_expect == "absent":
                self.assertNotIn("a", out_m)
            else:
                self.assertIsNone(out_m["a"])
            self.assertIsNone(ev(spec, {"a": None})["a"])
        spec = {"fields": {"a": {"missing": "reject", "null": "reject",
            "steps": [{"op": "upper"}]}}}
        with self.assertRaises(self.Reject):
            ev(spec, {})
        with self.assertRaises(self.Reject):
            ev(spec, {"a": None})
        # policy path must not run steps: null stays null even though upper
        # would reject a non-string
        spec2 = {"fields": {"a": {"missing": "null", "null": "preserve",
            "steps": [{"op": "upper"}]}}}
        self.assertIsNone(ev(spec2, {"a": None})["a"])

    def test_alias_passthrough_and_text_semantics(self):
        ev = self.evaluate
        spec = {"fields": {"city": {"missing": "preserve", "null": "reject",
            "steps": [{"op": "lower"},
                      {"op": "alias", "values": {"nyc": "new york"}}]}}}
        self.assertEqual(ev(spec, {"city": "NYC"})["city"], "new york")
        self.assertEqual(ev(spec, {"city": "LA"})["city"], "la")  # miss keeps value
        self.assertEqual(ev(spec, {"city": "LA", "other": 7})["other"], 7)
        # python title semantics
        st = {"fields": {"t": {"missing": "preserve", "null": "preserve",
            "steps": [{"op": "title"}]}}}
        self.assertEqual(ev(st, {"t": "hello world"})["t"], "Hello World")
        # non-string into a text op is a row rejection with field/op/reason
        with self.assertRaises(self.Reject) as ctx:
            ev(st, {"t": 5})
        self.assertEqual(ctx.exception.op, "title")

    def test_unconfigured_fields_and_original_value_usage(self):
        # every configured step chain starts from the SAME original value;
        # unconfigured top-level keys (incl. nested objects) are preserved
        spec = {"fields": {"a": {"missing": "preserve", "null": "preserve",
            "steps": [{"op": "upper"}]},
            "b": {"missing": "preserve", "null": "preserve",
            "steps": [{"op": "lower"}]}}}
        out = self.evaluate(spec, {"a": "X", "b": "Y", "keep": {"n": [1, 2]}})
        self.assertEqual(out, {"a": "X", "b": "y", "keep": {"n": [1, 2]}})

    def test_validation_battery(self):
        from pcg.rule_engine import RuleSpecError
        bad_docs = [
            {}, {"fields": {}},  # fields may be an empty object actually valid
            {"fields": {}, "extra": 1},
            {"fields": {"x": 1}},
            {"fields": {"x": {"missing": "maybe", "null": "null",
                              "steps": [{"op": "strip"}]}}},
            {"fields": {"x": {"missing": "null", "null": "null", "steps": []}}},
            {"fields": {"x": {"missing": "null", "null": "null",
                              "steps": [{"op": "strip", "extra": 1}]}}},
            {"fields": {"x": {"missing": "null", "null": "null",
                              "steps": [{"op": "decimal", "scale": 7,
                                         "rounding": "HALF_UP"}]}}},
            {"fields": {"x": {"missing": "null", "null": "null",
                              "steps": [{"op": "decimal", "scale": True,
                                         "rounding": "HALF_UP"}]}}},
            {"fields": {"x": {"missing": "null", "null": "null",
                              "steps": [{"op": "decimal", "scale": 1,
                                         "rounding": "DOWN"}]}}},
            {"fields": {"x": {"missing": "null", "null": "null",
                              "steps": [{"op": "alias"}]}}},
            {"fields": {"x": {"missing": "null", "null": "null",
                              "steps": [{"op": "alias", "values": {}}]}}},
            {"fields": {"x": {"missing": "null", "null": "null",
                              "steps": [{"op": "alias",
                                         "values": {"a": 1}}]}}},
            {"fields": {"x": {"missing": "null", "null": "null",
                              "steps": [{"op": "mystery"}]}}},
            "not-an-object",
        ]
        bad_docs.remove({"fields": {}})  # empty fields object is legal (no-op)
        for doc in bad_docs:
            with self.assertRaises(RuleSpecError, msg=f"should reject {doc}"):
                self.validate(doc)
        # the empty-fields rule is valid and behaves as identity
        self.assertEqual(self.validate({"fields": {}}), {"fields": {}})
        self.assertEqual(self.evaluate({"fields": {}}, {"a": 1}), {"a": 1})

    def test_identity_key_permutation_vs_step_order(self):
        from pcg.rule_engine import canonical_spec
        a = {"fields": {"x": {"missing": "null", "null": "null",
                              "steps": [{"op": "strip"}, {"op": "upper"}]}}}
        b = {"fields": {"x": {"steps": [{"op": "strip"}, {"op": "upper"}],
                              "null": "null", "missing": "null"}}}
        self.assertEqual(canonical_spec(a), canonical_spec(b))
        c = {"fields": {"x": {"missing": "null", "null": "null",
                              "steps": [{"op": "upper"}, {"op": "strip"}]}}}
        self.assertNotEqual(canonical_spec(a)[1], canonical_spec(c)[1])

    def test_field_diffs_distinguish_json_types_and_presence(self):
        from pcg.rule_review import _field_diffs
        # JSON number vs string: different types => changed
        diffs = _field_diffs({"a": 1}, {"a": "1"})
        self.assertEqual(len(diffs), 1)
        self.assertEqual(diffs[0]["base"]["type"], "number")
        self.assertEqual(diffs[0]["candidate"]["type"], "string")
        # bool vs number are distinct JSON types
        self.assertTrue(_field_diffs({"a": True}, {"a": 1}))
        # identical => no diff; 1 == 1 even across int/float encoding
        self.assertEqual(_field_diffs({"a": 1}, {"a": 1}), [])
        # field disappearance / appearance reads clearly
        d = _field_diffs({"a": 1}, {})
        self.assertEqual(d[0]["candidate"], {"present": False})
        self.assertTrue(d[0]["base"]["present"])
        d = _field_diffs({}, {"a": None})
        self.assertFalse(d[0]["base"]["present"])
        self.assertEqual(d[0]["candidate"]["type"], "null")


class TestUpgradeAndRegistration(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="pcgrule-")
        self.db = os.path.join(self.tmp, "g.db")

    def test_empty_db_upgrade_repeatable_and_legacy_default(self):
        rule_cmd(self.db, "status")
        rule_cmd(self.db, "status")  # repeat
        with open_db(self.db) as c:
            self.assertEqual(active_version(self.db), "legacy")
            self.assertEqual(publish_seq(self.db), 0)
            self.assertTrue(c.execute(
                "SELECT builtin FROM rule_versions WHERE version_id='legacy'"
            ).fetchone()[0])

    def test_populated_db_legacy_parity_and_repeatable_upgrade(self):
        src = make_source(self.tmp, [13, 0, 9], seed=11)
        pg = start_group(self.tmp, src)
        pg.admin("add", "w1")
        pg.start_worker("w1", batch_size=4)
        drain(pg, 22)
        pg.graceful_stop("w1")
        pg.stop_coordinator()
        pg.procs["__coord__"].wait(timeout=15)
        rule_cmd(self.db, "status")
        with open_db(self.db) as c:
            n = c.execute("SELECT COUNT(*) FROM records").fetchone()[0]
            mism = c.execute(
                "SELECT COUNT(*) FROM records r JOIN rule_results rr ON "
                "rr.version_id='legacy' AND rr.partition_id=r.partition_id "
                "AND rr.offset=r.offset WHERE rr.status!='ok' OR "
                "rr.result_json != r.clean_json").fetchone()[0]
            self.assertEqual(mism, 0)
            self.assertEqual(c.execute(
                "SELECT COUNT(*) FROM rule_results WHERE version_id='legacy'"
            ).fetchone()[0], n)
            # empty partition appears in the catalog => vector can show -1
            self.assertEqual([r[0] for r in c.execute(
                "SELECT partition_id FROM partitions ORDER BY partition_id")],
                [0, 1, 2])
        rule_cmd(self.db, "status")  # repeatable, no changes
        problems, _ = verify_database(self.db)
        self.assertEqual(problems, [])

    def test_register_retry_conflict_and_reserved_id(self):
        spec = {"fields": {"name": {"missing": "preserve", "null": "null",
            "steps": [{"op": "upper"}]}}}
        p1 = write_rule(self.tmp, "r1", spec)
        rule_cmd(self.db, "register", "v1", p1)
        rule_cmd(self.db, "register", "v1", p1)  # identical retry
        reordered = {"fields": {"name": {"steps": [{"op": "upper"}],
                                         "null": "null", "missing": "preserve"}}}
        p2 = write_rule(self.tmp, "r1b", reordered)
        rule_cmd(self.db, "register", "v1", p2)  # same canonical content
        changed = json.loads(json.dumps(spec))
        changed["fields"]["name"]["steps"].append({"op": "strip"})
        p3 = write_rule(self.tmp, "r1c", changed)
        r = rule_cmd(self.db, "register", "v1", p3, check=False)
        self.assertEqual(r.returncode, 4)
        r = rule_cmd(self.db, "register", "legacy", p1, check=False)
        self.assertEqual(r.returncode, 4)
        r = rule_cmd(self.db, "register", "bad",
                     write_rule(self.tmp, "bad", {"nope": 1}), check=False)
        self.assertEqual(r.returncode, 3)

    def test_recompute_works_without_source_directory(self):
        # history recomputation relies solely on committed source_json
        src = make_source(self.tmp, [6, 4], seed=5)
        pg = start_group(self.tmp, src)
        pg.admin("add", "w1")
        pg.start_worker("w1", batch_size=3)
        drain(pg, 10)
        pg.graceful_stop("w1")
        pg.stop_coordinator()
        pg.procs["__coord__"].wait(timeout=15)
        spec = {"fields": {"name": {"missing": "preserve", "null": "null",
            "steps": [{"op": "nfkc"}, {"op": "collapse_ws"}, {"op": "upper"}]}}}
        rule_cmd(self.db, "register", "v1", write_rule(self.tmp, "v1", spec))
        os.rename(src, src + ".hidden")
        try:
            rule_cmd(self.db, "recompute", "v1", "--page-size", "4")
        finally:
            os.rename(src + ".hidden", src)
        problems, _ = verify_database(self.db)
        self.assertEqual(problems, [])


def _make_v_rules(tmp: str):
    v1 = {"fields": {
        "name": {"missing": "preserve", "null": "null",
                 "steps": [{"op": "nfkc"}, {"op": "strip"},
                           {"op": "collapse_ws"}, {"op": "upper"}]},
        "city": {"missing": "preserve", "null": "preserve",
                 "steps": [{"op": "strip"}, {"op": "collapse_ws"},
                           {"op": "title"}]}}}
    v2 = {"fields": {
        "name": {"missing": "preserve", "null": "null",
                 "steps": [{"op": "nfkc"}, {"op": "strip"},
                           {"op": "collapse_ws"}, {"op": "lower"}]}}}
    return (write_rule(tmp, "v1", v1), write_rule(tmp, "v2", v2), v1, v2)


class TestPublishGates(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="pcgrule-")
        self.db = os.path.join(self.tmp, "g.db")
        src = make_source(self.tmp, [10, 8], seed=4)
        self.pg = start_group(self.tmp, src)
        self.pg.admin("add", "w1")
        self.pg.start_worker("w1", batch_size=5)
        drain(self.pg, 18)
        self.p1, self.p2, self.v1, self.v2 = _make_v_rules(self.tmp)

    def tearDown(self):
        self.pg.shutdown_all()

    def test_uncovered_candidate_cannot_publish(self):
        rule_cmd(self.db, "register", "v1", self.p1)
        # no recompute at all: every position uncovered
        out = json.loads(rule_cmd(self.db, "review", "v1", "--json").stdout)
        self.assertEqual(out["stats"]["uncovered"], 18)
        token = out["token"]
        r = rule_cmd(self.db, "activate", token, check=False)
        self.assertEqual(r.returncode, 6)
        self.assertIn("holes present", r.stdout)
        self.assertEqual(active_version(self.db), "legacy")

    def test_rejecting_candidate_cannot_publish(self):
        strict = {"fields": {"amount": {"missing": "reject", "null": "reject",
            "steps": [{"op": "decimal", "scale": 2, "rounding": "HALF_UP"}]}}}
        path = write_rule(self.tmp, "strict", strict)
        rule_cmd(self.db, "register", "strict", path)
        rule_cmd(self.db, "recompute", "strict", "--page-size", "6")
        out = json.loads(rule_cmd(self.db, "review", "strict", "--json").stdout)
        self.assertGreater(out["stats"]["candidate_rejected"], 0)
        r = rule_cmd(self.db, "activate", out["token"], check=False)
        self.assertEqual(r.returncode, 6)
        self.assertIn("rejects", r.stdout)
        self.assertEqual(active_version(self.db), "legacy")

    def test_full_publish_and_diff_stream(self):
        rule_cmd(self.db, "register", "v1", self.p1)
        rule_cmd(self.db, "recompute", "v1", "--page-size", "7")
        out = json.loads(rule_cmd(self.db, "review", "v1", "--json").stdout)
        self.assertEqual(out["stats"]["uncovered"], 0)
        self.assertEqual(out["stats"]["candidate_rejected"], 0)
        token = out["token"]
        # diff output is source-position ordered
        d = rule_cmd(self.db, "diff", token, "--json").stdout
        payload = json.loads(d)
        positions = [(i["partition_id"], i["offset"]) for i in payload["items"]]
        self.assertEqual(positions, sorted(positions))
        rule_cmd(self.db, "activate", token)
        self.assertEqual(active_version(self.db), "v1")
        self.assertEqual(publish_seq(self.db), 1)
        # export shows the new version on every row with full results
        exp = rule_cmd(self.db, "export", "--json").stdout.splitlines()
        header = json.loads(exp[0])["section"]
        rows = [json.loads(l) for l in exp[1:]]
        self.assertEqual(header["version_id"], "v1")
        self.assertEqual(len(rows), 18)
        self.assertTrue(all(r["version_id"] == "v1" for r in rows))
        problems, _ = verify_database(self.db)
        self.assertEqual(problems, [])


class TestRecomputePauseAndKill(unittest.TestCase):
    def _group_with_data(self, counts):
        tmp = tempfile.mkdtemp(prefix="pcgpause-")
        db = os.path.join(tmp, "g.db")
        src = make_source(tmp, counts, seed=8)
        pg = start_group(tmp, src)
        return tmp, db, src, pg

    def test_pause_after_page_while_ingestion_continues(self):
        tmp, db, src, pg = self._group_with_data([60, 0, 40])
        try:
            pg.admin("add", "w1"); pg.admin("add", "w2")
            pg.start_worker("w1", batch_size=10)
            pg.start_worker("w2", batch_size=10)
            # wait until a prefix exists but the source is not fully drained
            deadline = time.monotonic() + 30
            while time.monotonic() < deadline:
                with open_db(db) as c:
                    n = c.execute("SELECT COUNT(*) FROM records").fetchone()[0]
                if 15 <= n < 90:
                    break
                time.sleep(0.1)
            self.assertGreaterEqual(n, 15)
            spec = {"fields": {"name": {"missing": "preserve", "null": "null",
                "steps": [{"op": "nfkc"}, {"op": "collapse_ws"},
                          {"op": "upper"}]}}}
            rule_cmd(db, "register", "v1", write_rule(tmp, "v1", spec))
            pause = os.path.join(tmp, "PAUSE")
            open(pause, "w").close()
            parked = subprocess.Popen(
                [sys.executable, "-m", "pcg", "--db", db, "--clock", "real",
                 "rule", "recompute", "v1", "--page-size", "5",
                 "--pause-file", pause, "--pause-wait-s", "60"],
                env=ENV, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                text=True)
            # wait until the runner has computed its first page and is parked
            deadline = time.monotonic() + 30
            while time.monotonic() < deadline:
                with open_db(db) as c:
                    lock_row = c.execute(
                        "SELECT 1 FROM rule_results WHERE version_id='v1'"
                    ).fetchone()
                # parked => process alive AND zero persisted rows
                if parked.poll() is None and lock_row is None:
                    time.sleep(0.3)
                    with open_db(db) as c:
                        still_none = c.execute(
                            "SELECT 1 FROM rule_results WHERE version_id='v1'"
                        ).fetchone() is None
                    if parked.poll() is None and still_none:
                        break
            self.assertIsNone(parked.poll(), "recompute should be parked, alive")
            # the computed page was NOT persisted
            with open_db(db) as c:
                self.assertEqual(c.execute(
                    "SELECT COUNT(*) FROM rule_results WHERE version_id='v1'"
                ).fetchone()[0], 0)
                wm = c.execute(
                    "SELECT COUNT(*) FROM rule_recompute_progress "
                    "WHERE version_id='v1' AND done_offset >= 0").fetchone()[0]
                self.assertEqual(wm, 0)
            before = snapshot_immutables(db)
            # ingestion keeps advancing and the official read stays available
            time.sleep(1.5)
            exp = rule_cmd(db, "export").stdout
            self.assertIn("version=legacy", exp.splitlines()[0])
            os.remove(pause)
            out, _ = parked.communicate(timeout=60)
            self.assertEqual(parked.returncode, 0, out)
            drain(pg, 100)
            # recomputation never modified or deleted any pre-existing
            # collection row: the earlier row set is an exact subset of the
            # later one (ingestion may only have appended new positions).
            after = snapshot_immutables(db)
            self.assertEqual(set(before["records"]),
                             set(before["records"]) & set(after["records"]))
            self.assertEqual(set(before["progress"]),
                             set(before["progress"]) & set(after["progress"]))
            # finish the recompute and independently verify every position
            rule_cmd(db, "recompute", "v1", "--page-size", "12")
            with open_db(db) as c:
                self.assertEqual(c.execute(
                    "SELECT COUNT(*) FROM rule_results WHERE version_id='v1'"
                ).fetchone()[0], 100)
            problems, _ = verify_database(db)
            self.assertEqual(problems, [])
        finally:
            pg.shutdown_all()

    def test_real_sigkill_then_resume_per_position(self):
        tmp = tempfile.mkdtemp(prefix="pcgkill-")
        db = os.path.join(tmp, "g.db")
        src = make_source(tmp, [400, 300, 200], seed=13)
        pg = start_group(tmp, src)
        try:
            pg.admin("add", "w1")
            pg.start_worker("w1", batch_size=25)
            drain(pg, 900)
            spec = {"fields": {
                "name": {"missing": "preserve", "null": "null",
                         "steps": [{"op": "nfkc"}, {"op": "collapse_ws"},
                                   {"op": "upper"}]},
                "city": {"missing": "preserve", "null": "preserve",
                         "steps": [{"op": "strip"}, {"op": "collapse_ws"},
                                   {"op": "title"}]}}}
            rule_cmd(db, "register", "v1", write_rule(tmp, "v1", spec))
            before = snapshot_immutables(db)
            proc = subprocess.Popen(
                [sys.executable, "-m", "pcg", "--db", db, "--clock", "real",
                 "rule", "recompute", "v1", "--page-size", "3"],
                env=ENV, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                text=True)
            # let a few durable pages land, then hard-kill the process
            deadline = time.monotonic() + 30
            while time.monotonic() < deadline:
                self.assertIsNone(proc.poll(), "runner finished before the kill")
                with open_db(db) as c:
                    covered = c.execute(
                        "SELECT COALESCE(SUM(done_offset+1),0) n FROM "
                        "rule_recompute_progress WHERE version_id='v1'"
                    ).fetchone()[0]
                if covered >= 12:
                    break
                time.sleep(0.05)
            self.assertIsNone(proc.poll(), "runner finished before the kill")
            proc.kill()
            out, _ = proc.communicate(timeout=10)
            self.assertLess(proc.returncode, 0)  # killed by signal
            with open_db(db) as c:
                flushed = c.execute(
                    "SELECT COUNT(*) FROM rule_results WHERE version_id='v1'"
                ).fetchone()[0]
                watermarks = {r[0]: r[1] for r in c.execute(
                    "SELECT partition_id,done_offset FROM "
                    "rule_recompute_progress WHERE version_id='v1'")}
            # no partial page: rows equal the sum of watermark coverage
            self.assertEqual(flushed, sum(w + 1 for w in watermarks.values()))
            # resume: only the remaining positions are computed, no full rescan
            r = rule_cmd(db, "recompute", "v1", "--page-size", "7")
            self.assertIn(f"rows_computed={900 - flushed}", r.stdout)
            with open_db(db) as c:
                self.assertEqual(c.execute(
                    "SELECT COUNT(*) FROM rule_results WHERE version_id='v1'"
                ).fetchone()[0], 900)
            # independent per-position value/diagnostic check
            problems, _ = verify_database(db)
            self.assertEqual(problems, [])
            # collection state was never touched by recomputation
            self.assertEqual(snapshot_immutables(db)["records"],
                             before["records"])
            self.assertEqual(snapshot_immutables(db)["progress"],
                             before["progress"])
            self.assertEqual(snapshot_immutables(db)["state"],
                             before["state"])
        finally:
            pg.shutdown_all()

    def test_concurrent_recompute_lock(self):
        tmp = tempfile.mkdtemp(prefix="pcglock-")
        db = os.path.join(tmp, "g.db")
        src = make_source(tmp, [40, 40], seed=2)
        pg = start_group(tmp, src)
        try:
            pg.admin("add", "w1")
            pg.start_worker("w1", batch_size=20)
            drain(pg, 80)
            spec = {"fields": {"name": {"missing": "preserve", "null": "null",
                "steps": [{"op": "upper"}]}}}
            rule_cmd(db, "register", "v1", write_rule(tmp, "v1", spec))
            pause = os.path.join(tmp, "PAUSE"); open(pause, "w").close()
            first = subprocess.Popen(
                [sys.executable, "-m", "pcg", "--db", db, "--clock", "real",
                 "rule", "recompute", "v1", "--page-size", "5",
                 "--pause-file", pause, "--pause-wait-s", "60"], env=ENV,
                stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
            # wait until it is parked (alive, holding the lock, nothing flushed)
            deadline = time.monotonic() + 30
            while time.monotonic() < deadline:
                if first.poll() is None:
                    with open_db(db) as c:
                        n = c.execute(
                            "SELECT COUNT(*) FROM rule_results "
                            "WHERE version_id='v1'").fetchone()[0]
                    if n == 0:
                        time.sleep(0.3)
                        if first.poll() is None:
                            break
            self.assertIsNone(first.poll())
            r = rule_cmd(db, "recompute", "v1", check=False)
            self.assertEqual(r.returncode, 5)
            os.remove(pause)
            out, _ = first.communicate(timeout=30)
            self.assertEqual(first.returncode, 0, out)
        finally:
            pg.shutdown_all()


class TestReviewTokenInvalidation(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="pcgtok-")
        self.db = os.path.join(self.tmp, "g.db")
        self.src = make_source(self.tmp, [12, 12], seed=6)
        self.pg = start_group(self.tmp, self.src)
        self.pg.admin("add", "w1")
        self.pg.start_worker("w1", batch_size=6)
        drain(self.pg, 24)
        self.p1, self.p2, _, _ = _make_v_rules(self.tmp)
        rule_cmd(self.db, "register", "v1", self.p1)
        rule_cmd(self.db, "register", "v2", self.p2)
        rule_cmd(self.db, "recompute", "v1", "--page-size", "8")
        rule_cmd(self.db, "recompute", "v2", "--page-size", "8")

    def tearDown(self):
        self.pg.shutdown_all()

    def _review(self, candidate):
        return json.loads(rule_cmd(self.db, "review", candidate,
                                   "--json").stdout)["token"]

    def _activate(self, token, ok=True):
        r = rule_cmd(self.db, "activate", token, check=False)
        self.assertEqual(r.returncode, 0 if ok else 6, r.stdout)

    def test_new_commit_invalidates_token_even_after_catchup(self):
        token = self._review("v1")
        # one more source row arrives: append a second partition file tail by
        # restarting ingestion on a larger generated source instead. Simpler:
        # workers already drained 24 rows; generate additional records by
        # extending the source files in place (offsets stay immutable).
        with open(os.path.join(self.src, "p0.jsonl"), "a",
                  encoding="utf-8") as fh:
            fh.write(json.dumps({"offset": 12, "name": " New Person ",
                                 "city": "BERLIN"}) + "\n")
        with open(os.path.join(self.src, "manifest.json"), "w") as fh:
            json.dump({"partitions": 2, "counts": {"0": 13, "1": 12},
                       "immutable": True}, fh)
        self.pg.admin("add", "w2")
        self.pg.start_worker("w2", batch_size=3)
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            with open_db(self.db) as c:
                n = c.execute("SELECT COUNT(*) FROM records").fetchone()[0]
            if n >= 25:
                break
            time.sleep(0.1)
        # background catch-up makes the candidate cover the new row too...
        rule_cmd(self.db, "recompute", "v1", "--page-size", "5")
        with open_db(self.db) as c:
            self.assertEqual(c.execute(
                "SELECT COUNT(*) FROM rule_results WHERE version_id='v1'"
            ).fetchone()[0], 25)
        # ...but the old token is still invalid: its section vector is stale
        r = rule_cmd(self.db, "activate", token, check=False)
        self.assertEqual(r.returncode, 6)
        self.assertIn("vector changed", r.stdout)
        # a fresh review + activate works
        self._activate(self._review("v1"))
        self.assertEqual(active_version(self.db), "v1")

    def test_switch_away_and_back_kills_old_token(self):
        t1 = self._review("v1")
        self._activate(t1)
        self.assertEqual(publish_seq(self.db), 1)
        t2 = self._review("v2")
        self._activate(t2)
        self.assertEqual(publish_seq(self.db), 2)
        # the token used for the first switch cannot be reused to go back
        r = rule_cmd(self.db, "activate", t1, check=False)
        self.assertEqual(r.returncode, 6)
        # rollback to v1 goes through a fresh review
        self._activate(self._review("v1"))
        self.assertEqual(active_version(self.db), "v1")
        self.assertEqual(publish_seq(self.db), 3)
        # re-presenting the already-consumed v2 token cannot republish it
        r = rule_cmd(self.db, "activate", t2, check=False)
        self.assertEqual(r.returncode, 6)
        self.assertEqual(publish_seq(self.db), 3)  # failed switch changes nothing

    def test_review_is_immutable_after_later_changes(self):
        out = json.loads(rule_cmd(self.db, "review", "v1", "--json").stdout)
        token = out["token"]
        with open_db(self.db) as c:
            items_before = c.execute(
                "SELECT COUNT(*) FROM rule_review_items WHERE token=?",
                (token,)).fetchone()[0]
            row_before = tuple(c.execute(
                "SELECT offset_vector_json,stats_json,publish_seq FROM "
                "rule_reviews WHERE token=?", (token,)).fetchone())
        # more ingestion + recomputation happen after the review
        with open(os.path.join(self.src, "p1.jsonl"), "a",
                  encoding="utf-8") as fh:
            fh.write(json.dumps({"offset": 12, "name": "Late"}) + "\n")
        with open(os.path.join(self.src, "manifest.json"), "w") as fh:
            json.dump({"partitions": 2, "counts": {"0": 12, "1": 13},
                       "immutable": True}, fh)
        self.pg.admin("add", "w3")
        self.pg.start_worker("w3", batch_size=3)
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            with open_db(self.db) as c:
                n = c.execute("SELECT COUNT(*) FROM records").fetchone()[0]
            if n >= 25:
                break
            time.sleep(0.1)
        rule_cmd(self.db, "recompute", "v1", "--page-size", "5")
        with open_db(self.db) as c:
            items_after = c.execute(
                "SELECT COUNT(*) FROM rule_review_items WHERE token=?",
                (token,)).fetchone()[0]
            row_after = tuple(c.execute(
                "SELECT offset_vector_json,stats_json,publish_seq FROM "
                "rule_reviews WHERE token=?", (token,)).fetchone())
        self.assertEqual(items_before, items_after)
        self.assertEqual(row_before, row_after)


class TestInflightSwitch(unittest.TestCase):
    def test_batch_computed_before_switch_commits_under_new_version(self):
        tmp = tempfile.mkdtemp(prefix="pcginflight-")
        db = os.path.join(tmp, "g.db")
        src = custom_source(tmp, {0: [
            {"name": f"  person {i}  ", "city": "new_york",
             "amount": str(10 + i) + ".005"} for i in range(12)],
            1: [{"name": f"other {i}"} for i in range(8)]})
        pg = start_group(tmp, src, lease_ms=8000, tick_ms=50)
        try:
            pg.admin("add", "w1", "--lease-ms", "8000")
            pg.admin("add", "w2", "--lease-ms", "8000")
            # two partitions -> w1 owns p0 (barrier), w2 drains p1 concurrently
            pg.start_worker("w1", batch_size=4, barrier="w1:0:0")
            pg.start_worker("w2", batch_size=4)
            pg.wait_barrier_reached("w1:p0:o0", timeout=15)
            # w1 has computed p0 offsets 0..3 under legacy but has NOT committed
            with open_db(db) as c:
                p0 = c.execute(
                    "SELECT COUNT(*) FROM records WHERE partition_id=0"
                ).fetchone()[0]
            self.assertEqual(p0, 0)
            # publish v2 while the batch is parked, covering the empty section
            spec = {"fields": {"name": {"missing": "preserve", "null": "null",
                "steps": [{"op": "nfkc"}, {"op": "strip"},
                          {"op": "collapse_ws"}, {"op": "upper"}]}}}
            rule_cmd(db, "register", "v2", write_rule(tmp, "v2", spec))
            # catch up whatever p1 rows the concurrent worker already committed
            rule_cmd(db, "recompute", "v2", "--page-size", "5")
            token = json.loads(rule_cmd(db, "review", "v2",
                                        "--json").stdout)["token"]
            rule_cmd(db, "activate", token)
            self.assertEqual(active_version(db), "v2")
            # release the parked worker: the fence commits under v2 semantics
            pg.release_barrier("w1:p0:o0")
            drain(pg, 20)
            with open_db(db) as c:
                rows = c.execute(
                    "SELECT offset,result_json FROM rule_results "
                    "WHERE version_id='v2' AND partition_id=0 ORDER BY offset"
                ).fetchall()
                self.assertEqual(len(rows), 12)
                first = json.loads(rows[0]["result_json"])
                self.assertEqual(first["name"], "PERSON 0")
                # legacy clean_json still stores the old-calibre value
                legacy = json.loads(c.execute(
                    "SELECT clean_json FROM records WHERE partition_id=0 "
                    "AND offset=0").fetchone()["clean_json"])
                self.assertEqual(legacy["name"], "Person 0")
            # export never mixes versions and covers every position
            exp = [json.loads(l) for l in rule_cmd(db, "export",
                    "--json").stdout.splitlines()[1:]]
            self.assertEqual(len(exp), 20)
            self.assertEqual({r["version_id"] for r in exp}, {"v2"})
            problems, _ = verify_database(db)
            self.assertEqual(problems, [])
        finally:
            pg.shutdown_all()


class TestRollbackCatchup(unittest.TestCase):
    def test_legacy_catches_up_rows_received_while_inactive(self):
        tmp = tempfile.mkdtemp(prefix="pcgrollback-")
        db = os.path.join(tmp, "g.db")
        src = make_source(tmp, [10, 10], seed=21)
        pg = start_group(tmp, src)
        try:
            pg.admin("add", "w1")
            pg.start_worker("w1", batch_size=5)
            drain(pg, 20)
            p1, _, _, _ = _make_v_rules(tmp)
            rule_cmd(db, "register", "v1", p1)
            rule_cmd(db, "recompute", "v1", "--page-size", "6")
            token = json.loads(rule_cmd(db, "review", "v1",
                                        "--json").stdout)["token"]
            rule_cmd(db, "activate", token)
            self.assertEqual(active_version(db), "v1")
            # new data arrives while legacy is inactive
            with open(os.path.join(src, "p0.jsonl"), "a",
                      encoding="utf-8") as fh:
                for off in range(10, 15):
                    fh.write(json.dumps({"offset": off,
                                         "name": f"late {off}"}) + "\n")
            with open(os.path.join(src, "manifest.json"), "w") as fh:
                json.dump({"partitions": 2, "counts": {"0": 15, "1": 10},
                           "immutable": True}, fh)
            pg.admin("add", "w2")
            pg.start_worker("w2", batch_size=3)
            deadline = time.monotonic() + 30
            while time.monotonic() < deadline:
                with open_db(db) as c:
                    n = c.execute("SELECT COUNT(*) FROM records").fetchone()[0]
                if n >= 25:
                    break
                time.sleep(0.1)
            with open_db(db) as c:
                legacy = c.execute(
                    "SELECT COUNT(*) FROM rule_results WHERE version_id='legacy'"
                ).fetchone()[0]
                v1 = c.execute(
                    "SELECT COUNT(*) FROM rule_results WHERE version_id='v1'"
                ).fetchone()[0]
            self.assertEqual(legacy, 20)  # stopped at the switch point
            self.assertEqual(v1, 25)
            # rollback contract: backfill -> review -> activate, no pointer flip
            rule_cmd(db, "recompute", "legacy", "--page-size", "4")
            out = json.loads(rule_cmd(db, "review", "legacy",
                                      "--json").stdout)
            self.assertEqual(out["stats"]["uncovered"], 0)
            self.assertEqual(out["stats"]["candidate_rejected"], 0)
            rule_cmd(db, "activate", out["token"])
            self.assertEqual(active_version(db), "legacy")
            with open_db(db) as c:
                self.assertEqual(c.execute(
                    "SELECT COUNT(*) FROM rule_results WHERE version_id='legacy'"
                ).fetchone()[0], 25)
            problems, stats = verify_database(db)
            self.assertEqual(problems, [])
            self.assertEqual(stats["legacy"]["rejected"], 0)
        finally:
            pg.shutdown_all()


class TestExportConcurrency(unittest.TestCase):
    def test_real_process_export_vs_concurrent_publish(self):
        tmp = tempfile.mkdtemp(prefix="pcgexport-")
        db = os.path.join(tmp, "g.db")
        src = make_source(tmp, [900, 900], seed=31)
        pg = start_group(tmp, src)
        try:
            pg.admin("add", "w1"); pg.admin("add", "w2")
            pg.start_worker("w1", batch_size=50)
            pg.start_worker("w2", batch_size=50)
            drain(pg, 1800)
            p1, p2, _, _ = _make_v_rules(tmp)
            rule_cmd(db, "register", "v1", p1)
            rule_cmd(db, "register", "v2", p2)
            rule_cmd(db, "recompute", "v1", "--page-size", "200")
            rule_cmd(db, "recompute", "v2", "--page-size", "200")
            order = ["v1", "v2", "legacy"]
            # publisher process: alternates versions using a fresh review per
            # attempt; a stale token is retried rather than looping forever
            stop = os.path.join(tmp, "stop")
            publisher = os.path.join(tmp, "publisher.py")
            with open(publisher, "w") as fh:
                fh.write(
                    "import json,os,subprocess,sys,time\n"
                    f"DB={db!r};ENV=dict(os.environ,PYTHONPATH={SRC!r})\n"
                    f"ORDER={order!r};STOP={stop!r}\n"
                    "def pcg(*a):\n"
                    "    return subprocess.run([sys.executable,'-m','pcg','--db',"
                    "DB,'--clock','real','rule',*a],env=ENV,capture_output=True,"
                    "text=True)\n"
                    "i=0; switches=0\n"
                    "while not os.path.exists(STOP) and switches < 40:\n"
                    "    vid=ORDER[i%len(ORDER)];i+=1\n"
                    "    r=pcg('review',vid,'--json')\n"
                    "    if r.returncode: time.sleep(0.02); continue\n"
                    "    tok=json.loads(r.stdout)['token']\n"
                    "    a=pcg('activate',tok)\n"
                    "    if a.returncode == 0: switches += 1\n"
                    "    time.sleep(0.05)\n")
            pub = subprocess.Popen([sys.executable, "-I", publisher], env=ENV)
            try:
                for round_no in range(8):
                    out = os.path.join(tmp, f"export-{round_no}.jsonl")
                    with open(out, "w") as fh:
                        exp = subprocess.run(
                            [sys.executable, "-m", "pcg", "--db", db,
                             "--clock", "real", "rule", "export", "--json"],
                            env=ENV, stdout=fh, stderr=subprocess.PIPE, text=True)
                    self.assertEqual(exp.returncode, 0, exp.stderr)
                    with open(out) as fh:
                        lines = fh.read().splitlines()
                    section = json.loads(lines[0])["section"]
                    rows = [json.loads(l) for l in lines[1:]]
                    # one version + one publish seq for the whole export
                    self.assertEqual({r["version_id"] for r in rows},
                                     {section["version_id"]})
                    self.assertEqual({r["publish_seq"] for r in rows},
                                     {section["publish_seq"]})
                    # no position included in the section was dropped
                    self.assertEqual(len(rows), section["positions"])
                    # source order preserved
                    pos = [(r["partition_id"], r["offset"]) for r in rows]
                    self.assertEqual(pos, sorted(pos))
            finally:
                open(stop, "w").close()
                pub.wait(timeout=20)
        finally:
            pg.shutdown_all()

    def test_snapshot_semantics_with_switch_mid_iteration(self):
        # deterministic in-process proof: hold an export snapshot open across
        # an activation performed on another connection.
        from pcg.db import connect
        from pcg.rule_export import export_rows
        from pcg.rule_review import activate, create_review
        from pcg.clock import Clock
        tmp = tempfile.mkdtemp(prefix="pcgsnap-")
        db = os.path.join(tmp, "g.db")
        src = make_source(tmp, [8, 7], seed=41)
        pg = start_group(tmp, src)
        try:
            pg.admin("add", "w1")
            pg.start_worker("w1", batch_size=4)
            drain(pg, 15)
            p1, _, _, _ = _make_v_rules(tmp)
            rule_cmd(db, "register", "v1", p1)
            rule_cmd(db, "recompute", "v1", "--page-size", "5")
            conn = connect(db)
            gen = export_rows(conn)
            first = next(gen)
            self.assertEqual(first["version_id"], "legacy")
            # switch while the export snapshot is held open
            admin_conn = connect(db)
            clock = Clock(admin_conn, "real")
            review = create_review(admin_conn, "v1", None, clock.now_ms())
            admin_conn.commit()
            out = activate(admin_conn, review["token"], clock.now_ms())
            admin_conn.commit()
            self.assertEqual(out["action"], "switched")
            admin_conn.close()
            rest = list(gen)
            conn.close()
            all_rows = [first] + rest
            self.assertEqual(len(all_rows), 15)
            self.assertEqual({r["version_id"] for r in all_rows}, {"legacy"})
        finally:
            pg.shutdown_all()

    def test_concurrent_activations_serialise(self):
        tmp = tempfile.mkdtemp(prefix="pcgact-")
        db = os.path.join(tmp, "g.db")
        src = make_source(tmp, [6, 6], seed=51)
        pg = start_group(tmp, src)
        try:
            pg.admin("add", "w1")
            pg.start_worker("w1", batch_size=4)
            drain(pg, 12)
            p1, p2, _, _ = _make_v_rules(tmp)
            rule_cmd(db, "register", "v1", p1)
            rule_cmd(db, "register", "v2", p2)
            rule_cmd(db, "recompute", "v1", "--page-size", "6")
            rule_cmd(db, "recompute", "v2", "--page-size", "6")
            t1 = json.loads(rule_cmd(db, "review", "v1",
                                     "--json").stdout)["token"]
            t2 = json.loads(rule_cmd(db, "review", "v2",
                                     "--json").stdout)["token"]
            procs = [subprocess.Popen(
                [sys.executable, "-m", "pcg", "--db", db, "--clock", "real",
                 "rule", "activate", tok], env=ENV,
                stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
                for tok in (t1, t2)]
            results = [p.wait() for p in procs]
            self.assertEqual(sorted(results), [0, 6])  # exactly one winner
            self.assertEqual(publish_seq(db), 1)       # seq moved exactly once
            self.assertIn(active_version(db), {"v1", "v2"})
        finally:
            pg.shutdown_all()


class TestActiveVersionRejections(unittest.TestCase):
    def test_row_level_rejection_keeps_raw_and_progress(self):
        tmp = tempfile.mkdtemp(prefix="pcgrej-")
        db = os.path.join(tmp, "g.db")
        rows = [
            {"name": "ok one", "amount": "12.50"},
            {"name": "ok two", "amount": "1,000.00"},   # thousands sep => reject
            {"name": "ok three", "amount": 3.5},        # JSON number => reject
            {"name": "ok four", "amount": "7.255"},     # HALF_UP -> 7.26
        ]
        src = custom_source(tmp, {0: rows, 1: []})
        pg = start_group(tmp, src)
        try:
            pg.admin("add", "w1")
            strict = {"fields": {"amount": {"missing": "reject",
                "null": "reject",
                "steps": [{"op": "decimal", "scale": 2,
                           "rounding": "HALF_UP"}]}}}
            rule_cmd(db, "register", "strict",
                     write_rule(tmp, "strict", strict))
            # empty section review/activate before any ingestion
            token = json.loads(rule_cmd(db, "review", "strict",
                                        "--json").stdout)["token"]
            rule_cmd(db, "activate", token)
            pg.start_worker("w1", batch_size=2)
            drain(pg, 4)
            with open_db(db) as c:
                # raw rows and progress are all there
                self.assertEqual(c.execute(
                    "SELECT COUNT(*) FROM records").fetchone()[0], 4)
                prog = c.execute(
                    "SELECT committed_offset FROM progress WHERE partition_id=0"
                ).fetchone()[0]
                self.assertEqual(prog, 3)
                outcomes = {r["offset"]: (r["status"], r["result_json"],
                                          r["error_json"])
                            for r in c.execute(
                    "SELECT offset,status,result_json,error_json FROM "
                    "rule_results WHERE version_id='strict' ORDER BY offset")}
            self.assertEqual([k for k in outcomes], [0, 1, 2, 3])
            self.assertEqual(outcomes[0][0], "ok")
            self.assertEqual(json.loads(outcomes[0][1])["amount"], "12.50")
            self.assertEqual(json.loads(outcomes[3][1])["amount"], "7.26")
            for off in (1, 2):
                self.assertEqual(outcomes[off][0], "rejected")
                err = json.loads(outcomes[off][2])
                self.assertEqual(err["field"], "amount")
                self.assertEqual(err["op"], "decimal")
                self.assertTrue(err["reason"])
            # official read shows the rejections explicitly (no skip/default)
            exp = [json.loads(l) for l in rule_cmd(db, "export",
                    "--json").stdout.splitlines()[1:]]
            self.assertEqual(len(exp), 4)
            statuses = [r["status"] for r in exp]
            self.assertEqual(statuses, ["ok", "rejected", "rejected", "ok"])
            rejected = next(r for r in exp if r["status"] == "rejected")
            self.assertEqual(rejected["error"]["field"], "amount")
            problems, _ = verify_database(db)
            self.assertEqual(problems, [])
        finally:
            pg.shutdown_all()


class TestLongSkewedHistoryBounded(unittest.TestCase):
    def test_paged_catchup_on_skewed_partitions(self):
        tmp = tempfile.mkdtemp(prefix="pcglong-")
        db = os.path.join(tmp, "g.db")
        # skew: empty + very large + empty + medium partition
        src = make_source(tmp, [0, 4000, 0, 600], seed=71)
        pg = start_group(tmp, src)
        try:
            pg.admin("add", "w1"); pg.admin("add", "w2")
            pg.start_worker("w1", batch_size=200)
            pg.start_worker("w2", batch_size=200)
            drain(pg, 4600)
            spec = {"fields": {
                "name": {"missing": "preserve", "null": "null",
                         "steps": [{"op": "nfkc"}, {"op": "collapse_ws"},
                                   {"op": "upper"}]},
                "city": {"missing": "preserve", "null": "preserve",
                         "steps": [{"op": "strip"}, {"op": "collapse_ws"},
                                   {"op": "title"}]}}}
            rule_cmd(db, "register", "v1", write_rule(tmp, "v1", spec))
            # bounded-memory run in a separate process with a tight RSS budget;
            # page size 50 means at most ~50 rows are ever materialised.
            runner = f"""
import resource, subprocess, sys, os
sys.path.insert(0, {SRC!r})
def pre():
    # 160 MB address-space cap: a full-history load of 4600 rows would fail
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
            with open_db(db) as c:
                self.assertEqual(c.execute(
                    "SELECT COUNT(*) FROM rule_results WHERE version_id='v1'"
                ).fetchone()[0], 4600)
                # watermarks prove paging advanced from -1 without gaps
                wm = {row[0]: row[1] for row in c.execute(
                    "SELECT partition_id,done_offset FROM "
                    "rule_recompute_progress WHERE version_id='v1'")}
                self.assertEqual(wm, {1: 3999, 3: 599})
                # every page committed together: result count == watermark span
                cnt = c.execute(
                    "SELECT COUNT(*) FROM rule_results WHERE version_id='v1'"
                ).fetchone()[0]
                self.assertEqual(cnt, sum(v + 1 for v in wm.values()))
            problems, _ = verify_database(db)
            self.assertEqual(problems, [])
        finally:
            pg.shutdown_all()


class TestIndependentReferenceParity(unittest.TestCase):
    def test_reference_matches_engine_on_hand_crafted_rows(self):
        # direct cross-check of the two independent evaluators
        from pcg.rule_engine import evaluate as prod_eval
        from pcg.rule_engine import RuleReject as ProdReject
        from tools_rule_ref import ref_evaluate, RefReject
        spec = {"fields": {
            "name": {"missing": "preserve", "null": "null",
                     "steps": [{"op": "nfkc"}, {"op": "strip"},
                               {"op": "collapse_ws"}, {"op": "upper"}]},
            "amount": {"missing": "reject", "null": "reject",
                       "steps": [{"op": "decimal", "scale": 2,
                                  "rounding": "HALF_UP"}]},
            "city": {"missing": "preserve", "null": "preserve",
                     "steps": [{"op": "lower"},
                               {"op": "alias",
                                "values": {"nyc": "new york"}}]}}}
        rows = [
            {"name": "  Ａlice   Smith  ", "amount": "1.005", "city": "NYC"},
            {"name": "Bob", "amount": "-0", "city": "LA"},
            {"name": "Cid", "amount": "1,000", "city": "nyc"},
            {"name": "Di", "amount": 9.99, "city": "la"},
            {"amount": "2.5", "city": "la"},
            {"name": None, "amount": "3", "city": "la"},
            {"name": "E", "city": "la", "untouched": {"k": [True, None]}},
            {"name": "Big", "amount": "1e999999999", "city": "la"},
            {"name": "Long", "amount": "9" * 5000, "city": "la"},
        ]
        for row in rows:
            try:
                p = prod_eval(spec, row)
                p_status = "ok"
            except ProdReject:
                p_status = "rej"
            try:
                r = ref_evaluate(spec, row)
                r_status = "ok"
            except RefReject:
                r_status = "rej"
            self.assertEqual(p_status, r_status, row)
            if p_status == "ok":
                self.assertEqual(
                    json.dumps(p, sort_keys=True),
                    json.dumps(r, sort_keys=True), row)


if __name__ == "__main__":
    unittest.main(verbosity=2)
