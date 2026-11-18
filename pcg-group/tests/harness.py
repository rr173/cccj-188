"""Test harness: real OS processes coordinated through the shared database.

Every test spawns the coordinator and its workers as *separate processes*
(``python -m pcg ...``); the only channels between them are the SQLite
coordination database and the immutable JSONL source.  Manual clock mode is
used by default so lease/fence timing is deterministic and fast: the
coordinator advances ``meta.now_ms`` every tick, workers read it back.
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
TESTS = os.path.join(ROOT, "tests")
ENV = dict(os.environ, PYTHONPATH=SRC)


class ProcGroup:
    def __init__(self, db: str, source: str, lease_ms: int = 800,
                 ack_timeout_ms: int = 900, tick_ms: int = 50,
                 clock: str = "manual"):
        self.db, self.source = db, source
        self.lease_ms, self.ack_timeout_ms, self.tick_ms, self.clock = (
            lease_ms, ack_timeout_ms, tick_ms, clock)
        self.procs: dict[str, subprocess.Popen] = {}

    def _base(self) -> list[str]:
        return [sys.executable, "-m", "pcg"]

    def _common(self) -> list[str]:
        return ["--db", self.db, "--clock", self.clock]

    def start_coordinator(self, extra: list[str] | None = None) -> subprocess.Popen:
        cmd = self._base() + ["coordinator"] + self._common() + [
            "--source", self.source,
            "--lease-ms", str(self.lease_ms),
            "--ack-timeout-ms", str(self.ack_timeout_ms),
            "--tick-ms", str(self.tick_ms),
        ] + (extra or [])
        p = subprocess.Popen(cmd, env=ENV, stdout=subprocess.PIPE,
                             stderr=subprocess.STDOUT, text=True)
        self.procs["__coord__"] = p
        # wait until the coordinator has initialised its lock + epoch
        self.wait_for(
            lambda: self.sql_one("SELECT 1 FROM meta WHERE key='epoch'"),
            timeout=10.0)
        return p

    def start_worker(self, wid: str, batch_size: int = 5,
                     barrier: str | None = None) -> subprocess.Popen:
        env = dict(ENV)
        if barrier:
            env["PCG_BARRIER"] = barrier
        cmd = self._base() + ["worker"] + self._common() + [
            "--source", self.source, "--id", wid,
            "--lease-ms", str(self.lease_ms), "--batch-size", str(batch_size),
        ]
        p = subprocess.Popen(cmd, env=env, stdout=subprocess.PIPE,
                             stderr=subprocess.STDOUT, text=True)
        self.procs[wid] = p
        return p

    def admin(self, *args: str, check: bool = True) -> str:
        p = subprocess.run(self._base() + ["admin"] + self._common() + list(args),
                           env=ENV, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                           text=True)
        if check and p.returncode != 0:
            raise AssertionError(f"admin {' '.join(args)} failed: {p.stdout}")
        return p.stdout

    def sql(self, query: str, params=()):
        conn = sqlite3.connect(self.db, timeout=15)
        conn.row_factory = sqlite3.Row
        try:
            return [dict(r) for r in conn.execute(query, params).fetchall()]
        finally:
            conn.close()

    def sql_one(self, query: str, params=()):
        rows = self.sql(query, params)
        return rows[0] if rows else None

    # ------------------------------------------------------------- synchronization
    def wait_for(self, predicate, timeout: float = 10.0, interval: float = 0.05):
        deadline = time.monotonic() + timeout
        last = None
        while time.monotonic() < deadline:
            try:
                last = predicate()
            except sqlite3.Error:
                last = None
            if last:
                return last
            time.sleep(interval)
        raise AssertionError(f"condition timed out; last={last}")

    def wait_barrier_reached(self, name: str, timeout: float = 10.0) -> None:
        self.wait_for(
            lambda: self.sql_one(
                "SELECT 1 FROM barrier_events WHERE name=?", (name,)),
            timeout=timeout)

    def wait_registered(self, wid: str, timeout: float = 10.0) -> None:
        self.wait_for(lambda: self.sql_one(
            "SELECT 1 FROM members WHERE worker_id=?", (wid,)), timeout=timeout)

    def release_barrier(self, name: str) -> None:
        self.admin("barrier-release", name)

    def wait_committed(self, partition: int, offset: int, timeout: float = 15.0):
        return self.wait_for(lambda: (
            (self.sql_one("SELECT committed_offset FROM progress WHERE partition_id=?",
                          (partition,)) or {}).get("committed_offset", -2) >= offset
        ), timeout=timeout)

    def wait_plan_finalized(self, plan_id: int, timeout: float = 15.0):
        return self.wait_for(lambda: (
            self.sql_one("SELECT finalized FROM plans WHERE plan_id=?", (plan_id,))
            or {}).get("finalized") == 1, timeout=timeout)

    def wait_member_status(self, wid: str, status: str, timeout: float = 10.0):
        return self.wait_for(lambda: (
            self.sql_one("SELECT status FROM members WHERE worker_id=?", (wid,))
            or {}).get("status") == status, timeout=timeout)

    def wait_heartbeat_advancing(self, wid: str, timeout: float = 10.0) -> None:
        """Confirm the worker's main loop + heartbeat thread are live and the
        signal handler window has passed before we send SIGTERM."""
        first = self.wait_for(lambda: self.sql_one(
            "SELECT last_beat b FROM members WHERE worker_id=?", (wid,)),
            timeout=timeout)["b"]

        def advanced():
            row = self.sql_one(
                "SELECT last_beat b FROM members WHERE worker_id=?", (wid,))
            return row and row["b"] > first

        self.wait_for(advanced, timeout=timeout)

    # ------------------------------------------------------------- process control
    def sigterm(self, wid: str) -> None:
        self.procs[wid].send_signal(signal.SIGTERM)

    def sigkill(self, wid: str) -> None:
        self.procs[wid].kill()

    def wait_exit(self, wid: str, timeout: float = 10.0) -> int:
        return self.procs[wid].wait(timeout=timeout)

    def graceful_stop(self, wid: str, timeout: float = 15.0) -> int:
        """Ask a worker to drain. Verifies the *drain semantics* (the process
        reached its graceful 'leaving' shutdown path) rather than relying on the
        exact shell return code, which can be -15 if SIGTERM lands in a tiny
        CPython startup window. Retries the signal while the process is alive.
        """
        p = self.procs[wid]
        deadline = time.monotonic() + timeout
        p.send_signal(signal.SIGTERM)
        while time.monotonic() < deadline:
            rc = p.poll()
            if rc is not None:
                leaving = self.sql_one(
                    "SELECT 1 FROM members WHERE worker_id=? AND status='leaving'",
                    (wid,))
                if rc == 0 or leaving is not None:
                    return 0
                return rc  # genuinely terminated without the drain path
            time.sleep(0.1)
        try:
            return p.wait(timeout=1)
        except subprocess.TimeoutExpired:
            p.kill()
            raise AssertionError(f"{wid} did not exit after SIGTERM")

    def stop_coordinator(self, sig: int = signal.SIGTERM) -> None:
        p = self.procs.get("__coord__")
        if p and p.poll() is None:
            p.send_signal(sig)

    def output(self, wid: str) -> str:
        p = self.procs[wid]
        if p.poll() is None:
            return f"(still running pid={p.pid})"
        return p.stdout.read()

    def coordinator_output(self) -> str:
        return self.output("__coord__")

    def shutdown_all(self) -> None:
        for wid, p in list(self.procs.items()):
            if p.poll() is None:
                p.terminate()
        for wid, p in list(self.procs.items()):
            try:
                p.wait(timeout=5)
            except subprocess.TimeoutExpired:
                p.kill()
                p.wait(timeout=5)


def make_source(base: str, counts: list[int], seed: int = 7) -> str:
    src = os.path.join(base, "source")
    gen = os.path.join(ROOT, "tools", "gensource.py")
    args = [sys.executable, "-I", gen, "--out", src,
            "--partitions", str(len(counts)),
            "--counts", ",".join(str(c) for c in counts), "--seed", str(seed)]
    subprocess.run(args, check=True, stdout=subprocess.PIPE, text=True)
    return src


class PcgTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.mkdtemp(prefix="pcgtest-")
        self.db = os.path.join(self.tmp, "g.db")

    def tearDown(self) -> None:
        if hasattr(self, "pg"):
            self.pg.shutdown_all()

    def new_group(self, counts: list[int], **kw) -> ProcGroup:
        src = make_source(self.tmp, counts)
        self.pg = ProcGroup(self.db, src, **kw)
        self.pg.start_coordinator()
        return self.pg

    def reconcile(self) -> tuple[int, int]:
        import sys
        if TESTS not in sys.path:
            sys.path.insert(0, TESTS)
        from tools_reconcile import reconcile
        return reconcile(self.db, self.pg.source, verbose=False)
