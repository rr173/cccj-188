#!/usr/bin/env python3
"""Small-memory long-run (soak) driver.

Runs the *real* multi-process group against the wall clock (``--clock real``)
with a modest worker pool, periodically adds/removes a member so handoffs keep
happening, and reconciles the sink against an independent reference reader on
an interval.  Designed to run for a long time in little memory:

* small Python processes with bounded batch sizes;
* WAL SQLite, short-lived connections for heartbeats;
* the generated source lives outside git and is sized by CLI flags;
* a periodic reconcile is a streaming per-offset audit (no full load).

Usage:
  python3 tools/soak.py --runtime 120 --workers 3 --partitions 8 \\
      --per-partition 200 --out /tmp/pcg-soak
"""

from __future__ import annotations

import argparse
import os
import signal
import subprocess
import sys
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SRC = os.path.join(ROOT, "src")
ENV = dict(os.environ, PYTHONPATH=SRC)
PY = sys.executable


class Soak:
    def __init__(self, out: str, partitions: int, counts: list[int],
                 workers: int, lease_ms: int, batch_size: int):
        self.out = out
        self.source = os.path.join(out, "source")
        self.db = os.path.join(out, "g.db")
        self.partitions = partitions
        self.counts = counts
        self.worker_names = [f"sw{i}" for i in range(workers)]
        self.lease_ms = lease_ms
        self.batch_size = batch_size
        self.procs: dict[str, subprocess.Popen] = {}
        self._stopping = False

    def _pcg(self, *args: str) -> list[str]:
        return [PY, "-m", "pcg", "--db", self.db, "--clock", "real", *args]

    def _start(self, name: str) -> None:
        log = open(os.path.join(self.out, f"worker-{name}.log"), "ab")
        p = subprocess.Popen(
            self._pcg("worker", "--source", self.source, "--id", name,
                      "--lease-ms", str(self.lease_ms),
                      "--batch-size", str(self.batch_size)),
            env=ENV, stdout=log, stderr=subprocess.STDOUT)
        self.procs[name] = p

    def _admin(self, *args: str) -> None:
        subprocess.run(self._pcg("admin", *args), env=ENV, check=True,
                       stdout=subprocess.DEVNULL)

    def _reconcile(self, allow_incomplete: bool = False) -> int:
        cmd = [PY, "-I", os.path.join(ROOT, "tools", "reconcile.py"),
               "--db", self.db, "--source", self.source]
        if allow_incomplete:
            cmd.append("--allow-incomplete")
        r = subprocess.run(cmd, env=ENV, stdout=subprocess.PIPE, text=True)
        print(r.stdout.strip())
        return r.returncode

    def _stop_worker(self, name: str) -> None:
        p = self.procs.get(name)
        if p and p.poll() is None:
            p.terminate()
            try:
                p.wait(timeout=10)
            except subprocess.TimeoutExpired:
                p.kill()
                p.wait(timeout=5)

    def _kill_worker(self, name: str) -> None:
        p = self.procs.get(name)
        if p and p.poll() is None:
            p.kill()
            p.wait(timeout=5)

    def run(self, runtime: float, churn_every: float,
            reconcile_every: float) -> int:
        os.makedirs(self.out, exist_ok=True)
        # 1) generate source
        gen = [PY, "-I", os.path.join(ROOT, "tools", "gensource.py"),
               "--out", self.source, "--partitions", str(self.partitions),
               "--counts", ",".join(str(c) for c in self.counts)]
        subprocess.run(gen, check=True, stdout=subprocess.PIPE, text=True)

        # 2) coordinator (real clock), flock-guarded singleton
        clog = open(os.path.join(self.out, "coordinator.log"), "ab")
        coord = subprocess.Popen(
            self._pcg("coordinator", "--source", self.source,
                      "--lease-ms", str(self.lease_ms),
                      "--ack-timeout-ms", str(self.lease_ms + 400),
                      "--tick-ms", "100"),
            env=ENV, stdout=clog, stderr=subprocess.STDOUT)
        self.procs["__coord__"] = coord

        def _handle(sig, frame):
            self._stopping = True
        signal.signal(signal.SIGTERM, _handle)
        signal.signal(signal.SIGINT, _handle)

        # 3) pre-register + start the steady worker pool
        for name in self.worker_names:
            self._admin("add", name, "--lease-ms", str(self.lease_ms))
            self._start(name)
        print(f"soak: started coordinator pid={coord.pid} and "
              f"{len(self.worker_names)} workers")

        start = time.monotonic()
        next_churn = start + churn_every
        next_reconcile = start + reconcile_every
        extra = 0
        rc = 0
        try:
            while not self._stopping and time.monotonic() - start < runtime:
                now = time.monotonic()
                if now >= next_churn:
                    # alternate graceful remove/add and an occasional hard kill
                    name = f"burst{extra}"
                    if extra % 2 == 0:
                        self._admin("add", name, "--lease-ms", str(self.lease_ms))
                        self._start(name)
                        print(f"soak: churn add {name}")
                        time.sleep(min(churn_every / 2, 5))
                        self._admin("remove", name)
                        self._stop_worker(name)
                        print(f"soak: churn graceful remove {name}")
                    else:
                        self._admin("add", name, "--lease-ms", str(self.lease_ms))
                        self._start(name)
                        time.sleep(1.0)
                        self._kill_worker(name)  # SIGKILL -> lease/fence recovery
                        print(f"soak: churn SIGKILL {name} (lease recovery)")
                    extra += 1
                    next_churn = now + churn_every
                if now >= next_reconcile:
                    # in-progress audit: validates committed prefix while work continues
                    if self._reconcile(allow_incomplete=True) != 0:
                        rc = 1
                        break
                    next_reconcile = now + reconcile_every
                time.sleep(0.2)
        finally:
            print("soak: shutting down")
            for name in list(self.procs):
                if name == "__coord__":
                    continue
                self._stop_worker(name)
            if coord.poll() is None:
                coord.terminate()
                try:
                    coord.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    coord.kill()
            # strict full reconciliation only once processing has had a chance
            # to drain every partition
            self._wait_drained()
            final_rc = self._reconcile(allow_incomplete=False)
            rc = rc or final_rc
            print(f"soak: done rc={rc}")
        return rc

    def _wait_drained(self, timeout: float = 60.0) -> None:
        """Poll strict reconciliation until complete or timeout."""
        import json as _json
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            rc = self._reconcile(allow_incomplete=False)
            if rc == 0:
                return
            time.sleep(1.0)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--out", required=True)
    ap.add_argument("--runtime", type=float, default=120.0, help="seconds")
    ap.add_argument("--workers", type=int, default=3)
    ap.add_argument("--partitions", type=int, default=8)
    ap.add_argument("--per-partition", type=int, default=200)
    ap.add_argument("--skew", action="store_true",
                    help="use skewed counts including an empty partition")
    ap.add_argument("--lease-ms", type=int, default=1500)
    ap.add_argument("--batch-size", type=int, default=10)
    ap.add_argument("--churn-every", type=float, default=8.0)
    ap.add_argument("--reconcile-every", type=float, default=15.0)
    args = ap.parse_args()

    if args.skew:
        counts = [args.per_partition] * args.partitions
        counts[0] = 0
        counts[1] = args.per_partition * 3
    else:
        counts = [args.per_partition] * args.partitions

    soak = Soak(args.out, args.partitions, counts, args.workers,
                args.lease_ms, args.batch_size)
    return soak.run(args.runtime, args.churn_every, args.reconcile_every)


if __name__ == "__main__":
    raise SystemExit(main())
