"""Worker process.

A real OS process (started via ``python -m pcg worker ...``) that talks to the
coordinator exclusively through the shared SQLite database -- there is no
in-process simulation.

Threads inside the process:

* a heartbeat thread that refreshes ``members.last_beat`` against the shared
  clock while the member is ``alive``;
* the single-threaded main loop that observes revocations, reads its
  *assigned* source offsets, runs the deterministic cleaning, optionally
  rendezvous at a barrier *between compute and commit*, and commits through
  the transaction-bound fence in :mod:`pcg.sink`.

Ordering guarantee of one batch:

    read source -> clean (real compute) -> [barrier] -> fenced commit

SIGTERM: stop pulling new work, finish/abandon the current batch, ack every
pending revocation, then exit (drain).  SIGKILL: heartbeats stop and the
coordinator fences the dead generation after the lease.
"""

from __future__ import annotations

import os
import signal
import sqlite3
import threading
import time

from .clean import clean_record
from .clock import Clock
from .db import connect
from .sink import BatchItem, CommitRejected, commit_batch
from .source import SourceError, read_batch

DEFAULT_BATCH = 5
HEARTBEAT_MS = 120
LOOP_MS = 70
BARRIER_POLL_MS = 40
BARRIER_MAX_WAIT_MS = 60_000


class Worker:
    def __init__(self, db_path: str, source_dir: str, worker_id: str,
                 clock_mode: str = "real", lease_ms: int = 1000,
                 batch_size: int = DEFAULT_BATCH):
        self.db_path = db_path
        self.source_dir = source_dir
        self.worker_id = worker_id
        self.lease_ms = lease_ms
        self.batch_size = batch_size
        self._stop = threading.Event()
        self._graceful = False
        self.conn = connect(db_path)
        # serialise use of the shared connection between heartbeat and main threads
        self._dblock = threading.RLock()
        self.clock = Clock(self.conn, clock_mode)
        # (partition, gen) of a batch that has been computed but not yet settled
        self.inflight: tuple[int, int] | None = None
        # generations whose in-flight batch has been fully settled (committed,
        # rejected by the fence, or abandoned on SIGTERM). Such a gen may be
        # acked; a gen that merely lost its term without settlement may not.
        self.settled_gens: set[tuple[int, int]] = set()
        barrier = os.environ.get("PCG_BARRIER", "")
        self.barrier: tuple[str, int, int] | None = None
        if barrier:
            wid, part, start = barrier.split(":")
            self.barrier = (wid, int(part), int(start))

    # ------------------------------------------------------------------ lifecycle
    def register(self) -> None:
        now = self.clock.now_ms()
        row = self.conn.execute(
            "SELECT worker_id, status FROM members WHERE worker_id=?",
            (self.worker_id,),
        ).fetchone()
        if row is None:
            self.conn.execute(
                "INSERT INTO members(worker_id,pid,started_at,last_beat,lease_ms,status)"
                " VALUES(?,?,?,?,?,'alive')",
                (self.worker_id, os.getpid(), now, now, self.lease_ms),
            )
            self.conn.commit()
            return
        # A (re)starting process may only claim a placeholder ('alive' row made
        # by `admin add`) or recover a previously 'dead' member. It must NEVER
        # resurrect a member that is deliberately 'leaving', otherwise an
        # admin remove would be silently undone and handoff/rebalance blocked.
        if row["status"] == "leaving":
            print(f"[{self.worker_id}] sees itself leaving; will not re-register",
                  flush=True)
            self._graceful = True
            self._stop.set()
            return
        self.conn.execute(
            "UPDATE members SET pid=?, started_at=?, last_beat=?, lease_ms=?, "
            "status='alive' WHERE worker_id=? AND status!='leaving'",
            (os.getpid(), now, now, self.lease_ms, self.worker_id),
        )
        self.conn.commit()

    def _heartbeat_loop(self) -> None:
        # dedicated connection + clock for this thread: sharing the main-loop
        # connection across threads causes interleaved-statement errors
        hb_conn = connect(self.db_path)
        hb_clock = Clock(hb_conn, self.clock.mode)
        try:
            while not self._stop.is_set():
                try:
                    now = hb_clock.now_ms()
                    hb_conn.execute(
                        "UPDATE members SET last_beat=? WHERE worker_id=? "
                        "AND status='alive'",
                        (now, self.worker_id),
                    )
                    hb_conn.commit()
                except sqlite3.Error as exc:
                    print(f"[{self.worker_id}] heartbeat error: {exc!r}", flush=True)
                # heartbeats track the shared clock; the wait itself is wall time
                self._interruptible_sleep(HEARTBEAT_MS)
        finally:
            hb_conn.close()

    def _interruptible_sleep(self, ms: int) -> None:
        """Wall-time wait that ends early on SIGTERM.

        The *heartbeat value* always comes from the shared clock (which the
        coordinator advances in manual mode); only the polling wait is wall
        time, capped so signal latency stays small in both clock modes.
        """
        self._stop.wait(timeout=min(ms, 50) / 1000.0)

    def _on_signal(self, signum, frame):
        self._graceful = True
        self._stop.set()

    def install_signals(self) -> None:
        signal.signal(signal.SIGTERM, self._on_signal)
        signal.signal(signal.SIGINT, self._on_signal)
        # restart interrupted syscalls instead of letting the default
        # disposition terminate the process before the Python handler completes
        signal.siginterrupt(signal.SIGTERM, False)
        signal.siginterrupt(signal.SIGINT, False)

    # ------------------------------------------------------------------ barriers
    def _maybe_barrier(self, partition: int, start: int) -> bool:
        """Rendezvous between compute and commit.

        Blocks until an external ``admin barrier-release`` (or SIGTERM). The
        worker deliberately does NOT auto-leave when its generation changes:
        tests must be able to hold a fully-computed, uncommitted batch at this
        exact point while the coordinator revokes/fences the term, then
        release it to prove the late commit is rejected by the fence.
        """
        if self.barrier is None:
            return True
        wid, bpart, bstart = self.barrier
        if wid != self.worker_id or bpart != partition or bstart != start:
            return True
        now = self.clock.now_ms()
        name = f"{self.worker_id}:p{partition}:o{start}"
        self.conn.execute(
            "INSERT OR IGNORE INTO barrier_events(name,worker_id,partition_id,"
            "start_offset,reached_at,released) VALUES(?,?,?,?,?,0)",
            (name, self.worker_id, partition, start, now),
        )
        self.conn.commit()
        deadline = time.monotonic() + BARRIER_MAX_WAIT_MS / 1000.0
        while time.monotonic() < deadline:
            row = self.conn.execute(
                "SELECT released FROM barrier_events WHERE name=?", (name,)
            ).fetchone()
            if row is not None and row["released"]:
                return True
            if self._stop.is_set():
                return False
            time.sleep(BARRIER_POLL_MS / 1000.0)
        raise TimeoutError(f"barrier {name} was never released")

    # ------------------------------------------------------------------ revokes
    def _ack_revocations(self) -> None:
        """Ack waiting revocations addressed to this worker.

        The ack is bound to the revocation's (plan, partition, generation) and
        the coordinator rejects anything not matching its registry.  A
        partition whose batch is currently in flight (read/cleaned but not yet
        settled) is NEVER acked here: the worker must first commit it, abandon
        it, or observe that its term was fenced.  Acking it early would lie
        about draining and let a new write term start while the old batch could
        still be committed.
        """
        now = self.clock.now_ms()
        revs = self.conn.execute(
            "SELECT * FROM revocations WHERE worker_id=? AND state='waiting'",
            (self.worker_id,),
        ).fetchall()
        for rev in revs:
            key = (rev["partition_id"], rev["gen"])
            if key not in self.settled_gens:
                # an unsettled batch still exists (or existed) for this term;
                # claiming drained now would be a lie that could start a new
                # write term before the old batch is resolved.
                continue
            self.conn.execute(
                "INSERT OR REPLACE INTO rev_acks(partition_id,gen,plan_id,worker_id,"
                "acked_at,drained) VALUES(?,?,?,?,?,1)",
                (rev["partition_id"], rev["gen"], rev["plan_id"], self.worker_id,
                 now),
            )
        if revs:
            self.conn.commit()

    # ------------------------------------------------------------------ main loop
    def run(self) -> int:
        self.install_signals()
        hb = threading.Thread(target=self._heartbeat_loop, daemon=True)
        hb.start()
        exit_code = 0
        try:
            while not self._stop.is_set():
                member = self.conn.execute(
                    "SELECT status FROM members WHERE worker_id=?", (self.worker_id,)
                ).fetchone()
                if member is None or member["status"] == "dead":
                    # lease lost: coordinator has fenced / reassigned us
                    break
                if member["status"] == "leaving":
                    # admin asked us to leave: stop pulling, ack what we can,
                    # and exit gracefully so the coordinator can rebalance.
                    self._graceful = True
                    self._stop.set()
                    break

                self._ack_revocations()

                if self._graceful:
                    break  # SIGTERM: no new pulls; revocations already acked above

                assignments = self.conn.execute(
                    "SELECT partition_id, gen FROM assignments "
                    "WHERE worker_id=? AND state='active' ORDER BY partition_id",
                    (self.worker_id,),
                ).fetchall()
                for asg in assignments:
                    if self._stop.is_set():
                        break
                    exit_code = self._process_partition(asg["partition_id"], asg["gen"])
                    if exit_code != 0:
                        self._stop.set()
                        break
                self._interruptible_sleep(LOOP_MS)
        finally:
            # graceful shutdown path: mark leaving and flush any remaining acks
            try:
                self.conn.execute(
                    "UPDATE members SET status='leaving' WHERE worker_id=? AND status='alive'",
                    (self.worker_id,),
                )
                self.conn.commit()
                self._ack_revocations()
            except sqlite3.Error:
                pass
            self._stop.set()
            hb.join(timeout=1.0)
            self.conn.close()
        return exit_code

    def _process_partition(self, partition: int, gen: int) -> int:
        prog = self.conn.execute(
            "SELECT committed_offset FROM progress WHERE partition_id=?",
            (partition,),
        ).fetchone()
        start = (prog["committed_offset"] if prog else -1) + 1
        try:
            raw_batch = read_batch(self.source_dir, partition, start, self.batch_size)
        except SourceError as exc:
            # corrupt / discontinuous source: record it and refuse "safe completion"
            self.conn.execute(
                "INSERT OR REPLACE INTO meta(key,value) VALUES(?,?)",
                (f"error:partition:{partition}", f"source:{exc}"),
            )
            self.conn.commit()
            print(f"[{self.worker_id}] source error p{partition}: {exc}", flush=True)
            return 2
        if not raw_batch:
            return 0  # partition drained; nothing to pull

        # --- real compute happens here, strictly before the commit transaction
        items: list[BatchItem] = []
        for offset, obj, raw_line in raw_batch:
            items.append(BatchItem(offset=offset, source_raw=raw_line,
                                   clean_value=clean_record(obj)))

        self.inflight = (partition, gen)
        key = (partition, gen)
        try:
            released = self._maybe_barrier(partition, start)
            if not released:
                # SIGTERM while parked: still attempt the commit once so the
                # fence produces a real audit row if the term was revoked;
                # either way the batch is settled before we ack anything.
                pass
            try:
                new_off = commit_batch(
                    self.conn, self.clock, partition=partition, gen=gen,
                    worker_id=self.worker_id, items=items,
                )
                print(f"[{self.worker_id}] committed p{partition} "
                      f"offsets {start}..{new_off} (gen {gen})", flush=True)
            except CommitRejected as exc:
                # whole late batch refused; no rows, no progress were written
                print(f"[{self.worker_id}] REJECTED p{partition} gen {gen} "
                      f"offsets {start}..{items[-1].offset}: {exc.reason}", flush=True)
            if not released:
                print(f"[{self.worker_id}] settled p{partition} gen {gen} "
                      f"offsets {start}..{items[-1].offset} during drain", flush=True)
        finally:
            self.inflight = None
            self.settled_gens.add(key)
        return 0
