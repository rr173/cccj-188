"""Coordinator process.

Owns all plan/revocation state transitions.  Exactly one coordinator runs at
a time (flock singleton).  Every state transition below happens inside one
``BEGIN IMMEDIATE`` transaction, and all time judgements use the shared
:class:`~pcg.clock.Clock`, never the local wall clock.

Handoff protocol per transferred partition (generation = unique ownership
term):

1. new plan row created; the *old* assignment flips ``active -> revoking`` and
   a revocation with an explicit deadline is published;
2. the old owner stops pulling new data, drains or abandons its in-flight
   batch and writes an ack bound to ``(plan, partition, gen)``;
3a. on a valid ack the revocation becomes ``acked``/assignment ``revoked``;
3b. on deadline (or dead lease) the coordinator *fences* the generation;
4.  only then the new generation flips ``pending -> active`` and the plan is
    finalized -- the new owner receives write rights of a new generation.

Acks carrying an unknown/stale plan or generation are recorded in
``stale_acks`` and can never roll back a newer allocation.  After a restart,
any still-unresolved revocation of the previous incarnation is fenced
immediately (uniformly sealing the old generation instead of trusting an
unprovable lease).
"""

from __future__ import annotations

import os
import signal
import sqlite3
import threading

from .clock import Clock
from .db import CoordinatorLock, connect, tx
from .planner import plan_assignment
from .source import list_partitions

DEFAULT_LEASE_MS = 1000
DEFAULT_ACK_TIMEOUT_MS = 1200


class Coordinator:
    def __init__(self, db_path: str, source_dir: str, clock_mode: str = "manual",
                 lease_ms: int = DEFAULT_LEASE_MS,
                 ack_timeout_ms: int = DEFAULT_ACK_TIMEOUT_MS,
                 tick_ms: int = 100):
        self.db_path = db_path
        self.source_dir = source_dir
        self.clock_mode = clock_mode
        self.lease_ms = lease_ms
        self.ack_timeout_ms = ack_timeout_ms
        self.tick_ms = tick_ms
        self._stop = threading.Event()
        self._lock = CoordinatorLock(db_path)
        self.conn: sqlite3.Connection | None = None
        self.clock: Clock | None = None
        self.epoch = 0

    # ------------------------------------------------------------------ lifecycle
    def start(self) -> None:
        if not self._lock.acquire(timeout=0.0):
            raise RuntimeError("another coordinator is already running")
        self.conn = connect(self.db_path)
        self.clock = Clock(self.conn, self.clock_mode)
        self._init_epoch_and_seal_previous_incarnation()
        signal.signal(signal.SIGTERM, self._on_signal)
        signal.signal(signal.SIGINT, self._on_signal)

    def stop(self) -> None:
        self._stop.set()

    def _on_signal(self, signum, frame):
        self._stop.set()

    def _meta_get(self, key: str, default: int = 0) -> int:
        row = self.conn.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
        return int(row[0]) if row is not None else default

    def _meta_set(self, key: str, value: int) -> None:
        self.conn.execute(
            "INSERT INTO meta(key,value) VALUES(?,?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, str(value)),
        )

    def _init_epoch_and_seal_previous_incarnation(self) -> None:
        """Bump incarnation epoch; fence revocations left unresolved last time."""
        with tx(self.conn):
            now = self.clock.now_ms()
            prev_epoch = self._meta_get("epoch")
            self.epoch = prev_epoch + 1
            self._meta_set("epoch", self.epoch)
            rows = self.conn.execute(
                "SELECT partition_id, gen, worker_id, plan_id FROM revocations "
                "WHERE state='waiting'"
            ).fetchall()
            for r in rows:
                self.conn.execute(
                    "UPDATE revocations SET state='fenced' WHERE partition_id=? AND gen=?",
                    (r["partition_id"], r["gen"]),
                )
                self.conn.execute(
                    "UPDATE assignments SET state='fenced', updated_at=? "
                    "WHERE partition_id=? AND gen=?",
                    (now, r["partition_id"], r["gen"]),
                )
        self._finalize_ready_plans()

    # ------------------------------------------------------------------ main loop
    def run(self, max_ticks: int | None = None) -> None:
        ticks = 0
        while not self._stop.is_set():
            if self.clock_mode == "manual":
                # the coordinator owns manual time; advance it every tick.
                # (We must NOT sleep *on* this clock: that would wait for the
                # very advance that this same loop is responsible for.)
                self.clock.advance(self.tick_ms)
            self.tick()
            ticks += 1
            if max_ticks is not None and ticks >= max_ticks:
                break
            # wall-time pacing only; the logical clock is advanced above
            if self._stop.wait(timeout=self.tick_ms / 1000.0):
                break
        self._lock.release()

    def tick(self) -> None:
        """One deterministic coordination pass (also used directly by tests)."""
        self._register_partitions()
        self._expire_dead_members()
        self._maybe_create_plan()
        self._process_acks()
        self._process_revocations()
        self._finalize_ready_plans()

    def _register_partitions(self) -> None:
        """Keep the partition catalog current so empty partitions (no rows and
        no progress yet) still appear in review/export offset vectors as -1."""
        try:
            partitions = list_partitions(self.source_dir)
        except Exception:
            return
        now = self.clock.now_ms()
        with tx(self.conn):
            for pid in partitions:
                self.conn.execute(
                    "INSERT OR IGNORE INTO partitions(partition_id,first_seen_at)"
                    " VALUES(?,?)", (pid, now))

    # ------------------------------------------------------------------ members
    def _expire_dead_members(self) -> None:
        with tx(self.conn):
            now = self.clock.now_ms()
            # only actively-heartbeating members can lose their lease.
            # 'leaving' members are draining gracefully (SIGTERM): their
            # revocations carry an explicit ack deadline instead.
            self.conn.execute(
                "UPDATE members SET status='dead' "
                "WHERE status='alive' AND ? - last_beat > lease_ms",
                (now,),
            )

    # ------------------------------------------------------------------ planning
    def _latest_plan(self) -> sqlite3.Row | None:
        return self.conn.execute(
            "SELECT * FROM plans ORDER BY plan_id DESC LIMIT 1"
        ).fetchone()

    def _maybe_create_plan(self) -> None:
        with tx(self.conn):
            partitions = list_partitions(self.source_dir)
            now = self.clock.now_ms()
            latest = self._latest_plan()
            if latest is not None and not latest["finalized"]:
                return  # a handoff is still in flight; queue further changes behind it
            alive_rows = self.conn.execute(
                "SELECT worker_id FROM members WHERE status='alive' "
                "ORDER BY worker_id"
            ).fetchall()
            # 'leaving' members are draining out and take no part in a new plan
            alive = [r["worker_id"] for r in alive_rows]
            if not alive:
                return  # nobody to assign yet
            if latest is not None:
                prev_rows = self.conn.execute(
                    "SELECT partition_id, worker_id FROM assignments WHERE plan_id=?",
                    (latest["plan_id"],),
                ).fetchall()
                prev = {r["partition_id"]: r["worker_id"] for r in prev_rows}
                prev_member_csv = latest["member_csv"]
            else:
                prev, prev_member_csv = {}, ""
            new_map = plan_assignment(partitions, alive, prev)
            new_csv = ",".join(alive)
            # stable only if both the assignment map AND the live membership
            # set are unchanged (a member flipping alive->leaving must rebalance)
            if (latest is not None and new_map == prev and new_csv == prev_member_csv
                    and latest["finalized"]):
                return  # stable: no new plan needed, unchanged partitions keep running

            cur = self.conn.execute(
                "INSERT INTO plans(created_at, prev_plan_id, epoch, reason, member_csv) "
                "VALUES(?,?,?,?,?)",
                (now, latest["plan_id"] if latest else None, self.epoch,
                 "membership", new_csv),
            )
            plan_id = cur.lastrowid
            next_gen = self._meta_get("next_gen", 1)
            for p in partitions:
                gen = next_gen
                next_gen += 1
                new_owner = new_map[p]
                old_owner = prev.get(p)

                # Every partition in a new plan gets a FRESH generation.
                #  - brand-new partition: it starts active immediately;
                #  - otherwise the new term stays pending until the previous
                #    term (whether owned by the same worker or another) has
                #    been revoked/acked or fenced. Ownership *retention* is
                #    expressed by the planner keeping the same worker_id, so
                #    committed progress continues seamlessly; the generation
                #    fence is what guarantees two write terms never overlap.
                state = "active" if old_owner is None else "pending"
                self.conn.execute(
                    "INSERT INTO assignments(partition_id,plan_id,worker_id,gen,state,updated_at)"
                    " VALUES(?,?,?,?,?,?)",
                    (p, plan_id, new_owner, gen, state, now),
                )
                if old_owner is not None:
                    old_gen = self.conn.execute(
                        "SELECT gen FROM assignments WHERE partition_id=? AND plan_id=?",
                        (p, latest["plan_id"]),
                    ).fetchone()["gen"]
                    self.conn.execute(
                        "UPDATE assignments SET state='revoking', updated_at=? "
                        "WHERE partition_id=? AND gen=?",
                        (now, p, old_gen),
                    )
                    self.conn.execute(
                        "INSERT INTO revocations(partition_id,gen,plan_id,worker_id,"
                        "issued_at,deadline_at,state) VALUES(?,?,?,?,?,?,'waiting')",
                        (p, old_gen, plan_id, old_owner, now,
                         now + self.ack_timeout_ms),
                    )
            self._meta_set("next_gen", next_gen)

    # ------------------------------------------------------------------ acks
    def _process_acks(self) -> None:
        """Validate waiting acks (written by workers) strictly against the
        revocation registry.  Anything stale is quarantined, never applied."""
        with tx(self.conn):
            now = self.clock.now_ms()
            pending = self.conn.execute(
                "SELECT * FROM rev_acks ORDER BY partition_id, gen"
            ).fetchall()
            for ack in pending:
                self.conn.execute(
                    "DELETE FROM rev_acks WHERE partition_id=? AND gen=?",
                    (ack["partition_id"], ack["gen"]),
                )
                rev = self.conn.execute(
                    "SELECT * FROM revocations WHERE partition_id=? AND gen=?",
                    (ack["partition_id"], ack["gen"]),
                ).fetchone()
                if rev is None:
                    reason = "unknown generation"
                elif rev["plan_id"] != ack["plan_id"]:
                    reason = f"stale plan ack (ack plan {ack['plan_id']}, current {rev['plan_id']})"
                elif rev["worker_id"] != ack["worker_id"]:
                    reason = "ack from non-owner worker"
                elif rev["state"] != "waiting":
                    reason = f"duplicate/late ack after {rev['state']}"
                else:
                    self.conn.execute(
                        "UPDATE revocations SET state='acked' WHERE partition_id=? AND gen=?",
                        (ack["partition_id"], ack["gen"]),
                    )
                    self.conn.execute(
                        "UPDATE assignments SET state='revoked', updated_at=? "
                        "WHERE partition_id=? AND gen=?",
                        (now, ack["partition_id"], ack["gen"]),
                    )
                    continue
                self.conn.execute(
                    "INSERT INTO stale_acks(at_ms,partition_id,gen,plan_id,worker_id,reason)"
                    " VALUES(?,?,?,?,?,?)",
                    (now, ack["partition_id"], ack["gen"], ack["plan_id"],
                     ack["worker_id"], reason),
                )

    # ------------------------------------------------------------------ revocations
    def _process_revocations(self) -> None:
        with tx(self.conn):
            now = self.clock.now_ms()
            waiting = self.conn.execute(
                "SELECT * FROM revocations WHERE state='waiting'"
            ).fetchall()
            for rev in waiting:
                member = self.conn.execute(
                    "SELECT status, last_beat, lease_ms FROM members WHERE worker_id=?",
                    (rev["worker_id"],),
                ).fetchone()
                # a SIGKILLed ('dead') owner fences immediately; a gracefully
                # draining ('leaving') owner keeps its window until the ack
                # deadline rather than being killed by the lease clock.
                lease_dead = (
                    member is None
                    or member["status"] == "dead"
                    or (member["status"] == "alive"
                        and now - member["last_beat"] > member["lease_ms"])
                )
                if lease_dead or now >= rev["deadline_at"]:
                    # explicit fence: the old generation loses write rights even
                    # though no usable ack ever arrived
                    self.conn.execute(
                        "UPDATE revocations SET state='fenced' WHERE partition_id=? AND gen=?",
                        (rev["partition_id"], rev["gen"]),
                    )
                    self.conn.execute(
                        "UPDATE assignments SET state='fenced', updated_at=? "
                        "WHERE partition_id=? AND gen=?",
                        (now, rev["partition_id"], rev["gen"]),
                    )

    def _finalize_ready_plans(self) -> None:
        with tx(self.conn):
            now = self.clock.now_ms()
            pending_plans = self.conn.execute(
                "SELECT * FROM plans WHERE finalized=0 ORDER BY plan_id"
            ).fetchall()
            for plan in pending_plans:
                unresolved = self.conn.execute(
                    "SELECT 1 FROM revocations WHERE plan_id=? AND state='waiting' LIMIT 1",
                    (plan["plan_id"],),
                ).fetchone()
                if unresolved:
                    continue
                # handoff complete for every transferred partition: hand out the
                # new generation's write rights now
                self.conn.execute(
                    "UPDATE assignments SET state='active', updated_at=? "
                    "WHERE plan_id=? AND state='pending'",
                    (now, plan["plan_id"]),
                )
                self.conn.execute(
                    "UPDATE plans SET finalized=1 WHERE plan_id=?", (plan["plan_id"],)
                )
