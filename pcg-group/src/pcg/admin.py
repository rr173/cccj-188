"""Admin commands -- membership changes and barrier control, from a separate
process (real cross-process IPC via the coordination database).
"""

from __future__ import annotations

import os
import time

from .clock import Clock
from .db import connect


def add_member(db_path: str, worker_id: str, clock_mode: str,
               lease_ms: int = 1000) -> None:
    conn = connect(db_path)
    clock = Clock(conn, clock_mode)
    now = clock.now_ms()
    row = conn.execute("SELECT worker_id FROM members WHERE worker_id=?",
                       (worker_id,)).fetchone()
    if row is None:
        # placeholder; the worker process claims it on startup
        conn.execute(
            "INSERT INTO members(worker_id,pid,started_at,last_beat,lease_ms,status)"
            " VALUES(?,?,?,?,?,'alive')",
            (worker_id, os.getpid(), now, now, lease_ms),
        )
        print(f"admin: member {worker_id} added (placeholder lease {lease_ms}ms)")
    else:
        conn.execute(
            "UPDATE members SET status='alive', lease_ms=?, last_beat=? WHERE worker_id=?",
            (lease_ms, now, worker_id),
        )
        print(f"admin: member {worker_id} reactivated")
    conn.commit()
    conn.close()


def remove_member(db_path: str, worker_id: str, clock_mode: str) -> None:
    conn = connect(db_path)
    clock = Clock(conn, clock_mode)
    now = clock.now_ms()
    row = conn.execute("SELECT status FROM members WHERE worker_id=?",
                       (worker_id,)).fetchone()
    if row is None:
        print(f"admin: no such member {worker_id}")
    else:
        conn.execute(
            "UPDATE members SET status='leaving', last_beat=? WHERE worker_id=?",
            (now, worker_id),
        )
        print(f"admin: member {worker_id} marked leaving; coordinator will rebalance")
    conn.commit()
    conn.close()


def list_members(db_path: str, clock_mode: str) -> None:
    conn = connect(db_path)
    clock = Clock(conn, clock_mode)
    now = clock.now_ms()
    rows = conn.execute("SELECT * FROM members ORDER BY worker_id").fetchall()
    print(f"clock now_ms={now}")
    if not rows:
        print("(no members)")
    for m in rows:
        remaining = m["last_beat"] + m["lease_ms"] - now
        print(f"  {m['worker_id']:<14} pid={m['pid']:<7} {m['status']:<7} "
              f"last_beat={m['last_beat']} lease_remaining={remaining}ms")
    conn.close()


def barrier_release(db_path: str, name: str) -> None:
    conn = connect(db_path)
    cur = conn.execute(
        "UPDATE barrier_events SET released=1 WHERE name=?", (name,)
    )
    conn.commit()
    if cur.rowcount:
        print(f"admin: barrier {name} released")
    else:
        print(f"admin: barrier {name} not found (yet?)")
    conn.close()


def barrier_wait(db_path: str, name: str, timeout: float = 30.0) -> bool:
    """Block until a worker reaches the named barrier."""
    conn = connect(db_path)
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        row = conn.execute(
            "SELECT released FROM barrier_events WHERE name=?", (name,)
        ).fetchone()
        if row is not None:
            print(f"admin: barrier {name} reached"
                  + (" (already released)" if row["released"] else ""))
            conn.close()
            return True
        time.sleep(0.05)
    conn.close()
    print(f"admin: timeout waiting for barrier {name}")
    return False


def advance_clock(db_path: str, ms: int) -> None:
    """Manual clock control (used by tests / the deterministic demo)."""
    conn = connect(db_path)
    clock = Clock(conn, "manual")
    new = clock.advance(ms)
    print(f"admin: manual clock advanced {ms}ms -> now_ms={new}")
    conn.close()
