"""Controllable, process-wide unified clock.

All lease/heartbeat timing in the system is expressed against ONE clock
reference, in two modes:

* ``manual``: logical time lives in the coordination database
  (``meta.now_ms``) and only advances when the coordinator ticks (or an admin
  ``tick``). Every process reads the same stored value, so lease/fence timing
  is deterministic and reproducible in tests and the demo.
* ``real``  : every process reads the same wall-clock absolute milliseconds
  (``time.time()*1000``). Because it is an absolute epoch, independent
  processes agree without sharing a base; ``advance`` is a no-op.

A single :class:`Clock` instance per thread is enough; constructing a new one
never resets time in either mode.
"""

from __future__ import annotations

import threading
import time


class Clock:
    def __init__(self, conn, mode: str):
        if mode not in ("manual", "real"):
            raise ValueError(f"unknown clock mode: {mode}")
        self._conn = conn
        self.mode = mode
        self._lock = threading.Condition()
        # ensure the shared manual-clock row exists (race-safe across procs)
        conn.execute("INSERT OR IGNORE INTO meta(key,value) VALUES('now_ms','0')")
        conn.commit()
        row = conn.execute("SELECT value FROM meta WHERE key='now_ms'").fetchone()
        self._now = int(row[0])

    def now_ms(self) -> int:
        if self.mode == "real":
            # absolute wall epoch: identical reference for every process
            return int(time.time() * 1000)
        with self._lock:
            # re-read from the DB: the coordinator (or an admin tick) advances
            # the shared manual clock from another process/connection
            row = self._conn.execute(
                "SELECT value FROM meta WHERE key='now_ms'"
            ).fetchone()
            if row is not None:
                self._now = int(row[0])
            return self._now

    def advance(self, ms: int) -> int:
        """Move the manual clock forward; wake any waiters. Returns new time."""
        if ms < 0:
            raise ValueError("cannot rewind clock")
        if self.mode == "real":
            return self.now_ms()
        with self._lock:
            self._now += ms
            self._conn.execute(
                "UPDATE meta SET value=? WHERE key='now_ms'", (self._now,))
            self._conn.commit()
            self._lock.notify_all()
            return self._now

    def sleep(self, ms: int, stop: "threading.Event | None" = None) -> bool:
        """Sleep for *ms* on this clock. Returns True if it slept fully."""
        if ms <= 0:
            return True
        if self.mode == "real":
            if stop is not None:
                return not stop.wait(timeout=ms / 1000.0)
            time.sleep(ms / 1000.0)
            return True
        deadline = self.now_ms() + ms
        with self._lock:
            while self._now < deadline:
                remaining = (deadline - self._now) / 1000.0
                if stop is not None and stop.wait(timeout=min(remaining, 0.05)):
                    return False
                elif stop is None:
                    self._lock.wait(timeout=min(remaining, 0.05))
            return True
