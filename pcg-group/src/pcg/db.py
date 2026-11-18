"""Database helpers and the cross-process coordinator singleton lock."""

from __future__ import annotations

import fcntl
import os
import sqlite3
from contextlib import contextmanager

from .schema import SCHEMA


def connect(db_path: str) -> sqlite3.Connection:
    # check_same_thread=False: worker processes share one connection between
    # the main loop and the heartbeat thread; SQLite itself serialises writes.
    conn = sqlite3.connect(db_path, timeout=15, isolation_level=None,
                           check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=FULL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=15000")
    return conn


def init_db(db_path: str) -> None:
    """Create schema (idempotent). Safe to run from every command."""
    os.makedirs(os.path.dirname(os.path.abspath(db_path)), exist_ok=True)
    conn = connect(db_path)
    try:
        conn.executescript(SCHEMA)
    finally:
        conn.close()


class CoordinatorLock:
    """An exclusive flock held by exactly one coordinator process.

    The lock lives in a sidecar file so it survives WAL checkpoints and is
    released automatically by the kernel if the coordinator is SIGKILLed.
    """

    def __init__(self, db_path: str):
        self._path = db_path + ".coordlock"
        self._fh = None

    def acquire(self, timeout: float = 0.0) -> bool:
        self._fh = open(self._path, "w")
        flags = fcntl.LOCK_EX if timeout > 0 else fcntl.LOCK_EX | fcntl.LOCK_NB
        import time

        deadline = time.monotonic() + timeout
        while True:
            try:
                fcntl.flock(self._fh.fileno(), flags)
                self._fh.write(str(os.getpid()))
                self._fh.flush()
                return True
            except BlockingIOError:
                if time.monotonic() >= deadline:
                    self._fh.close()
                    self._fh = None
                    return False
                time.sleep(0.05)

    def release(self) -> None:
        if self._fh is not None:
            fcntl.flock(self._fh.fileno(), fcntl.LOCK_UN)
            self._fh.close()
            self._fh = None


@contextmanager
def tx(conn: sqlite3.Connection):
    """Serialised write transaction (SQLite BEGIN IMMEDIATE)."""
    conn.execute("BEGIN IMMEDIATE")
    try:
        yield conn
    except Exception:
        conn.execute("ROLLBACK")
        raise
    else:
        conn.execute("COMMIT")
