"""Batched, resumable historical recomputation for one rule version.

The recompute job reads *only* already-committed rows from ``records`` (the
immutable ``source_json`` is the recomputation basis), so it never needs the
original JSONL files and never touches collection progress or the active
version.  Properties required by the publishing contract:

* bounded memory -- one page of source positions at a time;
* short write transactions -- each page's results and its durable watermark
  are persisted together, never in one history-spanning transaction;
* crash-safe resume -- on restart every partition continues at
  ``done_offset + 1`` from ``rule_recompute_progress``; finished pages are
  neither recomputed nor rescanned (keyset pagination on ``(partition,offset)``);
* concurrency -- a sidecar flock admits one recompute process per database;
* controllable pause -- with ``--pause-file`` pointing at an existing file the
  runner computes exactly one page and blocks *before* flushing it (the page
  stays in process memory, nothing is persisted, and the recompute lock stays
  held), so acceptance can stop at "page computed, not persisted"; removing
  the file lets the same process flush the page and continue. A hard kill at
  the pause point likewise leaves the database exactly where it was.
"""

from __future__ import annotations

import fcntl
import json
import os
import sqlite3
import time
from dataclasses import dataclass, field

from .rule_store import build_outcome, load_version


class RecomputeLock:
    def __init__(self, db_path: str):
        self._path = db_path + ".recomputelock"
        self._fh = None

    def acquire(self) -> bool:
        self._fh = open(self._path, "w")
        try:
            fcntl.flock(self._fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            self._fh.close()
            self._fh = None
            return False
        self._fh.write(str(os.getpid()))
        self._fh.flush()
        return True

    def release(self) -> None:
        if self._fh is not None:
            fcntl.flock(self._fh.fileno(), fcntl.LOCK_UN)
            self._fh.close()
            self._fh = None


@dataclass
class PageReport:
    partition_id: int
    start_offset: int
    end_offset: int
    rows: int
    ok: int
    rejected: int
    flushed: bool


@dataclass
class RecomputeReport:
    version_id: str
    pages_flushed: int = 0
    rows_computed: int = 0
    ok: int = 0
    rejected: int = 0
    pages: list[PageReport] = field(default_factory=list)
    coverage: list[dict] = field(default_factory=list)
    complete: bool = False


def _partitions(conn: sqlite3.Connection) -> list[int]:
    # catalog plus anything present in records but not yet catalogued
    rows = conn.execute(
        "SELECT DISTINCT partition_id FROM ("
        " SELECT partition_id FROM partitions "
        " UNION SELECT partition_id FROM records "
        " UNION SELECT partition_id FROM progress) ORDER BY partition_id"
    ).fetchall()
    return [r["partition_id"] for r in rows]


def _watermark(conn: sqlite3.Connection, version_id: str,
               partition_id: int) -> int:
    row = conn.execute(
        "SELECT done_offset FROM rule_recompute_progress "
        "WHERE version_id=? AND partition_id=?",
        (version_id, partition_id),
    ).fetchone()
    return row["done_offset"] if row else -1


def _committed(conn: sqlite3.Connection, partition_id: int) -> int:
    row = conn.execute(
        "SELECT committed_offset FROM progress WHERE partition_id=?",
        (partition_id,),
    ).fetchone()
    return row["committed_offset"] if row else -1


def recompute(conn: sqlite3.Connection, version_id: str, *,
              page_size: int = 200, pause_file: str | None = None,
              pause_wait_s: float = 600.0,
              max_pages: int | None = None,
              progress_cb=None) -> RecomputeReport:
    """Run one catch-up sweep for *version_id* over currently committed rows.

    The sweep ends when every partition's watermark reaches its current
    committed offset.  New rows committed while the sweep runs are picked up
    by the next invocation (or the next sweep of a long-running caller).
    """
    version = load_version(conn, version_id)
    report = RecomputeReport(version_id=version_id)
    pages_done = 0

    while True:
        made_progress = False
        for pid in _partitions(conn):
            watermark = _watermark(conn, version_id, pid)
            committed = _committed(conn, pid)
            if committed <= watermark:
                continue
            rows = conn.execute(
                "SELECT partition_id, offset, source_json FROM records "
                "WHERE partition_id=? AND offset > ? ORDER BY offset LIMIT ?",
                (pid, watermark, page_size),
            ).fetchall()
            if not rows:
                # committed claims offsets that have no rows yet: another
                # writer is mid-transaction; retry on the next sweep
                continue

            # ---- compute the whole page in memory (bounded by page_size) ----
            outcomes = []
            n_ok = n_rej = 0
            for r in rows:
                src = json.loads(r["source_json"])
                status, payload = build_outcome(version, src)
                outcomes.append((r["offset"], status, payload))
                if status == "ok":
                    n_ok += 1
                else:
                    n_rej += 1
            page = PageReport(pid, rows[0]["offset"], rows[-1]["offset"],
                              len(rows), n_ok, n_rej, flushed=False)

            # ---- controllable stop: page computed, nothing persisted. The
            # lock stays held and we block here until the pause file is
            # removed (or a kill arrives), so ingestion can keep advancing
            # while this page is provably not on disk. ----
            if pause_file:
                deadline = time.monotonic() + pause_wait_s
                while os.path.exists(pause_file):
                    if time.monotonic() >= deadline:
                        raise TimeoutError(
                            f"pause file {pause_file!r} still present after "
                            f"{pause_wait_s}s; page p{pid} "
                            f"{page.start_offset}..{page.end_offset} computed "
                            "but not persisted")
                    time.sleep(0.1)

            # ---- results + watermark commit together in a short txn ----
            now = int(time.time() * 1000)
            conn.execute("BEGIN IMMEDIATE")
            try:
                for offset, status, payload in outcomes:
                    if status == "ok":
                        conn.execute(
                            "INSERT INTO rule_results(version_id,partition_id,"
                            "offset,status,result_json,error_json,computed_at) "
                            "VALUES(?,?,?,'ok',?,NULL,?) "
                            "ON CONFLICT(version_id,partition_id,offset) DO "
                            "UPDATE SET status='ok',result_json=excluded.result_json,"
                            "error_json=NULL,computed_at=excluded.computed_at",
                            (version_id, pid, offset, payload, now),
                        )
                    else:
                        conn.execute(
                            "INSERT INTO rule_results(version_id,partition_id,"
                            "offset,status,result_json,error_json,computed_at) "
                            "VALUES(?,?,?,'rejected',NULL,?,?) "
                            "ON CONFLICT(version_id,partition_id,offset) DO "
                            "UPDATE SET status='rejected',result_json=NULL,"
                            "error_json=excluded.error_json,"
                            "computed_at=excluded.computed_at",
                            (version_id, pid, offset,
                             json.dumps(payload, sort_keys=True,
                                        separators=(",", ":"), ensure_ascii=False),
                             now),
                        )
                conn.execute(
                    "INSERT INTO rule_recompute_progress(version_id,partition_id,"
                    "done_offset,updated_at) VALUES(?,?,?,?) "
                    "ON CONFLICT(version_id,partition_id) DO UPDATE SET "
                    "done_offset=excluded.done_offset,updated_at=excluded.updated_at",
                    (version_id, pid, rows[-1]["offset"], now),
                )
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise

            page.flushed = True
            report.pages.append(page)
            report.pages_flushed += 1
            report.rows_computed += page.rows
            report.ok += n_ok
            report.rejected += n_rej
            pages_done += 1
            made_progress = True
            if progress_cb:
                progress_cb(page)
            if max_pages is not None and pages_done >= max_pages:
                report.coverage = _coverage(conn, version_id)
                report.complete = _is_complete(conn, version_id)
                return report

        report.coverage = _coverage(conn, version_id)
        report.complete = _is_complete(conn, version_id)
        if report.complete or not made_progress:
            return report


def _is_complete(conn: sqlite3.Connection, version_id: str) -> bool:
    for pid in _partitions(conn):
        if _committed(conn, pid) > _watermark(conn, version_id, pid):
            return False
    return True


def _coverage(conn: sqlite3.Connection, version_id: str) -> list[dict]:
    out = []
    for pid in _partitions(conn):
        committed = _committed(conn, pid)
        watermark = _watermark(conn, version_id, pid)
        out.append({
            "partition_id": pid,
            "committed_offset": committed,
            "done_offset": watermark,
            "covered": max(0, watermark + 1),
            "total": committed + 1,
        })
    return out


def outcome_counts(conn: sqlite3.Connection, version_id: str) -> dict:
    row = conn.execute(
        "SELECT status, COUNT(*) c FROM rule_results WHERE version_id=? "
        "GROUP BY status", (version_id,),
    ).fetchall()
    counts = {"ok": 0, "rejected": 0}
    for r in row:
        counts[r["status"]] = r["c"]
    return counts
