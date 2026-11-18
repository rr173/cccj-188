"""Batched, resumable historical recomputation for one rule version.

The recompute job reads *only* already-committed rows from ``records`` plus
the current correction pointers, so it never needs the original JSONL files
and never touches collection progress or the active version.  Each position
is evaluated against its **current effective object**: the immutable
``source_json`` for uncorrected positions, or the newest correction revision's
complete replacement object.  Properties required by the publishing contract:

* bounded memory -- one page of source positions at a time;
* short write transactions -- each page's results and its durable watermark
  are persisted together, never in one history-spanning transaction;
* crash-safe resume -- on restart every partition continues at
  ``done_offset + 1`` from ``rule_recompute_progress``; finished pages are
  neither recomputed nor rescanned (keyset pagination on ``(partition,offset)``);
* correction catch-up -- positions whose stored result predates the current
  source revision (``rule_results.source_revision`` older than
  ``correction_state.revision``) are reprocessed in a separate keyset-paged
  sweep, so a version that was inactive while corrections landed becomes
  current again through the same resumable contract;
* flush-time revalidation -- every page's source revisions are re-read inside
  the flush transaction and changed positions recomputed on the spot, so a
  flushed page is never born stale;
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
    kind: str = "new"          # 'new' (watermark advance) or 'stale' refresh
    revalidated: int = 0       # outcomes recomputed at flush time


@dataclass
class RecomputeReport:
    version_id: str
    pages_flushed: int = 0
    rows_computed: int = 0
    ok: int = 0
    rejected: int = 0
    refreshed: int = 0         # stale (corrected) positions reprocessed
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


def _effective_page(conn: sqlite3.Connection, partition_id: int,
                    after_offset: int, page_size: int):
    """One page of committed rows joined with their current correction.

    Returns rows of ``(offset, source_revision, effective_json)`` where the
    effective JSON is the newest correction's replacement object when the
    position is corrected, else the immutable ``records.source_json``.
    """
    return conn.execute(
        "SELECT r.offset, r.source_json, "
        "COALESCE(cs.revision, 0) AS rev, sc.new_json AS corr_json "
        "FROM records r "
        "LEFT JOIN correction_state cs ON cs.partition_id=r.partition_id "
        "  AND cs.offset=r.offset "
        "LEFT JOIN source_corrections sc ON sc.partition_id=cs.partition_id "
        "  AND sc.offset=cs.offset AND sc.revision=cs.revision "
        "WHERE r.partition_id=? AND r.offset>? ORDER BY r.offset LIMIT ?",
        (partition_id, after_offset, page_size),
    ).fetchall()


def _stale_page(conn: sqlite3.Connection, version_id: str,
                after: tuple[int, int], page_size: int):
    """One page of positions whose stored result predates the current source
    revision (keyset-paginated over ``correction_state``)."""
    return conn.execute(
        "SELECT cs.partition_id, cs.offset, cs.revision AS rev, "
        "sc.new_json AS corr_json "
        "FROM correction_state cs "
        "JOIN source_corrections sc ON sc.partition_id=cs.partition_id "
        "  AND sc.offset=cs.offset AND sc.revision=cs.revision "
        "JOIN rule_results rr ON rr.version_id=? "
        "  AND rr.partition_id=cs.partition_id AND rr.offset=cs.offset "
        "WHERE rr.source_revision < cs.revision "
        "  AND (cs.partition_id > ? OR (cs.partition_id = ? AND cs.offset > ?)) "
        "ORDER BY cs.partition_id, cs.offset LIMIT ?",
        (version_id, after[0], after[0], after[1], page_size),
    ).fetchall()


def _revalidate(conn: sqlite3.Connection, version, outcomes,
                partition_id: int) -> int:
    """Re-read source revisions inside the flush transaction.

    ``outcomes`` is a list of ``(offset, revision, status, payload)`` computed
    before the write transaction.  Any position whose current revision changed
    meanwhile is recomputed on the spot from its fresh effective object, so a
    flushed page always reflects the revision set of its own commit.  Returns
    the number of recomputed positions.
    """
    changed = 0
    for i, (offset, rev, _status, _payload) in enumerate(outcomes):
        row = conn.execute(
            "SELECT COALESCE((SELECT revision FROM correction_state "
            "WHERE partition_id=? AND offset=?), 0) AS rev",
            (partition_id, offset),
        ).fetchone()
        cur_rev = row["rev"]
        if cur_rev == rev:
            continue
        if cur_rev == 0:
            obj = json.loads(conn.execute(
                "SELECT source_json FROM records WHERE partition_id=? "
                "AND offset=?", (partition_id, offset)).fetchone()[0])
        else:
            obj = json.loads(conn.execute(
                "SELECT new_json FROM source_corrections WHERE partition_id=? "
                "AND offset=? AND revision=?",
                (partition_id, offset, cur_rev)).fetchone()[0])
        status, payload = build_outcome(version, obj)
        outcomes[i] = (offset, cur_rev, status, payload)
        changed += 1
    return changed


def _flush(conn: sqlite3.Connection, version_id: str, partition_id: int,
           outcomes, watermark: int | None, now: int) -> None:
    """Persist one page's outcomes (+ optional watermark) in one short txn."""
    for offset, rev, status, payload in outcomes:
        if status == "ok":
            conn.execute(
                "INSERT INTO rule_results(version_id,partition_id,offset,"
                "status,result_json,error_json,computed_at,source_revision) "
                "VALUES(?,?,?,'ok',?,NULL,?,?) "
                "ON CONFLICT(version_id,partition_id,offset) DO "
                "UPDATE SET status='ok',result_json=excluded.result_json,"
                "error_json=NULL,computed_at=excluded.computed_at,"
                "source_revision=excluded.source_revision",
                (version_id, partition_id, offset, payload, now, rev),
            )
        else:
            conn.execute(
                "INSERT INTO rule_results(version_id,partition_id,offset,"
                "status,result_json,error_json,computed_at,source_revision) "
                "VALUES(?,?,?,'rejected',NULL,?,?,?) "
                "ON CONFLICT(version_id,partition_id,offset) DO "
                "UPDATE SET status='rejected',result_json=NULL,"
                "error_json=excluded.error_json,"
                "computed_at=excluded.computed_at,"
                "source_revision=excluded.source_revision",
                (version_id, partition_id, offset,
                 json.dumps(payload, sort_keys=True,
                            separators=(",", ":"), ensure_ascii=False),
                 now, rev),
            )
    if watermark is not None:
        conn.execute(
            "INSERT INTO rule_recompute_progress(version_id,partition_id,"
            "done_offset,updated_at) VALUES(?,?,?,?) "
            "ON CONFLICT(version_id,partition_id) DO UPDATE SET "
            "done_offset=excluded.done_offset,updated_at=excluded.updated_at",
            (version_id, partition_id, watermark, now),
        )


def recompute(conn: sqlite3.Connection, version_id: str, *,
              page_size: int = 200, pause_file: str | None = None,
              pause_wait_s: float = 600.0,
              max_pages: int | None = None,
              progress_cb=None) -> RecomputeReport:
    """Run one catch-up sweep for *version_id* over currently committed rows.

    Phase A advances each partition's watermark over newly committed offsets
    (evaluating every position's *current effective object*); phase B
    reprocesses positions whose stored result predates a source correction.
    The sweep ends when every watermark reaches its committed offset and no
    stale results remain.  Work committed or corrected while the sweep runs is
    picked up by the next loop iteration (or the next invocation).
    """
    version = load_version(conn, version_id)
    report = RecomputeReport(version_id=version_id)
    pages_done = 0

    def emit(page: PageReport) -> bool:
        """Account a flushed page; returns False when max_pages is reached."""
        nonlocal pages_done
        report.pages.append(page)
        report.pages_flushed += 1
        report.rows_computed += page.rows
        report.ok += page.ok
        report.rejected += page.rejected
        pages_done += 1
        if progress_cb:
            progress_cb(page)
        return max_pages is None or pages_done < max_pages

    while True:
        made_progress = False

        # ---- phase A: advance watermarks over newly committed offsets ----
        for pid in _partitions(conn):
            watermark = _watermark(conn, version_id, pid)
            committed = _committed(conn, pid)
            if committed <= watermark:
                continue
            rows = _effective_page(conn, pid, watermark, page_size)
            if not rows:
                # committed claims offsets that have no rows yet: another
                # writer is mid-transaction; retry on the next sweep
                continue

            # ---- compute the whole page in memory (bounded by page_size) ----
            outcomes = []
            n_ok = n_rej = 0
            for r in rows:
                src = json.loads(r["corr_json"] if r["rev"] else r["source_json"])
                status, payload = build_outcome(version, src)
                outcomes.append((r["offset"], r["rev"], status, payload))
                if status == "ok":
                    n_ok += 1
                else:
                    n_rej += 1
            page = PageReport(pid, rows[0]["offset"], rows[-1]["offset"],
                              len(rows), n_ok, n_rej, flushed=False,
                              kind="new")

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
                page.revalidated = _revalidate(conn, version, outcomes, pid)
                _flush(conn, version_id, pid, outcomes, rows[-1]["offset"], now)
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise

            page.flushed = True
            made_progress = True
            if not emit(page):
                report.coverage = _coverage(conn, version_id)
                report.complete = _is_complete(conn, version_id)
                return report

        # ---- phase B: refresh results that predate source corrections ----
        cursor = (-1, -1)
        while True:
            stale = _stale_page(conn, version_id, cursor, page_size)
            if not stale:
                break
            outcomes = []
            n_ok = n_rej = 0
            for r in stale:
                src = json.loads(r["corr_json"])
                status, payload = build_outcome(version, src)
                outcomes.append((r["offset"], r["rev"], status, payload))
                if status == "ok":
                    n_ok += 1
                else:
                    n_rej += 1
            now = int(time.time() * 1000)
            conn.execute("BEGIN IMMEDIATE")
            try:
                # revalidate per position against its own partition
                by_pid: dict[int, list] = {}
                for r, oc in zip(stale, outcomes):
                    by_pid.setdefault(r["partition_id"], []).append(oc)
                revalidated = 0
                for pid, pid_outcomes in by_pid.items():
                    revalidated += _revalidate(conn, version, pid_outcomes, pid)
                for pid, pid_outcomes in by_pid.items():
                    _flush(conn, version_id, pid, pid_outcomes, None, now)
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise
            page = PageReport(stale[0]["partition_id"], stale[0]["offset"],
                              stale[-1]["offset"], len(stale), n_ok, n_rej,
                              flushed=True, kind="stale",
                              revalidated=revalidated)
            report.refreshed += len(stale)
            made_progress = True
            cursor = (stale[-1]["partition_id"], stale[-1]["offset"])
            if not emit(page):
                report.coverage = _coverage(conn, version_id)
                report.complete = _is_complete(conn, version_id)
                return report

        report.coverage = _coverage(conn, version_id)
        report.complete = _is_complete(conn, version_id)
        if report.complete or not made_progress:
            return report


def _stale_count(conn: sqlite3.Connection, version_id: str) -> int:
    """Positions whose stored result predates the current source revision."""
    return conn.execute(
        "SELECT COUNT(*) c FROM correction_state cs JOIN rule_results rr "
        "ON rr.version_id=? AND rr.partition_id=cs.partition_id "
        "AND rr.offset=cs.offset WHERE rr.source_revision < cs.revision",
        (version_id,),
    ).fetchone()["c"]


def _is_complete(conn: sqlite3.Connection, version_id: str) -> bool:
    for pid in _partitions(conn):
        if _committed(conn, pid) > _watermark(conn, version_id, pid):
            return False
    return _stale_count(conn, version_id) == 0


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
