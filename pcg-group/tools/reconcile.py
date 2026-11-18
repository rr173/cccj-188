#!/usr/bin/env python3
"""Independent reference reconciler.

Re-reads the immutable JSONL source *itself*, recomputes every cleaned value
with the shared pure cleaning function (imported directly, not via worker
state), and compares against the SQLite sink.  This is a full per-offset
audit, not a row count:

* every source (partition, offset) must exist exactly once in ``records``;
* stored ``clean_json`` must equal the reference cleaning of the source line;
* stored ``source_json`` must equal the raw source line;
* ``progress.committed_offset`` must be contiguous (no gaps, no overshoot):
  committed offsets must be exactly 0..N for each partition;
* no extra sink rows may exist beyond the source;
* any recorded source-integrity error fails the run.

Exit code 0 = perfect reconciliation, 1 = mismatch (details printed).
"""

from __future__ import annotations

import argparse
import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from pcg.clean import clean_to_json  # noqa: E402
from pcg.db import connect  # noqa: E402
from pcg.source import iter_records, list_partitions, validate_source  # noqa: E402


def reconcile(db_path: str, source_dir: str, verbose: bool = True,
              expect_complete: bool = True) -> tuple[int, int]:
    """Audit the sink against an independent re-read of the source.

    expect_complete=True  : every source record must already be committed
                            (strict end-state check).
    expect_complete=False : tolerate records not yet processed (a run still in
                            progress); validate that every COMMITTED record is
                            correct and that the committed set is an exact
                            contiguous prefix 0..N matching progress.
    """
    problems: list[str] = []
    conn = connect(db_path)

    for e in conn.execute("SELECT key,value FROM meta WHERE key LIKE 'error:%'"):
        problems.append(f"recorded source error {e['key']}: {e['value']}")

    for msg in validate_source(source_dir):
        problems.append(f"source validation: {msg}")

    total = 0
    committed_total = 0
    parts = list_partitions(source_dir)
    sink_parts = {
        r["partition_id"] for r in conn.execute(
            "SELECT DISTINCT partition_id FROM records")
    }
    if sink_parts - set(parts):
        problems.append(f"sink contains unknown partitions: {sorted(sink_parts - set(parts))}")

    for p in parts:
        sink = {
            r["offset"]: r for r in conn.execute(
                "SELECT offset, source_json, clean_json FROM records "
                "WHERE partition_id=? ORDER BY offset", (p,))
        }
        src_total = 0
        for offset, obj in iter_records(source_dir, p):
            total += 1
            src_total += 1
            row = sink.pop(offset, None)
            if row is None:
                if expect_complete:
                    problems.append(f"p{p} offset {offset}: missing from sink")
                # in partial mode an unprocessed tail is expected
                continue
            committed_total += 1
            ref_clean = clean_to_json(obj)
            if row["clean_json"] != ref_clean:
                problems.append(
                    f"p{p} offset {offset}: clean value mismatch\n"
                    f"  sink={row['clean_json']}\n  ref ={ref_clean}"
                )
            if json.loads(row["source_json"]) != obj:
                problems.append(f"p{p} offset {offset}: source_json mismatch")
        if sink:
            problems.append(f"p{p}: {len(sink)} extra sink offsets: {sorted(sink)[:10]}")

        prog = conn.execute(
            "SELECT * FROM progress WHERE partition_id=?", (p,)
        ).fetchone()
        n_rows = conn.execute(
            "SELECT COUNT(*) c FROM records WHERE partition_id=?", (p,)
        ).fetchone()["c"]
        if prog is None:
            if n_rows:
                problems.append(f"p{p}: {n_rows} rows but no progress row")
            continue
        # rows must form exactly the contiguous prefix 0..committed_offset
        if n_rows != prog["committed_offset"] + 1:
            problems.append(
                f"p{p}: {n_rows} rows but progress claims offset "
                f"{prog['committed_offset']} (non-contiguous commit history)"
            )
        if expect_complete and prog["committed_offset"] != src_total - 1:
            problems.append(
                f"p{p}: progress committed_offset={prog['committed_offset']} "
                f"but source has {src_total} records (last valid offset {src_total - 1})"
            )
        gap = conn.execute(
            "SELECT a.offset+1 AS gap FROM records a "
            "WHERE a.partition_id=? AND NOT EXISTS (SELECT 1 FROM records b "
            "WHERE b.partition_id=a.partition_id AND b.offset=a.offset+1) "
            "AND a.offset < (SELECT MAX(offset) FROM records WHERE partition_id=?)",
            (p, p),
        ).fetchall()
        if gap:
            problems.append(f"p{p}: commit gaps after offsets {[r['gap'] for r in gap]}")

    rejected = conn.execute("SELECT COUNT(*) c FROM rejected_commits").fetchone()["c"]
    stale = conn.execute("SELECT COUNT(*) c FROM stale_acks").fetchone()["c"]
    conn.close()

    if verbose:
        mode = "complete" if expect_complete else "in-progress"
        print(f"reconcile[{mode}]: {committed_total}/{total} source records committed "
              f"across {len(parts)} partitions")
        print(f"  rejected late commits audited: {rejected}")
        print(f"  stale acks quarantined:        {stale}")
        if problems:
            print(f"RESULT: FAIL ({len(problems)} problems)")
            for msg in problems[:50]:
                print("  - " + msg)
            if len(problems) > 50:
                print(f"  ... and {len(problems) - 50} more")
        else:
            print("RESULT: OK — committed offsets, values and progress boundaries match")
    return len(problems), total


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--db", required=True)
    ap.add_argument("--source", required=True)
    ap.add_argument("--allow-incomplete", action="store_true",
                    help="in-progress mode: tolerate uncommitted source records")
    args = ap.parse_args()
    problems, _ = reconcile(args.db, args.source,
                           expect_complete=not args.allow_incomplete)
    return 1 if problems else 0


if __name__ == "__main__":
    raise SystemExit(main())
