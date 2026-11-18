"""Terminal commands for the source-record correction workflow.

Subcommands (``python -m pcg correct ...``):

  show      original vs current effective view of one committed position
  propose   propose a complete replacement record for a position
  restore   propose a replacement copied from an older revision (undo)
  list      list proposals (optionally filtered by state)
  get       show one proposal
  approve   review-and-effect a pending proposal (one atomic transaction)
  reject    operator-reject a pending proposal
  history   stream the immutable revision chain of one position

Exit codes:
  0  success
  2  bad arguments / unreadable record file / database error
  9  unknown correction id
  10 unknown coordinates (no committed record at the position)
  11 invalid correction content (non-object JSON, illegal reason, no-change)
  12 correction id reused with different content
  13 revision/concurrency conflict (stale expected revision, already decided)
"""

from __future__ import annotations

import json
import sys

from .clock import Clock
from .correct_store import (
    CorrectError,
    RevisionConflict,
    approve,
    collected_object,
    current_revision,
    get_proposal,
    iter_history,
    iter_proposals,
    propose,
    propose_restore,
    reject,
    show_position,
)
from .db import connect


def _fail(err: CorrectError) -> int:
    print(f"pcg correct: ERROR: {err}", file=sys.stderr)
    if isinstance(err, RevisionConflict) and err.actual_revision is not None:
        print(f"pcg correct: actual_revision={err.actual_revision}",
              file=sys.stderr)
    return err.exit_code


def _run(db_path: str, clock_mode: str, fn) -> int:
    conn = connect(db_path)
    clock = Clock(conn, clock_mode)
    try:
        try:
            out = fn(conn, clock)
        except CorrectError as exc:
            conn.rollback()
            return _fail(exc)
        conn.commit()
        return out
    finally:
        conn.close()


def _print_proposal(p, as_json: bool) -> None:
    payload = {
        "correction_id": p["correction_id"],
        "partition_id": p["partition_id"],
        "offset": p["offset"],
        "expected_revision": p["expected_revision"],
        "state": p["state"],
        "reason": p["reason"],
        "prev": json.loads(p["prev_json"]),
        "new": json.loads(p["new_json"]),
        "content_sha256": p["content_sha256"],
        "created_at": p["created_at"],
        "decided_at": p["decided_at"],
        "decided_note": p["decided_note"],
    }
    if as_json:
        print(json.dumps(payload, sort_keys=True, ensure_ascii=False))
        return
    print(f"proposal {payload['correction_id']} [{payload['state']}] "
          f"p{payload['partition_id']} offset {payload['offset']} "
          f"expected_revision={payload['expected_revision']}")
    print(f"  reason: {payload['reason']}")
    print(f"  prev: {json.dumps(payload['prev'], sort_keys=True, ensure_ascii=False)}")
    print(f"  new : {json.dumps(payload['new'], sort_keys=True, ensure_ascii=False)}")
    print(f"  content_sha256={payload['content_sha256']}")
    if payload["decided_at"] is not None:
        print(f"  decided_at={payload['decided_at']} "
              f"note={payload['decided_note']}")


def _read_record_arg(args) -> dict:
    """Parse the --record / --record-file / --record-stdin payload."""
    sources = [args.record is not None, args.record_file is not None,
               args.record_stdin]
    if sum(sources) != 1:
        raise CorrectError(
            "exactly one of --record, --record-file or --record-stdin "
            "is required")
    try:
        if args.record is not None:
            return json.loads(args.record)
        if args.record_file is not None:
            with open(args.record_file, encoding="utf-8") as fh:
                return json.load(fh)
        return json.load(sys.stdin)
    except (OSError, json.JSONDecodeError) as exc:
        raise CorrectError(f"cannot read replacement record: {exc}")


# --------------------------------------------------------------------- commands

def cmd_show(db_path: str, clock_mode: str, args) -> int:
    def go(conn, clock):
        out = show_position(conn, args.partition, args.offset)
        if args.json:
            print(json.dumps(out, sort_keys=True, ensure_ascii=False))
        else:
            print(f"p{out['partition_id']} offset {out['offset']} "
                  f"current_revision={out['current_revision']} "
                  f"correction_seq={out['correction_seq']}")
            print(f"  original  : {json.dumps(out['original'], sort_keys=True, ensure_ascii=False)}")
            print(f"  effective : {json.dumps(out['effective'], sort_keys=True, ensure_ascii=False)}")
            ar = out.get("active_result")
            if ar is None:
                print(f"  active version {out['active_version']}: no result")
            else:
                body = (json.dumps(ar.get("result" if ar["status"] == "ok"
                                        else "error"), sort_keys=True,
                                   ensure_ascii=False))
                print(f"  active version {out['active_version']} "
                      f"(publish_seq={out['publish_seq']}): {ar['status']} "
                      f"source_revision={ar['source_revision']} {body}")
        return 0
    return _run(db_path, clock_mode, go)


def cmd_propose(db_path: str, clock_mode: str, args) -> int:
    def go(conn, clock):
        new_obj = _read_record_arg(args)
        out = propose(conn, args.partition, args.offset,
                      correction_id=args.correction_id,
                      expected_revision=args.expected_revision,
                      new_obj=new_obj, reason=args.reason,
                      now_ms=clock.now_ms())
        p = out["proposal"]
        if args.json:
            print(json.dumps({"created": out["created"],
                              "correction_id": p["correction_id"],
                              "state": p["state"]}, sort_keys=True))
        else:
            verb = "created" if out["created"] else \
                "already exists with identical content (idempotent retry)"
            print(f"proposal {p['correction_id']}: {verb} "
                  f"[{p['state']}] p{p['partition_id']} offset {p['offset']} "
                  f"expected_revision={p['expected_revision']}")
        return 0
    return _run(db_path, clock_mode, go)


def cmd_restore(db_path: str, clock_mode: str, args) -> int:
    def go(conn, clock):
        out = propose_restore(conn, args.partition, args.offset,
                              to_revision=args.to_revision,
                              correction_id=args.correction_id,
                              expected_revision=args.expected_revision,
                              reason=args.reason, now_ms=clock.now_ms())
        p = out["proposal"]
        if args.json:
            print(json.dumps({"created": out["created"],
                              "correction_id": p["correction_id"],
                              "state": p["state"],
                              "restored_object": json.loads(p["new_json"])},
                             sort_keys=True, ensure_ascii=False))
        else:
            verb = "created" if out["created"] else \
                "already exists with identical content (idempotent retry)"
            print(f"restore proposal {p['correction_id']}: {verb} "
                  f"[{p['state']}] p{p['partition_id']} offset {p['offset']} "
                  f"back to revision {args.to_revision}")
        return 0
    return _run(db_path, clock_mode, go)


def cmd_list(db_path: str, clock_mode: str, args) -> int:
    def go(conn, clock):
        rows = iter_proposals(conn, args.state)
        n = 0
        for p in rows:
            _print_proposal(p, args.json)
            n += 1
            if args.limit is not None and n >= args.limit:
                break
        if not args.json and n == 0:
            print("(no proposals)")
        return 0
    return _run(db_path, clock_mode, go)


def cmd_get(db_path: str, clock_mode: str, args) -> int:
    def go(conn, clock):
        _print_proposal(get_proposal(conn, args.correction_id), args.json)
        return 0
    return _run(db_path, clock_mode, go)


def cmd_approve(db_path: str, clock_mode: str, args) -> int:
    def go(conn, clock):
        out = approve(conn, args.correction_id, clock.now_ms())
        if args.json:
            print(json.dumps(out, sort_keys=True))
        else:
            print(f"APPLIED correction {out['correction_id']} as revision "
                  f"{out['revision']} at p{out['partition_id']} "
                  f"offset {out['offset']}")
            print(f"  processed under version {out['rule_version']} "
                  f"(publish_seq={out['publish_seq']}) -> {out['outcome']}")
            print(f"  correction_seq={out['correction_seq']}")
        return 0
    return _run(db_path, clock_mode, go)


def cmd_reject(db_path: str, clock_mode: str, args) -> int:
    def go(conn, clock):
        out = reject(conn, args.correction_id, args.note, clock.now_ms())
        if args.json:
            print(json.dumps(out, sort_keys=True))
        else:
            print(f"REJECTED proposal {out['correction_id']}")
        return 0
    return _run(db_path, clock_mode, go)


def cmd_history(db_path: str, clock_mode: str, args) -> int:
    def go(conn, clock):
        # validates the coordinates (raises UnknownPosition when uncommitted)
        rev = current_revision(conn, args.partition, args.offset)
        original = collected_object(conn, args.partition, args.offset)
        if args.json:
            print(json.dumps({
                "partition_id": args.partition, "offset": args.offset,
                "current_revision": rev, "revision": 0,
                "object": original, "kind": "collected"},
                sort_keys=True, ensure_ascii=False))
        else:
            print(f"p{args.partition} offset {args.offset}: "
                  f"current_revision={rev}")
            print(f"  rev 0 (collected): "
                  f"{json.dumps(original, sort_keys=True, ensure_ascii=False)}")
        n = 0
        for r in iter_history(conn, args.partition, args.offset):
            n += 1
            if args.json:
                print(json.dumps({
                    "partition_id": r["partition_id"], "offset": r["offset"],
                    "revision": r["revision"],
                    "correction_id": r["correction_id"],
                    "prev": json.loads(r["prev_json"]),
                    "new": json.loads(r["new_json"]),
                    "reason": r["reason"],
                    "rule_version": r["rule_version"],
                    "publish_seq": r["publish_seq"],
                    "created_at": r["created_at"], "kind": "correction"},
                    sort_keys=True, ensure_ascii=False))
            else:
                print(f"  rev {r['revision']} correction={r['correction_id']} "
                      f"at {r['created_at']} under {r['rule_version']}"
                      f"(seq {r['publish_seq']})")
                print(f"      reason: {r['reason']}")
                print(f"      prev: {r['prev_json']}")
                print(f"      new : {r['new_json']}")
        if n == 0 and not args.json:
            print("  (no corrections; the collected record is in force)")
        return 0
    return _run(db_path, clock_mode, go)


# ------------------------------------------------------------------- argparse

def add_correct_parser(sub) -> None:
    c = sub.add_parser("correct", help="source-record correction workflow")
    c.add_argument("--db", default=None, help="coordination/sink SQLite database")
    c.add_argument("--clock", choices=["real", "manual"], default=None,
                   help="shared clock mode (default real)")
    csub = c.add_subparsers(dest="correct_cmd", required=True)

    cs = csub.add_parser("show", help="original vs current effective record")
    cs.add_argument("partition", type=int)
    cs.add_argument("offset", type=int)
    cs.add_argument("--json", action="store_true")
    cs.set_defaults(correct_fn=cmd_show)

    cp = csub.add_parser("propose", help="propose a complete replacement record")
    cp.add_argument("partition", type=int)
    cp.add_argument("offset", type=int)
    cp.add_argument("--correction-id", required=True)
    cp.add_argument("--expected-revision", type=int, required=True,
                    help="the current revision the operator saw (0 = collected "
                         "record still in force)")
    cp.add_argument("--reason", required=True)
    cp.add_argument("--record", default=None, help="replacement JSON object")
    cp.add_argument("--record-file", default=None,
                    help="read the replacement JSON object from a file")
    cp.add_argument("--record-stdin", action="store_true",
                    help="read the replacement JSON object from stdin")
    cp.add_argument("--json", action="store_true")
    cp.set_defaults(correct_fn=cmd_propose)

    cr = csub.add_parser("restore", help="propose restoring an older revision "
                                         "(undo appends a new revision)")
    cr.add_argument("partition", type=int)
    cr.add_argument("offset", type=int)
    cr.add_argument("--to-revision", type=int, required=True,
                    help="older revision whose object should be restored "
                         "(0 = the collected record)")
    cr.add_argument("--correction-id", required=True)
    cr.add_argument("--expected-revision", type=int, required=True)
    cr.add_argument("--reason", required=True)
    cr.add_argument("--json", action="store_true")
    cr.set_defaults(correct_fn=cmd_restore)

    cl = csub.add_parser("list", help="list correction proposals")
    cl.add_argument("--state", choices=["pending", "applied", "rejected"],
                    default=None)
    cl.add_argument("--limit", type=int, default=None)
    cl.add_argument("--json", action="store_true")
    cl.set_defaults(correct_fn=cmd_list)

    cg = csub.add_parser("get", help="show one proposal")
    cg.add_argument("correction_id")
    cg.add_argument("--json", action="store_true")
    cg.set_defaults(correct_fn=cmd_get)

    ca = csub.add_parser("approve", help="effect a pending proposal atomically")
    ca.add_argument("correction_id")
    ca.add_argument("--json", action="store_true")
    ca.set_defaults(correct_fn=cmd_approve)

    cj = csub.add_parser("reject", help="operator-reject a pending proposal")
    cj.add_argument("correction_id")
    cj.add_argument("--note", default=None)
    cj.add_argument("--json", action="store_true")
    cj.set_defaults(correct_fn=cmd_reject)

    ch = csub.add_parser("history", help="revision chain of one position")
    ch.add_argument("partition", type=int)
    ch.add_argument("offset", type=int)
    ch.add_argument("--json", action="store_true")
    ch.set_defaults(correct_fn=cmd_history)
