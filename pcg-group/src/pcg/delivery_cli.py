"""Terminal commands for durable change delivery (persistent consumer group).

Subcommands (``python -m pcg delivery ...``):

  create    create a named consumer (registered; no boundary pinned yet)
  start     pin the consistent initial-snapshot boundary and begin sync
  read      stream one page (keyset; re-read before ack replays the same page)
  ack       acknowledge the outstanding page with its cursor
  replay    re-read the unacknowledged page (same records and cursor)
  position  show boundary, confirmed cursor, outstanding page and lag
  list      list all named consumers and their positions
  batches   inspect the logical delivery batches (ingest/correction/publish)
  rejects   audit trail of invalid ack attempts

Exit codes:
  0  success
  2  bad arguments / database error
  20 generic delivery invariant failure (e.g. corrupted page or log)
  21 unknown consumer
  22 consumer id already exists
  23 initial sync not started (or consumer not readable yet)
  24 no outstanding page to replay/ack
  25 ack rejected (stale, skip-ahead, cross-consumer, content changed)
"""

from __future__ import annotations

import json
import sys

from .clock import Clock
from .db import connect
from .delivery_store import (
    AckRejected,
    ConsumerExists,
    ConsumerNotReady,
    DeliveryError,
    NoOutstandingPage,
    UnknownConsumer,
    ack_page,
    create_consumer,
    get_consumer,
    list_consumers,
    position,
    read_page,
    start_sync,
)


def _fail(msg: str, code: int) -> int:
    print(f"pcg delivery: ERROR: {msg}", file=sys.stderr)
    return code


def _print_page_text(page: dict) -> None:
    print(f"consumer {page['consumer_id']} state={page['state']} "
          f"boundary_seq={page['boundary_seq']}")
    complete = "BATCH-COMPLETE" if page.get("batch_complete") else "batch-chunk"
    print(f"page {page['from_seq']}..{page['to_seq']} "
          f"events={len(page['events'])} batch={page.get('batch_id')} "
          f"[{complete}] "
          f"{'[REPLAY]' if page['replayed'] else '[NEW]'}"
          f"{' [SNAPSHOT]' if page['snapshot_page'] else ''}"
          f"{' [BOUNDARY-PAGE]' if page['boundary_page'] else ''}")
    print(f"cursor: {page['cursor']}")
    for e in page["events"]:
        body = json.dumps(e["result"] if e["status"] == "ok" else e["error"],
                          sort_keys=True, ensure_ascii=False)
        print(f"  seq={e['seq']} batch={e['batch_id']} "
              f"p{e['partition_id']}:{e['offset']} "
              f"rev={e['source_revision']} ver={e['rule_version']} "
              f"pub={e['publish_seq']} {e['status'].upper()} {body}")


def _position_text(pos: dict) -> None:
    print(f"consumer {pos['consumer_id']} state={pos['state']}")
    print(f"  initial boundary_seq : {pos['boundary_seq']}")
    print(f"  confirmed acked_seq  : {pos['acked_seq']}")
    print(f"  log head_seq         : {pos['head_seq']}")
    print(f"  pending events       : {pos['pending_events']}")
    op = pos["outstanding_page"]
    if op is None:
        print("  outstanding page     : none (safe to read or shut down)")
    else:
        print(f"  outstanding page     : {op['from_seq']}..{op['to_seq']} "
              f"cursor={op['cursor']} (unacknowledged; replay will repeat it)")


# --------------------------------------------------------------------- commands

def cmd_create(db_path: str, clock_mode: str, args) -> int:
    conn = connect(db_path)
    clock = Clock(conn, clock_mode)
    try:
        try:
            out = create_consumer(conn, args.consumer, clock.now_ms())
        except ConsumerExists as exc:
            return _fail(str(exc), 22)
        conn.commit()
        if args.json:
            print(json.dumps(out, sort_keys=True))
        else:
            print(f"created consumer {out['consumer_id']} "
                  f"state={out['state']} (run 'delivery start' to pin a snapshot)")
        return 0
    finally:
        conn.close()


def cmd_start(db_path: str, clock_mode: str, args) -> int:
    conn = connect(db_path)
    clock = Clock(conn, clock_mode)
    try:
        try:
            out = start_sync(conn, args.consumer, clock.now_ms())
        except UnknownConsumer as exc:
            return _fail(str(exc), 21)
        conn.commit()
        if args.json:
            print(json.dumps(out, sort_keys=True))
        else:
            note = " (already started; boundary unchanged)" if out.get(
                "already_started") else ""
            print(f"consumer {out['consumer_id']}{note}: state={out['state']} "
                  f"boundary_seq={out['boundary_seq']}")
            print("  initial snapshot = official states reachable at seq <="
                  f" {out['boundary_seq']}; later changes arrive as live events")
        return 0
    finally:
        conn.close()


def cmd_read(db_path: str, clock_mode: str, args) -> int:
    conn = connect(db_path)
    try:
        try:
            page = read_page(conn, args.consumer, args.page_size,
                             Clock(conn, clock_mode).now_ms())
        except UnknownConsumer as exc:
            return _fail(str(exc), 21)
        except ConsumerNotReady as exc:
            return _fail(str(exc), 23)
        except DeliveryError as exc:
            return _fail(str(exc), 20)
        conn.commit()
        if args.json:
            print(json.dumps(page, sort_keys=True, ensure_ascii=False))
        else:
            if not page["events"]:
                print(f"consumer {args.consumer}: caught up at "
                      f"acked_seq={page['to_seq']} (head_seq={page['head_seq']}); "
                      "no outstanding page")
            else:
                _print_page_text(page)
        return 0
    finally:
        conn.close()


def cmd_ack(db_path: str, clock_mode: str, args) -> int:
    conn = connect(db_path)
    clock = Clock(conn, clock_mode)
    try:
        try:
            out = ack_page(conn, args.consumer, args.cursor, clock.now_ms())
        except UnknownConsumer as exc:
            return _fail(str(exc), 21)
        except NoOutstandingPage as exc:
            return _fail(str(exc), 24)
        except AckRejected as exc:
            return _fail(str(exc), 25)
        conn.commit()
        if args.json:
            print(json.dumps(out, sort_keys=True))
        else:
            print(f"ACK consumer {out['consumer_id']}: acked_seq="
                  f"{out['acked_seq']} state={out['state']} "
                  f"(head_seq={out['head_seq']})")
        return 0
    finally:
        conn.close()


def cmd_replay(db_path: str, clock_mode: str, args) -> int:
    conn = connect(db_path)
    try:
        try:
            consumer = get_consumer(conn, args.consumer)
        except UnknownConsumer as exc:
            return _fail(str(exc), 21)
        if consumer["page_cursor"] is None:
            return _fail(
                f"consumer {args.consumer!r} has no unacknowledged page; "
                "nothing to replay", 24)
        try:
            # read_page returns the pinned page unchanged when one is open
            page = read_page(conn, args.consumer, args.page_size,
                             Clock(conn, clock_mode).now_ms())
        except ConsumerNotReady as exc:
            return _fail(str(exc), 23)
        conn.commit()
        if args.json:
            print(json.dumps(page, sort_keys=True, ensure_ascii=False))
        else:
            _print_page_text(page)
        return 0
    finally:
        conn.close()


def cmd_position(db_path: str, clock_mode: str, args) -> int:
    conn = connect(db_path)
    try:
        try:
            pos = position(conn, args.consumer)
        except UnknownConsumer as exc:
            return _fail(str(exc), 21)
        if args.json:
            print(json.dumps(pos, sort_keys=True))
        else:
            _position_text(pos)
        return 0
    finally:
        conn.close()


def cmd_list(db_path: str, clock_mode: str, args) -> int:
    conn = connect(db_path)
    try:
        rows = list_consumers(conn)
        if args.json:
            print(json.dumps([dict(r) for r in rows], sort_keys=True))
            return 0
        if not rows:
            print("(no delivery consumers)")
        for r in rows:
            print(f"{r['consumer_id']}\tstate={r['state']}\t"
                  f"boundary={r['boundary_seq']}\tacked={r['acked_seq']}\t"
                  f"outstanding={'none' if r['page_cursor'] is None else r['page_cursor']}")
        return 0
    finally:
        conn.close()


def cmd_batches(db_path: str, clock_mode: str, args) -> int:
    conn = connect(db_path)
    try:
        rows = conn.execute(
            "SELECT b.*, MIN(e.seq) first_seq, MAX(e.seq) last_seq "
            "FROM delivery_batches b LEFT JOIN delivery_events e "
            "ON e.batch_id=b.batch_id GROUP BY b.batch_id "
            "ORDER BY b.batch_id DESC LIMIT ?", (args.limit,)).fetchall()
        if args.json:
            print(json.dumps([dict(r) for r in rows], sort_keys=True))
            return 0
        if not rows:
            print("(no delivery batches yet)")
        for r in rows:
            print(f"batch {r['batch_id']} [{r['kind']}] seq {r['first_seq']}.."
                  f"{r['last_seq']} ({r['event_count']} events) "
                  f"ver={r['rule_version']} pub={r['publish_seq']} "
                  f"ref={r['producer_ref']}")
        return 0
    finally:
        conn.close()


def cmd_rejects(db_path: str, clock_mode: str, args) -> int:
    conn = connect(db_path)
    try:
        rows = conn.execute(
            "SELECT * FROM delivery_ack_rejects ORDER BY id DESC LIMIT ?",
            (args.limit,)).fetchall()
        if args.json:
            print(json.dumps([dict(r) for r in rows], sort_keys=True))
            return 0
        if not rows:
            print("(no rejected acknowledgements)")
        for r in rows:
            print(f"#{r['id']} at={r['at_ms']} consumer={r['consumer_id']} "
                  f"cursor={r['cursor']}")
            print(f"    reason: {r['reason']}")
        return 0
    finally:
        conn.close()


# ------------------------------------------------------------------- argparse

def add_delivery_parser(sub) -> None:
    d = sub.add_parser("delivery",
                       help="durable change delivery / persistent consumers")
    d.add_argument("--db", default=None, help="coordination/sink SQLite database")
    d.add_argument("--clock", choices=["real", "manual"], default=None,
                   help="shared clock mode (default real)")
    dsub = d.add_subparsers(dest="delivery_cmd", required=True)

    dc = dsub.add_parser("create", help="create a named consumer")
    dc.add_argument("consumer")
    dc.add_argument("--json", action="store_true")
    dc.set_defaults(delivery_fn=cmd_create)

    ds = dsub.add_parser("start", help="pin initial-snapshot boundary, start sync")
    ds.add_argument("consumer")
    ds.add_argument("--json", action="store_true")
    ds.set_defaults(delivery_fn=cmd_start)

    dr = dsub.add_parser("read", help="read one page (re-read replays if unacked)")
    dr.add_argument("consumer")
    dr.add_argument("--page-size", type=int, default=100)
    dr.add_argument("--json", action="store_true")
    dr.set_defaults(delivery_fn=cmd_read)

    da = dsub.add_parser("ack", help="acknowledge the outstanding page")
    da.add_argument("consumer")
    da.add_argument("cursor")
    da.add_argument("--json", action="store_true")
    da.set_defaults(delivery_fn=cmd_ack)

    dp = dsub.add_parser("replay", help="re-read the unacknowledged page")
    dp.add_argument("consumer")
    dp.add_argument("--page-size", type=int, default=100)
    dp.add_argument("--json", action="store_true")
    dp.set_defaults(delivery_fn=cmd_replay)

    dpo = dsub.add_parser("position", help="show current delivery position")
    dpo.add_argument("consumer")
    dpo.add_argument("--json", action="store_true")
    dpo.set_defaults(delivery_fn=cmd_position)

    dl = dsub.add_parser("list", help="list named consumers")
    dl.add_argument("--json", action="store_true")
    dl.set_defaults(delivery_fn=cmd_list)

    db_ = dsub.add_parser("batches", help="inspect logical delivery batches")
    db_.add_argument("--limit", type=int, default=50)
    db_.add_argument("--json", action="store_true")
    db_.set_defaults(delivery_fn=cmd_batches)

    dj = dsub.add_parser("rejects", help="audit invalid ack attempts")
    dj.add_argument("--limit", type=int, default=50)
    dj.add_argument("--json", action="store_true")
    dj.set_defaults(delivery_fn=cmd_rejects)
