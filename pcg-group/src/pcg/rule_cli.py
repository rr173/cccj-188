"""Terminal commands for the cleaning-rule publishing workflow.

Subcommands (``python -m pcg rule ...``):

  register    register a rule version from a JSON file
  recompute   batched historical recompute / resumable catch-up
  status      versions, active pointer, publish history, backfill coverage
  review      fixed-section diff snapshot -> persistent review token
  diff        stream a saved review's per-position, per-field differences
  activate    atomically validate a review token and switch the active version
  export      official read: one snapshot section, version id + outcome per row

Exit codes (shared by every rule command):
  0  success
  2  bad arguments / unreadable rule file / database error
  3  rule document invalid at registration time
  4  version conflict (same id, different content; or reserved built-in id)
  5  recompute already running (lock held)
  6  activation refused (stale token, changed section, rejection or hole)
  7  unknown version or review token
  8  recompute pause wait expired (page computed, never persisted)
"""

from __future__ import annotations

import json
import os
import sys
import time

from .clock import Clock
from .db import connect
from .rule_engine import RuleSpecError
from .rule_recompute import (
    RecomputeLock,
    _partitions,
    outcome_counts,
    recompute,
)
from .rule_review import (
    ActivationError,
    create_review,
    get_review,
    iter_review_items,
)
from .rule_review import activate as review_activate
from .rule_export import export_rows, export_section
from .rule_store import (
    RuleConflict,
    UnknownVersion,
    get_active,
    list_versions,
    load_version,
    register_rule,
)


def _fail(msg: str, code: int) -> int:
    print(f"pcg rule: ERROR: {msg}", file=sys.stderr)
    return code


# --------------------------------------------------------------------- register

def cmd_register(db_path: str, clock_mode: str, args) -> int:
    try:
        with open(args.rule_file, encoding="utf-8") as fh:
            spec = json.load(fh)
    except (OSError, json.JSONDecodeError) as exc:
        return _fail(f"cannot read rule file {args.rule_file!r}: {exc}", 2)
    conn = connect(db_path)
    clock = Clock(conn, clock_mode)
    try:
        try:
            created, digest = register_rule(
                conn, args.version_id, spec, clock.now_ms())
        except RuleConflict as exc:
            return _fail(str(exc), 4)
        except RuleSpecError as exc:
            return _fail(f"invalid rule: {exc}", 3)
        conn.commit()
        verb = "registered" if created else "already registered (identical content)"
        print(f"rule {args.version_id}: {verb}")
        print(f"  spec_sha256={digest}")
        return 0
    finally:
        conn.close()


# ------------------------------------------------------------------- recompute

def cmd_recompute(db_path: str, clock_mode: str, args) -> int:
    lock = RecomputeLock(db_path)
    if not lock.acquire():
        return _fail("another recompute process is already running", 5)
    conn = connect(db_path)
    clock = Clock(conn, clock_mode)
    try:
        try:
            load_version(conn, args.version_id)
        except UnknownVersion as exc:
            return _fail(str(exc), 7)

        def cb(page):
            state = "PAUSED(not flushed)" if not page.flushed else "flushed"
            print(f"  p{page.partition_id} offsets {page.start_offset}.."
                  f"{page.end_offset}: {page.rows} rows "
                  f"(ok={page.ok} rejected={page.rejected}) {state}", flush=True)

        print(f"recompute {args.version_id}: page_size={args.page_size}"
              + (f" pause_file={args.pause_file}" if args.pause_file else ""))
        try:
            report = recompute(
                conn, args.version_id, page_size=args.page_size,
                pause_file=args.pause_file, pause_wait_s=args.pause_wait_s,
                max_pages=args.max_pages, progress_cb=cb)
        except TimeoutError as exc:
            return _fail(str(exc), 8)
        counts = outcome_counts(conn, args.version_id)
        print(f"  pages_flushed={report.pages_flushed} "
              f"rows_computed={report.rows_computed} "
              f"ok={report.ok} rejected={report.rejected} "
              f"complete={'yes' if report.complete else 'no'}")
        for c in report.coverage:
            print(f"  p{c['partition_id']}: covered {c['covered']}/{c['total']} "
                  f"(watermark offset {c['done_offset']}, committed "
                  f"{c['committed_offset']})")
        print(f"  stored totals for version: ok={counts['ok']} "
              f"rejected={counts['rejected']}")
        return 0
    finally:
        conn.close()
        lock.release()


# ---------------------------------------------------------------------- status

def cmd_status(db_path: str, clock_mode: str, args) -> int:
    conn = connect(db_path)
    clock = Clock(conn, clock_mode)
    now = clock.now_ms()
    state = get_active(conn)
    print(f"clock now_ms={now} ({clock.mode})")
    print(f"active_version={state['active_version']} publish_seq={state['publish_seq']}")
    print("\n== versions ==")
    for v in list_versions(conn):
        tag = " [ACTIVE]" if v["is_active"] else ""
        built = " (built-in)" if v["builtin"] else ""
        print(f"  {v['version_id']}{tag}{built} sha256={v['spec_sha256'][:16]}… "
              f"created_at={v['created_at']}")
    print("\n== publish history ==")
    rows = list(conn.execute("SELECT * FROM rule_publishes ORDER BY seq"))
    if not rows:
        print("  (no publishes yet; active version is the upgrade default)")
    for r in rows:
        print(f"  seq {r['seq']}: {r['version_id']} at {r['at_ms']} ({r['reason']})")
    print("\n== backfill coverage ==")
    versions = [r["version_id"] for r in list_versions(conn)]
    for vid in versions:
        print(f"  version {vid}:")
        for pid in _partitions(conn):
            prog = conn.execute(
                "SELECT committed_offset FROM progress WHERE partition_id=?",
                (pid,)).fetchone()
            committed = prog["committed_offset"] if prog else -1
            wm = conn.execute(
                "SELECT done_offset FROM rule_recompute_progress "
                "WHERE version_id=? AND partition_id=?", (vid, pid)).fetchone()
            done = wm["done_offset"] if wm else -1
            n_ok = conn.execute(
                "SELECT COUNT(*) c FROM rule_results WHERE version_id=? "
                "AND partition_id=? AND status='ok'", (vid, pid)).fetchone()["c"]
            n_rej = conn.execute(
                "SELECT COUNT(*) c FROM rule_results WHERE version_id=? "
                "AND partition_id=? AND status='rejected'",
                (vid, pid)).fetchone()["c"]
            print(f"    p{pid}: committed={committed} watermark={done} "
                  f"ok={n_ok} rejected={n_rej}")
    conn.close()
    return 0


# ---------------------------------------------------------------------- review

def cmd_review(db_path: str, clock_mode: str, args) -> int:
    conn = connect(db_path)
    clock = Clock(conn, clock_mode)
    try:
        try:
            out = create_review(conn, args.candidate, args.base, clock.now_ms(),
                                db_path=db_path)
        except UnknownVersion as exc:
            return _fail(str(exc), 7)
        conn.commit()
    finally:
        conn.close()
    if args.json:
        print(json.dumps(out, sort_keys=True, indent=2))
        return 0
    print(f"review token: {out['token']}")
    print(f"  candidate={out['candidate']} base={out['base']}")
    print(f"  spec_sha256={out['spec_sha256']}")
    print(f"  section publish_seq={out['publish_seq']}")
    print("  offset vector (partition: last committed offset, -1 = empty):")
    for pid, off in out["vector"].items():
        print(f"    p{pid}: {off}")
    s = out["stats"]
    print("  stats:")
    print(f"    positions={s['positions']} same={s['same']} changed={s['changed']}")
    print(f"    candidate_rejected={s['candidate_rejected']} "
          f"uncovered={s['uncovered']} "
          f"base_rejected_candidate_ok={s['base_rejected_candidate_ok']}")
    return 0


# ------------------------------------------------------------------------ diff

def cmd_diff(db_path: str, clock_mode: str, args) -> int:
    conn = connect(db_path)
    try:
        try:
            review = get_review(conn, args.token)
        except ActivationError as exc:
            return _fail(str(exc), 7)
        items = list(iter_review_items(conn, args.token))
        if args.json:
            payload = {
                "token": args.token,
                "candidate": review["version_id"],
                "base": review["base_version"],
                "stats": json.loads(review["stats_json"]),
                "items": [
                    {"partition_id": r["partition_id"], "offset": r["offset"],
                     "kind": r["kind"], "detail": json.loads(r["detail_json"])}
                    for r in items],
            }
            print(json.dumps(payload, sort_keys=True, indent=2))
            return 0
        print(f"review {args.token}: candidate={review['version_id']} "
              f"base={review['base_version']}")
        print(f"stats: {review['stats_json']}")
        if not items:
            print("(no differences, rejections or uncovered positions)")
        for r in items:
            print(f"p{r['partition_id']} offset {r['offset']} [{r['kind']}]")
            detail = json.loads(r["detail_json"])
            if r["kind"] == "changed":
                for f in detail["fields"]:
                    print(f"    field {f['field']!r}:")
                    print(f"      base      : {json.dumps(f['base'], ensure_ascii=False)}")
                    print(f"      candidate : {json.dumps(f['candidate'], ensure_ascii=False)}")
            else:
                print(f"    {json.dumps(detail, ensure_ascii=False)}")
        return 0
    finally:
        conn.close()


# -------------------------------------------------------------------- activate

def cmd_activate(db_path: str, clock_mode: str, args) -> int:
    conn = connect(db_path)
    clock = Clock(conn, clock_mode)
    try:
        try:
            out = review_activate(conn, args.token, clock.now_ms(),
                                  reason=args.reason)
        except ActivationError as exc:
            return _fail(str(exc), 6)
        conn.commit()
    finally:
        conn.close()
    if out["action"] == "noop":
        print(f"no switch needed: {out['message']} (publish_seq={out['publish_seq']})")
    else:
        print(f"ACTIVATED {out['version_id']} at publish_seq={out['publish_seq']}")
    return 0


# ---------------------------------------------------------------------- export

def cmd_export(db_path: str, clock_mode: str, args) -> int:
    conn = connect(db_path)
    try:
        if args.version_id is not None:
            try:
                load_version(conn, args.version_id)
            except UnknownVersion as exc:
                return _fail(str(exc), 7)
        try:
            section = export_section(conn, args.version_id)
        except UnknownVersion as exc:
            return _fail(str(exc), 7)
        if args.json:
            print(json.dumps({"section": section}, sort_keys=True))
            try:
                for row in export_rows(conn, args.version_id):
                    print(json.dumps(row, sort_keys=True, ensure_ascii=False))
            except RuntimeError as exc:
                return _fail(str(exc) + " (run 'rule recompute' to cover it)", 6)
            return 0
        print(f"# export section: version={section['version_id']} "
              f"publish_seq={section['publish_seq']} "
              f"positions={section['positions']}")
        print("# offset vector: " +
              " ".join(f"p{k}={v}" for k, v in
                       sorted(section["offset_vector"].items())))
        try:
            rows = export_rows(conn, args.version_id)
            for row in rows:
                if row["status"] == "ok":
                    print(f"{row['version_id']}\tp{row['partition_id']}:"
                          f"{row['offset']}\tOK\t"
                          f"{json.dumps(row['result'], sort_keys=True, ensure_ascii=False)}")
                else:
                    print(f"{row['version_id']}\tp{row['partition_id']}:"
                          f"{row['offset']}\tREJECTED\t"
                          f"{json.dumps(row['error'], sort_keys=True, ensure_ascii=False)}")
        except RuntimeError as exc:
            return _fail(str(exc) + " (run 'rule recompute' to cover it)", 6)
        return 0
    finally:
        conn.close()


# ------------------------------------------------------------------- argparse

def add_rule_parser(sub) -> None:
    r = sub.add_parser("rule", help="cleaning-rule publishing workflow")
    r.add_argument("--db", default=None, help="coordination/sink SQLite database")
    r.add_argument("--clock", choices=["real", "manual"], default=None,
                   help="shared clock mode (default real)")
    rsub = r.add_subparsers(dest="rule_cmd", required=True)

    rg = rsub.add_parser("register", help="register a rule version from JSON")
    rg.add_argument("version_id")
    rg.add_argument("rule_file")
    rg.set_defaults(rule_fn=cmd_register)

    rc = rsub.add_parser("recompute", help="batched historical recompute / catch-up")
    rc.add_argument("version_id")
    rc.add_argument("--page-size", type=int, default=200)
    rc.add_argument("--pause-file", default=None,
                    help="while this file exists, compute one page and block "
                         "without persisting it (lock held; remove to continue)")
    rc.add_argument("--pause-wait-s", type=float, default=600.0,
                    help="maximum seconds to wait at a pause point (default 600)")
    rc.add_argument("--max-pages", type=int, default=None,
                    help="flush at most N pages then stop (progress is durable)")
    rc.set_defaults(rule_fn=cmd_recompute)

    rs = rsub.add_parser("status", help="versions, pointer, history, coverage")
    rs.set_defaults(rule_fn=cmd_status)

    rv = rsub.add_parser("review", help="fixed-section diff; prints a review token")
    rv.add_argument("candidate")
    rv.add_argument("--base", default=None)
    rv.add_argument("--json", action="store_true")
    rv.set_defaults(rule_fn=cmd_review)

    rd = rsub.add_parser("diff", help="stream saved review differences")
    rd.add_argument("token")
    rd.add_argument("--json", action="store_true")
    rd.set_defaults(rule_fn=cmd_diff)

    ra = rsub.add_parser("activate", help="validate token and switch active version")
    ra.add_argument("token")
    ra.add_argument("--reason", default="operator")
    ra.set_defaults(rule_fn=cmd_activate)

    re_ = rsub.add_parser("export", help="official consistent-snapshot read")
    re_.add_argument("--version-id", default=None,
                     help="export a non-active registered version (section still "
                          "uses current committed offsets)")
    re_.add_argument("--json", action="store_true")
    re_.set_defaults(rule_fn=cmd_export)
