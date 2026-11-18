"""pcg: partitioned collection consumer group with safe in-flight handoff.

Commands:
  coordinator   run the coordination loop (single instance via flock)
  worker        run a worker process
  admin         membership / barrier / clock control
  inspect       dump the full group state (read-only)
"""

from __future__ import annotations

import argparse
import sys

from . import admin as admin_mod
from .db import init_db
from .inspect import inspect


def _add_common(p: argparse.ArgumentParser) -> None:
    # default None so a value supplied BEFORE the subcommand (top-level) is
    # not silently overwritten; main() merges the two locations.
    p.add_argument("--db", default=None, help="coordination/sink SQLite database")
    p.add_argument("--clock", choices=["real", "manual"], default=None,
                   help="shared clock mode (default real)")


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="pcg", description=__doc__)
    # also accepted at the top level: `pcg --db X --clock manual worker ...`
    p.add_argument("--db", dest="top_db", default=None)
    p.add_argument("--clock", dest="top_clock", choices=["real", "manual"],
                   default=None)
    sub = p.add_subparsers(dest="cmd", required=True)

    c = sub.add_parser("coordinator", help="run the coordinator")
    _add_common(c)
    c.add_argument("--source", required=True)
    c.add_argument("--lease-ms", type=int, default=1000)
    c.add_argument("--ack-timeout-ms", type=int, default=1200)
    c.add_argument("--tick-ms", type=int, default=100)
    c.add_argument("--max-ticks", type=int, default=None,
                   help="exit after N ticks (test use)")

    w = sub.add_parser("worker", help="run a worker process")
    _add_common(w)
    w.add_argument("--source", required=True)
    w.add_argument("--id", required=True, dest="worker_id")
    w.add_argument("--lease-ms", type=int, default=1000)
    w.add_argument("--batch-size", type=int, default=5)

    a = sub.add_parser("admin", help="administrative actions")
    _add_common(a)
    asub = a.add_subparsers(dest="act", required=True)
    aa = asub.add_parser("add")
    aa.add_argument("worker_id")
    aa.add_argument("--lease-ms", type=int, default=1000)
    ar = asub.add_parser("remove")
    ar.add_argument("worker_id")
    asub.add_parser("list")
    ab = asub.add_parser("barrier-release")
    ab.add_argument("name")
    aw = asub.add_parser("barrier-wait")
    aw.add_argument("name")
    aw.add_argument("--timeout", type=float, default=30.0)
    at = asub.add_parser("tick")
    at.add_argument("ms", type=int)

    i = sub.add_parser("inspect", help="read-only state dump")
    _add_common(i)
    i.add_argument("--json", action="store_true")
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    # merge top-level and subcommand locations of the shared options
    args.db = args.db or args.top_db
    args.clock = args.clock or args.top_clock or "real"
    if not args.db:
        print("pcg: --db is required", file=sys.stderr)
        return 2
    init_db(args.db)

    if args.cmd == "coordinator":
        from .coordinator import Coordinator
        coord = Coordinator(args.db, args.source, clock_mode=args.clock,
                            lease_ms=args.lease_ms,
                            ack_timeout_ms=args.ack_timeout_ms,
                            tick_ms=args.tick_ms)
        coord.start()
        coord.run(max_ticks=args.max_ticks)
        return 0

    if args.cmd == "worker":
        from .worker import Worker
        worker = Worker(args.db, args.source, args.worker_id,
                        clock_mode=args.clock, lease_ms=args.lease_ms,
                        batch_size=args.batch_size)
        # install graceful signal handlers BEFORE registration so a SIGTERM
        # arriving during startup still drains rather than hard-killing us
        worker.install_signals()
        worker.register()
        return worker.run()

    if args.cmd == "admin":
        if args.act == "add":
            admin_mod.add_member(args.db, args.worker_id, args.clock,
                                 lease_ms=args.lease_ms)
        elif args.act == "remove":
            admin_mod.remove_member(args.db, args.worker_id, args.clock)
        elif args.act == "list":
            admin_mod.list_members(args.db, args.clock)
        elif args.act == "barrier-release":
            admin_mod.barrier_release(args.db, args.name)
        elif args.act == "barrier-wait":
            return 0 if admin_mod.barrier_wait(args.db, args.name,
                                               timeout=args.timeout) else 1
        elif args.act == "tick":
            admin_mod.advance_clock(args.db, args.ms)
        return 0

    if args.cmd == "inspect":
        inspect(args.db, clock_mode=args.clock, as_json=args.json)
        return 0

    return 2


if __name__ == "__main__":
    sys.exit(main())
