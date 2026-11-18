"""Read-only inspection of the whole group's state.

Shows, in one consistent snapshot per section:

* members and the evidence behind liveness (last heartbeat, lease, deadline);
* plans and, per partition, current vs target ownership, generation,
  assignment/revocation state;
* committed offsets;
* rejected late commits and stale acks (the safety audit trails);
* barrier rendezvous state.
"""

from __future__ import annotations

import json

from .clock import Clock
from .db import connect


def _fmt_age(now: int, t: int) -> str:
    return f"{now - t}ms ago"


def inspect(db_path: str, clock_mode: str = "real", as_json: bool = False) -> dict:
    conn = connect(db_path)
    clock = Clock(conn, clock_mode)
    now = clock.now_ms()
    out: dict = {"now_ms": now}

    members = []
    for m in conn.execute("SELECT * FROM members ORDER BY worker_id"):
        members.append({
            "worker_id": m["worker_id"],
            "pid": m["pid"],
            "status": m["status"],
            "last_beat": m["last_beat"],
            "lease_ms": m["lease_ms"],
            "lease_deadline": m["last_beat"] + m["lease_ms"],
            "lease_remaining_ms": m["last_beat"] + m["lease_ms"] - now,
            "alive_evidence": (
                f"heartbeat {_fmt_age(now, m['last_beat'])}, "
                f"lease {m['lease_ms']}ms, "
                f"deadline in {m['last_beat'] + m['lease_ms'] - now}ms"
            ),
        })
    out["members"] = members

    plans = []
    for p in conn.execute("SELECT * FROM plans ORDER BY plan_id"):
        plans.append({
            "plan_id": p["plan_id"],
            "epoch": p["epoch"],
            "finalized": bool(p["finalized"]),
            "reason": p["reason"],
            "members": p["member_csv"].split(",") if p["member_csv"] else [],
            "created_at": p["created_at"],
        })
    out["plans"] = plans

    latest = conn.execute("SELECT plan_id FROM plans ORDER BY plan_id DESC LIMIT 1").fetchone()
    latest_id = latest["plan_id"] if latest else None
    partitions = []
    for a in conn.execute(
        "SELECT a.*, "
        "  (SELECT r.state FROM revocations r WHERE r.partition_id=a.partition_id "
        "   ORDER BY r.gen DESC LIMIT 1) AS last_rev_state, "
        "  (SELECT r.gen FROM revocations r WHERE r.partition_id=a.partition_id "
        "   ORDER BY r.gen DESC LIMIT 1) AS last_rev_gen "
        "FROM assignments a WHERE a.plan_id=? ORDER BY a.partition_id",
        (latest_id,),
    ) if latest_id is not None else []:
        prev = conn.execute(
            "SELECT worker_id, gen, state FROM assignments "
            "WHERE partition_id=? AND plan_id=(SELECT MAX(plan_id) FROM assignments "
            "WHERE partition_id=? AND plan_id < ?)",
            (a["partition_id"], a["partition_id"], latest_id),
        ).fetchone()
        prog = conn.execute(
            "SELECT * FROM progress WHERE partition_id=?", (a["partition_id"],)
        ).fetchone()
        partitions.append({
            "partition": a["partition_id"],
            "target_worker": a["worker_id"],
            "target_gen": a["gen"],
            "target_state": a["state"],
            "current_worker": prev["worker_id"] if prev else None,
            "current_gen": prev["gen"] if prev else None,
            "current_state": prev["state"] if prev else None,
            "revocation_gen": a["last_rev_gen"],
            "revocation_state": a["last_rev_state"],
            "committed_offset": prog["committed_offset"] if prog else -1,
            "committed_gen": prog["gen"] if prog else None,
            "committed_by": prog["worker_id"] if prog else None,
        })
    out["partitions"] = partitions

    out["rejected_commits"] = [
        dict(r) for r in conn.execute(
            "SELECT * FROM rejected_commits ORDER BY id"
        )
    ]
    out["stale_acks"] = [
        dict(r) for r in conn.execute("SELECT * FROM stale_acks ORDER BY id")
    ]
    out["barriers"] = [
        dict(b) for b in conn.execute("SELECT * FROM barrier_events ORDER BY name")
    ]
    out["meta_errors"] = [
        {"key": r["key"], "value": r["value"]}
        for r in conn.execute("SELECT key,value FROM meta WHERE key LIKE 'error:%'")
    ]
    conn.close()

    if as_json:
        print(json.dumps(out, indent=2, sort_keys=True))
        return out

    print(f"clock now_ms={now} ({clock.mode})")
    print("\n== members (liveness evidence) ==")
    if not members:
        print("  (none)")
    for m in members:
        print(f"  {m['worker_id']:<14} pid={m['pid']:<7} {m['status']:<7} {m['alive_evidence']}")

    print("\n== plans ==")
    if not plans:
        print("  (none)")
    for p in plans:
        flag = "finalized" if p["finalized"] else "IN-PROGRESS"
        print(f"  plan {p['plan_id']} epoch={p['epoch']} {flag} reason={p['reason']} "
              f"members=[{','.join(p['members'])}]")

    print("\n== partitions (current -> target) ==")
    if not partitions:
        print("  (no partitions)")
    for p in partitions:
        cur = (f"{p['current_worker']}(gen {p['current_gen']},{p['current_state']})"
               if p["current_worker"] else "-")
        print(f"  p{p['partition']}: {cur} -> {p['target_worker']}"
              f"(gen {p['target_gen']},{p['target_state']}) "
              f"rev={p['revocation_state'] or '-'} committed={p['committed_offset']}"
              f"@{p['committed_by'] or '-'}")

    print("\n== rejected late commits ==")
    if not out["rejected_commits"]:
        print("  (none)")
    for r in out["rejected_commits"]:
        print(f"  #{r['id']} t={r['at_ms']} {r['worker_id']} p{r['partition_id']} "
              f"gen {r['gen']} off {r['start_offset']}..{r['end_offset']}: {r['reason']}")

    print("\n== stale acks ==")
    if not out["stale_acks"]:
        print("  (none)")
    for r in out["stale_acks"]:
        print(f"  #{r['id']} t={r['at_ms']} {r['worker_id']} p{r['partition_id']} "
              f"gen {r['gen']} plan {r['plan_id']}: {r['reason']}")

    print("\n== barriers ==")
    if not out["barriers"]:
        print("  (none)")
    for b in out["barriers"]:
        state = "released" if b["released"] else "WAITING"
        print(f"  {b['name']}: {state} (reached at {b['reached_at']})")

    if out["meta_errors"]:
        print("\n== ERRORS ==")
        for e in out["meta_errors"]:
            print(f"  {e['key']}: {e['value']}")
    return out
