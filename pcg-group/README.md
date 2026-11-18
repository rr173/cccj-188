# pcg — Partitioned Collection Consumer Group with Safe In-Flight Handoff

A real, multi-process consumer group over a read-only, per-partition
zero-based JSONL source, coordinated and sunk through one local SQLite
database. The focus is **safe handoff of a partition that is actively being
processed** — not a mock allocator and not a new message broker.

Two workers can never write the same partition at the same time. A partition
is transferred only after the old owner's write generation is revoked and
either explicitly drained/acked or fenced; only then does the new generation
receive write rights.

## Components (all real OS processes)

| Process      | Role |
|--------------|------|
| `coordinator` | Single instance (flock-guarded). Owns plans, membership expiry, revocations, fences, generation lifecycle. |
| `worker`      | Heartbeats, observes its active assignments, reads assigned source offsets, runs deterministic cleaning, commits through the transaction-bound fence. |
| `admin`       | Separate process: add/remove members, release/wait barriers, advance the manual clock. |
| `inspect`     | Read-only state dump: liveness evidence, current vs target ownership, generations, revocation state, committed offsets, rejected late commits, stale acks. |

The only channels between processes are the shared SQLite database (WAL mode)
and the immutable source files — no in-process simulation.

## The safety model

### Unified clock and leases
* `--clock manual`: logical time lives in `meta.now_ms`, advanced only by the
  coordinator each tick; every process reads the same value. Used for
  deterministic tests and the demo.
* `--clock real`: every process reads the same wall-clock absolute
  milliseconds (`time.time()*1000`), so independent processes agree without a
  shared base. Used by the long-run script.
* Lease expiry is always judged against that one clock, never a local timer.

### Handoff protocol (per transferred partition)
1. A new plan is published. The **old** assignment flips to `revoking` and a
   revocation with an explicit deadline is written; the new generation stays
   `pending`.
2. The old owner stops pulling new data, drains or abandons its in-flight
   batch, and writes an ack **bound to `(plan, partition, generation)`**.
3a. On a matching ack the revocation becomes `acked` / assignment `revoked`.
3b. On deadline or a dead lease the coordinator **fences** the old generation
    instead of waiting forever.
4. Only when no revocation for the plan is still waiting does the new
   generation flip `pending -> active` and the plan finalize.

Acks for an unknown/stale plan or generation are quarantined in `stale_acks`
and can never roll back a newer allocation. On coordinator restart the
incarnation epoch is bumped and any revocation left unresolved by the previous
incarnation is uniformly fenced (conservative sealing rather than trusting an
unprovable lease).

### The commit fence (`src/pcg/sink.py`)
Every sink batch runs `read → clean → (optional barrier) → commit`, and the
commit does **all** of the following inside one `BEGIN IMMEDIATE` transaction:

1. the `(partition, gen)` assignment exists, belongs to this worker, and is
   `active` (a revoked/fenced term can never commit, however long ago it
   computed);
2. the member still holds a valid lease **at commit time**;
3. the batch starts exactly at `committed_offset + 1` (progress continuity);
4. batch offsets are contiguous;
5. none of the offsets already exist as business rows (a plain `INSERT`, so a
   collision is a rejected duplicate — never hidden by an idempotent UPSERT).

On failure the business transaction is rolled back and a `rejected_commits`
audit row is persisted in its own transaction, so a refused late batch leaves
durable evidence. Already-committed source offsets are therefore delivered
exactly once; uncommitted computation may be redone by the next owner.

### Deterministic planning (`src/pcg/planner.py`)
* every partition assigned to exactly one live member;
* per-member partition counts differ by at most one (`q,r = divmod(P,M)`);
* among balanced plans, maximises retention of current ownership;
* explicit tie-breaks: members sorted lexicographically, lowest partition ids
  kept on over-capacity owners, free capacity slots filled in member order.
  Same inputs always produce the same plan.

Unchanged ownership keeps the same worker, so committed progress continues
seamlessly; the generation fence is still what prevents overlapping write
terms.

## Layout

```
src/pcg/            coordinator, worker, admin, inspect, sink, planner, clock, source, clean
tools/gensource.py  generate an immutable JSONL source (never committed)
tools/reconcile.py  independent reference audit (re-reads source, recomputes values)
tools/demo.sh       one-command reproducible terminal demo
tools/soak.py       small-memory long-run with membership churn + reconciliation
tools/run_tests.sh  full automated test suite
tests/              real multi-process end-to-end tests
```

## Quick start

Standard library only (Python 3.11, SQLite >= 3.40). No third-party packages.

```bash
# 1) one-command demo (manual clock, deterministic)
./tools/demo.sh

# 2) automated tests (real processes; ~25s)
./tools/run_tests.sh

# 3) small-memory long run (wall clock; graceful + SIGKILL churn)
python3 tools/soak.py --out /tmp/pcg-soak --runtime 120 --workers 3 \
    --partitions 8 --per-partition 200 --skew
```

Generated sources and databases are written under the `--out` directory and
are git-ignored; large generated data never enters git.

## CLI

```bash
export PYTHONPATH="$PWD/src"
python3 -m pcg --db g.db --clock manual coordinator --source ./srcdir [opts]
python3 -m pcg --db g.db --clock manual worker     --source ./srcdir --id w1
python3 -m pcg --db g.db --clock manual admin add|remove|list|barrier-release|barrier-wait|tick
python3 -m pcg --db g.db --clock manual inspect [--json]
```
The shared `--db/--clock` options are accepted either before or after the
subcommand.

### Barriers (deterministic compute/commit separation)
Set `PCG_BARRIER=worker:partition:start` for a worker; after it has read and
cleaned that batch but before the commit transaction it blocks in
`barrier_events` until `admin barrier-release <name>`. This is what the
headline test uses to prove a fully-computed, uncommitted batch is rejected
after ownership moves.

## What the tests prove (real processes, not mocks)

* **Barrier handoff** — a worker computes offset 0..N but does not commit; the
  partition is transferred; on release the whole late batch is rejected, the
  successor completes it, and the new values are never overwritten.
* **Graceful revoke** — SIGTERM drains with a bound ack.
* **Kill mid-revoke** — SIGKILL with no ack; the coordinator fences the term.
* **Coordinator restart** — epoch bump and uniform sealing of an old term.
* **Late stale ack** — an ack from an old plan is quarantined, never applied.
* **Two back-to-back membership rounds** — converges to a balanced plan.
* **Empty + skewed source** — empty partitions are assigned but produce no
  rows; skew still drains and reconciles.
* **Planner invariants** and **discontinuous-source rejection**.

The independent reconciler (`tools/reconcile.py`) re-reads the source itself
and checks **every** `(partition, offset)`: presence exactly once, raw source,
recomputed clean value, and exact contiguous progress — not just row counts.
`--allow-incomplete` audits the committed prefix while a run is still in
progress.

## Signals and failure semantics
* **SIGTERM** — stop pulling, settle the current batch (commit or take the
  fence rejection), ack revocations, exit gracefully.
* **SIGKILL** — heartbeats stop; after the lease the coordinator marks the
  member dead and fences the generation, then reassigns it.
* Corrupt or discontinuous source is recorded and never reported as safe
  completion.
