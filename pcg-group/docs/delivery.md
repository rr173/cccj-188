# Durable change delivery: persistent cursor consumers

This document describes the change-delivery workflow that lets any number of
independent downstream programs follow the official results of this database
**without repeatedly exporting full data or joining internal tables**.
Everything lives in the same SQLite database as collection, rules and
corrections; there is no separate broker.

A consumer starts from one consistent snapshot of the official state and then
receives only the new or changed official records as durable, ordered events.
The delivery contract is **at-least-once**: a page is repeated until it is
acknowledged, and every event has a stable identifier so the downstream can
apply it idempotently.

## The event model

| Concept | Rule |
|---|---|
| event | one durable row in `delivery_events`, identified by a gap-free global `seq` (1, 2, 3, ...) |
| source position | the stable key `(partition_id, offset)` — a correction or republish is a NEW event at the SAME position, never a new position |
| event payload | the source revision in force, the official cleaning `rule_version`, and either the result or an explicit rejection diagnostic |
| batch | the set of events committed by one producer transaction (`delivery_batches`); kinds: `bootstrap`, `ingest`, `correction`, `publish` |
| cursor | opaque token `from:to:digest` naming one delivered page; presented back to acknowledge it |
| consumer | a named, independent cursor (`delivery_consumers`) with its own snapshot boundary, confirmed position and at most one outstanding page |

### Which changes enter the stream

* **newly collected records** — the worker's fenced commit writes the rows,
  progress and the `ingest` delivery events in one transaction;
* **approved source corrections** — only the approval transaction appends a
  `correction` event (revision +1 at the same position). Pending proposals
  never emit anything;
* **rule publishing** — the activation transaction emits the whole historical
  recompute section as ONE `publish` batch. Candidate recomputes and failed
  activations leave no trace in the stream.

The producer transaction and its delivery events commit together, so there is
never a committed official change without its event, nor an event without its
official state. Rejection diagnostics are first-class events (`status =
'rejected'` with the same error payload the official export shows).

## Consumers

### Lifecycle

1. `create` — register a name. Nothing is pinned yet.
2. `start` — pin the **snapshot boundary**: the current maximum event seq.
   All events `1..boundary` are the consistent initial snapshot; everything
   after it is the live feed. The boundary never moves for this consumer
   (re-starting returns the same value). An empty database pins boundary 0 and
   the consumer is immediately `active`.
3. `read` — fetch the next whole-batch page (or re-fetch the pinned
   outstanding page). Pages covering `seq <= boundary` are snapshot pages; the
   ack that reaches the boundary promotes the consumer to `active`.
4. `ack` — after the downstream has durably applied the page, acknowledge it
   with the page's cursor. The position advances exactly to the page end.
5. `replay` / `position` — inspect or re-read the unacknowledged page; a new
   process after a crash resumes from the persisted position.

```bash
python -m pcg --db g.db delivery create analytics
python -m pcg --db g.db delivery start analytics
python -m pcg --db g.db delivery read analytics --page-size 100 --json
python -m pcg --db g.db delivery ack analytics "<cursor from the page>"
python -m pcg --db g.db delivery position analytics
python -m pcg --db g.db delivery replay analytics --json   # same page, pre-ack
```

Other commands: `delivery list` (all consumers), `delivery batches` (batch
inspection), `delivery rejects` (audit of refused acknowledgements). Every
command accepts `--json` for programmatic use.

### Page and batching rules

* pages are keyset/streamed and **strictly bounded** by `--page-size`: at
  most that many events are ever materialised at once, so neither the full
  business history nor the whole backlog is loaded into memory;
* a page **never mixes batches**: every event in one page carries the same
  `batch_id`. A large batch (for example a rule publish over thousands of
  positions) is delivered as several page-sized *chunks* of that one batch;
* each read returns `batch_id` and a boolean `batch_complete`. Only the final
  chunk of a logical batch has `batch_complete=true`. A consumer therefore
  cannot regard a rule publish (or any multi-page batch) as applied until it
  has processed the chunk that completes it; a confirmation can never be
  recorded past a batch while part of it is still undelivered;
* a small batch (e.g. a one-event correction immediately before a large
  publish) is never merged into a neighbouring batch's page;
* at most one page is outstanding per consumer. Until it is acknowledged,
  every `read`/`replay` returns the identical records and cursor;
* an empty `read` (no events) reports `caught_up=true` and pins no page.

### Acknowledgement validation

An ack is accepted only when the cursor names the consumer's actual
outstanding page: the right consumer id, `from = acked_seq + 1` continuity,
the exact `to` bound and the unchanged token digest. Every other attempt is
refused with a distinct exit code and a durable row in
`delivery_ack_rejects` — the position never silently skips:

| Attempt | Result |
|---|---|
| correct cursor | advances exactly once |
| same cursor again after acking | 24 no outstanding page |
| unknown consumer | 21 |
| read before `start` | 23 |
| forged/unknown bounds (skip ahead, stale, backwards) | 25, audited |
| cursor token of another consumer | 25, audited |
| tampered token digest | 25, audited |

### At-least-once and idempotency

The downstream writes first and acknowledges afterwards. If the process exits
after writing but before acking (or while reading), the next process sees the
same outstanding page via `position`/`replay` and applies it again. Because
the event `seq` and the source position `(partition_id, offset)` are stable,
the downstream applies events idempotently: key the materialised row by
position and overwrite it with the newest `seq`, or deduplicate on `seq`.

### Independent consumers

Consumers never share state. Creating, reading, pausing or acknowledging on
one consumer does not move any other consumer's boundary, acked seq or
outstanding page; two consumers may follow the same stream at different
speeds and each sees every event exactly once per its own cursor.

## Crash, restart and upgrade safety

* all delivery state is in the main SQLite database (WAL, `synchronous=FULL`);
  a process restart loses nothing — the consumer row, boundary, acked seq and
  pinned page are durable;
* producers and delivery events share one transaction, so a crash either
  commits both or neither. A rule activation killed before commit leaves the
  old active version and no publish events; retrying after the crash publishes
  the complete batch;
* the upgrade is idempotent and repeatable. On the **first** upgrade of a
  database that already contains official results, one immutable `bootstrap`
  batch seeds the stream with the current official state of every committed
  position (paged in short transactions), so a consumer created afterwards
  starts from the same snapshot a full export would produce. Databases created
  after the upgrade simply log every change from its producer transaction.
  Re-running the upgrade never rewrites source records, results, progress,
  corrections, active rules or delivery positions.

## Independent verification

`tests/tools_delivery_ref.py` opens the database with the standard library
and imports **none** of the production code. It re-derives, from the raw
tables: gap-free sequencing and tiling batches, per-position revision order,
payload equality against the official active-version rows, batch contracts
(one correction event; publish section equals its review token; no
candidate-only events), consumer cursor continuity and batch-aligned pages,
two independent rebuilds of every boundary snapshot, and the ack-reject audit.

```bash
python3 -I tests/tools_delivery_ref.py --db g.db
```

## Minimal downstream client pattern

```python
import json, subprocess

def follow(db, consumer, page_size=100):
    subprocess.run(["python", "-m", "pcg", "--db", db, "delivery", "create",
                    consumer], check=False)
    subprocess.run(["python", "-m", "pcg", "--db", db, "delivery", "start",
                    consumer], check=True)
    while True:
        page = json.loads(subprocess.run(
            ["python", "-m", "pcg", "--db", db, "delivery", "read", consumer,
             "--page-size", str(page_size), "--json"],
            check=True, capture_output=True, text=True).stdout)
        if not page["events"]:
            wait_for_new_data()          # caught up; poll or sleep, then retry
            continue
        for event in page["events"]:     # idempotent: upsert keyed by position
            upsert_downstream(event)     # overwrite with the newest seq
        checkpoint_downstream()          # durable downstream write
        subprocess.run(["python", "-m", "pcg", "--db", db, "delivery", "ack",
                        consumer, page["cursor"]], check=True)
```

## Tables

| Table | Purpose |
|---|---|
| `delivery_batches` | one row per producer transaction (kind, ref, version, publish seq, exact event count) |
| `delivery_events` | gap-free immutable event stream with payloads and per-event source revision/version |
| `delivery_consumers` | boundary, acked seq, state and the single pinned outstanding page (cursor + digest) |
| `delivery_ack_rejects` | durable audit of every invalid acknowledgement attempt |
