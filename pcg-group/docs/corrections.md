# Source-record corrections: propose, review, effect, undo

This document describes the operational workflow for correcting an
**already-committed** source record — e.g. upstream confirms that a customer
record's city was mistyped — without rewriting any collection fact.
Everything lives in the existing SQLite database; the read-only JSONL files
are never touched.

## What a correction is (and is not)

A correction is a **complete replacement object** for one committed
`(partition_id, offset)` position, effected as an **appended, immutable
revision**. It never rewrites:

* `records.source_json` / `records.clean_json` (the collected facts),
* the original JSONL files,
* `progress` (collection progress),
* other rule versions' already-computed `rule_results`.

The originally collected record is always recoverable: it is revision 0 of
the position, and the official read of every historical revision is one
query away (`correct history`).

### The revision model

| Concept | Rule |
|---|---|
| revision 0 | the originally collected `records.source_json` |
| revision N | the Nth effected correction at that position (`source_corrections`) |
| numbering | per-position, strictly increasing from 1, never reused |
| revision row | coordinates, revision, correction id, **complete** prev/new JSON objects, reason, active rule version + publish seq at effect time, timestamp |
| head pointer | `correction_state(partition_id, offset)` -> current revision (absent = 0) |
| global sequence | `meta.correction_seq`, bumped by exactly 1 per effected correction |

The **effective object** of a position is its head revision's `new_json`
(revision 0 = the collected record). The global correction sequence
identifies the *entire* revision set, because revisions are append-only and
immutable: two databases with the same `correction_seq` have the same
corrections. Undoing a correction is itself a correction (a new revision
whose `new_json` is copied from an older revision), so the sequence moves on
undos too — a value comparison can never masquerade as "nothing happened".

### Proposal vs effected revision

Corrections follow a two-phase, review-gated flow:

1. **propose** creates a *proposal* (`correction_proposals`, state
   `pending`). A proposal is inert: it changes no export, no rule recompute,
   no review/diff, no official read.
2. **approve** effects it inside ONE `BEGIN IMMEDIATE` transaction that
   re-validates everything and appends the revision (below).
3. **reject** is the operator's negative decision on a pending proposal.

## CLI

Common form: `python -m pcg --db DB --clock real correct <command> ...`

| Command | Purpose |
|---|---|
| `show P O [--json]` | original vs current effective object, current revision, active-version result |
| `propose P O --correction-id ID --expected-revision N --reason R (--record J \| --record-file F \| --record-stdin)` | propose a complete replacement object |
| `restore P O --to-revision N --correction-id ID --expected-revision M --reason R` | propose an undo: replacement copied from revision N (0 = collected record) |
| `list [--state pending\|applied\|rejected] [--limit N] [--json]` | list proposals |
| `get ID [--json]` | show one proposal |
| `approve ID [--json]` | effect a pending proposal atomically |
| `reject ID [--note N]` | operator-reject a pending proposal |
| `history P O [--json]` | stream the position's revision chain (revision 0 first) |

### Exit codes

| code | meaning |
|---|---|
| 0 | success |
| 2 | bad arguments / unreadable record file / database error |
| 9 | unknown correction id |
| 10 | unknown coordinates (no committed record at the position) |
| 11 | invalid correction content (non-object JSON, illegal reason, no-change replacement) |
| 12 | correction id reused with different content |
| 13 | revision/concurrency conflict (stale expected revision, already-decided proposal) |

Every diagnostic names the position and, for stale-revision conflicts, the
**actual** current revision is printed as `actual_revision=N` on stderr.

## Rules

### Object replacement semantics

* The replacement must be a **complete JSON object**; it wholly replaces the
  effective object for subsequent cleaning. There is no field-level merge:
  fields absent from the replacement are absent from the effective object.
* The replacement must differ from the current effective object (a no-change
  "correction" is refused, exit 11).
* `reason` must be a non-empty string of at most 500 characters without
  control characters.

### Correction ids

* The caller-provided `--correction-id` is the idempotency key of the
  proposal. Its *content* is `(partition, offset, expected_revision,
  replacement object, reason)`, compared by SHA-256 over canonical JSON.
* Retrying a propose with the same id and identical content is a safe no-op
  that returns the existing proposal (even across JSON key reordering).
* Reusing an id with any different content is refused (exit 12). Use a fresh
  id for a new attempt — including a re-proposal with an updated expected
  revision.

### Expected revisions and staleness

* `show`/`history` report the position's current revision; `propose` must
  carry that number as `--expected-revision` (0 when no correction exists).
* If the revision moved before the proposal is submitted, the propose is
  refused (exit 13) with the actual revision.
* If it moves between propose and approve (someone else's correction took
  effect), the approval is refused (exit 13), the stale proposal is marked
  `rejected` with the actual revision in its note, and no revision is
  appended. Re-read the position and propose again with a **new** id.
* A proposal can be decided exactly once: approving an already-applied or
  already-rejected proposal is a conflict (exit 13). When two management
  processes approve the same proposal concurrently, exactly one wins; the
  loser gets the conflict diagnostic and the winner's revision is the only
  change.

### The approval transaction

`approve` runs one `BEGIN IMMEDIATE` transaction that:

1. re-checks the proposal is pending and the expected revision equals the
   position's current revision;
2. reads the **current** active rule version and publish sequence;
3. appends the immutable revision row and moves the head pointer;
4. reprocesses the new complete object under that rule version and lands
   the outcome as the active version's official `rule_results` row for the
   position — a full result **or an explicit rejection diagnostic** when the
   rule refuses the object;
5. bumps the global correction sequence by one and marks the proposal
   `applied`.

Any failure rolls the whole transaction back: a failed approval never leaves
half a revision and never changes an official result. Collection progress is
never touched.

## Interaction with cleaning-rule publishing

* **Recompute** evaluates every position's *current effective object* (not
  always the original `source_json`) and records the source revision it used
  in `rule_results.source_revision`. Positions whose stored result predates
  a correction are found by a keyset-paged "stale" sweep and reprocessed;
  page flushes re-validate revisions inside their own transaction, so a
  flushed page is never born stale. Memory stays bounded by the page size.
* **Review tokens** additionally bind the global correction sequence. Any
  correction effected after the review — even one later undone back to the
  original value — makes activation refuse the token (exit 6). Diff output
  marks positions whose candidate result is stale as kind `stale`.
* **Activation** requires the candidate to cover the section with *current*
  `ok` results: a result older than its position's head revision counts as
  uncovered. An old version that was inactive while corrections landed must
  therefore be recomputed (catch-up) and re-reviewed before it can be
  republished — the same contract as for newly collected rows.
* **Export** pins the version, publish sequence, correction sequence and
  every position's revision in ONE read snapshot, and tags each row with the
  `source_revision` its value was computed from. For the active version a
  row's revision always equals the position's head revision inside the
  snapshot (a violation is a hard error, never a skipped row); exports of
  non-active versions flag stale rows explicitly instead of presenting them
  as current.

## Interaction with live collection

* Workers only ever commit *new* offsets, which are always at revision 0;
  the fenced commit is unchanged. A correction can only target an
  already-committed position, so a worker batch can never collide with a
  correction in the commit transaction.
* A batch that was computed before a correction but commits afterwards (e.g.
  a parked barrier batch that loses its generation) is rejected by the
  existing commit fence as usual; it cannot overwrite the correction's
  results nor append a revision.
* Approvals and activations serialise on the same database write lock, so
  the active version's results are updated atomically with every effected
  correction.

## Upgrading an existing database

The upgrade runs automatically on the first run of any `pcg` command and is
idempotent and non-destructive:

1. `CREATE TABLE IF NOT EXISTS` adds `correction_proposals`,
   `source_corrections`, `correction_state`;
2. `ALTER TABLE` adds `rule_results.source_revision` and
   `rule_reviews.correction_seq`, both `DEFAULT 0` — SQLite fills constant
   defaults at read time, so **no existing row is rewritten** and historical
   results simply read as "computed from the collected record";
3. `meta.correction_seq` is initialised to 0.

Repeating the upgrade is a no-op. Pre-upgrade `clean_json` values, rule
results and review tokens behave exactly as before (revision 0 semantics);
a review token created before the first correction remains valid until the
first correction lands.
