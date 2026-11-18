# Cleaning-rule publishing: versioned recompute, review and activation

This document describes the operational workflow for changing a cleaning
calibre **without stopping collection, clearing the sink, or re-reading the
original JSONL files**. Everything is coordinated inside the existing SQLite
database; collection commands, raw records, the historical `clean_json`,
`progress` and their meanings are unchanged.

## Concepts

| Object | Meaning |
|---|---|
| rule version | A JSON document registered under a caller-chosen `version_id`. Content-addressed by canonical JSON + SHA-256. |
| `legacy` | The fixed built-in cleaner (`pcg.clean.clean_record`), reserved and impossible to overwrite or delete. |
| `rule_results` | One row per `(version, partition_id, offset)`: either a full result (`status='ok'`) or an explicit rejection (`status='rejected'`). A missing row means *not covered yet*. |
| backfill watermark | `rule_recompute_progress.done_offset` per version/partition; resumable recompute starts at `done_offset+1`. |
| active version | Singleton pointer in `rule_state` with a strictly increasing `publish_seq`. |
| review token | A persisted, immutable section snapshot (offset vector, active version, publish seq, candidate hash + materialised findings). |
| export section | One read transaction fixing the active version, publish sequence and committed-offset vector for the whole export. |

## The rule contract

A rule file contains exactly one top-level key:

```json
{"fields": {"<field name>": {
  "missing": "preserve | null | reject",
  "null":    "preserve | null | reject",
  "steps":   [ {"op": "..."}, ... ] } } }
```

* Fields are **top-level keys only**, never paths. Unconfigured fields are
  copied through untouched. Every configured chain always starts from the
  **original** source object's value.
* `missing` applies when the key is absent; `null` when the value is an
  explicit JSON null. `preserve` keeps the absence/null, `null` writes an
  explicit null, `reject` produces a row diagnostic. When a policy applies,
  the steps are **not** executed.
* `steps` is a non-empty array; its order is significant. Object key order in
  the JSON file is irrelevant to identity.

| op | parameters | input | semantics |
|---|---|---|---|
| `nfkc` | – | string | Unicode NFKC normalisation |
| `strip` | – | string | trim both ends |
| `collapse_ws` | – | string | `" ".join(s.split())` |
| `lower` / `upper` / `title` | – | string | Python casing semantics |
| `alias` | `values`: {string: string} | string | exact map; a miss keeps the current string |
| `decimal` | `scale` 0..6 integer (not boolean), `rounding` `HALF_EVEN`/`HALF_UP` | string | exact decimal arithmetic |

`decimal` accepts only strings containing optional surrounding whitespace and
a finite decimal numeral (optional sign, digits/decimal point, decimal
exponent). Empty strings, booleans, JSON numbers, `NaN`/`Infinity`, thousands
separators and underscores are rejected. Computation uses `decimal.Decimal`
with an explicit local context, so the process-wide decimal precision and
rounding cannot affect results; output always has exactly `scale` fractional
digits and negative zero is canonicalised to positive zero. Examples:
`"1.005"` at scale 2 → `"1.00"` (`HALF_EVEN`) or `"1.01"` (`HALF_UP"`).

Errors in the rule document itself (unknown op/parameter, bad scale, wrong
policy, malformed shape) are refused at **registration** time. Errors that
depend on row values are **execution** rejections carrying the source
position, field, op and reason; that position gets no partial cleaned object.

## CLI

Common form: `python -m pcg --db DB --clock real rule <command> ...`
(the `--db/--clock` options are also accepted directly after `rule`).

| Command | Purpose |
|---|---|
| `register VERSION_ID FILE` | Validate and register a rule; idempotent for identical content |
| `recompute VERSION_ID [--page-size N] [--pause-file F] [--pause-wait-s S] [--max-pages N]` | Batched, resumable historical recompute/catch-up |
| `status` | Versions, active pointer, publish history, per-version coverage |
| `review CANDIDATE [--base B] [--json]` | Fixed-section diff; prints a review token and stats |
| `diff TOKEN [--json]` | Stream the saved findings in source-position order |
| `activate TOKEN [--reason R]` | Atomically validate the token/section and switch |
| `export [--version-id V] [--json]` | Official consistent-snapshot read |

Exit codes: `0` ok · `2` arguments/IO · `3` invalid rule document ·
`4` version conflict or reserved id · `5` recompute already running ·
`6` activation refused · `7` unknown version/token · `8` pause wait expired.

### Diagnostic formats

* Rejection rows (in `rule_results.error_json` and export output):
  `{"type":"rule_reject","version_id":...,"field":...,"op":...,"reason":...}`
  with `op=null` for missing/null policy rejections.
* Review findings kinds: `changed` (with per-field `base`/`candidate`
  presence, JSON type and value), `candidate_rejected`,
  `base_rejected_candidate_ok`, `uncovered`.
* Export rows (JSON mode): one header line `{"section": {...}}` then one JSON
  object per position containing `version_id`, `publish_seq`,
  `partition_id`, `offset`, `status` and either `result` or `error`.

## The publishing workflow

1. **Register** the candidate rule from a JSON file. The same id with the
   same content is a safe retry; any content change under an existing id is
   rejected (pick a new id instead).
2. **Recompute** in pages. It reads only committed `records.source_json`, so
   the original JSONL directory need not be accessible. Pages and watermarks
   commit together; a killed process resumes at the next offset without
   rescanning finished history. Collection progress and the active version
   are never touched.
3. **Review** the candidate against the currently active version on one
   explicit section. The token binds the rule hash, the offset vector
   (empty partition = -1), the active version and the publish sequence; all
   findings are materialised, so later writes cannot alter this review.
4. **Activate**. A single `BEGIN IMMEDIATE` transaction re-checks that the
   token still matches the active version, publish sequence, latest offset
   vector and rule hash, and that every section position has an `ok`
   candidate result (no rejections, no holes). Only then does the pointer
   move, with `publish_seq` incremented. Any failed check leaves the current
   selection untouched.
5. **Export** through the official read entry point, which pins one version
   and one section for the whole stream and prints rejections explicitly
   rather than skipping rows or substituting defaults.

Rollback uses the identical contract: recompute the old version so it
catches rows received while it was inactive, review, activate. There is no
direct pointer flip to a version that lacks recent results.

## Interaction with live collection

* The active version is selected **inside** the worker's fenced commit
  transaction. A batch computed before a switch but committed afterwards is
  re-evaluated under the new version inside that same transaction, so old
  calibre values can never masquerade as new-version results. Raw rows,
  progress and the versioned outcome (result or rejection) commit atomically.
* Rows arriving after a version becomes active but which fail that version's
  rule still advance collection: the raw row and progress persist and an
  explicit rejection is stored for that version.
* Multiple management processes cannot activate two versions at once: every
  activation serialises on the same write transaction and bumps one sequence.
* A long recompute never holds a history-spanning write lock: each page is a
  short transaction, and ingestion commits concurrently between pages.

## Upgrading an existing database

Upgrade happens automatically on the first run of any `pcg` command
(`init_db`), and is idempotent and repeatable on both empty and populated
databases:

1. the `legacy` built-in version and the singleton pointer are created;
2. historical `records.clean_json` values are copied verbatim into
   `rule_results` for `legacy` in bounded pages (short transactions);
3. per-partition watermarks are seeded from `progress` and a durable
   migration marker (`meta.rule_migration_legacy_v1`) records completion, so
   later worker processes never silently backfill an inactive version.

After upgrading, a `legacy` export is byte-identical to the pre-upgrade
`clean_json` values.
