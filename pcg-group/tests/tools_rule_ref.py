"""Independent reference implementation for rule-publishing acceptance tests.

This module deliberately imports NONE of the production rule/cleaning code
(no ``pcg.rule_engine``, no ``pcg.clean``, no ``pcg.rule_store``). It
re-implements the declarative contract from scratch using only the standard
library, so a test that compares every stored value/diagnostic against this
module is not trusting the code that produced them.

It also provides whole-database verification helpers used by the acceptance
suite and the one-command demo.
"""

from __future__ import annotations

import json
import re
import unicodedata
from decimal import Context, Decimal, ROUND_HALF_EVEN, ROUND_HALF_UP, localcontext

TEXT_OPS = ("nfkc", "strip", "collapse_ws", "lower", "upper", "title")
ROUNDINGS = {"HALF_EVEN": ROUND_HALF_EVEN, "HALF_UP": ROUND_HALF_UP}
_DECIMAL_RE = re.compile(r"^[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?$")
_MAX_DIGITS = 1000
_MAX_EXPONENT = 1000


class RefReject(Exception):
    def __init__(self, field, op, reason_key, detail):
        super().__init__(f"{field} {op} {reason_key}")
        self.field = field
        self.op = op
        self.reason_key = reason_key
        self.detail = detail


def ref_evaluate(spec, source):
    """Independent evaluator. Returns a dict or raises RefReject."""
    out = dict(source)
    for field, cfg in spec["fields"].items():
        if field not in source:
            policy = cfg["missing"]
            if policy == "preserve":
                out.pop(field, None)
            elif policy == "null":
                out[field] = None
            else:
                raise RefReject(field, None, "missing_reject", "missing")
            continue
        value = source[field]
        if value is None:
            policy = cfg["null"]
            if policy == "preserve":
                out[field] = None
            elif policy == "null":
                out[field] = None
            else:
                raise RefReject(field, None, "null_reject", "null")
            continue
        cur = value
        for step in cfg["steps"]:
            op = step["op"]
            if op in TEXT_OPS:
                if not isinstance(cur, str):
                    raise RefReject(field, op, "not_string",
                                    f"type={type(cur).__name__}")
                if op == "nfkc":
                    cur = unicodedata.normalize("NFKC", cur)
                elif op == "strip":
                    cur = cur.strip()
                elif op == "collapse_ws":
                    cur = " ".join(cur.split())
                elif op == "lower":
                    cur = cur.lower()
                elif op == "upper":
                    cur = cur.upper()
                else:
                    cur = cur.title()
            elif op == "alias":
                if not isinstance(cur, str):
                    raise RefReject(field, op, "not_string",
                                    f"type={type(cur).__name__}")
                cur = step["values"].get(cur, cur)
            elif op == "decimal":
                if not isinstance(cur, str):
                    raise RefReject(field, op, "not_string",
                                    f"type={type(cur).__name__}")
                s = cur.strip()
                if s == "":
                    raise RefReject(field, op, "empty", "empty string")
                if not _DECIMAL_RE.match(s):
                    raise RefReject(field, op, "bad_decimal", cur)
                d = Decimal(s)
                if not d.is_finite():
                    raise RefReject(field, op, "bad_decimal", cur)
                tup = d.as_tuple()
                if (len(tup.digits) > _MAX_DIGITS
                        or abs(tup.exponent) > _MAX_EXPONENT):
                    raise RefReject(field, op, "bad_decimal", cur)
                scale = step["scale"]
                ctx = Context(prec=max(80, len(tup.digits) + scale + 10))
                try:
                    with localcontext(ctx):
                        q = d.quantize(Decimal(1).scaleb(-scale),
                                       rounding=ROUNDINGS[step["rounding"]])
                except Exception:
                    raise RefReject(field, op, "bad_decimal", cur)
                if q == 0:
                    cur = format(Decimal(0), f".{scale}f")
                else:
                    cur = str(q)
        out[field] = cur
    return out


def ref_outcome(spec, source_obj):
    """Return ('ok', canonical_json) or ('rejected', RefReject)."""
    try:
        obj = ref_evaluate(spec, source_obj)
    except RefReject as exc:
        return "rejected", exc
    return "ok", json.dumps(obj, sort_keys=True, separators=(",", ":"),
                            ensure_ascii=False)


# ---------------------------------------------------------------------------
# Independent re-implementation of the FIXED built-in cleaner (the 'legacy'
# version). Used only to audit results computed from CORRECTED objects; the
# revision-0 audit stays a pure byte comparison against records.clean_json.
# Written from the documented contract in src/pcg/clean.py without importing
# or calling any production cleaning function.
# ---------------------------------------------------------------------------

_REF_BOOL_TRUE = {"true", "1", "yes", "on"}
_REF_BOOL_FALSE = {"false", "0", "no", "off"}
_REF_KNOWN = {"name", "email", "city", "amount", "active"}


def _ref_collapse(s):
    return " ".join(s.split())


def ref_clean_fixed(src):
    def name(v):
        if v is None:
            return ""
        return _ref_collapse(unicodedata.normalize("NFKC", str(v)).strip()).title()

    def email(v):
        if v is None:
            return ""
        return unicodedata.normalize("NFKC", str(v)).strip().lower()

    def city(v):
        if v is None:
            return ""
        s = str(v).replace("_", " ").replace("-", " ")
        return _ref_collapse(unicodedata.normalize("NFKC", s).strip()).title()

    def amount(v):
        if isinstance(v, bool):
            return 1.0 if v else 0.0
        if isinstance(v, (int, float)):
            return round(float(v), 2)
        try:
            return round(float(str(v).strip().replace(",", "")), 2)
        except (ValueError, TypeError):
            return 0.0

    def active(v):
        if isinstance(v, bool):
            return v
        if isinstance(v, (int, float)):
            return v != 0
        s = str(v).strip().lower()
        if s in _REF_BOOL_TRUE:
            return True
        if s in _REF_BOOL_FALSE:
            return False
        return False

    out = {"name": name(src.get("name")), "email": email(src.get("email")),
           "city": city(src.get("city")), "amount": amount(src.get("amount")),
           "active": active(src.get("active"))}
    for k in sorted(src):
        if k not in _REF_KNOWN:
            out[k] = src[k]
    return out


def _canon(obj):
    return json.dumps(obj, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=False)


def _effective_objects(conn):
    """(pid, offset) -> {revision: object}, revision 0 = collected record."""
    out = {}
    for r in conn.execute(
            "SELECT partition_id, offset, source_json FROM records"):
        out.setdefault((r["partition_id"], r["offset"]), {})[0] = \
            json.loads(r["source_json"])
    for r in conn.execute(
            "SELECT partition_id, offset, revision, new_json FROM "
            "source_corrections"):
        out.setdefault((r["partition_id"], r["offset"]), {})[r["revision"]] = \
            json.loads(r["new_json"])
    return out


def _reason_matches(prod_reason, key):
    table = {
        "not_string": ["must be a string"],
        "empty": ["empty string"],
        "bad_decimal": ["finite decimal", "unparseable", "out of supported"],
        "missing_reject": ["missing value rejected"],
        "null_reject": ["null value rejected"],
    }
    return any(marker in prod_reason for marker in table[key])


def verify_version(conn, version_row, problems, effective=None):
    """Check every stored outcome of one version against the reference.

    Each result row is audited against the effective object of the source
    revision it claims (``source_revision``): revision 0 is the immutable
    collected ``records.source_json``; revision N is the Nth correction's
    complete replacement object.
    """
    vid = version_row["version_id"]
    if effective is None:
        effective = _effective_objects(conn)
    if version_row["builtin"]:
        rows = conn.execute(
            "SELECT rr.partition_id, rr.offset, rr.status, rr.result_json, "
            "rr.error_json, rr.source_revision, r.clean_json FROM "
            "rule_results rr JOIN records r ON r.partition_id=rr.partition_id "
            "AND r.offset=rr.offset WHERE rr.version_id=?", (vid,))
        for row in rows:
            pos = (row["partition_id"], row["offset"])
            rev = row["source_revision"]
            if row["status"] != "ok":
                problems.append(
                    f"legacy p{pos[0]} o{pos[1]}: unexpected rejection")
                continue
            if rev == 0:
                # byte-identity with the immutable sink value
                if row["result_json"] != row["clean_json"]:
                    problems.append(
                        f"legacy mismatch p{pos[0]} o{pos[1]}: "
                        f"{row['result_json']!r} != clean_json")
            else:
                # computed from the corrected object by the fixed cleaner;
                # audit with the independent re-implementation
                obj = effective.get(pos, {}).get(rev)
                if obj is None:
                    problems.append(
                        f"legacy p{pos[0]} o{pos[1]}: unknown source "
                        f"revision {rev}")
                    continue
                want = _canon(ref_clean_fixed(obj))
                if row["result_json"] != want:
                    problems.append(
                        f"legacy p{pos[0]} o{pos[1]} rev {rev}: "
                        f"{row['result_json']!r} != reference {want!r}")
        return
    spec = json.loads(version_row["spec_json"])
    rows = conn.execute(
        "SELECT partition_id, offset, status, result_json, error_json, "
        "source_revision FROM rule_results WHERE version_id=? "
        "ORDER BY partition_id, offset", (vid,))
    for row in rows:
        pos = (row["partition_id"], row["offset"])
        rev = row["source_revision"]
        source = effective.get(pos, {}).get(rev)
        if source is None:
            problems.append(
                f"p{pos[0]} o{pos[1]} version {vid}: result references "
                f"unknown source revision {rev}")
            continue
        status, payload = ref_outcome(spec, source)
        if status != row["status"]:
            problems.append(
                f"p{pos[0]} o{pos[1]} version {vid} rev {rev}: "
                f"status {row['status']} != reference {status}")
            continue
        if status == "ok":
            if payload != row["result_json"]:
                problems.append(
                    f"p{pos[0]} o{pos[1]} version {vid} rev {rev}: "
                    f"value {row['result_json']!r} != reference {payload!r}")
        else:
            err = json.loads(row["error_json"])
            if (err.get("field") != payload.field or
                    err.get("op") != payload.op or
                    not _reason_matches(err.get("reason", ""),
                                        payload.reason_key)):
                problems.append(
                    f"p{pos[0]} o{pos[1]} version {vid} rev {rev}: "
                    f"diagnostic {err} != reference "
                    f"field={payload.field} op={payload.op} "
                    f"key={payload.reason_key}")


def verify_corrections(conn, problems):
    """Audit the correction log's integrity without trusting the workflow."""
    # the global sequence counts exactly the appended revisions
    row = conn.execute(
        "SELECT value FROM meta WHERE key='correction_seq'").fetchone()
    seq = int(row[0]) if row else 0
    total = conn.execute("SELECT COUNT(*) c FROM source_corrections").fetchone()["c"]
    if seq != total:
        problems.append(
            f"correction_seq {seq} != number of revisions {total}")

    # per-position chains: contiguous from 1, linked prev->new, pointer in
    # correction_state equals the head revision
    positions = [tuple(r) for r in conn.execute(
        "SELECT DISTINCT partition_id, offset FROM source_corrections")]
    covered = set()
    for pid, off in positions:
        chain = list(conn.execute(
            "SELECT * FROM source_corrections WHERE partition_id=? AND "
            "offset=? ORDER BY revision", (pid, off)))
        covered.add((pid, off))
        revs = [r["revision"] for r in chain]
        if revs != list(range(1, len(chain) + 1)):
            problems.append(
                f"p{pid} o{off}: revision chain not contiguous: {revs}")
            continue
        original = conn.execute(
            "SELECT source_json FROM records WHERE partition_id=? AND offset=?",
            (pid, off)).fetchone()
        if original is None:
            problems.append(f"p{pid} o{off}: correction without committed record")
            continue
        prev_obj = json.loads(original["source_json"])
        for r in chain:
            if json.loads(r["prev_json"]) != prev_obj:
                problems.append(
                    f"p{pid} o{off} rev {r['revision']}: prev_json does not "
                    f"match the previous effective object")
            prev_obj = json.loads(r["new_json"])
            prop = conn.execute(
                "SELECT state FROM correction_proposals WHERE correction_id=?",
                (r["correction_id"],)).fetchone()
            if prop is None or prop["state"] != "applied":
                problems.append(
                    f"p{pid} o{off} rev {r['revision']}: correction_id "
                    f"{r['correction_id']!r} is not an applied proposal")
        pointer = conn.execute(
            "SELECT revision FROM correction_state WHERE partition_id=? "
            "AND offset=?", (pid, off)).fetchone()
        if pointer is None or pointer["revision"] != len(chain):
            problems.append(
                f"p{pid} o{off}: correction_state pointer "
                f"{None if pointer is None else pointer['revision']} != head "
                f"revision {len(chain)}")

    # no dangling pointers, and no applied proposal without its revision
    for r in conn.execute("SELECT partition_id, offset FROM correction_state"):
        if (r["partition_id"], r["offset"]) not in covered:
            problems.append(
                f"p{r['partition_id']} o{r['offset']}: correction_state "
                "without any revision")
    for r in conn.execute(
            "SELECT correction_id, partition_id, offset, new_json FROM "
            "correction_proposals WHERE state='applied'"):
        rev = conn.execute(
            "SELECT 1 FROM source_corrections WHERE correction_id=?",
            (r["correction_id"],)).fetchone()
        if rev is None:
            problems.append(
                f"applied proposal {r['correction_id']!r} has no revision row")


def verify_database(db_path):
    """Full independent audit. Returns (problems, stats)."""
    import sqlite3
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    problems = []
    effective = _effective_objects(conn)
    versions = list(conn.execute("SELECT * FROM rule_versions"))
    stats = {}
    for v in versions:
        verify_version(conn, v, problems, effective)
        counts = {"ok": 0, "rejected": 0}
        for r in conn.execute(
            "SELECT status, COUNT(*) c FROM rule_results WHERE version_id=? "
            "GROUP BY status", (v["version_id"],)):
            counts[r["status"]] = r["c"]
        stats[v["version_id"]] = counts

    verify_corrections(conn, problems)

    # active version covers every committed position with exactly one outcome
    state = conn.execute("SELECT * FROM rule_state WHERE id=1").fetchone()
    active = state["active_version"]
    for p in conn.execute("SELECT DISTINCT partition_id FROM records "
                         "ORDER BY partition_id"):
        pid = p["partition_id"]
        prog = conn.execute(
            "SELECT committed_offset FROM progress WHERE partition_id=?",
            (pid,)).fetchone()
        last = prog["committed_offset"] if prog else -1
        n_total = conn.execute(
            "SELECT COUNT(*) c FROM rule_results WHERE version_id=? "
            "AND partition_id=? AND offset BETWEEN 0 AND ?",
            (active, pid, last)).fetchone()["c"]
        if n_total != last + 1:
            problems.append(
                f"active version {active}: p{pid} has {n_total} outcomes for "
                f"{last + 1} committed positions (hole or extra row)")

    # the active version must be current at every corrected position, and no
    # result may reference a revision newer than the position's head
    for r in conn.execute("SELECT * FROM correction_state"):
        pid, off, rev = r["partition_id"], r["offset"], r["revision"]
        res = conn.execute(
            "SELECT source_revision FROM rule_results WHERE version_id=? "
            "AND partition_id=? AND offset=?", (active, pid, off)).fetchone()
        if res is None:
            problems.append(
                f"active version {active}: no result at corrected p{pid} o{off}")
        elif res["source_revision"] != rev:
            problems.append(
                f"active version {active}: stale result at p{pid} o{off} "
                f"(source_revision {res['source_revision']} != current {rev})")
    for r in conn.execute(
            "SELECT cs.partition_id, cs.offset, cs.revision, rr.version_id, "
            "rr.source_revision FROM correction_state cs JOIN rule_results rr "
            "ON rr.partition_id=cs.partition_id AND rr.offset=cs.offset "
            "WHERE rr.source_revision > cs.revision"):
        problems.append(
            f"version {r['version_id']} at p{r['partition_id']} "
            f"o{r['offset']}: result from future revision "
            f"{r['source_revision']} > head {r['revision']}")

    # per-partition progress continuity against records
    for p in conn.execute("SELECT DISTINCT partition_id FROM records "
                         "ORDER BY partition_id"):
        pid = p["partition_id"]
        n_rows = conn.execute(
            "SELECT COUNT(*) c FROM records WHERE partition_id=?",
            (pid,)).fetchone()["c"]
        prog = conn.execute(
            "SELECT committed_offset FROM progress WHERE partition_id=?",
            (pid,)).fetchone()
        if prog is None or n_rows != prog["committed_offset"] + 1:
            problems.append(f"p{pid}: progress/records continuity broken")
    conn.close()
    return problems, stats


def snapshot_immutables(db_path):
    """Snapshot the tables a recompute must never modify."""
    import sqlite3
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    records = [tuple(r) for r in conn.execute(
        "SELECT partition_id,offset,gen,worker_id,source_json,clean_json,"
        "committed_at FROM records ORDER BY partition_id,offset")]
    progress = [tuple(r) for r in conn.execute(
        "SELECT partition_id,committed_offset,gen,worker_id,updated_at "
        "FROM progress ORDER BY partition_id")]
    state = tuple(conn.execute(
        "SELECT active_version,publish_seq FROM rule_state WHERE id=1").fetchone())
    conn.close()
    return {"records": records, "progress": progress, "state": state}


def main(argv=None):
    import argparse
    ap = argparse.ArgumentParser(
        description="Independent reference audit of rule-publishing results")
    ap.add_argument("--db", required=True)
    args = ap.parse_args(argv)
    problems, stats = verify_database(args.db)
    for vid, counts in sorted(stats.items()):
        print(f"{vid}: ok={counts['ok']} rejected={counts['rejected']}")
    if problems:
        print(f"REFERENCE AUDIT FAILED ({len(problems)} problems)")
        for msg in problems[:50]:
            print("  - " + msg)
        return 1
    print("REFERENCE AUDIT OK: every position, value and diagnostic matches "
          "the independent implementation")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
