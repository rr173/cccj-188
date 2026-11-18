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


def _reason_matches(prod_reason, key):
    table = {
        "not_string": ["must be a string"],
        "empty": ["empty string"],
        "bad_decimal": ["finite decimal", "unparseable", "out of supported"],
        "missing_reject": ["missing value rejected"],
        "null_reject": ["null value rejected"],
    }
    return any(marker in prod_reason for marker in table[key])


def verify_version(conn, version_row, problems):
    """Check every stored outcome of one version against the reference."""
    vid = version_row["version_id"]
    if version_row["builtin"]:
        # built-in outcomes must be byte-identical to the immutable sink value
        rows = conn.execute(
            "SELECT rr.partition_id, rr.offset, rr.status, rr.result_json, "
            "rr.error_json, r.clean_json FROM rule_results rr "
            "JOIN records r ON r.partition_id=rr.partition_id "
            "AND r.offset=rr.offset WHERE rr.version_id=?", (vid,))
        for row in rows:
            if row["status"] != "ok" or row["result_json"] != row["clean_json"]:
                problems.append(
                    f"legacy mismatch p{row['partition_id']} o{row['offset']}: "
                    f"{row['status']} {row['result_json']!r} != clean_json")
        return
    spec = json.loads(version_row["spec_json"])
    rows = conn.execute(
        "SELECT rr.partition_id, rr.offset, rr.status, rr.result_json, "
        "rr.error_json, r.source_json FROM rule_results rr "
        "JOIN records r ON r.partition_id=rr.partition_id "
        "AND r.offset=rr.offset WHERE rr.version_id=? "
        "ORDER BY rr.partition_id, rr.offset", (vid,))
    for row in rows:
        source = json.loads(row["source_json"])
        status, payload = ref_outcome(spec, source)
        if status != row["status"]:
            problems.append(
                f"p{row['partition_id']} o{row['offset']} version {vid}: "
                f"status {row['status']} != reference {status}")
            continue
        if status == "ok":
            if payload != row["result_json"]:
                problems.append(
                    f"p{row['partition_id']} o{row['offset']} version {vid}: "
                    f"value {row['result_json']!r} != reference {payload!r}")
        else:
            err = json.loads(row["error_json"])
            if (err.get("field") != payload.field or
                    err.get("op") != payload.op or
                    not _reason_matches(err.get("reason", ""),
                                        payload.reason_key)):
                problems.append(
                    f"p{row['partition_id']} o{row['offset']} version {vid}: "
                    f"diagnostic {err} != reference "
                    f"field={payload.field} op={payload.op} "
                    f"key={payload.reason_key}")


def verify_database(db_path):
    """Full independent audit. Returns (problems, stats)."""
    import sqlite3
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    problems = []
    versions = list(conn.execute("SELECT * FROM rule_versions"))
    stats = {}
    for v in versions:
        verify_version(conn, v, problems)
        counts = {"ok": 0, "rejected": 0}
        for r in conn.execute(
            "SELECT status, COUNT(*) c FROM rule_results WHERE version_id=? "
            "GROUP BY status", (v["version_id"],)):
            counts[r["status"]] = r["c"]
        stats[v["version_id"]] = counts

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
