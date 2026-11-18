"""Declarative cleaning-rule engine.

The engine implements one small, fixed contract.  It never executes user code:
a registered rule is pure data (JSON) and every operation is one of a closed
set of built-in, deterministic transforms.

Contract (the only accepted rule shape)::

    {"fields": {<field name>: {"missing": <policy>, "null": <policy>,
                               "steps": [<op>, ...]}}}

* ``missing`` / ``null`` are one of ``preserve`` / ``null`` / ``reject``;
* steps form a non-empty array; object key order never matters (canonical
  JSON sorts keys), while the order of the steps array is part of the rule's
  identity;
* fields are top-level keys only; unconfigured fields are copied through
  untouched and every configured field is always taken from the *original*
  source object;
* registration-time problems raise :class:`RuleSpecError`; value-dependent
  problems raise :class:`RuleReject` while a concrete row is being evaluated.
"""

from __future__ import annotations

import hashlib
import json
import re
import unicodedata
from decimal import (
    Context,
    Decimal,
    InvalidOperation,
    ROUND_HALF_EVEN,
    ROUND_HALF_UP,
    localcontext,
)
from typing import Any

#: id of the fixed cleaner; reserved and impossible to register over.
LEGACY_VERSION = "legacy"

POLICIES = ("preserve", "null", "reject")
TEXT_OPS = ("nfkc", "strip", "collapse_ws", "lower", "upper", "title")
DECIMAL_ROUNDING = {"HALF_EVEN": ROUND_HALF_EVEN, "HALF_UP": ROUND_HALF_UP}

# finite decimal numerals: optional sign, integer/fractional digits, optional
# decimal exponent. Everything else (NaN, Infinity, thousands separators,
# underscores, internal blanks, hex, ...) is rejected at execution time.
_DECIMAL_RE = re.compile(
    r"^[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?$"
)
# a defensive bound so an absurd exponent cannot allocate an unbounded number
# of digits; covers any realistic monetary/measurement value while keeping the
# engine independent of the process decimal context.
_DECIMAL_MAX_DIGITS = 1000
_DECIMAL_MAX_EXPONENT = 1000


class RuleSpecError(ValueError):
    """The rule specification itself is invalid (rejected at registration)."""


class RuleReject(Exception):
    """A concrete row violates the rule. The row yields no partial object."""

    def __init__(self, field: str, op: str | None, reason: str):
        super().__init__(f"field={field!r} op={op or '-'}: {reason}")
        self.field = field
        self.op = op
        self.reason = reason


def canonical_spec(spec: dict) -> tuple[str, str]:
    """Return ``(canonical_json, sha256_hex)``.

    Object keys are sorted so JSON key permutation does not change identity;
    arrays (the steps list, the values mapping is an object) keep their order.
    """
    text = json.dumps(spec, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=False)
    return text, hashlib.sha256(text.encode("utf-8")).hexdigest()


def _require(cond: bool, msg: str) -> None:
    if not cond:
        raise RuleSpecError(msg)


def validate_spec(spec: Any) -> dict:
    """Validate a parsed rule document; return it unchanged on success."""
    _require(isinstance(spec, dict), "rule document must be a JSON object")
    _require(set(spec.keys()) == {"fields"},
             "top level must contain exactly one key: 'fields'")
    fields = spec["fields"]
    _require(isinstance(fields, dict), "'fields' must be an object")
    for field, cfg in fields.items():
        _require(isinstance(field, str) and field != "",
                 "field names must be non-empty strings")
        _require(isinstance(cfg, dict),
                 f"field {field!r}: config must be an object")
        _require(set(cfg.keys()) == {"missing", "null", "steps"},
                 f"field {field!r}: config must contain exactly "
                 "'missing', 'null', 'steps'")
        _require(cfg["missing"] in POLICIES,
                 f"field {field!r}: missing policy must be one of {POLICIES}")
        _require(cfg["null"] in POLICIES,
                 f"field {field!r}: null policy must be one of {POLICIES}")
        steps = cfg["steps"]
        _require(isinstance(steps, list) and len(steps) >= 1,
                 f"field {field!r}: steps must be a non-empty array")
        for i, step in enumerate(steps):
            _validate_step(field, i, step)
    return spec


def _validate_step(field: str, i: int, step: Any) -> None:
    where = f"field {field!r} step {i}"
    _require(isinstance(step, dict), f"{where}: operation must be an object")
    _require("op" in step, f"{where}: missing 'op'")
    op = step["op"]
    _require(isinstance(op, str), f"{where}: 'op' must be a string")
    if op in TEXT_OPS:
        _require(set(step.keys()) == {"op"},
                 f"{where}: op {op!r} takes no parameters")
    elif op == "alias":
        _require(set(step.keys()) == {"op", "values"},
                 f"{where}: alias must contain exactly 'op' and 'values'")
        values = step["values"]
        _require(isinstance(values, dict) and values,
                 f"{where}: alias 'values' must be a non-empty object")
        for k, v in values.items():
            _require(isinstance(k, str) and isinstance(v, str),
                     f"{where}: alias mapping must be string -> string")
    elif op == "decimal":
        _require(set(step.keys()) == {"op", "scale", "rounding"},
                 f"{where}: decimal must contain exactly 'op', 'scale', "
                 "'rounding'")
        scale = step["scale"]
        _require(isinstance(scale, int) and not isinstance(scale, bool)
                 and 0 <= scale <= 6,
                 f"{where}: scale must be an integer in 0..6 (not a boolean)")
        _require(step["rounding"] in DECIMAL_ROUNDING,
                 f"{where}: rounding must be one of "
                 f"{sorted(DECIMAL_ROUNDING)}")
    else:
        raise RuleSpecError(
            f"{where}: unknown op {op!r}; supported ops are "
            f"{TEXT_OPS + ('alias', 'decimal')}")


# -------------------------------------------------------------------------- ops

def _as_str(field: str, op: str, cur: Any) -> str:
    if not isinstance(cur, str):
        raise RuleReject(field, op,
                         f"input must be a string, got {json_type_name(cur)}")
    return cur


def _apply_text(field: str, op: str, cur: Any) -> str:
    s = _as_str(field, op, cur)
    if op == "nfkc":
        return unicodedata.normalize("NFKC", s)
    if op == "strip":
        return s.strip()
    if op == "collapse_ws":
        return " ".join(s.split())
    if op == "lower":
        return s.lower()
    if op == "upper":
        return s.upper()
    if op == "title":
        return s.title()
    raise RuleReject(field, op, "unsupported text op")  # pragma: no cover


def _apply_alias(field: str, step: dict, cur: Any) -> str:
    s = _as_str(field, "alias", cur)
    return step["values"].get(s, s)


def _apply_decimal(field: str, step: dict, cur: Any) -> str:
    # JSON numbers and booleans are rejected by the string type check itself;
    # Decimal() would silently accept NaN/Infinity/underscores, so the grammar
    # check below is what enforces the contract.
    s = _as_str(field, "decimal", cur).strip()
    if s == "":
        raise RuleReject(field, "decimal", "empty string is not a decimal")
    if not _DECIMAL_RE.match(s):
        raise RuleReject(field, "decimal",
                         f"not a finite decimal numeral: {cur!r}")
    try:
        d = Decimal(s)
    except InvalidOperation:
        raise RuleReject(field, "decimal", f"unparseable decimal: {cur!r}")
    if not d.is_finite():
        raise RuleReject(field, "decimal",
                         f"not a finite decimal numeral: {cur!r}")
    tup = d.as_tuple()
    if (len(tup.digits) > _DECIMAL_MAX_DIGITS
            or abs(tup.exponent) > _DECIMAL_MAX_EXPONENT):
        raise RuleReject(field, "decimal",
                         f"decimal out of supported range: {cur!r}")
    scale: int = step["scale"]
    quantum = Decimal(1).scaleb(-scale)
    # an explicit, generous local context makes the result independent of the
    # process-wide decimal precision (no binary float anywhere)
    ctx = Context(prec=max(80, len(tup.digits) + scale + 10))
    try:
        with localcontext(ctx):
            q = d.quantize(quantum,
                           rounding=DECIMAL_ROUNDING[step["rounding"]])
    except InvalidOperation:
        # quantize overflow (e.g. an exponent past the supported magnitude)
        raise RuleReject(field, "decimal",
                         f"decimal out of supported range: {cur!r}")
    if q == 0:
        # canonicalise negative zero to positive zero
        return format(Decimal(0), f".{scale}f")
    return str(q)


def json_type_name(v: Any) -> str:
    """JSON-level type name used in diagnostics and diffs."""
    if v is None:
        return "null"
    if isinstance(v, bool):
        return "boolean"
    if isinstance(v, str):
        return "string"
    if isinstance(v, int):
        return "number"
    if isinstance(v, float):
        return "number"
    if isinstance(v, list):
        return "array"
    if isinstance(v, dict):
        return "object"
    return type(v).__name__


def _apply_policy(field: str, which: str, policy: str, out: dict) -> None:
    if policy == "preserve":
        if which == "missing":
            out.pop(field, None)
        else:
            out[field] = None
    elif policy == "null":
        out[field] = None
    else:  # reject
        raise RuleReject(field, None,
                         f"{which} value rejected by policy '{policy}'")


def evaluate(spec: dict, source: dict) -> dict:
    """Evaluate one validated rule against one original source object.

    Raises :class:`RuleReject` on any row-level violation; the caller must
    persist a rejection diagnostic instead of a partial cleaned object.
    """
    if not isinstance(source, dict):
        raise RuleReject("<row>", None, "source record is not a JSON object")
    out: dict = dict(source)  # unconfigured fields are carried through as-is
    for field, cfg in spec["fields"].items():
        if field not in source:
            _apply_policy(field, "missing", cfg["missing"], out)
            continue
        cur = source[field]  # always the ORIGINAL value, never a step output
        if cur is None:
            _apply_policy(field, "null", cfg["null"], out)
            continue
        for step in cfg["steps"]:
            op = step["op"]
            if op in TEXT_OPS:
                cur = _apply_text(field, op, cur)
            elif op == "alias":
                cur = _apply_alias(field, step, cur)
            else:  # decimal
                cur = _apply_decimal(field, step, cur)
        out[field] = cur
    return out


def result_to_json(obj: dict) -> str:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=False)


#: informational contract-shaped document for the built-in version. The fixed
#: cleaner (pcg.clean.clean_record) is *not* expressible by the declarative
#: contract (e.g. it maps missing/null name to "" and never rejects), so this
#: blob is descriptive only; builtin rows are always evaluated by the fixed
#: cleaner, never by :func:`evaluate`.
LEGACY_SPEC = {
    "fields": {
        "name": {"missing": "preserve", "null": "preserve",
                 "steps": [{"op": "nfkc"}, {"op": "strip"},
                           {"op": "collapse_ws"}, {"op": "title"}]},
        "email": {"missing": "preserve", "null": "preserve",
                  "steps": [{"op": "nfkc"}, {"op": "strip"},
                            {"op": "lower"}]},
        "city": {"missing": "preserve", "null": "preserve",
                 "steps": [{"op": "nfkc"}, {"op": "strip"},
                           {"op": "collapse_ws"}, {"op": "title"}]},
        "amount": {"missing": "preserve", "null": "preserve",
                   "steps": [{"op": "decimal", "scale": 2,
                              "rounding": "HALF_UP"}]},
    }
}
