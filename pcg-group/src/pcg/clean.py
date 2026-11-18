"""Deterministic field cleaning.

The same pure function is used by:

* the worker when it writes sink rows, and
* the independent reference reconciler (``tools/reconcile.py``),

which imports *only* this module and reads the JSONL source itself, so the
reconciliation is not trusting the worker's own output.

Rules (pure, order-independent, no clocks, no randomness):

* ``name``  : NFKC-normalise, strip, collapse internal whitespace, title-case
* ``email`` : lower-case, strip
* ``city``  : strip, replace ``_``/``-`` with space, collapse whitespace, title
* ``amount``: parse number/string to a float rounded to 2 decimals; invalid -> 0.0
* ``active``: coerce to a strict boolean (true/1/yes/on vs false/0/no/off)
* unknown extra fields are carried through unchanged but sorted by key
"""

from __future__ import annotations

import json
import unicodedata
from typing import Any

_BOOL_TRUE = {"true", "1", "yes", "on"}
_BOOL_FALSE = {"false", "0", "no", "off"}


def _collapse_ws(s: str) -> str:
    return " ".join(s.split())


def clean_name(v: Any) -> str:
    if v is None:
        return ""
    s = unicodedata.normalize("NFKC", str(v)).strip()
    return _collapse_ws(s).title()


def clean_email(v: Any) -> str:
    if v is None:
        return ""
    return unicodedata.normalize("NFKC", str(v)).strip().lower()


def clean_city(v: Any) -> str:
    if v is None:
        return ""
    s = str(v).replace("_", " ").replace("-", " ")
    return _collapse_ws(unicodedata.normalize("NFKC", s).strip()).title()


def clean_amount(v: Any) -> float:
    if isinstance(v, bool):
        return 1.0 if v else 0.0
    if isinstance(v, (int, float)):
        return round(float(v), 2)
    try:
        return round(float(str(v).strip().replace(",", "")), 2)
    except (ValueError, TypeError):
        return 0.0


def clean_active(v: Any) -> bool:
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return v != 0
    s = str(v).strip().lower()
    if s in _BOOL_TRUE:
        return True
    if s in _BOOL_FALSE:
        return False
    return False


_KNOWN = {"name", "email", "city", "amount", "active"}


def clean_record(src: dict) -> dict:
    """Return the canonical cleaned record for one source JSON object."""
    out = {
        "name": clean_name(src.get("name")),
        "email": clean_email(src.get("email")),
        "city": clean_city(src.get("city")),
        "amount": clean_amount(src.get("amount")),
        "active": clean_active(src.get("active")),
    }
    extras = {k: src[k] for k in sorted(src) if k not in _KNOWN}
    out.update(extras)
    return out


def clean_to_json(src: dict) -> str:
    return json.dumps(clean_record(src), sort_keys=True, separators=(",", ":"),
                      ensure_ascii=False)
