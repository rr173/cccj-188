"""Thin shim so tests can call the standalone reference reconciler."""

import os
import sys

_TOOLS = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "tools")
if _TOOLS not in sys.path:
    sys.path.insert(0, _TOOLS)

from reconcile import reconcile  # noqa: E402,F401
