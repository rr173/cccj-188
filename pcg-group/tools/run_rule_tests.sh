#!/usr/bin/env bash
# Acceptance suite for the versioned cleaning-rule publishing workflow.
# Real coordinator/worker/manager OS processes throughout; value and
# diagnostic checks go through the independent reference implementation in
# tests/tools_rule_ref.py (which imports none of the production cleaner).
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export PYTHONPATH="$ROOT/src"
cd "$ROOT/tests"
python3 -m unittest test_rules_e2e -v "$@"
