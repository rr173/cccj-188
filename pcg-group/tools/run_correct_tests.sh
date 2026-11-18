#!/usr/bin/env bash
# Acceptance suite for the source-record correction workflow.
# Real coordinator/worker/manager OS processes throughout; value, diagnostic
# and revision-chain checks go through the independent reference
# implementation in tests/tools_rule_ref.py (which imports none of the
# production cleaning code).
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export PYTHONPATH="$ROOT/src"
cd "$ROOT/tests"
python3 -m unittest test_correct_e2e -v "$@"
