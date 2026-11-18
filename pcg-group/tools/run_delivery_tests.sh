#!/usr/bin/env bash
# Acceptance suite for durable change delivery (real OS processes).
# Every test drives coordinator/worker/management/delivery commands as
# separate processes and finishes with an independent reference audit that
# imports none of the production code. Set ONLY to run a single test class.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export PYTHONPATH="$ROOT/src"
cd "$ROOT/tests"
if [ "${1:-}" = "--help" ]; then
  echo "usage: $0 [unittest filters, e.g. TestInitialSync or TestAckValidation.test_double_ack_refused]"
  exit 0
fi
if [ "$#" -gt 0 ]; then
  python3 -m unittest test_delivery_e2e "$@" -v
else
  python3 -m unittest test_delivery_e2e -v
fi
