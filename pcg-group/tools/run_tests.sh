#!/usr/bin/env bash
# Run the automated test suites (real multi-process end-to-end tests).
#   test_e2e           collection, fencing and safe handoff
#   test_rules_e2e     versioned cleaning-rule publishing (see run_rule_tests.sh)
#   test_correct_e2e   source-record corrections (see run_correct_tests.sh)
#   test_delivery_e2e  durable change delivery / persistent consumers
# Set ONLY=e2e, rules, correct, delivery or all to select a suite.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export PYTHONPATH="$ROOT/src"
cd "$ROOT/tests"
case "${ONLY:-all}" in
  e2e)      python3 -m unittest test_e2e -v "$@" ;;
  rules)    python3 -m unittest test_rules_e2e -v "$@" ;;
  correct)  python3 -m unittest test_correct_e2e -v "$@" ;;
  delivery) python3 -m unittest test_delivery_e2e -v "$@" ;;
  all)      python3 -m unittest test_e2e test_rules_e2e test_correct_e2e \
                test_delivery_e2e -v "$@" ;;
  *) echo "ONLY must be e2e, rules, correct, delivery or all" >&2; exit 2 ;;
esac
