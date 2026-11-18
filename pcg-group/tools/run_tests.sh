#!/usr/bin/env bash
# Run the full automated test suite (real multi-process end-to-end tests).
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export PYTHONPATH="$ROOT/src"
cd "$ROOT/tests"
python3 -m unittest test_e2e -v "$@"
