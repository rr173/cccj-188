#!/usr/bin/env bash
# One-command, reproducible terminal demo of the partitioned consumer group.
#
# Everything runs as REAL OS processes (coordinator + workers + admin),
# coordinated only through the shared SQLite database, on the controllable
# manual clock so lease/fence timing is deterministic. The headline scenario
# parks a worker between compute and commit with a barrier, transfers the
# partition, proves the late whole-batch is rejected, then reconciles every
# offset/value with an independent reference reader.
#
# Usage:  ./tools/demo.sh [workdir]
set -u

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export PYTHONPATH="$ROOT/src"
PY="python3"
WORK="${1:-/tmp/pcg-demo}"
rm -rf "$WORK"
mkdir -p "$WORK"
SRC="$WORK/source"
DB="$WORK/g.db"
PCG=("$PY" -m pcg --db "$DB" --clock manual)

say() { printf '\n\033[1;36m== %s ==\033[0m\n' "$*"; }
run() { printf '\033[0;33m$ %s\033[0m\n' "$*"; "$@"; }

say "0. Generate immutable JSONL source (4 partitions, incl. an empty one)"
"$PY" -I "$ROOT/tools/gensource.py" --out "$SRC" --partitions 4 \
    --counts 12,0,9,15 --seed 20261118

say "1. Start the coordinator (single instance, flock-guarded; manual clock)"
"${PCG[@]}" coordinator --source "$SRC" --lease-ms 6000 \
    --ack-timeout-ms 3000 --tick-ms 50 >"$WORK/coord.log" 2>&1 &
COORD=$!
sleep 0.8

say "2. Pre-register two members and start their worker processes"
"${PCG[@]}" admin add alpha --lease-ms 6000 >/dev/null
"${PCG[@]}" admin add beta  --lease-ms 6000 >/dev/null
# beta will block AFTER computing p3 offset 0..4 and BEFORE commit
PCG_BARRIER="beta:3:0" "${PCG[@]}" worker --source "$SRC" --id beta \
    --lease-ms 6000 --batch-size 5 >"$WORK/beta.log" 2>&1 &
BETA=$!
"${PCG[@]}" worker --source "$SRC" --id alpha \
    --lease-ms 6000 --batch-size 5 >"$WORK/alpha.log" 2>&1 &
ALPHA=$!
sleep 2

say "3. Inspect: members, plan, per-partition ownership, progress"
"${PCG[@]}" inspect | sed -n '1,30p'

say "4. Wait until beta reaches the compute->commit barrier on p3"
"${PCG[@]}" admin barrier-wait "beta:p3:o0" --timeout 15 >/dev/null
run "$PY" - "$DB" <<'PYEOF'
import sqlite3, sys
c = sqlite3.connect(sys.argv[1]); c.row_factory = sqlite3.Row
print("p3 committed progress before handoff:",
      c.execute("SELECT committed_offset FROM progress WHERE partition_id=3").fetchone())
print("barrier:", dict(c.execute("SELECT * FROM barrier_events").fetchone()))
PYEOF

say "5. Membership change: alpha leaves -> beta must take over p3 (a new generation)"
OLDGEN=$("$PY" - "$DB" <<'PYEOF'
import sqlite3, sys
c = sqlite3.connect(sys.argv[1])
print(c.execute("SELECT gen FROM assignments WHERE partition_id=3 "
                "ORDER BY gen DESC LIMIT 1").fetchone()[0])
PYEOF
)
echo "old generation for p3: $OLDGEN"
"${PCG[@]}" admin remove alpha >/dev/null
sleep 0.5
kill -TERM "$ALPHA" 2>/dev/null || true
wait "$ALPHA" 2>/dev/null || true
sleep 1
say "   Revocation published; old term is revoking (beta still parked at barrier)"
"$PY" - "$DB" "$OLDGEN" <<'PYEOF'
import sqlite3, sys
c = sqlite3.connect(sys.argv[1]); c.row_factory = sqlite3.Row
g = sys.argv[2]
print(dict(c.execute("SELECT partition_id,gen,state FROM revocations "
                     "WHERE partition_id=3 AND gen=?", (g,)).fetchone()))
print(dict(c.execute("SELECT gen,worker_id,state FROM assignments "
                     "WHERE partition_id=3 ORDER BY gen").fetchall()[0]))
PYEOF

say "6. Release beta: its fully-computed but now-unauthorised batch is REJECTED"
"${PCG[@]}" admin barrier-release "beta:p3:o0" >/dev/null
sleep 2
echo "--- beta worker log (note the REJECTED line):"
grep -E "REJECTED|committed" "$WORK/beta.log" | head -8
"$PY" - "$DB" <<'PYEOF'
import sqlite3, sys
c = sqlite3.connect(sys.argv[1]); c.row_factory = sqlite3.Row
r = c.execute("SELECT * FROM rejected_commits ORDER BY id LIMIT 1").fetchone()
print("rejected_commits audit:", dict(r) if r else None)
PYEOF

say "7. Let the new generation finish; show final ownership and progress"
sleep 4
"${PCG[@]}" inspect | sed -n '/== partitions/,/== rejected/p'

say "8. Independent reference reconciliation (re-reads source, recomputes values)"
"$PY" -I "$ROOT/tools/reconcile.py" --db "$DB" --source "$SRC"
RECON=$?

say "9. Final inspect: rejected late commits + stale-ack audit trails"
"${PCG[@]}" inspect | sed -n '/== rejected/,$p'

say "10. SIGKILL beta to demonstrate lease-based failure recovery"
kill -KILL "$BETA" 2>/dev/null
sleep 2
"${PCG[@]}" admin list || true

kill -TERM "$COORD" 2>/dev/null
wait "$COORD" 2>/dev/null
echo
if [ "$RECON" -eq 0 ]; then
  echo "DEMO OK: every offset/value reconciled, late batch rejected, no double delivery."
else
  echo "DEMO FAILED reconciliation (rc=$RECON)"; exit 1
fi
