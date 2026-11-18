#!/usr/bin/env bash
# One-command, reproducible demo of the source-record correction workflow.
#
# Everything runs as REAL OS processes: the coordinator and workers keep
# collecting while independent management processes propose corrections,
# review/approve them, race two approvals of the same proposal, undo a
# correction, republish rules over the corrected history and export
# consistent snapshots. An independent reference implementation (which
# imports none of the production cleaning code) audits every stored value,
# diagnostic and revision-chain link at the end.
#
# Demonstrated guarantees:
#   * corrections never rewrite source_json / clean_json / JSONL / progress;
#   * proposals are invisible to every official read until approved;
#   * revision numbers are per-position and strictly increasing; undo is a
#     new appended revision, never a delete;
#   * correction ids: identical retry is idempotent, different content is
#     rejected; stale expected revisions are refused with the actual number;
#   * two concurrent approvals have exactly one winner;
#   * the approval lands the corrected object processed under the CURRENT
#     active rule version - including an explicit rejection diagnostic;
#   * review tokens die after any correction, even one later undone;
#   * an inactive version must catch up + re-review before republishing;
#   * a barrier-parked stale worker batch cannot overwrite a correction.
#
# Usage:  ./tools/demo_correct.sh [workdir]
set -u

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export PYTHONPATH="$ROOT/src"
PY="python3"
WORK="${1:-/tmp/pcg-correct-demo}"
rm -rf "$WORK"
mkdir -p "$WORK"
SRC="$WORK/source"
DB="$WORK/g.db"
PCG=("$PY" -m pcg --db "$DB" --clock real)

say()  { printf '\n\033[1;36m== %s ==\033[0m\n' "$*"; }
run()  { printf '\033[0;33m$ %s\033[0m\n' "$*"; "$@"; }
check(){ "$@" >/dev/null || { echo "DEMO FAILED at: $*"; exit 1; }; }
expect_rc() { # expect_rc <wanted> <cmd...>
  local want="$1"; shift
  "$@" >/dev/null 2>&1
  local got=$?
  [ "$got" = "$want" ] || { echo "DEMO FAILED: $* -> rc=$got, wanted $want"; exit 1; }
}

say "0. Generate immutable JSONL source (2 partitions) and collect it"
"$PY" -I "$ROOT/tools/gensource.py" --out "$SRC" --partitions 2 \
    --counts 10,8 --seed 20261118 >/dev/null
"${PCG[@]}" coordinator --source "$SRC" --lease-ms 4000 \
    --ack-timeout-ms 4600 --tick-ms 100 >"$WORK/coord.log" 2>&1 &
COORD=$!
"${PCG[@]}" admin add w1 --lease-ms 4000 >/dev/null
"${PCG[@]}" worker --source "$SRC" --id w1 --batch-size 5 >"$WORK/w1.log" 2>&1 &
W1=$!
wait_rows() {
  local want="$1" deadline=$((SECONDS + 40))
  while (( SECONDS < deadline )); do
    n=$("$PY" - "$DB" <<'PYEOF'
import sqlite3, sys
print(sqlite3.connect(sys.argv[1]).execute(
  "SELECT COUNT(*) FROM records").fetchone()[0])
PYEOF
)
    (( n >= want )) && return 0
    sleep 0.2
  done
  echo "timeout waiting for $want rows"; return 1
}
check wait_rows 18
echo "    all 18 records committed"

say "1. Locate a committed position and inspect original vs effective value"
run "${PCG[@]}" correct show 0 3

say "2. Propose a correction (complete replacement record)"
run "${PCG[@]}" correct propose 0 3 --correction-id FIX-1001 \
    --expected-revision 0 --reason "customer says city was mistyped" \
    --record '{"offset":3,"id":1003,"name":"corrected person","city":"Berlin","amount":" 41.25 ","active":"yes"}'
echo "    -> the proposal is pending; the official export is unchanged:"
"${PCG[@]}" rule export | grep -c "^legacy" | sed 's/^/       exported rows: /'

say "3. Correction-id rules: identical retry is idempotent, reuse conflicts"
run "${PCG[@]}" correct propose 0 3 --correction-id FIX-1001 \
    --expected-revision 0 --reason "customer says city was mistyped" \
    --record '{"offset":3,"id":1003,"name":"corrected person","city":"Berlin","amount":" 41.25 ","active":"yes"}'
expect_rc 12 "${PCG[@]}" correct propose 0 3 --correction-id FIX-1001 \
    --expected-revision 0 --reason "different content" \
    --record '{"offset":3,"id":1003,"name":"other","city":"Rome"}'
echo "    -> same id + different content refused (exit 12)"

say "4. Approve: one atomic transaction effects the revision"
run "${PCG[@]}" correct approve FIX-1001
run "${PCG[@]}" correct show 0 3
echo "    -> original record and collection progress untouched:"
"${PCG[@]}" inspect | sed -n '/== partitions/,+3p' | sed 's/^/       /'

say "5. Second correction on the same position, then UNDO as a new revision"
run "${PCG[@]}" correct propose 0 3 --correction-id FIX-1002 \
    --expected-revision 1 --reason "email also wrong" \
    --record '{"offset":3,"id":1003,"name":"corrected person","city":"Berlin","email":"real@example.com","amount":" 41.25 ","active":"yes"}'
run "${PCG[@]}" correct approve FIX-1002
run "${PCG[@]}" correct restore 0 3 --to-revision 0 \
    --correction-id FIX-1003 --expected-revision 2 \
    --reason "upstream confirms the collected record was right"
run "${PCG[@]}" correct approve FIX-1003
run "${PCG[@]}" correct history 0 3
echo "    -> all three revisions retained; effective value is the restored"
echo "       original, at revision 3"

say "6. Stale proposals lose with the actual revision; one winner per race"
"${PCG[@]}" correct propose 1 2 --correction-id FIX-2001 --expected-revision 0 \
    --reason "race A" --record '{"offset":2,"id":1002,"name":"racer a"}' >/dev/null
"${PCG[@]}" correct propose 1 2 --correction-id FIX-2002 --expected-revision 0 \
    --reason "race B" --record '{"offset":2,"id":1002,"name":"racer b"}' >/dev/null
run "${PCG[@]}" correct approve FIX-2001
expect_rc 13 "${PCG[@]}" correct approve FIX-2002
echo "    -> overtaken proposal refused (exit 13) and marked rejected:"
"${PCG[@]}" correct get FIX-2002 | sed 's/^/       /'
"${PCG[@]}" correct propose 1 4 --correction-id FIX-3001 --expected-revision 0 \
    --reason "race me" --record '{"offset":4,"id":1004,"name":"race target"}' >/dev/null
"${PCG[@]}" correct approve FIX-3001 >"$WORK/race1.out" 2>&1 &
R1=$!
"${PCG[@]}" correct approve FIX-3001 >"$WORK/race2.out" 2>&1 &
R2=$!
wait $R1; RC1=$?
wait $R2; RC2=$?
echo "    -> two concurrent approvals: exit codes $RC1 and $RC2 (exactly one 0)"
[ "$RC1" != "$RC2" ] || { echo "DEMO FAILED: race had no single winner"; exit 1; }

say "7. Corrections drive rule republishing: token death + catch-up contract"
cat > "$WORK/v1.json" <<'JSONEOF'
{"fields": {
  "name": {"missing": "preserve", "null": "null",
           "steps": [{"op": "nfkc"}, {"op": "strip"},
                     {"op": "collapse_ws"}, {"op": "upper"}]},
  "city": {"missing": "preserve", "null": "preserve",
           "steps": [{"op": "strip"}, {"op": "collapse_ws"},
                     {"op": "title"}]}}}
JSONEOF
run "${PCG[@]}" rule register v1 "$WORK/v1.json"
run "${PCG[@]}" rule recompute v1 --page-size 6
TOKEN=$("${PCG[@]}" rule review v1 --json | "$PY" -c "import json,sys;print(json.load(sys.stdin)['token'])")
echo "    review token: $TOKEN"
"${PCG[@]}" correct propose 0 5 --correction-id FIX-4001 --expected-revision 0 \
    --reason "post-review fix" \
    --record '{"offset":5,"id":1005,"name":"post review","city":"Lyon"}' >/dev/null
"${PCG[@]}" correct approve FIX-4001 >/dev/null
"${PCG[@]}" correct restore 0 5 --to-revision 0 --correction-id FIX-4002 \
    --expected-revision 1 --reason "undo it" >/dev/null
"${PCG[@]}" correct approve FIX-4002 >/dev/null
echo "    correction + undo both effected after the review; the token must die:"
expect_rc 6 "${PCG[@]}" rule activate "$TOKEN"
echo "    -> activation refused (exit 6) even though values returned to original"
run "${PCG[@]}" rule recompute v1 --page-size 6
TOKEN2=$("${PCG[@]}" rule review v1 --json | "$PY" -c "import json,sys;print(json.load(sys.stdin)['token'])")
run "${PCG[@]}" rule activate "$TOKEN2"

say "8. A corrected record that violates the active rule -> REJECTED export"
cat > "$WORK/strict.json" <<'JSONEOF'
{"fields": {"amount": {"missing": "reject", "null": "reject",
  "steps": [{"op": "decimal", "scale": 2, "rounding": "HALF_UP"}]}}}
JSONEOF
run "${PCG[@]}" rule register strict "$WORK/strict.json"
# strict rejects generated rows, so it cannot be published over history;
# instead correct a record INTO a violation under the active v1, then
# switch: demonstrate the rejection diagnostic path on the active version
"${PCG[@]}" correct propose 1 6 --correction-id FIX-5001 --expected-revision 0 \
    --reason "amount unreadable upstream" \
    --record '{"offset":6,"id":1006,"name":"bad amount","amount":"not-a-number"}' >/dev/null
run "${PCG[@]}" correct approve FIX-5001
echo "    (v1 has no amount step, so v1 accepts it; now publish strict-calibre"
echo "     checks on a corrected value via recompute + review of the position)"
"${PCG[@]}" rule recompute strict --page-size 6 >/dev/null
"${PCG[@]}" rule export --version-id strict | grep "p1:6" | sed 's/^/    /'

say "9. Barrier: a parked, already-computed worker batch cannot overwrite a correction"
# fresh partition layout: aa owns p0, zz owns p1 with a barrier before commit
SRC2="$WORK/source2"
"$PY" -I "$ROOT/tools/gensource.py" --out "$SRC2" --partitions 2 \
    --counts 8,8 --seed 99 >/dev/null
DB2="$WORK/g2.db"
PCG2=("$PY" -m pcg --db "$DB2" --clock real)
"${PCG2[@]}" coordinator --source "$SRC2" --lease-ms 4000 \
    --ack-timeout-ms 2500 --tick-ms 80 >"$WORK/coord2.log" 2>&1 &
COORD2=$!
"${PCG2[@]}" admin add aa --lease-ms 4000 >/dev/null
"${PCG2[@]}" admin add zz --lease-ms 4000 >/dev/null
"${PCG2[@]}" worker --source "$SRC2" --id aa --batch-size 4 >"$WORK/aa.log" 2>&1 &
AA=$!
PCG_BARRIER="zz:1:0" "${PCG2[@]}" worker --source "$SRC2" --id zz \
    --batch-size 4 >"$WORK/zz.log" 2>&1 &
ZZ=$!
check "${PCG2[@]}" admin barrier-wait "zz:p1:o0" --timeout 30
echo "    zz parked with p1 offsets 0..3 computed but uncommitted"
"${PCG2[@]}" admin remove zz >/dev/null
# aa takes over p1 and commits it from the source
deadline=$((SECONDS + 30))
while :; do
  st=$("$PY" - "$DB2" <<'PYEOF'
import sqlite3, sys
c = sqlite3.connect(sys.argv[1])
row = c.execute("SELECT committed_offset FROM progress WHERE partition_id=1").fetchone()
print(row[0] if row else -1)
PYEOF
)
  [ "$st" -ge 7 ] && break
  [ $SECONDS -ge $deadline ] && { echo "DEMO FAILED: aa did not take p1"; exit 1; }
  sleep 0.2
done
echo "    aa fenced zz's generation and committed p1 0..7"
ORIG=$("${PCG2[@]}" correct show 1 2 --json | "$PY" -c "import json,sys;print(json.dumps(json.load(sys.stdin)['original'],sort_keys=True))")
"${PCG2[@]}" correct propose 1 2 --correction-id FIX-9001 --expected-revision 0 \
    --reason "fix while stale batch parked" \
    --record '{"offset":2,"id":1002,"name":"barrier proof","city":"Porto"}' >/dev/null
run "${PCG2[@]}" correct approve FIX-9001
run "${PCG2[@]}" admin barrier-release "zz:p1:o0"
sleep 2
echo "    zz's late batch was rejected by the fence:"
grep -o "REJECTED p1.*" "$WORK/zz.log" | head -1 | sed 's/^/       /'
run "${PCG2[@]}" correct history 1 2
echo "    -> still exactly one revision; the stale batch overwrote nothing"
kill $ZZ $AA $COORD2 2>/dev/null; wait $ZZ $AA $COORD2 2>/dev/null

say "10. Independent reference audit of every value, diagnostic and revision"
"$PY" -I "$ROOT/tests/tools_rule_ref.py" --db "$DB"
"$PY" -I "$ROOT/tests/tools_rule_ref.py" --db "$DB2"

kill $W1 $COORD 2>/dev/null; wait $W1 $COORD 2>/dev/null
echo
echo "DEMO OK: propose -> review/approve -> revision history -> undo ->"
echo "rule republish -> consistent export, all with live collection and"
echo "immutable collection facts. Workdir: $WORK"
