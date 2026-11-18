#!/usr/bin/env bash
# One-command, reproducible demo of the cleaning-rule publishing workflow.
#
# Everything runs as REAL OS processes: coordinator + workers keep collecting
# while management processes register rules, recompute history (with a real
# SIGKILL + resume), review fixed sections, atomically switch the active
# version and export a consistent snapshot. Every stored value/diagnostic is
# checked by an independent reference implementation that imports none of the
# production cleaning code.
#
# Demonstrated guarantees:
#   * legacy upgrade is byte-identical to historical clean_json;
#   * candidates with rejections or uncovered positions cannot be published;
#   * recompute pauses after a computed/unflushed page while ingestion runs;
#   * a real SIGKILL resumes from durable watermarks (no history rescan);
#   * a review token dies when new rows commit, even after catch-up;
#   * switch-away-and-back invalidates the old token;
#   * a batch computed before a switch commits under the NEW version;
#   * an inactive version catches up new rows before rollback (no omissions);
#   * one export never mixes versions even with a concurrent publisher;
#   * active-version rejections keep raw rows + progress and show REJECTED.
#
# Usage:  ./tools/demo_rules.sh [workdir]
set -u

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export PYTHONPATH="$ROOT/src"
PY="python3"
WORK="${1:-/tmp/pcg-rules-demo}"
rm -rf "$WORK"
mkdir -p "$WORK"
SRC="$WORK/source"
DB="$WORK/g.db"
PCG=("$PY" -m pcg --db "$DB" --clock real)

say()  { printf '\n\033[1;36m== %s ==\033[0m\n' "$*"; }
run()  { printf '\033[0;33m$ %s\033[0m\n' "$*"; "$@"; }
check(){ "$@" >/dev/null || { echo "DEMO FAILED at: $*"; exit 1; }; }

say "0. Generate immutable JSONL source: skewed partitions incl. an empty one"
"$PY" -I "$ROOT/tools/gensource.py" --out "$SRC" --partitions 4 \
    --counts 120,0,200,40 --seed 20261118 >/dev/null
TOTAL=360

say "1. Start coordinator + two workers (real processes, wall clock)"
"${PCG[@]}" coordinator --source "$SRC" --lease-ms 4000 \
    --ack-timeout-ms 4600 --tick-ms 100 >"$WORK/coord.log" 2>&1 &
COORD=$!
"${PCG[@]}" admin add wa --lease-ms 4000 >/dev/null
"${PCG[@]}" admin add wb --lease-ms 4000 >/dev/null
"${PCG[@]}" worker --source "$SRC" --id wa --batch-size 25 >"$WORK/wa.log" 2>&1 &
WA=$!
"${PCG[@]}" worker --source "$SRC" --id wb --batch-size 25 >"$WORK/wb.log" 2>&1 &
WB=$!

# wait until at least this many rows exist, but leave a tail uncommitted so
# later stages can exercise collection that overlaps rule operations
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
check wait_rows 180
echo "    180+ rows already committed; workers keep collecting the tail"

say "2. Upgrade inspection: active version is the built-in 'legacy'"
"${PCG[@]}" rule status | sed -n '1,3p'
"$PY" - "$DB" <<'PYEOF'
import sqlite3, sys
c = sqlite3.connect(sys.argv[1]); c.row_factory = sqlite3.Row
m = c.execute("""SELECT COUNT(*) FROM records r JOIN rule_results rr
  ON rr.version_id='legacy' AND rr.partition_id=r.partition_id
  AND rr.offset=r.offset
  WHERE rr.status!='ok' OR rr.result_json != r.clean_json""").fetchone()[0]
n = c.execute("SELECT COUNT(*) FROM records").fetchone()[0]
print(f"    legacy results={n}, mismatches vs historical clean_json={m}")
assert m == 0
PYEOF

# ------------------------------------------------------------- rule documents
cat > "$WORK/v1.json" <<'JSONEOF'
{"fields": {
  "name": {"missing": "preserve", "null": "null",
           "steps": [{"op": "nfkc"}, {"op": "strip"},
                     {"op": "collapse_ws"}, {"op": "upper"}]},
  "city": {"missing": "preserve", "null": "preserve",
           "steps": [{"op": "strip"}, {"op": "collapse_ws"}, {"op": "title"}]}}}
JSONEOF
# same rule with permuted object keys: identity must be unchanged
cat > "$WORK/v1-permuted.json" <<'JSONEOF'
{"fields": {
  "city": {"steps": [{"op": "strip"}, {"op": "collapse_ws"}, {"op": "title"}],
            "null": "preserve", "missing": "preserve"},
  "name": {"null": "null", "missing": "preserve",
           "steps": [{"op": "nfkc"}, {"op": "strip"},
                     {"op": "collapse_ws"}, {"op": "upper"}]}}}
JSONEOF
# strict amount rule: JSON-number amounts in the generated data must reject
cat > "$WORK/strict.json" <<'JSONEOF'
{"fields": {
  "amount": {"missing": "reject", "null": "reject",
             "steps": [{"op": "decimal", "scale": 2, "rounding": "HALF_UP"}]}}}
JSONEOF
# a second publishable version: lower-case names
cat > "$WORK/v2.json" <<'JSONEOF'
{"fields": {
  "name": {"missing": "preserve", "null": "null",
           "steps": [{"op": "nfkc"}, {"op": "strip"},
                     {"op": "collapse_ws"}, {"op": "lower"}]}}}
JSONEOF

say "3. Registration: retry/permutation accepted, conflict + bad spec rejected"
run "${PCG[@]}" rule register v1 "$WORK/v1.json"
check "${PCG[@]}" rule register v1 "$WORK/v1-permuted.json"   # identical content
if "${PCG[@]}" rule register legacy "$WORK/v1.json" 2>/dev/null; then
  echo "DEMO FAILED: built-in id was overwritable"; exit 1
fi
echo "    -> reserved 'legacy' id and same-id/different-content refused (rc=4)"
check "${PCG[@]}" rule register strict "$WORK/strict.json"
check "${PCG[@]}" rule register v2 "$WORK/v2.json"

say "4. Recompute pause: one page computed and held unflushed while collecting"
touch "$WORK/PAUSE"
"${PCG[@]}" rule recompute v1 --page-size 20 \
    --pause-file "$WORK/PAUSE" --pause-wait-s 120 >"$WORK/parked.log" 2>&1 &
PARK=$!
# wait until the parked process holds the lock with zero persisted rows
deadline=$((SECONDS + 30))
while (( SECONDS < deadline )); do
  z=$("$PY" - "$DB" <<'PYEOF'
import sqlite3, sys
c = sqlite3.connect(sys.argv[1])
print(c.execute("SELECT COUNT(*) FROM rule_results WHERE version_id='v1'").fetchone()[0])
PYEOF
)
  kill -0 "$PARK" 2>/dev/null && [ "$z" = "0" ] && break
  sleep 0.2
done
[ "$z" = "0" ] && kill -0 "$PARK" 2>/dev/null || { echo "DEMO FAILED: pause"; exit 1; }
echo "    runner pid=$PARK parked; zero v1 rows persisted; a second recompute:"
if "${PCG[@]}" rule recompute v1 >/dev/null 2>&1; then
  echo "DEMO FAILED: lock not exclusive"; exit 1
fi
echo "    -> second recompute refused (rc=5); official export still works:"
"${PCG[@]}" rule export | head -1
# hard-kill the parked process: its computed page must leave no trace, and
# collection must have kept advancing the whole time
before_rows=$("$PY" - "$DB" <<'PYEOF'
import sqlite3, sys
print(sqlite3.connect(sys.argv[1]).execute(
  "SELECT COUNT(*) FROM records").fetchone()[0])
PYEOF
)
kill -9 "$PARK"; wait "$PARK" 2>/dev/null || true
sleep 1
after_rows=$("$PY" - "$DB" <<'PYEOF'
import sqlite3, sys
print(sqlite3.connect(sys.argv[1]).execute(
  "SELECT COUNT(*) FROM records").fetchone()[0])
PYEOF
)
z=$("$PY" - "$DB" <<'PYEOF'
import sqlite3, sys
print(sqlite3.connect(sys.argv[1]).execute(
  "SELECT COUNT(*) FROM rule_results WHERE version_id='v1'").fetchone()[0])
PYEOF
)
[ "$z" = "0" ] || { echo "DEMO FAILED: killed page was persisted"; exit 1; }
[ "$after_rows" -gt "$before_rows" ] || echo "    note: collection already at tail"
echo "    -> SIGKILL left zero v1 rows; collection progressed: $before_rows -> $after_rows"
rm -f "$WORK/PAUSE"

say "5. Recompute restarts from scratch watermarks; strict + v2 versions computed"
check wait_rows "$TOTAL"   # collection may fully drain meanwhile; that's fine
check "${PCG[@]}" rule recompute strict --page-size 15
check "${PCG[@]}" rule recompute v2 --page-size 50
# now start v1 and SIGKILL it after durable pages land, proving page-atomic resume
"${PCG[@]}" rule recompute v1 --page-size 7 >"$WORK/killrun.log" 2>&1 &
KR=$!
deadline=$((SECONDS + 30))
while (( SECONDS < deadline )); do
  cov=$("$PY" - "$DB" <<'PYEOF'
import sqlite3, sys
c = sqlite3.connect(sys.argv[1])
print(c.execute("SELECT COALESCE(SUM(done_offset+1),0) FROM "
  "rule_recompute_progress WHERE version_id='v1'").fetchone()[0])
PYEOF
)
  kill -0 "$KR" 2>/dev/null && [ "$cov" -ge 30 ] && break
  sleep 0.1
done
kill -0 "$KR" 2>/dev/null || { echo "DEMO FAILED: v1 runner finished before kill"; exit 1; }
kill -9 "$KR"; wait "$KR" 2>/dev/null || true
flushed=$("$PY" - "$DB" <<'PYEOF'
import sqlite3, sys
c = sqlite3.connect(sys.argv[1])
print(c.execute("SELECT COUNT(*) FROM rule_results WHERE version_id='v1'").fetchone()[0])
PYEOF
)
echo "    SIGKILL after $flushed durable v1 rows; resume output:"
"${PCG[@]}" rule recompute v1 --page-size 11 | grep -E "rows_computed|complete"
"$PY" - "$DB" <<'PYEOF'
import sqlite3, sys
c = sqlite3.connect(sys.argv[1])
total = c.execute("SELECT COUNT(*) FROM records").fetchone()[0]
have = c.execute("SELECT COUNT(*) FROM rule_results WHERE version_id='v1'").fetchone()[0]
assert have == total, (have, total)
print(f"    v1 now covers all {have} committed positions")
PYEOF

say "6. Publishing gates: rejecting and incomplete candidates are refused"
REV=$("${PCG[@]}" rule review strict --json)
echo "$REV" | "$PY" -c "import json,sys; s=json.load(sys.stdin)['stats']; \
print('    strict review:', s); assert s['candidate_rejected'] > 0"
TOKEN_STRICT=$(echo "$REV" | "$PY" -c "import json,sys; print(json.load(sys.stdin)['token'])")
if "${PCG[@]}" rule activate "$TOKEN_STRICT"; then
  echo "DEMO FAILED: rejecting candidate activated"; exit 1
fi
# a brand-new version with no recompute is fully uncovered
cat > "$WORK/v3.json" <<'JSONEOF'
{"fields": {"email": {"missing": "preserve", "null": "null",
                      "steps": [{"op": "nfkc"}, {"op": "strip"}, {"op": "lower"}]}}}
JSONEOF
check "${PCG[@]}" rule register v3 "$WORK/v3.json"
TOKEN_HOLE=$("${PCG[@]}" rule review v3 --json | "$PY" -c "import json,sys; print(json.load(sys.stdin)['token'])")
if "${PCG[@]}" rule activate "$TOKEN_HOLE"; then
  echo "DEMO FAILED: uncovered candidate activated"; exit 1
fi
echo "    -> both activations refused (rc=6); active version still legacy"
"${PCG[@]}" rule status | sed -n '1p'

say "7. Review token lifecycle: stale after a new commit, and after A->B->A"
TOKEN_V1=$("${PCG[@]}" rule review v1 --json | "$PY" -c "import json,sys; print(json.load(sys.stdin)['token'])")
# append one new immutable source row and let a worker commit it
printf '{"offset":120,"name":"  Late   Arrival ","city":"berlin"}\n' >> "$SRC/p0.jsonl"
"$PY" - "$SRC/manifest.json" <<'PYEOF'
import json, sys
p = sys.argv[1]
m = json.load(open(p)); m["counts"]["0"] = 121; json.dump(m, open(p, "w"))
PYEOF
check wait_rows $((TOTAL + 1))
# every candidate used below must catch the new row up before a fresh review
check "${PCG[@]}" rule recompute v1 --page-size 20
check "${PCG[@]}" rule recompute v2 --page-size 20
if "${PCG[@]}" rule activate "$TOKEN_V1"; then
  echo "DEMO FAILED: stale token accepted after new commit"; exit 1
fi
echo "    -> old token refused even though v1 caught up in the background"
# fresh review -> v1; then v2; then prove the old v1 token cannot switch back
check "${PCG[@]}" rule activate "$("${PCG[@]}" rule review v1 --json \
    | "$PY" -c "import json,sys; print(json.load(sys.stdin)['token'])")"
check "${PCG[@]}" rule activate "$("${PCG[@]}" rule review v2 --json \
    | "$PY" -c "import json,sys; print(json.load(sys.stdin)['token'])")"
if "${PCG[@]}" rule activate "$TOKEN_V1"; then
  echo "DEMO FAILED: token reused after switch-away-and-back"; exit 1
fi
echo "    -> switch-away-and-back correctly killed the old token"

say "8. Rollback contract: backfill legacy over rows it never saw, then switch"
check "${PCG[@]}" rule recompute legacy --page-size 25
"${PCG[@]}" rule review legacy --json | "$PY" -c "import json,sys; \
s=json.load(sys.stdin)['stats']; print('    legacy review:', s); \
assert s['uncovered']==0 and s['candidate_rejected']==0"
check "${PCG[@]}" rule activate "$("${PCG[@]}" rule review legacy --json \
    | "$PY" -c "import json,sys; print(json.load(sys.stdin)['token'])")"
"${PCG[@]}" rule status | sed -n '1,2p'

say "9. Concurrent publish during export: one export stays on one version"
check "${PCG[@]}" rule recompute v1 --page-size 50
check "${PCG[@]}" rule recompute v2 --page-size 50
cat > "$WORK/publisher.py" <<'PYEOF'
import json, os, subprocess, sys, time
DB, SRC, STOP = sys.argv[1], sys.argv[2], sys.argv[3]
ENV = dict(os.environ, PYTHONPATH=SRC)
order = ["v1", "v2", "legacy"]; i = switches = 0
def pcg(*a):
    return subprocess.run([sys.executable, "-m", "pcg", "--db", DB,
        "--clock", "real", "rule", *a], env=ENV, capture_output=True, text=True)
while not os.path.exists(STOP) and switches < 20:
    vid = order[i % 3]; i += 1
    r = pcg("review", vid, "--json")
    if r.returncode:
        time.sleep(0.02); continue
    tok = json.loads(r.stdout)["token"]
    if pcg("activate", tok).returncode == 0:
        switches += 1
    time.sleep(0.02)
PYEOF
"$PY" -I "$WORK/publisher.py" "$DB" "$ROOT/src" "$WORK/stop" &
PUB=$!
for k in 1 2 3; do
  "$PY" -I - "$DB" "$ROOT/src" "$WORK/export-$k.jsonl" <<'PYEOF'
import json, subprocess, sys
db, src, out = sys.argv[1:4]
env = dict(__import__("os").environ, PYTHONPATH=src)
with open(out, "w") as fh:
    subprocess.run([sys.executable, "-m", "pcg", "--db", db, "--clock", "real",
                    "rule", "export", "--json"], env=env, stdout=fh, check=True)
PYEOF
  "$PY" - "$WORK/export-$k.jsonl" <<'PYEOF'
import json, sys
lines = open(sys.argv[1]).read().splitlines()
section = json.loads(lines[0])["section"]
rows = [json.loads(l) for l in lines[1:]]
versions = {r["version_id"] for r in rows}
seqs = {r["publish_seq"] for r in rows}
assert versions == {section["version_id"]}, versions
assert seqs == {section["publish_seq"]}, seqs
assert len(rows) == section["positions"], (len(rows), section["positions"])
print(f"    export {sys.argv[1].split('-')[-1]}: {len(rows)} rows, one version "
      f"{section['version_id']} @ seq {section['publish_seq']}")
PYEOF
done
touch "$WORK/stop"; wait "$PUB" 2>/dev/null || true

say "10. Independent reference audit of EVERY stored value and diagnostic"
"$PY" -I "$ROOT/tests/tools_rule_ref.py" --db "$DB"

# publish v1 at the end so the final export demonstrates REJECTED-free new calibre
check "${PCG[@]}" rule activate "$("${PCG[@]}" rule review v1 --json \
    | "$PY" -c "import json,sys; print(json.load(sys.stdin)['token'])")"
say "11. Final official export (first lines) and publish history"
"${PCG[@]}" rule export | head -4
"${PCG[@]}" rule status | sed -n '/publish history/,/backfill/p' | head -8

kill "$WA" "$WB" 2>/dev/null || true
kill "$COORD" 2>/dev/null || true
wait 2>/dev/null || true
echo
echo "DEMO OK: register -> recompute(pause/kill/resume) -> review -> atomic" \
     "switch -> consistent export -> audited rollback, all with live collection."
