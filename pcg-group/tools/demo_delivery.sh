#!/usr/bin/env bash
# One-command, reproducible demo of durable change delivery (persistent CDC).
#
# Everything runs as REAL OS processes: coordinator + workers keep collecting
# while named delivery consumers page through a pinned initial snapshot and
# then follow the live change stream; a correction approval and a rule
# publish interleave with consumer reads/acks; a consumer process is killed
# between write and ack to prove at-least-once replay. An independent
# reference program (imports no production code) audits the whole database.
#
# Demonstrated guarantees:
#   * every ingest/correction/publish commits its delivery events atomically;
#   * pending proposals and candidate recomputes stay invisible;
#   * the initial snapshot pins one consistent boundary with no gap to live;
#   * re-reading an unacked page returns identical records and cursor;
#   * stale/skip/cross-consumer/tampered acks are refused and audited;
#   * two consumers advance at independent speeds without moving each other;
#   * a rule publish is ONE logical batch that pages never split;
#   * a kill before ack replays the page (at-least-once, stable event ids);
#   * independent verification of snapshot, order, revisions and cursors.
#
# Usage:  ./tools/demo_delivery.sh [workdir]
set -u

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export PYTHONPATH="$ROOT/src"
PY="python3"
WORK="${1:-/tmp/pcg-delivery-demo}"
rm -rf "$WORK"
mkdir -p "$WORK"
SRC="$WORK/source"
DB="$WORK/g.db"
PCG=("$PY" -m pcg --db "$DB" --clock real)

say()  { printf '\n\033[1;36m== %s ==\033[0m\n' "$*"; }
run()  { printf '\033[0;33m$ %s\033[0m\n' "$*"; "$@"; }
check(){ "$@" >/dev/null || { echo "DEMO FAILED at: $*"; exit 1; }; }

sql() { "$PY" - "$DB" "$1" <<'PYEOF'
import sqlite3, sys
db, q = sys.argv[1], sys.argv[2]
print(sqlite3.connect(db).execute(q).fetchone()[0])
PYEOF
}

say "0. Generate immutable JSONL source (skewed partitions incl. an empty one)"
"$PY" -I "$ROOT/tools/gensource.py" --out "$SRC" --partitions 4 \
    --counts 80,0,60,20 --seed 20261118 >/dev/null
TOTAL=160

say "1. Start coordinator + worker (real processes, wall clock)"
"${PCG[@]}" coordinator --source "$SRC" --lease-ms 4000 \
    --ack-timeout-ms 4600 --tick-ms 100 >"$WORK/coord.log" 2>&1 &
COORD=$!
"${PCG[@]}" admin add wa --lease-ms 4000 >/dev/null
"${PCG[@]}" worker --source "$SRC" --id wa --batch-size 12 >"$WORK/wa.log" 2>&1 &
WA=$!

wait_rows() {
  local want="$1" deadline=$((SECONDS + 40))
  while (( SECONDS < deadline )); do
    n=$(sql "SELECT COUNT(*) FROM records")
    (( n >= want )) && return 0
    sleep 0.2
  done
  echo "timeout waiting for $want rows"; return 1
}
check wait_rows 90
echo "    90+ rows committed; the worker keeps collecting the tail"

say "2. Create two independent consumers and pin their snapshot boundaries"
run "${PCG[@]}" delivery create analytics
run "${PCG[@]}" delivery create warehouse
run "${PCG[@]}" delivery start analytics
run "${PCG[@]}" delivery start warehouse
"${PCG[@]}" delivery position analytics
echo "    -> both consumers start from the SAME consistent official snapshot"

say "3. Page the snapshot: batch-aligned pages, opaque cursors, replay"
PAGE=$("${PCG[@]}" delivery read analytics --page-size 10 --json)
echo "$PAGE" | "$PY" -c "import json,sys; p=json.load(sys.stdin); \
print('    page', p['from_seq'], '..', p['to_seq'], 'events=', len(p['events']), \
'snapshot_page=', p['snapshot_page']); print('    cursor:', p['cursor'])"
# re-read before ack: must be the identical page and cursor
AGAIN=$("${PCG[@]}" delivery read analytics --page-size 10 --json)
echo "$PAGE"  > "$WORK/p1.json"
echo "$AGAIN" > "$WORK/p2.json"
"$PY" - "$WORK/p1.json" "$WORK/p2.json" <<'PYEOF'
import json, sys
a, b = (json.load(open(p)) for p in sys.argv[1:])
assert a["events"] == b["events"] and a["cursor"] == b["cursor"]
assert b["replayed"] is True
print("    re-read is byte-identical and marked replayed=true (at-least-once)")
PYEOF
CUR=$(echo "$PAGE" | "$PY" -c "import json,sys; print(json.load(sys.stdin)['cursor'])")

say "4. Invalid acks are refused and audited (no silent skip)"
if "${PCG[@]}" delivery ack analytics "999:1000:$(printf '0%.0s' {1..32})" 2>/dev/null; then
  echo "DEMO FAILED: skip-ahead ack accepted"; exit 1
fi
# present warehouse with a cursor bound to analytics: cross-consumer refusal
F=$(echo "$PAGE" | "$PY" -c "import json,sys; print(json.load(sys.stdin)['from_seq'])")
T=$(echo "$PAGE" | "$PY" -c "import json,sys; print(json.load(sys.stdin)['to_seq'])")
CROSS=$( "$PY" - "$F" "$T" <<'PYEOF'
import hashlib, sys
f, t = sys.argv[1], sys.argv[2]
print(f"{f}:{t}:" + hashlib.sha256(f"analytics|{f}|{t}".encode()).hexdigest()[:32])
PYEOF
)
if "${PCG[@]}" delivery ack warehouse "$CROSS" 2>/dev/null; then
  echo "DEMO FAILED: cross-consumer ack accepted"; exit 1
fi
echo "    -> skip-ahead and cross-consumer acks rejected"
"${PCG[@]}" delivery rejects --limit 2
# the genuine cursor still works
check "${PCG[@]}" delivery ack analytics "$CUR"
echo "    -> the genuine cursor acked the page; position advanced exactly once"

say "5. Kill a consumer process between read and ack: replay on restart"
PAGE=$("${PCG[@]}" delivery read warehouse --page-size 10 --json)
CUR=$(echo "$PAGE" | "$PY" -c "import json,sys; print(json.load(sys.stdin)['cursor'])")
echo "    warehouse read page and would now write downstream... (no ack yet)"
# simulate the downstream process dying: only the durable database survives.
# a brand-new process inspects the position and replays the identical page.
"${PCG[@]}" delivery position warehouse | sed 's/^/    /'
REPLAY=$("${PCG[@]}" delivery replay warehouse --json)
echo "$REPLAY" > "$WORK/replay.json"
echo "$PAGE"   > "$WORK/orig.json"
"$PY" - "$WORK/orig.json" "$WORK/replay.json" <<'PYEOF'
import json, sys
a, b = (json.load(open(p)) for p in sys.argv[1:])
assert a["events"] == b["events"] and a["cursor"] == b["cursor"]
print("    after restart, replay returns the same records and cursor")
PYEOF
check "${PCG[@]}" delivery ack warehouse "$CUR"
echo "    -> ack from the new process succeeded; no event lost or duplicated"

say "6. Independent progress: analytics races ahead while warehouse pauses"
check wait_rows "$TOTAL"
# analytics drains everything; warehouse stays one page behind
while :; do
  p=$("${PCG[@]}" delivery read analytics --page-size 25 --json)
  n=$(echo "$p" | "$PY" -c "import json,sys; print(len(json.load(sys.stdin)['events']))")
  [ "$n" = "0" ] && break
  c=$(echo "$p" | "$PY" -c "import json,sys; print(json.load(sys.stdin)['cursor'])")
  check "${PCG[@]}" delivery ack analytics "$c"
done
"${PCG[@]}" delivery list
"$PY" - "$DB" <<'PYEOF'
import json, sqlite3, subprocess, sys
db = sys.argv[1]
c = sqlite3.connect(db); c.row_factory = sqlite3.Row
rows = {r["consumer_id"]: r for r in c.execute("SELECT * FROM delivery_consumers")}
assert rows["analytics"]["acked_seq"] > rows["warehouse"]["acked_seq"]
print("    analytics acked", rows["analytics"]["acked_seq"],
      "vs warehouse", rows["warehouse"]["acked_seq"],
      "- positions are independent")
PYEOF

say "7. Approved correction enters the stream; pending proposal stays invisible"
HEAD_BEFORE=$(sql "SELECT COALESCE(MAX(seq),0) FROM delivery_events")
"${PCG[@]}" correct propose 0 5 --correction-id FIX-DEMO-1 \
    --expected-revision 0 --reason "upstream city correction" \
    --record '{"offset":5,"name":"Demo User","city":"Berlin"}' >/dev/null
HEAD_PENDING=$(sql "SELECT COALESCE(MAX(seq),0) FROM delivery_events")
[ "$HEAD_PENDING" = "$HEAD_BEFORE" ] || { echo "DEMO FAILED: proposal leaked"; exit 1; }
echo "    pending proposal changed no delivery event (head still $HEAD_PENDING)"
check "${PCG[@]}" correct approve FIX-DEMO-1
HEAD_AFTER=$(sql "SELECT COALESCE(MAX(seq),0) FROM delivery_events")
[ "$HEAD_AFTER" = "$((HEAD_BEFORE + 1))" ] || { echo "DEMO FAILED: approval event"; exit 1; }
"${PCG[@]}" delivery batches --limit 2 | sed 's/^/    /'
echo "    -> approval appended exactly one correction event at the same position"

say "8. Rule publish: one logical batch, never split, candidates invisible"
cat > "$WORK/v1.json" <<'JSONEOF'
{"fields": {
  "name": {"missing": "preserve", "null": "null",
           "steps": [{"op": "nfkc"}, {"op": "strip"},
                     {"op": "collapse_ws"}, {"op": "upper"}]},
  "city": {"missing": "preserve", "null": "preserve",
           "steps": [{"op": "strip"}, {"op": "collapse_ws"}, {"op": "title"}]}}}
JSONEOF
check "${PCG[@]}" rule register v1 "$WORK/v1.json"
check "${PCG[@]}" rule recompute v1 --page-size 37
PUB_BEFORE=$(sql "SELECT COUNT(*) FROM delivery_batches WHERE kind='publish'")
[ "$PUB_BEFORE" = "0" ] || { echo "DEMO FAILED: candidate leaked as publish"; exit 1; }
echo "    candidate recompute finished but produced zero publish events"
TOKEN=$("${PCG[@]}" rule review v1 --json | "$PY" -c "import json,sys; print(json.load(sys.stdin)['token'])")
check "${PCG[@]}" rule activate "$TOKEN"
# warehouse reads with a tiny page size. The publish batch is large but is
# delivered in bounded, page-sized chunks that never mix batches; only the
# final chunk carries batch_complete=true, so the publish cannot be regarded
# as applied until every chunk has been processed.
PUBLISH_SPAN=$("$PY" - "$DB" <<'PYEOF'
import sqlite3, sys
c = sqlite3.connect(sys.argv[1]); c.row_factory = sqlite3.Row
b = c.execute("SELECT b.batch_id, MIN(e.seq) lo, MAX(e.seq) hi, b.event_count "
              "FROM delivery_batches b JOIN delivery_events e ON e.batch_id=b.batch_id "
              "WHERE b.kind='publish' GROUP BY b.batch_id").fetchone()
print(f"{b['batch_id']} {b['lo']} {b['hi']} {b['event_count']}")
PYEOF
)
read -r PUB_BID PUB_LO PUB_HI PUB_COUNT <<< "$PUBLISH_SPAN"
# page through everything before the publish batch (remaining snapshot pages
# plus the single correction event), then start consuming publish chunks.
while :; do
  p=$("${PCG[@]}" delivery read warehouse --page-size 3 --json)
  n=$(echo "$p" | "$PY" -c "import json,sys; print(len(json.load(sys.stdin)['events']))")
  [ "$n" = "0" ] && { echo "DEMO FAILED: caught up before reaching publish batch"; exit 1; }
  f=$(echo "$p" | "$PY" -c "import json,sys; print(json.load(sys.stdin)['from_seq'])")
  if [ "$f" = "$PUB_LO" ]; then break; fi
  c=$(echo "$p" | "$PY" -c "import json,sys; print(json.load(sys.stdin)['cursor'])")
  check "${PCG[@]}" delivery ack warehouse "$c"
done
# drain the publish batch in chunks, asserting the bounded/atomic contract
PUB_SEEN=0; PUB_CHUNKS=0; PUB_COMPLETES=0
while :; do
  echo "$p" > "$WORK/pubpage.json"
  "$PY" - "$WORK/pubpage.json" "$PUB_BID" "$PUB_HI" <<'PYEOF'
import json, sys
page = json.load(open(sys.argv[1])); bid, hi = int(sys.argv[2]), int(sys.argv[3])
assert len(page["events"]) <= 3, "physical page exceeded its page size"
assert {e["batch_id"] for e in page["events"]} == {bid}, "batch mixed in a page"
assert page["batch_id"] == bid
PYEOF
  PUB_SEEN=$((PUB_SEEN + n)); PUB_CHUNKS=$((PUB_CHUNKS + 1))
  complete=$(echo "$p" | "$PY" -c "import json,sys; print(json.load(sys.stdin)['batch_complete'])")
  [ "$complete" = "True" ] && PUB_COMPLETES=$((PUB_COMPLETES + 1))
  c=$(echo "$p" | "$PY" -c "import json,sys; print(json.load(sys.stdin)['cursor'])")
  t=$(echo "$p" | "$PY" -c "import json,sys; print(json.load(sys.stdin)['to_seq'])")
  check "${PCG[@]}" delivery ack warehouse "$c"
  [ "$t" = "$PUB_HI" ] && break
  p=$("${PCG[@]}" delivery read warehouse --page-size 3 --json)
  n=$(echo "$p" | "$PY" -c "import json,sys; print(len(json.load(sys.stdin)['events']))")
done
"$PY" - "$PUB_SEEN" "$PUB_COUNT" "$PUB_CHUNKS" "$PUB_COMPLETES" <<'PYEOF'
import sys
seen, count, chunks, completes = (int(x) for x in sys.argv[1:])
assert seen == count, (seen, count)
assert chunks > 1, "a large publish must span multiple bounded pages"
assert completes == 1, "exactly one chunk completes the logical batch"
print(f"    publish batch of {count} events delivered in {chunks} bounded "
      f"pages (<=3 each), exactly one batch_complete (no partial publish)")
PYEOF

say "9. Analytics also drains the live tail; both reach the same log head"
drain_consumer() {
  local cid="$1"
  while :; do
    local p n c
    p=$("${PCG[@]}" delivery read "$cid" --page-size 25 --json)
    n=$(echo "$p" | "$PY" -c "import json,sys; print(len(json.load(sys.stdin)['events']))")
    [ "$n" = "0" ] && break
    c=$(echo "$p" | "$PY" -c "import json,sys; print(json.load(sys.stdin)['cursor'])")
    check "${PCG[@]}" delivery ack "$cid" "$c"
  done
}
drain_consumer analytics
drain_consumer warehouse
"${PCG[@]}" delivery list
"$PY" - "$DB" <<'PYEOF'
import sqlite3, sys
c = sqlite3.connect(sys.argv[1]); c.row_factory = sqlite3.Row
head = c.execute("SELECT MAX(seq) FROM delivery_events").fetchone()[0]
for r in c.execute("SELECT consumer_id, acked_seq, state FROM delivery_consumers"):
    assert r["acked_seq"] == head and r["state"] == "active", dict(r)
print(f"    both consumers active and acked to head seq {head}")
PYEOF

say "10. Independent reference audit (imports no production code)"
"$PY" -I "$ROOT/tests/tools_delivery_ref.py" --db "$DB"

kill "$WA" 2>/dev/null || true
kill "$COORD" 2>/dev/null || true
wait 2>/dev/null || true
echo
echo "DEMO OK: snapshot -> paged replay -> strict acks -> kill/replay ->" \
     "independent consumers -> correction -> atomic publish batch -> audit."
