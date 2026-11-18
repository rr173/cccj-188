"""SQLite schema for the coordination + sink database.

One database file plays two roles:

* coordination boundary (members, plans, revocations, acks, rejected commits)
* local sink (cleaned business rows keyed by (partition, offset))

WAL mode lets the long-lived coordinator and short-lived worker/admin/inspect
processes read/write concurrently.  Every state-changing coordinator action
runs inside ``BEGIN IMMEDIATE`` and every worker sink commit does the same, so
the write-fence check and the row insert are atomic with respect to each
other.
"""

SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA synchronous=FULL;
PRAGMA foreign_keys=ON;
PRAGMA busy_timeout=10000;

CREATE TABLE IF NOT EXISTS meta(
  key   TEXT PRIMARY KEY,
  value TEXT NOT NULL
);

-- registry of workers that have ever joined; 'alive' rows hold an active lease
CREATE TABLE IF NOT EXISTS members(
  worker_id   TEXT PRIMARY KEY,
  pid         INTEGER NOT NULL,
  started_at  INTEGER NOT NULL,
  last_beat   INTEGER NOT NULL,
  lease_ms    INTEGER NOT NULL,
  status      TEXT NOT NULL CHECK(status IN ('alive','leaving','dead')),
  epoch       INTEGER NOT NULL DEFAULT 0
);

-- one row per published stable plan
CREATE TABLE IF NOT EXISTS plans(
  plan_id        INTEGER PRIMARY KEY AUTOINCREMENT,
  created_at     INTEGER NOT NULL,
  prev_plan_id   INTEGER,
  epoch          INTEGER NOT NULL,
  reason         TEXT NOT NULL,
  member_csv     TEXT NOT NULL,
  finalized      INTEGER NOT NULL DEFAULT 0
);

-- per-partition assignment inside a plan. A generation is a reusable
-- ownership term: when ownership is unchanged the same (partition,gen) is
-- carried into the new plan row, so the primary key is (plan,partition).
-- Fencing/sink checks locate the live term by (partition,gen), which is
-- indexed (non-unique, since the term is carried across plans).
CREATE TABLE IF NOT EXISTS assignments(
  partition_id   INTEGER NOT NULL,
  plan_id        INTEGER NOT NULL,
  worker_id      TEXT NOT NULL,
  gen            INTEGER NOT NULL,
  state          TEXT NOT NULL CHECK(state IN ('pending','active','revoking','revoked','fenced')),
  updated_at     INTEGER NOT NULL,
  PRIMARY KEY(plan_id, partition_id),
  FOREIGN KEY(plan_id) REFERENCES plans(plan_id)
);

-- revoke requests emitted by the coordinator
CREATE TABLE IF NOT EXISTS revocations(
  partition_id   INTEGER NOT NULL,
  gen            INTEGER NOT NULL,
  plan_id        INTEGER NOT NULL,
  worker_id      TEXT NOT NULL,
  issued_at      INTEGER NOT NULL,
  deadline_at    INTEGER NOT NULL,
  state          TEXT NOT NULL CHECK(state IN ('waiting','acked','fenced')),
  PRIMARY KEY(partition_id, gen)
);

-- worker acknowledgements, bound to (plan, partition, generation)
CREATE TABLE IF NOT EXISTS rev_acks(
  partition_id   INTEGER NOT NULL,
  gen            INTEGER NOT NULL,
  plan_id        INTEGER NOT NULL,
  worker_id      TEXT NOT NULL,
  acked_at       INTEGER NOT NULL,
  drained        INTEGER NOT NULL,
  PRIMARY KEY(partition_id, gen)
);

-- sink: business rows, keyed by source position
CREATE TABLE IF NOT EXISTS records(
  partition_id   INTEGER NOT NULL,
  offset         INTEGER NOT NULL,
  gen            INTEGER NOT NULL,
  worker_id      TEXT NOT NULL,
  source_json    TEXT NOT NULL,
  clean_json     TEXT NOT NULL,
  committed_at   INTEGER NOT NULL,
  PRIMARY KEY(partition_id, offset)
) WITHOUT ROWID;

-- durable per-partition commit progress
CREATE TABLE IF NOT EXISTS progress(
  partition_id   INTEGER PRIMARY KEY,
  committed_offset INTEGER NOT NULL,  -- last committed offset, or -1 if none
  gen            INTEGER NOT NULL,
  worker_id      TEXT NOT NULL,
  updated_at     INTEGER NOT NULL
);

-- audit trail of every rejected late/out-of-fence commit attempt
CREATE TABLE IF NOT EXISTS rejected_commits(
  id             INTEGER PRIMARY KEY AUTOINCREMENT,
  at_ms          INTEGER NOT NULL,
  partition_id   INTEGER NOT NULL,
  gen            INTEGER NOT NULL,
  plan_id        INTEGER NOT NULL,
  worker_id      TEXT NOT NULL,
  start_offset   INTEGER NOT NULL,
  end_offset     INTEGER NOT NULL,
  reason         TEXT NOT NULL
);

-- acks that arrived for an old plan/generation and therefore cannot apply
CREATE TABLE IF NOT EXISTS stale_acks(
  id             INTEGER PRIMARY KEY AUTOINCREMENT,
  at_ms          INTEGER NOT NULL,
  partition_id   INTEGER NOT NULL,
  gen            INTEGER NOT NULL,
  plan_id        INTEGER NOT NULL,
  worker_id      TEXT NOT NULL,
  reason         TEXT NOT NULL
);

-- barrier rendezvous set by tests/demo; workers block (after compute, before
-- commit) until the admin releases the named barrier
CREATE TABLE IF NOT EXISTS barrier_events(
  name           TEXT PRIMARY KEY,
  worker_id      TEXT NOT NULL,
  partition_id   INTEGER NOT NULL,
  start_offset   INTEGER NOT NULL,
  reached_at     INTEGER NOT NULL,
  released       INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_assign_worker ON assignments(worker_id);
CREATE INDEX IF NOT EXISTS idx_records_po ON records(partition_id, offset);
"""
