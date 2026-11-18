#!/usr/bin/env python3
"""Generate a read-only JSONL source: one p<id>.jsonl per partition.

Offsets are implicit zero-based line numbers, continuous from 0.  Generated
data intentionally contains messy fields (odd whitespace, mixed case,
underscores, string amounts, boolean variants) so the deterministic cleaner
has real work to do.  A ``manifest.json`` pins per-partition counts so later
truncation is detectable.

Generated data directories are disposable runtime artifacts (see
.gitignore), never committed.
"""

from __future__ import annotations

import argparse
import json
import os
import random
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from pcg.source import list_partitions  # noqa: E402

FIRST = ["ana", " BEN", "chen-hao", "DIEGO ", "eve", " farah", "gabor", "HANA",
         "iván", " jay  k ", "kim", " LEO", "mia", "noor", " olaf ", "priya"]
CITIES = ["new_york", "SAN-FRANCISCO", "berlin", "tokyo ", " cairo", "rio",
          "buenos aires", "SEOUL", "london_", "-madrid-"]
EMAIL_DOMAINS = ["Example.COM", "mail.test", "CORP.IO", "work.net"]
ACTIVE_VARIANTS = [True, False, "yes", "no", "1", "0", "on", "off", 1, 0]


def gen_record(rng: random.Random, offset: int) -> dict:
    return {
        "id": 1000 + offset,
        "name": rng.choice(FIRST) + "  " + rng.choice(FIRST),
        "email": (" User.Name" + str(offset) + "@" + rng.choice(EMAIL_DOMAINS)),
        "city": rng.choice(CITIES),
        "amount": rng.choice([
            round(rng.uniform(-50, 500), 4),
            str(round(rng.uniform(0, 999), 2)),
            f" {rng.randint(0, 1000)} ",
            "not-a-number",
        ]),
        "active": rng.choice(ACTIVE_VARIANTS),
        "offset": offset,
    }


def generate(out_dir: str, partitions: int, counts: list[int], seed: int) -> None:
    os.makedirs(out_dir, exist_ok=True)
    rng = random.Random(seed)
    manifest_counts = {}
    for p in range(partitions):
        n = counts[p]
        path = os.path.join(out_dir, f"p{p}.jsonl")
        with open(path, "w", encoding="utf-8") as fh:
            for off in range(n):
                rec = gen_record(rng, off)
                fh.write(json.dumps(rec, ensure_ascii=False) + "\n")
        manifest_counts[str(p)] = n
    with open(os.path.join(out_dir, "manifest.json"), "w", encoding="utf-8") as fh:
        json.dump({"partitions": partitions, "counts": manifest_counts,
                   "seed": seed, "immutable": True}, fh, indent=2)
    total = sum(counts)
    print(f"generated {partitions} partitions, {total} records in {out_dir}")
    for p in list_partitions(out_dir):
        print(f"  p{p}: {manifest_counts[str(p)]} records")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--out", required=True)
    ap.add_argument("--partitions", type=int, default=4)
    ap.add_argument("--counts", default="",
                    help="comma per-partition counts, e.g. 20,0,33,7 (skew/empty); "
                         "default: uniform --per-partition")
    ap.add_argument("--per-partition", type=int, default=50)
    ap.add_argument("--seed", type=int, default=20261118)
    args = ap.parse_args()
    if args.counts:
        counts = [int(x) for x in args.counts.split(",")]
        if len(counts) != args.partitions:
            ap.error("--counts must have one entry per partition")
    else:
        counts = [args.per_partition] * args.partitions
    generate(args.out, args.partitions, counts, args.seed)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
