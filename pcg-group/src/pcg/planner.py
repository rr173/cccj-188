"""Deterministic, stable, balanced partition planning.

Given the set of live members and the previous plan's effective ownership,
produce a new ``partition -> worker`` map that:

1. assigns *every* partition to exactly one live member (or no one, if there
   are no live members);
2. is balanced: per-member partition counts differ by at most one;
3. among all balanced plans, retains as many current ownerships as possible;
4. breaks every tie by an explicit, documented rule so the same inputs always
   yield the same plan.

Tie-break rules (do not change without updating the docs/demo):

* members are ordered lexicographically by ``worker_id``;
* let ``q, r = divmod(P, M)`` -- the first *r* members in that order receive
  ``q+1`` partitions, the rest ``q`` (idle members get 0 when M > P);
* retention pass: partitions are scanned in ascending id; a member keeps its
  previous partitions up to its capacity, always keeping the *lowest* ids
  when over capacity (so dropped ones are deterministic);
* floating partitions (dead/over-capacity previous owners) are taken in
  ascending id and placed into free capacity slots laid out in member order;
  i.e. member slots form the repeating list
  ``[m0, m0, ..., m1, ...]`` and are consumed in order.

Because a partition can only be retained by its unique previous owner, the
greedy retention pass is globally optimal for retention count: it keeps
exactly ``sum_m min(cap_m, prev_count_m)`` ownerships.
"""

from __future__ import annotations

from typing import Mapping, Sequence


def plan_assignment(
    partitions: Sequence[int],
    alive: Sequence[str],
    prev: Mapping[int, str],
) -> dict[int, str]:
    alive_sorted = sorted(alive)
    parts_sorted = sorted(partitions)
    m = len(alive_sorted)
    if m == 0:
        return {}

    q, r = divmod(len(parts_sorted), m)
    caps: dict[str, int] = {
        mem: q + (1 if i < r else 0) for i, mem in enumerate(alive_sorted)
    }

    retained: dict[str, list[int]] = {mem: [] for mem in alive_sorted}
    floating: list[int] = []
    for p in parts_sorted:  # ascending: over-cap owners deterministically keep lowest ids
        owner = prev.get(p)
        if owner is not None and owner in caps and len(retained[owner]) < caps[owner]:
            retained[owner].append(p)
        else:
            floating.append(p)

    assignment: dict[int, str] = {}
    for mem, ps in retained.items():
        for p in ps:
            assignment[p] = mem

    # free capacity slots, laid out in lexicographic member order
    slots: list[str] = []
    for mem in alive_sorted:
        slots.extend([mem] * (caps[mem] - len(retained[mem])))

    for p, mem in zip(sorted(floating), slots):
        assignment[p] = mem

    # invariant check: every partition assigned exactly once, balanced
    assert len(assignment) == len(parts_sorted)
    counts = {mem: 0 for mem in alive_sorted}
    for mem in assignment.values():
        counts[mem] += 1
    if counts:
        lo, hi = min(counts.values()), max(counts.values())
        assert hi - lo <= 1
    return assignment


def retention_count(
    partitions: Sequence[int],
    alive: Sequence[str],
    prev: Mapping[int, str],
) -> int:
    new = plan_assignment(partitions, alive, prev)
    return sum(1 for p, mem in new.items() if prev.get(p) == mem)
