"""Archive completeness: which archiver objects exactly cover a published offset range.

The archiver (Generator/.../archiver.py) writes events/<topic>/<run_id>/part-<partition>-<first>-<last>.ndjson.gz
and commits Kafka offsets only after the upload. After a crash it can write a second object whose range
overlaps an earlier one, so completeness means: some subset of objects tiles [start, end) exactly.
"""
from __future__ import annotations

import re
from dataclasses import dataclass

OBJECT_PATTERN = re.compile(r"part-(\d+)-(\d+)-(\d+)\.ndjson\.gz$")


@dataclass(frozen=True)
class Part:
    key: str
    partition: int
    first: int
    last: int  # inclusive

    @property
    def records(self) -> int:
        return self.last - self.first + 1


def parse_parts(keys: list[str]) -> list[Part]:
    parts = []
    for key in keys:
        match = OBJECT_PATTERN.search(key)
        if match:
            partition, first, last = (int(group) for group in match.groups())
            parts.append(Part(key, partition, first, last))
    return parts


def tile(parts: list[Part], start: int, end: int) -> list[Part] | None:
    """Return parts whose ranges exactly cover [start, end) without overlap, or None.

    Prefers the longest object at each position; backtracks if that leads to a gap.
    """
    by_first: dict[int, list[Part]] = {}
    for part in parts:
        if start <= part.first and part.last < end:
            by_first.setdefault(part.first, []).append(part)
    for options in by_first.values():
        options.sort(key=lambda p: p.last, reverse=True)

    dead_ends: set[int] = set()

    def walk(cursor: int) -> list[Part] | None:
        if cursor == end:
            return []
        if cursor in dead_ends:
            return None
        for part in by_first.get(cursor, []):
            rest = walk(part.last + 1)
            if rest is not None:
                return [part, *rest]
        dead_ends.add(cursor)
        return None

    return walk(start)


def first_gap(parts: list[Part], start: int, end: int) -> int:
    """First offset in [start, end) that no object covers (for an actionable error message)."""
    covered = sorted((p.first, p.last) for p in parts if p.last >= start and p.first < end)
    cursor = start
    for first, last in covered:
        if first > cursor:
            break
        cursor = max(cursor, last + 1)
    return cursor


def coverage(keys: list[str], ranges: dict[int, tuple[int, int]]) -> dict:
    """Check every partition's [start, end) range. Returns chosen keys, counts and overlaps."""
    parts = parse_parts(keys)
    chosen: list[Part] = []
    problems: list[str] = []
    for partition, (start, end) in sorted(ranges.items()):
        candidates = [p for p in parts if p.partition == partition]
        if start == end:
            continue
        selection = tile(candidates, start, end)
        if selection is None:
            problems.append(f"partition {partition}: offsets [{start}, {end}) not covered; first gap at {first_gap(candidates, start, end)}")
        else:
            chosen.extend(selection)
    chosen_keys = {p.key for p in chosen}
    return {
        "complete": not problems,
        "problems": problems,
        "objects": [p.key for p in chosen],
        "records": sum(p.records for p in chosen),
        "overlapping_objects_ignored": sorted(p.key for p in parts if p.key not in chosen_keys),
    }
