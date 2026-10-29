from water_orchestration.archive import coverage, tile, parse_parts


def keys(*ranges, partition=0, topic="water.meter-readings.v1", run="run-0123456789abcdef"):
    return [f"events/{topic}/{run}/part-{partition}-{a}-{b}.ndjson.gz" for a, b in ranges]


def test_contiguous_objects_cover_range():
    result = coverage(keys((0, 9), (10, 19), (20, 24)), {0: (0, 25)})
    assert result["complete"] and result["records"] == 25 and len(result["objects"]) == 3


def test_gap_is_reported_with_first_missing_offset():
    result = coverage(keys((0, 9), (12, 24)), {0: (0, 25)})
    assert not result["complete"]
    assert "first gap at 10" in result["problems"][0]


def test_overlap_after_crash_is_tiled_and_reported():
    # Crash between upload and commit: 10-19 uploaded, then re-read as 10-14 and 15-24.
    result = coverage(keys((0, 9), (10, 19), (10, 14), (15, 24)), {0: (0, 25)})
    assert result["complete"] and result["records"] == 25
    assert result["overlapping_objects_ignored"]


def test_backtracks_when_longest_object_leads_to_gap():
    parts = parse_parts(keys((0, 14), (0, 9), (10, 19)))
    chosen = tile(parts, 0, 20)
    assert [(p.first, p.last) for p in chosen] == [(0, 9), (10, 19)]


def test_range_starts_after_earlier_publications():
    # A second run published later occupies offsets 100..129 of the same topic.
    result = coverage(keys((100, 119), (120, 129)), {0: (100, 130)})
    assert result["complete"] and result["records"] == 30


def test_incomplete_tail_not_complete():
    result = coverage(keys((0, 9)), {0: (0, 25)})
    assert not result["complete"]
