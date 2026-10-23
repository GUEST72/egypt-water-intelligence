import json
from copy import deepcopy
import pytest

from egypt_water_intelligence.generator.config import RunConfig, load_config
from egypt_water_intelligence.generator.outputs import _faulted_delivery
def test_faulted_delivery_nulls_measurement_and_preserves_clean_source(tmp_path) -> None:
    source = tmp_path / "source.ndjson"
    destination = tmp_path / "delivery.ndjson"

    rows = [
        {
            "event_id": f"e{i}",
            "event_time_utc": f"2026-01-01T00:0{i}:00Z",
            "availability_time_utc": f"2026-01-01T00:0{i}:00Z",
            "consumed_volume_m3": float(i),
            "quality_code": "valid",
        }
        for i in range(1, 5)
    ]

    source.write_text(
        "\n".join(json.dumps(row) for row in rows) + "\n",
        encoding="utf-8",
    )
    original_source = source.read_text(encoding="utf-8")

    raw = deepcopy(load_config("configs/pilot.yml").raw)
    raw["faults"] = [
        {
            "stream": "meter",
            "type": "null_value",
            "every_n": 2,
            "field": "consumed_volume_m3",
        }
    ]
    config = RunConfig(raw)

    audit = _faulted_delivery(config, source, destination, "meter")
    delivered = [
        json.loads(line)
        for line in destination.read_text(encoding="utf-8").splitlines()
    ]

    assert len(delivered) == 4
    assert len(audit) == 2
    assert source.read_text(encoding="utf-8") == original_source

    by_id = {row["event_id"]: row for row in delivered}
    assert by_id["e1"]["consumed_volume_m3"] == 1.0
    assert by_id["e2"]["consumed_volume_m3"] is None
    assert by_id["e2"]["quality_code"] == "corrupted"
    assert by_id["e3"]["consumed_volume_m3"] == 3.0
    assert by_id["e4"]["consumed_volume_m3"] is None
    assert by_id["e4"]["quality_code"] == "corrupted"

    assert all(item["fault_type"] == "null_value" for item in audit)
    assert all(
        item["expected_result"] == "null_measurement_flagged"
        for item in audit
    )


def test_faulted_delivery_sorts_by_availability_time(tmp_path) -> None:
    source = tmp_path / "source.ndjson"
    destination = tmp_path / "delivery.ndjson"

    rows = [
        {
            "event_id": "later",
            "availability_time_utc": "2026-01-01T00:02:00Z",
            "consumed_volume_m3": 2.0,
            "quality_code": "valid",
        },
        {
            "event_id": "earlier",
            "availability_time_utc": "2026-01-01T00:01:00Z",
            "consumed_volume_m3": 1.0,
            "quality_code": "valid",
        },
        {
            "event_id": "middle",
            "availability_time_utc": "2026-01-01T00:01:30Z",
            "consumed_volume_m3": 1.5,
            "quality_code": "valid",
        },
    ]

    source.write_text(
        "\n".join(json.dumps(row) for row in rows) + "\n",
        encoding="utf-8",
    )

    raw = deepcopy(load_config("configs/pilot.yml").raw)
    raw["faults"] = []
    config = RunConfig(raw)

    audit = _faulted_delivery(config, source, destination, "meter")
    delivered = [
        json.loads(line)
        for line in destination.read_text(encoding="utf-8").splitlines()
    ]

    assert audit == []
    assert [row["event_id"] for row in delivered] == [
        "earlier",
        "middle",
        "later",
    ]



def test_faulted_delivery_duplicates_records_and_preserves_source(tmp_path) -> None:
    source = tmp_path / "source.ndjson"
    destination = tmp_path / "delivery.ndjson"

    rows = [
        {
            "event_id": f"e{i}",
            "availability_time_utc": f"2026-01-01T00:0{i}:00Z",
            "consumed_volume_m3": float(i),
            "quality_code": "valid",
        }
        for i in range(1, 5)
    ]

    source.write_text(
        "\n".join(json.dumps(row) for row in rows) + "\n",
        encoding="utf-8",
    )
    original_source = source.read_text(encoding="utf-8")

    raw = deepcopy(load_config("configs/pilot.yml").raw)
    raw["faults"] = [
        {"stream": "meter", "type": "duplicate", "every_n": 2}
    ]
    config = RunConfig(raw)

    audit = _faulted_delivery(config, source, destination, "meter")
    delivered = [
        json.loads(line)
        for line in destination.read_text(encoding="utf-8").splitlines()
    ]

    assert len(delivered) == 6
    assert source.read_text(encoding="utf-8") == original_source

    from collections import Counter

    counts = Counter(row["event_id"] for row in delivered)
    assert counts == {"e1": 1, "e2": 2, "e3": 1, "e4": 2}

    assert len(audit) == 2
    assert all(item["fault_type"] == "duplicate" for item in audit)
    assert all(
        item["expected_result"] == "duplicate_event_id"
        for item in audit
    )
