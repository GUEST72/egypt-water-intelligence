import importlib.util
import json
from pathlib import Path
from unittest.mock import Mock

import pytest

path = Path(__file__).resolve().parents[1] / "batch/jobs/handoff_smoke.py"
spec = importlib.util.spec_from_file_location("handoff_smoke", path)
smoke = importlib.util.module_from_spec(spec)
spec.loader.exec_module(smoke)


def test_raw_reader_counts_ndjson_across_chunks_and_checks_run_id():
    response = Mock()
    payload = b'\n'.join(json.dumps({"run_id": "selected", "event_id": str(i)}).encode() for i in range(3))
    response.stream.return_value = [payload[:5], payload[5:51], payload[51:]]
    client = Mock()
    client.get_object.return_value = response
    assert smoke.count_records(client, "raw", "events.ndjson", "selected") == 3
    response.close.assert_called_once()
    response.release_conn.assert_called_once()


def test_raw_reader_rejects_foreign_run_and_closes_stream():
    response = Mock()
    response.stream.return_value = [b'{"run_id":"foreign"}\n']
    client = Mock()
    client.get_object.return_value = response
    with pytest.raises(ValueError, match="Foreign run"):
        smoke.count_records(client, "raw", "events.ndjson", "selected")
    response.close.assert_called_once()
    response.release_conn.assert_called_once()
