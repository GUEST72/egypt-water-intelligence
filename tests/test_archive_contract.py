import json
import sys
import types
from unittest.mock import Mock

import pytest

from egypt_water_intelligence.generator.integrations import archive_minio


def archive_fixture(tmp_path):
    root = tmp_path / "run-0123456789abcdef"
    names = [
        "_RUN_COMPLETE", "run-manifest.json", "validation-report.json", "config.yml",
        "public/districts.ndjson", "public/meters.ndjson", "public/network.inp",
        "delivery/district-sensors.ndjson", "delivery/meter-readings.ndjson",
        "restricted/scenario-truth.ndjson", "restricted/fault-truth.ndjson",
    ]
    for name in names:
        file = root / name
        file.parent.mkdir(parents=True, exist_ok=True)
        file.write_text("{}\n")
    # Clean public events must never be substituted for the delivery inputs.
    (root / "public/district-sensors.ndjson").write_text("clean\n")
    return root


def fake_minio(monkeypatch):
    client = Mock()
    client.bucket_exists.return_value = True
    module = types.ModuleType("minio")
    module.Minio = Mock(return_value=client)
    monkeypatch.setitem(sys.modules, "minio", module)
    return client


def test_archive_separates_truth_and_publishes_completion_marker_last(tmp_path, monkeypatch):
    root = archive_fixture(tmp_path)
    client = fake_minio(monkeypatch)
    count = archive_minio(root, "minio:9000", "key", "secret", "raw", "restricted")
    uploads = [call.args for call in client.fput_object.call_args_list]
    assert count == 11
    assert uploads[-1][:2] == ("raw", f"{root.name}/_RUN_COMPLETE")
    assert any(bucket == "raw" and key.endswith("/delivery/district-sensors.ndjson") for bucket, key, _ in uploads)
    assert not any("/public/" in key for _, key, _ in uploads)
    assert not any(bucket == "raw" and ("truth" in key or "config.yml" in key) for bucket, key, _ in uploads)
    assert any(bucket == "restricted" and key.endswith("/config.yml") for bucket, key, _ in uploads)


def test_missing_run_file_prevents_partial_archive(tmp_path, monkeypatch):
    root = archive_fixture(tmp_path)
    (root / "delivery/meter-readings.ndjson").unlink()
    client = fake_minio(monkeypatch)
    with pytest.raises(ValueError, match="incomplete run"):
        archive_minio(root, "minio:9000", "key", "secret", "raw", "restricted")
    client.fput_object.assert_not_called()


def test_upload_failure_does_not_publish_completion_marker(tmp_path, monkeypatch):
    root = archive_fixture(tmp_path)
    client = fake_minio(monkeypatch)
    client.fput_object.side_effect = OSError("upload failed")
    with pytest.raises(OSError):
        archive_minio(root, "minio:9000", "key", "secret", "raw", "restricted")
    assert all(not c.args[1].endswith("_RUN_COMPLETE") for c in client.fput_object.call_args_list)
