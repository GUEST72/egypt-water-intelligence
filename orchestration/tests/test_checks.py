import json

import pytest

from water_orchestration import checks


def write_run(tmp_path, manifest, district_lines=3, meter_lines=2, complete=True):
    run = tmp_path / manifest.get("run_id", "run-0123456789abcdef")
    (run / "delivery").mkdir(parents=True)
    (run / "delivery" / "district-sensors.ndjson").write_text("{}\n" * district_lines)
    (run / "delivery" / "meter-readings.ndjson").write_text("{}\n" * meter_lines)
    (run / "run-manifest.json").write_text(json.dumps(manifest))
    if complete:
        (run / "_RUN_COMPLETE").write_text("complete\n")
    return run


GOOD = {"run_id": "run-0123456789abcdef", "status": "complete", "logical_interval_count": 30,
        "delivery_district_records": 3, "delivery_meter_records": 2}


def test_current_manifest_and_counts_pass(tmp_path):
    run = write_run(tmp_path, GOOD)
    manifest = checks.read_manifest(run)
    assert checks.check_delivery_counts(run, manifest) == {"district": 3, "meter": 2}


def test_stale_generator_manifest_is_rejected_with_advice(tmp_path):
    # Shape of the old derived_faulted_delivery manifest: no run_id/status/logical_interval_count.
    stale = {"artifact_type": "derived_faulted_delivery", "source_run_id": "run-0123456789abcdef",
             "delivery_district_records": 3, "delivery_meter_records": 2}
    run = write_run(tmp_path, stale)
    with pytest.raises(checks.CheckFailed, match="regenerate"):
        checks.read_manifest(run)


def test_unfinished_run_is_rejected(tmp_path):
    run = write_run(tmp_path, GOOD, complete=False)
    with pytest.raises(checks.CheckFailed, match="_RUN_COMPLETE"):
        checks.read_manifest(run)


def test_count_mismatch_fails(tmp_path):
    run = write_run(tmp_path, GOOD, meter_lines=5)
    with pytest.raises(checks.CheckFailed, match="meter file has 5"):
        checks.check_delivery_counts(run, checks.read_manifest(run))


def test_compare_uploaded_reports_missing_and_size_differences():
    problems = checks.compare_uploaded({"a": 1, "b": 2, "c": 3}, {"a": 1, "b": 9, "c": None})
    assert problems == ["b: MinIO size 9 != local size 2", "c: missing in MinIO"]


def test_files_signature_changes_when_a_file_changes(tmp_path):
    run = write_run(tmp_path, GOOD)
    before = checks.files_signature(run)
    assert checks.files_signature(run) == before
    (run / "delivery" / "meter-readings.ndjson").write_text("{}\n" * 3)
    assert checks.files_signature(run) != before


def test_run_id_format():
    assert checks.require_run_id("run-72b96af88a342375")
    with pytest.raises(checks.CheckFailed):
        checks.require_run_id("pilot-first-run")
