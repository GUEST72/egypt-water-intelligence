import runpy
import sys
import types
from collections import namedtuple
from pathlib import Path
from unittest.mock import Mock

import pytest


@pytest.mark.parametrize("already_initialized", [False, True])
def test_initialization_creates_missing_resources_without_reset(monkeypatch, already_initialized):
    topics = {
        "water.district-sensors.v1", "water.meter-readings.v1",
        "water.district-sensors.clean.v1", "water.dead-letter.district.v1",
        "water.district-features.15m.v1", "water.district-relative.15m.v1", "water.district-alerts.v1",
    }
    admin = Mock()
    admin.list_topics.return_value = topics if already_initialized else []
    admin_module = types.ModuleType("kafka.admin")
    admin_module.KafkaAdminClient = Mock(return_value=admin)
    admin_module.NewTopic = namedtuple("NewTopic", "name num_partitions replication_factor")
    client = Mock()
    client.bucket_exists.return_value = already_initialized
    minio_module = types.ModuleType("minio")
    minio_module.Minio = Mock(return_value=client)
    monkeypatch.setitem(sys.modules, "kafka.admin", admin_module)
    monkeypatch.setitem(sys.modules, "minio", minio_module)
    monkeypatch.setenv("MINIO_ACCESS_KEY", "test")
    monkeypatch.setenv("MINIO_SECRET_KEY", "test-secret")
    for variable in ("MINIO_RAW_BUCKET", "MINIO_RESTRICTED_BUCKET", "MINIO_EVENT_BUCKET", "MINIO_WAREHOUSE_BUCKET"):
        monkeypatch.delenv(variable, raising=False)
    script = Path(__file__).resolve().parents[1] / "Generator/scripts/init_services.py"
    runpy.run_path(str(script))["main"]()
    if already_initialized:
        admin.create_topics.assert_not_called()
        client.make_bucket.assert_not_called()
    else:
        created = admin.create_topics.call_args.args[0]
        assert {t.name for t in created} == topics
        assert all(t.num_partitions == 1 for t in created)
        assert {c.args[0] for c in client.make_bucket.call_args_list} == {
            "water-raw", "water-restricted", "water-events", "water-warehouse",
        }
    admin.close.assert_called_once()
