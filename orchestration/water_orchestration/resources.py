"""Thin clients for the Compose services. Each method is one command a person could type."""
from __future__ import annotations

import subprocess
from typing import Callable

from dagster import ConfigurableResource, Failure
from minio import Minio

from .settings import PATHS, setting


class ComposeResource(ConfigurableResource):
    """Runs `docker compose` against the repository's docker-compose.yml."""

    project_dir: str = str(PATHS.repo)

    def _base(self) -> list[str]:
        return ["docker", "compose", "--project-directory", self.project_dir,
                "-f", f"{self.project_dir}/docker-compose.yml"]

    def call(self, args: list[str], log: Callable[[str], None] | None = None) -> str:
        """Run a compose command, stream its output to `log`, return stdout+stderr."""
        command = self._base() + args
        if log:
            log("$ " + " ".join(command[2:]))
        process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                   text=True, encoding="utf-8", errors="replace")
        lines = []
        assert process.stdout is not None
        for line in process.stdout:
            line = line.rstrip()
            lines.append(line)
            if log and line and not line.startswith(" Container "):
                log(line)
        output = "\n".join(lines)
        if process.wait() != 0:
            raise Failure(
                description=f"`docker compose {' '.join(args[:3])} ...` exited with {process.returncode}",
                metadata={"output_tail": "\n".join(lines[-40:])},
            )
        return output

    def run(self, service: str, args: list[str], log=None, flags: list[str] | None = None) -> str:
        """`docker compose run --rm --no-deps [flags] <service> <args>`: one finite command."""
        return self.call(["run", "--rm", "--no-deps", *(flags or []), service, *args], log)

    def exec(self, service: str, args: list[str], log=None) -> str:
        return self.call(["exec", "-T", service, *args], log)


class KafkaResource(ConfigurableResource):
    """Offset inspection and topic creation. Never joins a consumer group, so it cannot affect consumers."""

    bootstrap: str = setting("KAFKA_HOST_BOOTSTRAP")

    def ensure_topics(self, names: list[str]) -> list[str]:
        from kafka.admin import KafkaAdminClient, NewTopic
        admin = KafkaAdminClient(bootstrap_servers=self.bootstrap, client_id="ewi-orchestration")
        try:
            missing = [name for name in names if name not in set(admin.list_topics())]
            if missing:
                admin.create_topics([NewTopic(name, num_partitions=1, replication_factor=1) for name in missing])
            return missing
        finally:
            admin.close()

    def offsets(self, topic: str) -> dict[int, tuple[int, int]]:
        """{partition: (beginning, end)} where end is the next offset to be written."""
        from kafka import KafkaConsumer, TopicPartition
        consumer = KafkaConsumer(bootstrap_servers=self.bootstrap, group_id=None, enable_auto_commit=False)
        try:
            partitions = [TopicPartition(topic, p) for p in sorted(consumer.partitions_for_topic(topic) or [])]
            begin, end = consumer.beginning_offsets(partitions), consumer.end_offsets(partitions)
            return {tp.partition: (begin[tp], end[tp]) for tp in partitions}
        finally:
            consumer.close()

    def count(self, topic: str) -> int:
        return sum(end - begin for begin, end in self.offsets(topic).values())

    def committed(self, group: str, topic: str) -> dict[int, int | None]:
        from kafka.admin import KafkaAdminClient
        admin = KafkaAdminClient(bootstrap_servers=self.bootstrap, client_id="ewi-orchestration")
        try:
            offsets = admin.list_consumer_group_offsets(group)
        finally:
            admin.close()
        return {tp.partition: meta.offset for tp, meta in offsets.items() if tp.topic == topic}


class FlinkResource(ConfigurableResource):
    """Read-only Flink REST API."""

    url: str = setting("FLINK_REST_URL")

    def _get(self, path: str) -> dict:
        import requests
        response = requests.get(self.url + path, timeout=15)
        response.raise_for_status()
        return response.json()

    def jobs(self) -> list[dict]:
        return self._get("/jobs/overview")["jobs"]

    def vertex_states(self, job_id: str) -> list[str]:
        return [vertex["status"] for vertex in self._get(f"/jobs/{job_id}")["vertices"]]

    def completed_checkpoints(self, job_id: str) -> int:
        return self._get(f"/jobs/{job_id}/checkpoints")["counts"]["completed"]


class TrinoResource(ConfigurableResource):
    """Trino REST protocol (same as batch/jobs/validate_batch.py); reads every page so late errors are not missed."""

    url: str = setting("TRINO_URL")
    user: str = "dagster"

    def query(self, sql: str) -> list[list]:
        import requests
        headers = {"X-Trino-User": self.user}
        page = requests.post(f"{self.url}/v1/statement", data=sql.encode("utf-8"), headers=headers, timeout=60).json()
        rows = []
        while True:
            if "error" in page:
                raise Failure(description=f"Trino: {page['error'].get('message')}", metadata={"sql": sql[:2000]})
            rows.extend(page.get("data", []))
            if not page.get("nextUri"):
                return rows
            page = requests.get(page["nextUri"], headers=headers, timeout=60).json()


class MinioResource(ConfigurableResource):
    endpoint: str = setting("MINIO_HOST_ENDPOINT")
    access_key: str = setting("MINIO_ACCESS_KEY")
    secret_key: str = setting("MINIO_SECRET_KEY")

    def client(self) -> Minio:
        return Minio(self.endpoint, access_key=self.access_key, secret_key=self.secret_key, secure=False)

    def read_json(self, bucket: str, key: str) -> dict | None:
        import json
        if self.size(bucket, key) is None:
            return None
        response = self.client().get_object(bucket, key)
        try:
            return json.loads(response.read())
        finally:
            response.close()
            response.release_conn()

    def write_json(self, bucket: str, key: str, value: dict) -> None:
        import io
        import json
        data = json.dumps(value, indent=2, sort_keys=True).encode("utf-8")
        self.client().put_object(bucket, key, io.BytesIO(data), length=len(data), content_type="application/json")

    def list_keys(self, bucket: str, prefix: str) -> list[str]:
        return [o.object_name for o in self.client().list_objects(bucket, prefix=prefix, recursive=True)]

    def size(self, bucket: str, key: str) -> int | None:
        from minio.error import S3Error
        try:
            return self.client().stat_object(bucket, key).size
        except S3Error as exc:
            if exc.code in {"NoSuchKey", "NoSuchObject", "NotFound"}:
                return None
            raise
