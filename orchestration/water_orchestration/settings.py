"""Settings shared with docker-compose.yml: the repository .env plus the same defaults."""
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]

DEFAULTS = {
    "MINIO_ACCESS_KEY": "wateradmin",
    "MINIO_SECRET_KEY": "wateradminsecret",
    "MINIO_RAW_BUCKET": "water-raw",
    "MINIO_RESTRICTED_BUCKET": "water-restricted",
    "MINIO_EVENT_BUCKET": "water-events",
    "MINIO_WAREHOUSE_BUCKET": "water-warehouse",
    # Host-side addresses of the Compose services.
    "MINIO_HOST_ENDPOINT": "localhost:9000",
    "KAFKA_HOST_BOOTSTRAP": "localhost:29092",
    "FLINK_REST_URL": "http://localhost:8081",
    "TRINO_URL": "http://localhost:8082",
}


def _dotenv(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    if path.is_file():
        for line in path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, value = line.split("=", 1)
                values[key.strip()] = value.strip().strip('"').strip("'")
    return values


def setting(name: str) -> str:
    """Process environment wins, then the repository .env, then the Compose default."""
    return os.environ.get(name) or _dotenv(REPO_ROOT / ".env").get(name) or DEFAULTS[name]


@dataclass(frozen=True)
class Paths:
    repo: Path = REPO_ROOT
    generator_output: Path = REPO_ROOT / "Generator" / "output"

    def run_dir(self, run_id: str) -> Path:
        return self.generator_output / run_id


PATHS = Paths()
