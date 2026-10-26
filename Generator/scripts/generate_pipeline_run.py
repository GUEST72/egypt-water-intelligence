"""Generate once per deterministic config; emit a small Airflow XCom receipt."""
import argparse
import fcntl
import json
import subprocess
from pathlib import Path

from egypt_water_intelligence.generator.config import load_config
from egypt_water_intelligence.generator.outputs import run_id


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", choices=("smoke", "pilot"), required=True)
    args = parser.parse_args()
    config_path = Path("configs") / f"{args.config}.yml"
    config = load_config(config_path)
    rid = run_id(config)
    output = Path(config.raw["run"]["output_root"])
    output.mkdir(parents=True, exist_ok=True)
    root = output / rid
    with (output / f".{rid}.lock").open("w") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        if not (root / "_RUN_COMPLETE").is_file():
            if root.exists():
                raise RuntimeError(f"Incomplete generation at {root}; inspect and move it before retrying")
            subprocess.run(["water-generator", "generate", "--config", str(config_path)], check=True)
        manifest = json.loads((root / "run-manifest.json").read_text())
        if manifest["run_id"] != rid or manifest["status"] != "complete":
            raise RuntimeError("Run manifest does not match the selected config")
    print(json.dumps({"run_id": rid, "manifest": manifest}))


if __name__ == "__main__":
    main()
