import os
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
os.environ.setdefault("AIRFLOW_HOME", tempfile.mkdtemp(prefix="ewi-airflow-test-"))
sys.path.insert(0, str(ROOT / "orchestration" / "dags"))
sys.path.insert(0, str(ROOT / "Generator" / "src"))
