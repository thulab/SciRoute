#!/usr/bin/env python3
"""Run the GEOS direct query probe with credentials inherited from a live IoTDB process."""
import os
from pathlib import Path
import subprocess
import sys

if os.environ.get("GEOS_PYARROW_SITE"):
    sys.path.append(os.environ["GEOS_PYARROW_SITE"])
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "arrow_training"))
from a12_migration_smoke import connection_environment


if __name__ == "__main__":
    root = Path(sys.argv[1])
    request_name = sys.argv[2] if len(sys.argv) > 2 else "D-request.json"
    output_name = sys.argv[3] if len(sys.argv) > 3 else "D-raw.jsonl"
    binary = Path(sys.argv[4]) if len(sys.argv) > 4 else root / "geos_pilot_direct"
    env = connection_environment(1001546, "h09probe1790939889dbfc1f")
    env["IOTDB_CONFIG_FILE"] = str(root / "run-01/profile.properties")
    completed = subprocess.run([str(binary), str(root / request_name),
                                str(root / output_name)], env=env)
    raise SystemExit(completed.returncode)
