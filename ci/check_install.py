#!/usr/bin/env python3
"""Validate CMake installation and exercise the installed Web API in a fresh prefix."""

import argparse
import os
from pathlib import Path
import subprocess
import tempfile


ROOT = Path(__file__).resolve().parent.parent


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--build-dir", type=Path, required=True)
    args = parser.parse_args()
    # A new prefix ensures old installed files cannot hide missing install rules.
    with tempfile.TemporaryDirectory(prefix="iotdbfs-install-check-") as temporary:
        prefix = Path(temporary)
        subprocess.run(["cmake", "--install", str(args.build_dir.resolve()),
                        "--prefix", str(prefix)], check=True, timeout=60)
        expected = ["bin/iotdb_fs", "bin/iotdb_fs_web", "bin/grib_client"]
        expected += [str(path.relative_to(ROOT)) for path in (ROOT / "include").rglob("*")
                     if path.suffix in {".h", ".hpp"} and path.is_file()]
        expected += [str(path.relative_to(ROOT)) for path in (ROOT / "resources").rglob("*.properties")]
        for name in expected:
            installed = prefix / name
            if not installed.is_file() or installed.stat().st_size == 0:
                raise RuntimeError(f"Missing or empty installed file: {name}")
        for name in ("iotdb_fs", "iotdb_fs_web", "grib_client"):
            binary = prefix / "bin" / name
            if not os.access(binary, os.X_OK):
                raise RuntimeError(f"Installed binary is not executable: {name}")
            result = subprocess.run(["ldd", str(binary)], capture_output=True,
                                    text=True, timeout=15, check=False)
            if result.returncode or "not found" in result.stdout + result.stderr:
                raise RuntimeError(f"Installed runtime dependencies failed for {name}:\n"
                                   + result.stdout + result.stderr)
        for name in ("iotdb_fs_web", "grib_client"):
            subprocess.run([str(prefix / "bin" / name), "--help"], check=True, timeout=15)
        subprocess.run(["bash", str(ROOT / "tests/integration/web_api_smoke.sh"),
                        str(prefix / "bin/iotdb_fs_web")], check=True, timeout=90)
        print(f"Installation check passed: {len(expected)} files, 3 loadable binaries, installed Web API.")


if __name__ == "__main__":
    main()
