#!/usr/bin/env python3
"""Build otherwise identical catalog permission reuse/reread binaries.

The control changes only the typed registration authorization argument.
The original source and normal build are restored even when the ablation fails.
This is an experiment tool, not a runtime bypass option.
"""
import argparse
import hashlib
import json
import shutil
import subprocess
from pathlib import Path


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--build", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    source = args.source / "src" / "query" / "frontend" / "QueryRegistrationSocket.cpp"
    original = source.read_bytes()
    token = b"authorize_(peer, sources, &selected.value())"
    if original.count(token) != 1:
        raise ValueError("expected exactly one immediate typed authorization selection")
    ablation = original.replace(token, b"authorize_(peer, sources, nullptr)")
    args.output.mkdir(parents=True, exist_ok=True)

    def build(label):
        with (args.output / f"build-{label}.log").open("wb") as log:
            subprocess.run(["cmake", "--build", str(args.build),
                            "--target", "iotdb_fs", "-j8"], stdout=log,
                           stderr=subprocess.STDOUT, check=True)

    build("permission-reuse")
    binary = args.build / "bin" / "iotdb_fs"
    shutil.copy2(binary, args.output / "iotdb_fs-permission-reuse")
    (args.output / "socket-permission-reuse.cpp").write_bytes(original)
    (args.output / "socket-permission-reread.cpp").write_bytes(ablation)
    try:
        source.write_bytes(ablation)
        build("permission-reread")
        shutil.copy2(binary, args.output / "iotdb_fs-permission-reread")
    finally:
        source.write_bytes(original)
        build("restored")
    hashes = {
        "protocol": "iotdbfs-catalog-permission-ablation-build/1",
        "changed_caller_sites": 1,
        "source_reuse_sha256": hashlib.sha256(original).hexdigest(),
        "source_reread_sha256": hashlib.sha256(ablation).hexdigest(),
        "binary_reuse_sha256": hashlib.sha256((args.output / "iotdb_fs-permission-reuse").read_bytes()).hexdigest(),
        "binary_reread_sha256": hashlib.sha256((args.output / "iotdb_fs-permission-reread").read_bytes()).hexdigest(),
    }
    (args.output / "ablation-build.json").write_text(json.dumps(hashes, indent=2) + "\n")
    print(json.dumps(hashes))


if __name__ == "__main__":
    main()
