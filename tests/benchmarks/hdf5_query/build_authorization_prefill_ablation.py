#!/usr/bin/env python3
"""Build matched binaries differing only in publication permission-cache prefill."""
import argparse
import hashlib
import json
from pathlib import Path
import shutil
import subprocess


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--build", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    source = args.source / "src/main.cpp"
    original = source.read_bytes()
    token = b"constexpr bool kPrefillPublishedAuthorization = true;"
    if original.count(token) != 1:
        raise ValueError("expected exactly one publication-prefill compile-time constant")
    disabled = original.replace(token, b"constexpr bool kPrefillPublishedAuthorization = false;")
    args.output.mkdir(parents=True, exist_ok=False)

    def build(label):
        with (args.output / f"build-{label}.log").open("wb") as log:
            subprocess.run(["cmake", "--build", str(args.build), "--target", "iotdb_fs", "-j8"],
                           stdout=log, stderr=subprocess.STDOUT, check=True)

    build("after")
    binary = args.build / "bin/iotdb_fs"
    shutil.copy2(binary, args.output / "iotdb_fs-after")
    (args.output / "main-after.cpp").write_bytes(original)
    (args.output / "main-before.cpp").write_bytes(disabled)
    try:
        source.write_bytes(disabled)
        build("before")
        shutil.copy2(binary, args.output / "iotdb_fs-before")
    finally:
        source.write_bytes(original)
        build("restored")
    hashes = {"protocol": "iotdbfs-authorization-prefill-build/1", "changed_caller_sites": 1,
              "source_after_sha256": hashlib.sha256(original).hexdigest(),
              "source_before_sha256": hashlib.sha256(disabled).hexdigest(),
              "binary_after_sha256": hashlib.sha256((args.output / "iotdb_fs-after").read_bytes()).hexdigest(),
              "binary_before_sha256": hashlib.sha256((args.output / "iotdb_fs-before").read_bytes()).hexdigest()}
    (args.output / "ablation-build.json").write_text(json.dumps(hashes, indent=2) + "\n")
    print(json.dumps(hashes))


if __name__ == "__main__":
    main()
