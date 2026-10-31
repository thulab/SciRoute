#!/usr/bin/env python3
"""Build otherwise identical catalog-filter on/off binaries for a live A/B.

The off variant changes only the two query caller sites to the unfiltered
inventory API. The original source and normal build are restored even when
the ablation fails. This is an experiment tool, not a runtime bypass option.
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
    source = args.source / "src" / "IoTDBStructuredBlobBackend.cpp"
    original = source.read_bytes()
    tokens = (
        (b"auto snapshot = load_published_manifests(selector);",
         b"auto snapshot = load_published_manifests();"),
        (b"auto snapshot = backend->load_published_snapshot(selector);",
         b"auto snapshot = backend->load_published_snapshot(SourceSelector{});"),
    )
    if any(original.count(token) != 1 for token, _ in tokens):
        raise ValueError("expected exactly two query catalog caller sites")
    ablation = original
    for token, replacement in tokens:
        ablation = ablation.replace(token, replacement)
    args.output.mkdir(parents=True, exist_ok=True)

    def build(label):
        with (args.output / f"build-{label}.log").open("wb") as log:
            subprocess.run(["cmake", "--build", str(args.build),
                            "--target", "iotdb_fs", "-j8"], stdout=log,
                           stderr=subprocess.STDOUT, check=True)

    build("pushdown")
    binary = args.build / "bin" / "iotdb_fs"
    shutil.copy2(binary, args.output / "iotdb_fs-pushdown")
    (args.output / "backend-pushdown.cpp").write_bytes(original)
    (args.output / "backend-no-pushdown.cpp").write_bytes(ablation)
    try:
        source.write_bytes(ablation)
        build("no-pushdown")
        shutil.copy2(binary, args.output / "iotdb_fs-no-pushdown")
    finally:
        source.write_bytes(original)
        build("restored")
    hashes = {
        "protocol": "iotdbfs-catalog-ablation-build/1",
        "changed_caller_sites": 2,
        "source_pushdown_sha256": hashlib.sha256(original).hexdigest(),
        "source_no_pushdown_sha256": hashlib.sha256(ablation).hexdigest(),
        "binary_pushdown_sha256": hashlib.sha256((args.output / "iotdb_fs-pushdown").read_bytes()).hexdigest(),
        "binary_no_pushdown_sha256": hashlib.sha256((args.output / "iotdb_fs-no-pushdown").read_bytes()).hexdigest(),
    }
    (args.output / "ablation-build.json").write_text(json.dumps(hashes, indent=2) + "\n")
    print(json.dumps(hashes))


if __name__ == "__main__":
    main()
