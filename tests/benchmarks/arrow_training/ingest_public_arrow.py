#!/usr/bin/env python3
"""Copy a pinned public Arrow stream into an isolated IoTDB-FS mount."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path


CHUNK_BYTES = 512 * 1024


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(CHUNK_BYTES), b""):
            digest.update(block)
    return digest.hexdigest()


def ingest(work_dir: Path, mount: Path, dataset: str) -> dict:
    manifest = json.loads(
        (work_dir / "manifests" / f"{dataset}-pb00.json").read_text(encoding="utf-8")
    )
    if manifest["dataset_id"] != dataset:
        raise ValueError("dataset manifest identity mismatch")
    info = manifest["arrow"]
    source = work_dir / "arrow" / info["path"]
    destination = mount / info["path"]
    expected = info["sha256"]
    if source.stat().st_size != info["bytes"] or sha256(source) != expected:
        raise ValueError("local Arrow source differs from pinned manifest")
    if not destination.exists():
        fd = os.open(destination, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o644)
        try:
            with source.open("rb") as handle:
                for block in iter(lambda: handle.read(CHUNK_BYTES), b""):
                    view = memoryview(block)
                    while view:
                        written = os.write(fd, view)
                        if written <= 0:
                            raise OSError("IoTDB-FS write made no progress")
                        view = view[written:]
            os.fsync(fd)
        finally:
            os.close(fd)
    if destination.stat().st_size != info["bytes"] or sha256(destination) != expected:
        raise ValueError("IoTDB-FS readback differs from pinned Arrow bytes")
    container = os.getxattr(destination, "user.arrow.container").decode("ascii")
    if container != "stream":
        raise ValueError(f"IoTDB-FS did not register an Arrow stream: {container}")
    return {
        "dataset_id": dataset,
        "path": str(destination),
        "bytes": info["bytes"],
        "sha256": expected,
        "arrow_container": container,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--work-dir", type=Path, required=True)
    parser.add_argument("--mount", type=Path, required=True)
    parser.add_argument("--dataset", choices=("ETTm1", "UCI_Electricity_370"), required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = ingest(args.work_dir, args.mount, args.dataset)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
