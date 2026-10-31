#!/usr/bin/env python3
"""Prepare byte-distinct Arrow IPC codec variants from frozen public streams.

The source streams, canonical data, sampler, and published baseline paths are
read-only. New variants are written under a separate local directory and
published under a separate directory in an existing IoTDB-FS mount.
"""

from __future__ import annotations

import argparse
import hashlib
from itertools import zip_longest
import json
import os
from pathlib import Path
import tempfile
import time

import pyarrow.ipc as ipc

from public_iotdbfs import source_version


DATASETS = ("ETTm1", "UCI_Electricity_370")
CODECS = ("none", "lz4", "zstd")
CHUNK_BYTES = 512 * 1024
PROTOCOL = "iotdbfs-arrow-query-variants/1"


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(CHUNK_BYTES), b""):
            digest.update(chunk)
    return digest.hexdigest()


def source_info(work_dir: Path, dataset: str) -> tuple[Path, dict]:
    declaration = json.loads((work_dir / "manifests" / f"{dataset}-pb00.json")
                             .read_text(encoding="utf-8"))
    if declaration["dataset_id"] != dataset:
        raise ValueError(f"pinned dataset identity differs: {dataset}")
    info = declaration["arrow"]
    if info["compression"] != "none":
        raise ValueError(f"frozen Arrow source must be uncompressed: {dataset}")
    path = work_dir / "arrow" / info["path"]
    if path.stat().st_size != info["bytes"] or sha256_file(path) != info["sha256"]:
        raise ValueError(f"frozen Arrow source differs from PB00: {dataset}")
    return path, info


def write_variant(source: Path, destination: Path, codec: str) -> None:
    """Write one stream through Arrow IPC without changing batch boundaries."""
    if codec == "none":
        raise ValueError("none reuses the frozen local source")
    if destination.exists():
        return
    destination.parent.mkdir(parents=True, exist_ok=True)
    fd, pending_name = tempfile.mkstemp(prefix=f".{destination.name}.",
                                        suffix=".pending", dir=destination.parent)
    pending = Path(pending_name)
    try:
        with os.fdopen(fd, "wb") as output, source.open("rb") as input_handle:
            with ipc.open_stream(input_handle) as reader:
                with ipc.new_stream(output, reader.schema,
                                    options=ipc.IpcWriteOptions(compression=codec)) as writer:
                    for batch in reader:
                        writer.write_batch(batch)
            output.flush()
            os.fsync(output.fileno())
        # link fails if another process created the exact destination; it
        # cannot silently replace a frozen experiment artifact.
        os.link(pending, destination)
    finally:
        pending.unlink(missing_ok=True)


def logical_fingerprint_and_compare(source: Path, variant: Path) -> tuple[str, list[int]]:
    """Compare every field of every batch, including nulls and schema metadata."""
    source_hash = hashlib.sha256()
    variant_hash = hashlib.sha256()
    counts: list[int] = []
    marker = object()
    with source.open("rb") as original_file, variant.open("rb") as variant_file:
        with ipc.open_stream(original_file) as original, ipc.open_stream(variant_file) as current:
            if not original.schema.equals(current.schema, check_metadata=True):
                raise ValueError(f"Arrow schema differs: {variant}")
            schema_bytes = original.schema.serialize().to_pybytes()
            source_hash.update(schema_bytes)
            variant_hash.update(current.schema.serialize().to_pybytes())
            for index, (expected, actual) in enumerate(zip_longest(original, current,
                                                                    fillvalue=marker)):
                if expected is marker or actual is marker:
                    raise ValueError(f"Arrow batch count differs: {variant}")
                if not expected.equals(actual):
                    raise ValueError(f"Arrow logical batch {index} differs: {variant}")
                counts.append(expected.num_rows)
                source_hash.update(expected.serialize().to_pybytes())
                variant_hash.update(actual.serialize().to_pybytes())
    if source_hash.digest() != variant_hash.digest():
        raise ValueError(f"Arrow logical fingerprint differs: {variant}")
    return source_hash.hexdigest(), counts


def ingest_exact(source: Path, destination: Path, expected_size: int,
                 expected_sha: str) -> tuple[float, float]:
    """Create once and verify the entire mounted byte stream on every run."""
    ingestion_seconds = 0.0
    if not destination.exists():
        started = time.perf_counter()
        fd = os.open(destination, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o644)
        try:
            with source.open("rb") as input_handle:
                for chunk in iter(lambda: input_handle.read(CHUNK_BYTES), b""):
                    view = memoryview(chunk)
                    while view:
                        written = os.write(fd, view)
                        if written <= 0:
                            raise OSError("IoTDB-FS write made no progress")
                        view = view[written:]
            os.fsync(fd)
        finally:
            os.close(fd)
        ingestion_seconds = time.perf_counter() - started
    started = time.perf_counter()
    if destination.stat().st_size != expected_size or sha256_file(destination) != expected_sha:
        raise ValueError(f"mounted Arrow bytes differ from local variant: {destination}")
    return ingestion_seconds, time.perf_counter() - started


def prepare(work_dir: Path, mount: Path, socket_path: Path, output: Path) -> dict:
    local_variants = work_dir / "arrow-query-variants-20261001"
    mounted_variants = mount / "query-ext-20261001"
    local_variants.mkdir(parents=True, exist_ok=True)
    if not mounted_variants.exists():
        mounted_variants.mkdir(mode=0o755)
    if not mounted_variants.is_dir():
        raise ValueError("query variant mount path is not a directory")

    items = []
    for dataset in DATASETS:
        frozen, pinned = source_info(work_dir, dataset)
        for codec in CODECS:
            local = (frozen if codec == "none" else
                     local_variants / f"{dataset}-{codec}-8192.arrows")
            if codec != "none":
                write_variant(frozen, local, codec)
            logical_sha, counts = logical_fingerprint_and_compare(frozen, local)
            if counts != pinned["batch_row_counts"] or sum(counts) != len(pinned["rows"]):
                raise ValueError(f"Arrow batch segmentation differs: {local}")
            byte_size = local.stat().st_size
            byte_sha = sha256_file(local)
            if codec == "none" and (byte_size != pinned["bytes"] or
                                    byte_sha != pinned["sha256"]):
                raise ValueError(f"none variant differs from frozen source: {dataset}")

            logical_path = f"/query-ext-20261001/{dataset}-{codec}.arrows"
            destination = mount / logical_path.lstrip("/")
            ingestion_seconds, readback_seconds = ingest_exact(
                local, destination, byte_size, byte_sha)
            container = os.getxattr(destination, "user.arrow.container").decode("ascii")
            if container != "stream":
                raise ValueError(f"Arrow stream identity is missing: {logical_path}")
            version = source_version(socket_path, logical_path)
            item = {
                "dataset": dataset,
                "codec": codec,
                "local": str(local),
                "logical": logical_path,
                "mounted": str(destination),
                "version": version,
                "bytes": byte_size,
                "sha256": byte_sha,
                "source_sha256": pinned["sha256"],
                "logical_sha256": logical_sha,
                "batch_row_counts": counts,
                "batches": len(counts),
                "rows": sum(counts),
                "arrow_container": container,
                "ingestion_seconds": ingestion_seconds,
                "readback_seconds": readback_seconds,
            }
            items.append(item)
            print(json.dumps({"dataset": dataset, "codec": codec,
                              "version": version, "bytes": byte_size,
                              "ingestion_seconds": ingestion_seconds},
                             ensure_ascii=False), flush=True)

    result = {"protocol": PROTOCOL, "variants": items}
    output.parent.mkdir(parents=True, exist_ok=True)
    if output.exists():
        previous = json.loads(output.read_text(encoding="utf-8"))
        # Timing is observation-specific; all identities must be unchanged.
        def identities(value: dict) -> list[dict]:
            return [{key: item[key] for key in item if key not in
                     ("ingestion_seconds", "readback_seconds")}
                    for item in value["variants"]]
        if previous.get("protocol") != PROTOCOL or identities(previous) != identities(result):
            raise ValueError("existing variant manifest has different identities")
    else:
        output.write_text(json.dumps(result, indent=2, ensure_ascii=False) + "\n",
                          encoding="utf-8")
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--work-dir", type=Path, required=True)
    parser.add_argument("--mount", type=Path, required=True)
    parser.add_argument("--socket", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = prepare(args.work_dir, args.mount, args.socket, args.output)
    print(json.dumps({"protocol": result["protocol"], "manifest": str(args.output),
                      "variants": len(result["variants"])}, ensure_ascii=False))


if __name__ == "__main__":
    main()
