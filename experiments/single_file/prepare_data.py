#!/usr/bin/env python3
"""Prepare dense controlled HDF5 tiers; never generate inside query timing.

All tiers share the generator, layout and query coordinates. Only time extent
changes. Source SHA computation explicitly warms OS source pages. No scientific
coordinate datasets or bindings are implied by the synthetic physical values.
"""
from __future__ import annotations

import argparse
import copy
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import shutil
import tempfile
import time

import h5py
import numpy as np

WORKLOAD_PATH = Path(__file__).with_name("workload.json")
REQUIRED_HDF5_VERSION = "1.14.4"


def workload() -> dict:
    return json.loads(WORKLOAD_PATH.read_text(encoding="utf-8"))


def scaled_test_workload() -> dict:
    """Explicitly non-production fixture; it cannot pass real size-tier gates."""
    result = workload()
    result["test_only_scaled"] = True
    result["spatial_shape"] = [4, 6]
    result["chunks"] = [1, 1, 2, 3]
    for count, tier in enumerate(result["tiers"].values(), 2):
        tier["time_count"] = count
    result["cases"] = {
        "point": {"origin": [1, 0, 2, 3], "shape": [1, 1, 1, 1]},
        "small_roi": {"origin": [0, 0, 0, 0], "shape": [2, 1, 2, 3]},
        "cross_chunk": {"origin": [0, 0, 1, 2], "shape": [2, 1, 2, 2]},
    }
    return result


def assert_runtime_version() -> None:
    actual = h5py.version.hdf5_version
    if actual != REQUIRED_HDF5_VERSION:
        raise ValueError(f"h5py runtime HDF5 must be exactly {REQUIRED_HDF5_VERSION}; got {actual}")


def validate_config(config: dict) -> None:
    expected = workload()
    if config == expected:
        return
    if config != scaled_test_workload():
        raise ValueError("only the frozen production workload or explicit scaled-test fixture is permitted")


def finite_chunk(rng: np.random.Generator, shape: tuple[int, ...]) -> np.ndarray:
    # Almost uniformly distributed bits minimize artificial compressibility.
    # Exponent 255 is remapped to 254; signs/mantissas are preserved. No NaN/Inf.
    words = rng.integers(0, 1 << 32, size=shape, dtype=np.uint32)
    special = (words & np.uint32(0x7F800000)) == np.uint32(0x7F800000)
    words[special] ^= np.uint32(0x00800000)
    return words.astype("<u4", copy=False).view("<f4")


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def array_digest(value: np.ndarray, dtype: str) -> str:
    return hashlib.sha256(np.asarray(value, dtype=dtype, order="C").tobytes(order="C")).hexdigest()


def validate_size_gate(actual_bytes: int, tier: dict) -> None:
    if not (tier["minimum_file_bytes"] <= actual_bytes and
            (tier["maximum_file_bytes"] is None or actual_bytes <= tier["maximum_file_bytes"])):
        raise ValueError(f"actual file size {actual_bytes} does not satisfy size-tier gate {tier}")


def fsync_file(path: Path) -> None:
    with path.open("rb") as stream:
        os.fsync(stream.fileno())


def fsync_directory(path: Path) -> None:
    fd = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def generate_tier(path: Path, tier: dict, config: dict) -> dict:
    start = time.perf_counter_ns()
    shape = (tier["time_count"], 1, *config["spatial_shape"])
    chunks = tuple(config["chunks"])
    rng = np.random.Generator(np.random.PCG64(config["seed"]))
    expected_chunks = math.prod(math.ceil(n / c) for n, c in zip(shape, chunks))
    cache_bytes = math.prod(chunks) * 4 * 2
    with h5py.File(path, "x", rdcc_nbytes=cache_bytes, rdcc_nslots=17) as file:
        dataset = file.create_dataset(config["dataset"], shape=shape,
            dtype=np.dtype(config["dtype"]), chunks=chunks, track_times=False, **config["filters"])
        for key, value in config["attributes"].items():
            # Fixed-length ASCII avoids unbounded string-attribute reads.
            dataset.attrs[key] = np.bytes_(value)
        for t in range(shape[0]):
            for y in range(0, shape[2], chunks[2]):
                for x in range(0, shape[3], chunks[3]):
                    block_shape = (1, 1, min(chunks[2], shape[2] - y), min(chunks[3], shape[3] - x))
                    block = finite_chunk(rng, block_shape)
                    dataset[t:t + 1, :, y:y + block_shape[2], x:x + block_shape[3]] = block
        file.flush()
        allocated_chunks = dataset.id.get_num_chunks()
        storage_bytes = dataset.id.get_storage_size()
    fsync_file(path)
    generation_ns = time.perf_counter_ns() - start
    actual_bytes = path.stat().st_size
    logical_bytes = math.prod(shape) * 4
    if allocated_chunks != expected_chunks or storage_bytes < logical_bytes * 0.95:
        raise ValueError("dataset is not fully allocated dense random data")
    scaled = config.get("test_only_scaled", False)
    if not scaled:
        validate_size_gate(actual_bytes, tier)

    oracle_start = time.perf_counter_ns()
    cases = {}
    with h5py.File(path, "r", rdcc_nbytes=cache_bytes, rdcc_nslots=17) as file:
        dataset = file[config["dataset"]]
        actual_filters = {"shuffle": dataset.shuffle, "compression": dataset.compression,
            "compression_opts": dataset.compression_opts, "fletcher32": dataset.fletcher32}
        if (list(file) != ["T"] or dataset.shape != shape or dataset.dtype != np.dtype("<f4")
                or dataset.chunks != chunks or actual_filters != config["filters"]):
            raise ValueError("reopened dataset schema differs from frozen workload")
        for name, selection in config["cases"].items():
            slices = tuple(slice(o, o + n) for o, n in zip(selection["origin"], selection["shape"]))
            if any(o < 0 or n <= 0 or o + n > extent for o, n, extent in
                    zip(selection["origin"], selection["shape"], shape)):
                raise ValueError(f"case {name} exceeds shape")
            value = np.asarray(dataset[slices], dtype="<f4", order="C")
            if list(value.shape) != selection["shape"] or not np.isfinite(value).all():
                raise ValueError(f"case {name} has invalid values or shape")
            mask = np.ones(value.shape, dtype=np.uint8)
            cases[name] = {**copy.deepcopy(selection), "dtype": "<f4", "order": "C",
                "rows": value.size, "value_bytes": value.nbytes,
                "value_sha256": array_digest(value, "<f4"),
                "mask_dtype": "uint8", "mask_semantics": "1=valid; all generated values finite",
                "mask_sha256": array_digest(mask, "u1")}
    oracle_ns = time.perf_counter_ns() - oracle_start
    hash_start = time.perf_counter_ns()
    source_sha256 = sha256_file(path)
    hash_ns = time.perf_counter_ns() - hash_start
    stat = path.stat()
    return {"file": path.name, "bytes": actual_bytes, "sha256": source_sha256,
        "shape": list(shape), "logical_bytes": logical_bytes,
        "allocated_chunk_count": allocated_chunks, "expected_chunk_count": expected_chunks,
        "allocated_dataset_bytes": storage_bytes,
        "filters": actual_filters, "generation_rdcc_max_bytes": cache_bytes,
        "filesystem_allocated_bytes": getattr(stat, "st_blocks", 0) * 512,
        "real_size_gate_passed": not scaled, "cases": cases,
        "timings_ns": {"generation_and_source_fsync": generation_ns,
            "oracle_build": oracle_ns, "source_sha256": hash_ns}}


def prepare(output: Path, config: dict | None = None) -> dict:
    config = workload() if config is None else copy.deepcopy(config)
    validate_config(config)
    assert_runtime_version()  # Before creating directories/files or allocating data.
    output = output.absolute()
    if output.exists() or output.is_symlink():
        raise FileExistsError(f"output already exists: {output}")
    output.parent.mkdir(parents=True, exist_ok=True)
    lock_path = output.parent / ("." + output.name + ".prepare.lock")
    lock_fd = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    temporary = None
    try:
        if output.exists() or output.is_symlink():
            raise FileExistsError(f"output already exists: {output}")
        temporary = Path(tempfile.mkdtemp(prefix="." + output.name + ".preparing-", dir=output.parent))
        raw_files = temporary / "raw_files"
        raw_files.mkdir()
        started = time.perf_counter_ns()
        sources = {name: generate_tier(raw_files / (name + ".h5"), tier, config)
                 for name, tier in config["tiers"].items()}
        cases = sources["small"]["cases"]
        for source in sources.values():
            if source["cases"] != cases:
                raise ValueError("common prefix query oracles differ across tiers")
            source.pop("cases")
            source["file"] = "raw_files/" + source["file"]
        build_start = time.perf_counter_ns()
        manifest = {"protocol": "iotdbfs-single-file-hdf5-data/1",
            "synthetic_controlled": True, "public_dataset": False,
            "test_only_scaled": bool(config.get("test_only_scaled", False)),
            "all_real_size_gates_passed": not config.get("test_only_scaled", False),
            "workload": config, "sources": sources, "cases": cases,
            "libraries": {"h5py": h5py.__version__, "hdf5_runtime": h5py.version.hdf5_version,
                "numpy": np.__version__, "python": platform.python_version()},
            "source_sha_warms_os_pages": True,
            "timing_semantics": "Offline preparation only; generation includes HDF5 close/source fsync. Oracle and whole-source SHA are separate and excluded from query timings. Final manifest fsync/rename excluded from preparation_before_manifest_ns.",
            "preparation_before_manifest_ns": time.perf_counter_ns() - started}
        manifest["manifest_build_ns"] = time.perf_counter_ns() - build_start
        manifest_path = temporary / "manifest.json"
        with manifest_path.open("x", encoding="utf-8") as stream:
            json.dump(manifest, stream, indent=2, sort_keys=True, allow_nan=False)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        fsync_directory(temporary)
        fsync_directory(raw_files)
        if output.exists() or output.is_symlink():
            raise FileExistsError(f"output already exists: {output}")
        temporary.rename(output)
        temporary = None
        fsync_directory(output.parent)
        return manifest
    finally:
        if temporary is not None:
            shutil.rmtree(temporary)
        os.close(lock_fd)
        lock_path.unlink()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    manifest = prepare(args.output)
    print(json.dumps({"manifest": str(args.output.absolute() / "manifest.json"),
        "source_bytes": {k: v["bytes"] for k, v in manifest["sources"].items()},
        "all_real_size_gates_passed": manifest["all_real_size_gates_passed"]}))


if __name__ == "__main__":
    main()
