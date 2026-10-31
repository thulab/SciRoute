#!/usr/bin/env python3
"""Reproduce H00 formula, native HDF5, raw-range, and public-data checks.

No IoTDB writes, mounts, adapter registration, or production query fallback.
The C++ trace measures value payload only, not libhdf5 metadata/oracle I/O.
"""
from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import platform
from pathlib import Path
import subprocess
import sys
import time
import urllib.request
import urllib.parse

import h5py
import numpy as np

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[2]
spec = importlib.util.spec_from_file_location(
    "h00_generator", ROOT / "tests/fixtures/hdf5/generate_h00.py")
generator = importlib.util.module_from_spec(spec)
spec.loader.exec_module(generator)


def sha(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


class HTTPSOnlyRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, url):
        require(urllib.parse.urlsplit(url).scheme == "https", "insecure redirect")
        return super().redirect_request(req, fp, code, msg, headers, url)


def public_file(entry, directory, download):
    path = directory / entry["filename"]
    if not path.exists():
        require(download, f"Missing public file: {path}; use --download")
        require(urllib.parse.urlsplit(entry["url"]).scheme == "https", "insecure URL")
        partial = path.with_suffix(path.suffix + ".partial")
        try:
            opener = urllib.request.build_opener(HTTPSOnlyRedirect())
            with opener.open(entry["url"], timeout=60) as response, partial.open("wb") as out:
                total = 0
                for block in iter(lambda: response.read(65536), b""):
                    total += len(block)
                    require(total <= entry["size_bytes"], "public download exceeds locked size")
                    out.write(block)
            require(sha(partial) == entry["sha256"], "public data checksum mismatch")
            partial.replace(path)
        finally:
            partial.unlink(missing_ok=True)
    require(path.stat().st_size == entry["size_bytes"] and sha(path) == entry["sha256"],
            f"Public data differs from lock: {path.name}")
    return path


def validate_ranges(result, path, file, excluded_datasets):
    """Check payload accounting and prove omitted datasets were not read."""
    size = path.stat().st_size
    require(result.get("source_bytes") == size, "reported source size differs")
    ranges = result["ranges"]
    for item in ranges:
        offset, length = item["offset"], item["length"]
        require(type(offset) is int and type(length) is int and offset >= 0 and length > 0
                and offset <= size and length <= size - offset, "trace exceeds file bounds")
    require(result.get("raw_payload_calls") == len(ranges), "payload call count differs from trace")
    require(result.get("raw_payload_bytes") == sum(item["length"] for item in ranges),
            "payload byte count differs from trace")
    for name in excluded_datasets:
        dataset = file[name]
        if dataset.chunks is not None:
            allocations = []
            for i in range(dataset.id.get_num_chunks()):
                chunk = dataset.id.get_chunk_info(i)
                if chunk.byte_offset is not None and chunk.size:
                    allocations.append((chunk.byte_offset, chunk.size))
        else:
            offset = dataset.id.get_offset()
            length = dataset.id.get_storage_size()
            allocations = [(offset, length)] if offset is not None and length else []
        require(allocations, f"excluded dataset {name} has no payload; exclusion test is vacuous")
        for offset, length in allocations:
            for item in ranges:
                require(item["offset"] + item["length"] <= offset or
                        offset + length <= item["offset"],
                        f"payload trace reads excluded dataset {name}")


def invoke(probe, path, selection, output, name):
    report, values = output / (name + ".json"), output / (name + ".bin")
    # A crashed invocation must not inherit a successful report from a prior run.
    report.unlink(missing_ok=True)
    values.unlink(missing_ok=True)
    # Scalars/null dataspaces are rejected by the profile before selection validation.
    start, count = selection["start"] or [0], selection["count"] or [1]
    command = [str(probe), "--file", str(path), "--dataset", selection["dataset"],
               "--start", ",".join(map(str, start)), "--count", ",".join(map(str, count)),
               "--output", str(report), "--values-output", str(values)]
    before = time.perf_counter()
    process = subprocess.run(command, capture_output=True, text=True, timeout=30)
    elapsed = time.perf_counter() - before
    require(report.exists(), f"probe did not write report: {process.stderr[:1000]}")
    result = json.loads(report.read_text())
    require(result.get("schema_version") == "iotdbfs-hdf5-h00-probe/1", "unexpected probe schema")
    status = result["status"]
    require(result.get("dynamic_plugins_enabled") is False, "dynamic plugins must be disabled")
    require(result.get("library_version") == "1.14.4", "probe must use locked HDF5 1.14.4")
    require(result.get("library_threadsafe") is True, "thread-safe library required")
    if selection.get("expected_status") == "corrupt":
        require(process.returncode == 1 and status == "error" and "checksum" in result.get("error", "").lower(),
                f"expected checksum rejection, got {result.get('error', status)}")
    elif not selection.get("support", True):
        require(process.returncode == 0 and status == "unsupported", f"expected unsupported, got {result.get('error', status)}")
    else:
        require(process.returncode == 0 and status == "ok", result.get("error", "probe failed"))
        require(result["query"]["exact_match"], "raw decoder and H5Dread differ")
        expected = selection.get("sha256") or selection["expected"]["sha256"]
        require(sha(values) == expected, "raw output differs from independent/frozen oracle")
        with h5py.File(path, "r") as file:
            slices = tuple(slice(a, a + n) for a, n in zip(selection["start"], selection["count"]))
            native = file[selection["dataset"]][slices]
            require(generator.digest(native) == expected, "h5py differs from oracle")
            validate_ranges(result, path, file, selection.get("excluded_datasets", []))
    values.unlink(missing_ok=True)
    return {"id": name, "status": status, "report": report.name,
            "report_sha256": sha(report), "raw_payload_bytes": result.get("raw_payload_bytes", 0),
            "raw_payload_calls": result.get("raw_payload_calls", 0),
            "output_bytes": (result.get("query") or {}).get("output_elements", 0) *
                            (result.get("query") or {}).get("dataset", {}).get("element_bytes", 0),
            "subprocess_wall_seconds": elapsed}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--probe", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--public-dir", type=Path, required=True)
    parser.add_argument("--download", action="store_true")
    args = parser.parse_args()
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=True)
    # Preparation/download failures must not leave an older passing summary visible.
    (output / "summary.json").unlink(missing_ok=True)
    args.public_dir.mkdir(parents=True, exist_ok=True)
    traces = output / "traces"
    traces.mkdir(exist_ok=True)
    fixture_dir = output / "fixtures"
    manifest = generator.generate(fixture_dir)
    lock = json.loads((HERE / "public-data.lock.json").read_text())
    summary = {"schema_version": "iotdbfs-hdf5-h00-validation/1", "passed": False,
               "environment": {"platform": platform.platform(), "python": platform.python_version(),
                               "numpy": np.__version__, "h5py": h5py.__version__,
                               "h5py_hdf5": h5py.version.hdf5_version},
               "probe_sha256": sha(args.probe), "fixture_manifest_sha256": sha(fixture_dir / "manifest.json"),
               "public_lock_sha256": sha(HERE / "public-data.lock.json"),
               "fixture_files": len(manifest["files"]), "fixture_bytes": manifest["total_bytes"],
               "cases": [], "failures": []}
    jobs = []
    for entry in manifest["files"]:
        path = fixture_dir / entry["path"]
        require(sha(path) == entry["sha256"], "fixture changed after generation")
        for selection in entry["selections"]:
            jobs.append((path, selection, entry["id"] + "__" + selection["id"]))
    for entry in lock["files"]:
        path = public_file(entry, args.public_dir.resolve(), args.download)
        with h5py.File(path, "r") as file:
            dataset = file[entry["dataset"]]
            for key in ("shape", "chunks", "compression", "compression_opts", "shuffle", "fletcher32"):
                actual = getattr(dataset, key)
                if isinstance(actual, tuple):
                    actual = list(actual)
                require(actual == entry[key], f"public dataset property differs: {key}")
            require(dataset.dtype.str == entry["dtype"], "public datatype differs")
        for selection in entry["selections"]:
            jobs.append((path, dict(selection, dataset=entry["dataset"]),
                         "public_" + entry["id"] + "__" + selection["id"]))
    for path, selection, name in jobs:
        try:
            summary["cases"].append(invoke(args.probe.resolve(), path, selection, traces, name))
        except Exception as error:
            summary["failures"].append({"id": name, "error": str(error)})
    summary["passed"] = not summary["failures"]
    summary["case_count"] = len(jobs)
    summary["status_counts"] = {status: sum(c["status"] == status for c in summary["cases"])
                                for status in ("ok", "unsupported", "error")}
    (output / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    print(json.dumps({k: summary[k] for k in ("passed", "case_count", "status_counts", "failures")}))
    return 0 if summary["passed"] else 1


if __name__ == "__main__":
    sys.exit(main())
