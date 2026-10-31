#!/usr/bin/env python3
"""Validate H02 source-backed native reads against H00's independent oracles.

This records all VFD source calls, including metadata and library read-ahead.
It is a correctness diagnostic, not the future raw-chunk query benchmark.
"""
from __future__ import annotations
import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import platform
import subprocess
import time

HERE = Path(__file__).resolve().parent
GENERATOR = HERE.parents[1] / "fixtures/hdf5/generate_h00.py"

def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()

def require(condition, message):
    if not condition:
        raise AssertionError(message)

def validate_trace(result, size):
    reads = result["reads"]
    require(all(r["success"] and 0 <= r["offset"] <= size and
                0 < r["length"] <= size-r["offset"] for r in reads),
            "failed, zero, or out-of-source read in successful trace")
    metadata = sum(r["length"] for r in reads if not r["raw_data"])
    raw = sum(r["length"] for r in reads if r["raw_data"])
    require(result["metadata_bytes"] == metadata and result["raw_data_bytes"] == raw,
            "metadata/raw trace accounting mismatch")
    require(result["callback_calls"] == len(reads) and
            result["callback_bytes"] == metadata+raw,
            "source callback and VFD trace differ")
    require(result["raw_chunk_cache_bytes"] == 0, "raw cache should be disabled")

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--probe", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--public-dir", type=Path, required=True)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    traces = args.output / "traces"
    traces.mkdir(exist_ok=True)
    spec = importlib.util.spec_from_file_location("h02_fixture_generator", GENERATOR)
    generator = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(generator)
    import h5py
    fixtures = args.output / "fixtures"
    manifest = generator.generate(fixtures)
    jobs = []
    for entry in manifest["files"]:
        for selection in entry["selections"]:
            jobs.append((fixtures / entry["path"], selection,
                         entry["id"] + "__" + selection["id"]))
    public = json.loads((HERE / "public-data.lock.json").read_text())
    for entry in public["files"]:
        path = args.public_dir / entry["filename"]
        require(path.stat().st_size == entry["size_bytes"] and sha(path) == entry["sha256"],
                "public source size/hash mismatch")
        for selection in entry["selections"]:
            jobs.append((path, dict(selection, dataset=entry["dataset"]),
                         "public_" + entry["id"] + "__" + selection["id"]))
    summary = {"schema_version": "iotdbfs-hdf5-h02-validation/1",
               "platform": platform.platform(), "probe_sha256": sha(args.probe),
               "oracle_h5py": h5py.__version__,
               "oracle_hdf5": h5py.version.hdf5_version,
               "fixture_manifest_sha256": sha(fixtures / "manifest.json"),
               "scope": "formula/public oracle, unsupported and corrupt cases; lifetime/security also covered by unit tests",
               "cases": [], "environment_checks": [], "failures": []}
    for path, selection, name in jobs:
        try:
            report, values = traces / (name + ".json"), traces / (name + ".bin")
            report.unlink(missing_ok=True)
            values.unlink(missing_ok=True)
            command = [str(args.probe.resolve()), "--file", str(path.resolve()),
                       "--dataset", selection["dataset"], "--start",
                       ",".join(map(str, (selection["start"] or [0]))), "--count",
                       ",".join(map(str, (selection["count"] or [1]))),
                       "--output", str(report.resolve()), "--values-output", str(values.resolve())]
            begin = time.perf_counter()
            process = subprocess.run(command, capture_output=True, text=True, timeout=30)
            elapsed = time.perf_counter() - begin
            require(report.is_file(), f"missing probe report: {process.stderr[:1000]}")
            result = json.loads(report.read_text())
            if not selection.get("support", True):
                expected_code = ("CorruptPayload" if selection.get("expected_status") == "corrupt"
                                 else "UnsupportedCapability")
                require(process.returncode == 1 and result["status"] == "error" and
                        result["error_code"] == expected_code,
                        f"expected {expected_code}, got {result.get('error_code')}: {result.get('error')}")
                require(not values.exists(), "failed query published value bytes")
                validate_trace(result, path.stat().st_size)
                summary["cases"].append({"id": name, "status": expected_code,
                    "report": report.name, "report_sha256": sha(report),
                    "metadata_bytes": result["metadata_bytes"], "raw_data_bytes": result["raw_data_bytes"],
                    "source_calls": result["callback_calls"]})
                continue
            require(process.returncode == 0 and result["status"] == "ok", result.get("error", process.stderr))
            require(result["schema_version"] == "iotdbfs-hdf5-h02-source-probe/1", "wrong schema")
            expected = selection.get("sha256") or selection["expected"]["sha256"]
            require(sha(values) == expected, "VFD native values differ from independent/frozen oracle")
            with h5py.File(path, "r") as file:
                sliced = file[selection["dataset"]][tuple(slice(a,a+n) for a,n in
                                                         zip(selection["start"], selection["count"]))]
                require(generator.digest(sliced) == expected, "h5py values differ from oracle")
            validate_trace(result, path.stat().st_size)
            summary["cases"].append({"id": name, "status": "ok", "report": report.name,
                "report_sha256": sha(report), "values_sha256": expected,
                "output_bytes": values.stat().st_size,
                "metadata_bytes": result["metadata_bytes"], "raw_data_bytes": result["raw_data_bytes"],
                "source_calls": result["callback_calls"], "subprocess_wall_seconds": elapsed})
        except Exception as exc:
            summary["failures"].append({"id": name, "error": str(exc)})
    # These fresh processes exercise rejection BEFORE any HDF5 API initializes
    # a driver/VOL selected from the environment. The caller's env is not edited.
    path, selection, _ = jobs[0]
    for variable in ("HDF5_DRIVER", "HDF5_VOL_CONNECTOR"):
        name = "environment_" + variable.lower()
        try:
            report, values = traces / (name + ".json"), traces / (name + ".bin")
            report.unlink(missing_ok=True)
            values.unlink(missing_ok=True)
            command = [str(args.probe.resolve()), "--file", str(path.resolve()),
                       "--dataset", selection["dataset"], "--start",
                       ",".join(map(str, (selection["start"] or [0]))), "--count",
                       ",".join(map(str, (selection["count"] or [1]))),
                       "--output", str(report.resolve()), "--values-output", str(values.resolve())]
            environment = os.environ.copy()
            environment[variable] = "iotdbfs-must-never-load-this-plugin"
            process = subprocess.run(command, env=environment, capture_output=True,
                                     text=True, timeout=30)
            result = json.loads(report.read_text())
            require(process.returncode == 1 and result["status"] == "error" and
                    "environment overrides" in result["error"], "configuration not rejected")
            require(result["callback_calls"] == 0 and not values.exists(),
                    "polluted process accessed source or produced values")
            summary["environment_checks"].append({"variable": variable,
                "passed": True, "report_sha256": sha(report)})
        except Exception as exc:
            summary["failures"].append({"id": name, "error": str(exc)})
    summary["expected_cases"] = len(jobs)
    summary["passed"] = len(summary["cases"]) == len(jobs) and not summary["failures"]
    (args.output / "summary.json").write_text(json.dumps(summary, indent=2)+"\n")
    print(json.dumps({"passed": summary["passed"], "cases": len(summary["cases"]),
                      "expected_cases": len(jobs), "failures": summary["failures"]}, indent=2))
    return 0 if summary["passed"] else 1

if __name__ == "__main__":
    raise SystemExit(main())
