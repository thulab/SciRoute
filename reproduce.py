#!/usr/bin/env python3
"""Verify catalog fixtures and reanalyze archived collection timings, offline.

Default: Python >=3.10, standard library only. Native mode additionally needs
Linux and the dependencies documented in reproducibility/README.md.
This is not an end-to-end reproduction of all paper experiments.
"""
from __future__ import annotations

import argparse
from collections import defaultdict
import csv
from datetime import datetime, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import shutil
import statistics
import subprocess
import sys
import uuid
import xml.etree.ElementTree as ET

ROOT = Path(__file__).resolve().parent


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def save(path, value):
    path.write_text(json.dumps(value, indent=2) + "\n", encoding="utf-8")


def command(args, log):
    with log.open("w", encoding="utf-8") as stream:
        subprocess.run(args, cwd=ROOT, stdout=stream, stderr=subprocess.STDOUT,
                       check=True)


def smoke(out):
    # Isolate imports from the caller's cwd and PYTHONPATH. Tests use temporary
    # fixture files, not databases, mounts, native libraries or external data.
    code = """
import json, pathlib, sys, unittest
sys.dont_write_bytecode = True
sys.path.insert(0, sys.argv[1])
suite = unittest.defaultTestLoader.loadTestsFromNames([
    'fs_index.tests.test_lifecycle', 'fs_index.tests.test_hdf5_handles'])
result = unittest.TextTestRunner(verbosity=2).run(suite)
record = dict(tests=result.testsRun, failures=len(result.failures),
              errors=len(result.errors), skipped=len(result.skipped))
pathlib.Path(sys.argv[2]).write_text(json.dumps(record, indent=2)+'\\n')
sys.exit(0 if result.wasSuccessful() and result.testsRun > 0 and not result.skipped else 1)
"""
    command([sys.executable, "-I", "-c", code, str(ROOT), str(out / "smoke.json")],
            out / "smoke.log")
    return json.loads((out / "smoke.json").read_text())


def analyze(out):
    base = ROOT / "reproducibility"
    manifest = json.loads((base / "manifest.json").read_text())
    for item in manifest["files"]:
        path = base / item["path"]
        require(path.is_file() and digest(path) == item["sha256"],
                "Missing or changed input: " + item["path"])
    with (base / "data/collection-query-timings.csv").open(newline="") as stream:
        records = list(csv.DictReader(stream))
    with (base / "data/query-summary.csv").open(newline="") as stream:
        expected = list(csv.DictReader(stream))
    require(len(records) == manifest["projection_records"], "Projection record count differs")
    groups = defaultdict(list)
    for row in records:
        require(row["phase"] in {"measurement", "warmup"}, "Unknown timing phase")
        require(int(row["total_ns"]) == int(row["discovery_ns"]) + int(row["value_ns"]),
                "Per-request timing stages do not sum to total")
        if row["phase"] != "measurement":
            continue
        arm = row["arm"]
        if arm == "IoTDB-FS":
            arm += "-" + row["configuration"]
        groups[row["group_id"], arm].append(row)
    require(sum(map(len, groups.values())) == manifest["measurement_records"],
            "Measured request count differs")
    require(len(groups) == len(expected) == manifest["cells"], "Cell count differs")
    output = []
    seen = set()
    for ref in expected:
        key = ref["group_id"], ref["implementation"]
        require(key not in seen and key in groups, "Duplicate or missing summary cell")
        seen.add(key)
        rows = groups[key]
        require(len(rows) == int(ref["measured_queries"]), "Cell sample count differs")
        require(all(x["case_id"] == ref["case_id"] and x["corpus_id"] == ref["corpus_id"]
                    for x in rows), "Source/case identities differ")
        values = [int(x["total_ns"]) / 1e6 for x in rows]
        median = statistics.median(values)
        require(math.isclose(median, float(ref["T_ms"]), rel_tol=1e-12, abs_tol=1e-9),
                "Recomputed latency differs from archived median: " + str(key))
        output.append(dict(group_id=ref["group_id"], format=ref["format"],
                           N=int(ref["N"]), C=int(ref["C"]), S=ref["S"], K=int(ref["K"]),
                           implementation=ref["implementation"], n=len(values),
                           median_ms=median, min_ms=min(values), max_ms=max(values)))
    with (out / "collection-summary.csv").open("w", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(output[0]))
        writer.writeheader()
        writer.writerows(output)
    headline = {x["implementation"]: x["median_ms"] for x in output
                if x["format"] == "grib" and x["N"] == x["C"] == 512 and x["S"] == "S0"}
    result = dict(cells_checked=len(output), measured_requests=manifest["measurement_records"],
                  statistic="median of warm requests within one campaign",
                  latency_ms={k: headline[k] for k in ["FS-scan", "FS-index", "IoTDB-FS-SEQ4"]},
                  scan_over_index=headline["FS-scan"] / headline["FS-index"],
                  scan_over_managed=headline["FS-scan"] / headline["IoTDB-FS-SEQ4"],
                  input_manifest_sha256=digest(base / "manifest.json"))
    save(out / "headline.json", result)
    return output, result


def plot(rows, out):
    os.environ.setdefault("MPLCONFIGDIR", str(out / "matplotlib-cache"))
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    fig, axes = plt.subplots(1, 2, figsize=(8, 3.6), constrained_layout=True)
    for ax, fmt in zip(axes, ["grib", "hdf5"]):
        for arm, label in [("FS-scan", "FS-scan"), ("FS-index", "FS-index"),
                           ("IoTDB-FS-SEQ4", "IoTDB-FS")]:
            cells = sorted([x for x in rows if x["format"] == fmt and x["implementation"] == arm
                            and x["N"] == x["C"] and x["S"] == "S0"], key=lambda x: x["N"])
            ax.errorbar([x["N"] for x in cells], [x["median_ms"] for x in cells],
                        yerr=[[x["median_ms"] - x["min_ms"] for x in cells],
                              [x["max_ms"] - x["median_ms"] for x in cells]],
                        marker="o", capsize=3, label=label)
        ax.set(xscale="log", yscale="log", xlabel="Files (N=C)", ylabel="Latency (ms)",
               title=fmt.upper())
        ax.set_xticks([8, 128, 512], labels=["8", "128", "512"])
        ax.grid(alpha=0.2)
    axes[0].legend(fontsize=8)
    fig.savefig(out / "collection.png", dpi=180)
    plt.close(fig)
    return dict(matplotlib=matplotlib.__version__, file="collection.png",
                scope="Same 18 cells and min-max ranges; simplified layout, not paper pixel reproduction")


def native(args, out):
    require(platform.system() == "Linux", "Native profile requires Linux/WSL2; quick mode is portable")
    require(args.hdf5_dir and args.hdf5_dir.is_dir(),
            "Provide --hdf5-dir for thread-safe HDF5 1.14.4's CMake package")
    require(shutil.which("cmake") and shutil.which("ctest"), "cmake and ctest are required")
    command([sys.executable, "-c", "import h5py, numpy; print(h5py.version.info); print(numpy.__version__)"],
            out / "python-dependencies.log")
    build = out / "native-build"
    command(["cmake", "-S", str(ROOT / "fs_index"), "-B", str(build),
             "-DCMAKE_BUILD_TYPE=Release", "-DFS_INDEX_BUILD_TESTS=ON",
             "-DHDF5_DIR=" + str(args.hdf5_dir.resolve()), "-DPython3_EXECUTABLE=" + sys.executable],
            out / "configure.log")
    command(["cmake", "--build", str(build), "--target", "fs_index_native", "--parallel", "2"],
            out / "build.log")
    command(["ctest", "--test-dir", str(build), "-R", "^fs_index_(native_functional|maintenance)$",
             "--no-tests=error", "--output-on-failure", "--output-junit", str(out / "native-tests.xml")],
            out / "native-tests.log")
    cases = list(ET.parse(out / "native-tests.xml").iter("testcase"))
    require(len(cases) == 2 and all(c.find("failure") is None and c.find("skipped") is None
                                   for c in cases), "Both native CTest gates must execute successfully")
    return dict(ctest_gates=len(cases), scope="Native fixture functionality and recovery; no benchmark")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=["quick", "native"], default="quick")
    parser.add_argument("--plots", action="store_true", help="Also plot collection results; requires Matplotlib >=3.5")
    parser.add_argument("--hdf5-dir", type=Path, help="HDF5 1.14.4 CMake package directory (native mode)")
    parser.add_argument("--output", type=Path, help="New output directory; existing directories are never overwritten")
    args = parser.parse_args()
    if sys.version_info < (3, 10):
        parser.error("Python >=3.10 is required")
    out = (args.output or ROOT / ".tmp" / ("reproduce-" + uuid.uuid4().hex[:12])).resolve()
    try:
        out.mkdir(parents=True, exist_ok=False)
    except OSError as error:
        parser.error(str(error))
    report = dict(status="running", mode=args.mode, full_paper_reproduced=False,
                  started_utc=datetime.now(timezone.utc).isoformat(), python=sys.version,
                  platform=platform.platform(), driver_sha256=digest(Path(__file__)),
                  scope="Catalog fixture verification and archived collection reanalysis")
    report["source_files_sha256"] = {
        name: digest(ROOT / name) for name in [
            "fs_index/lifecycle.py", "fs_index/hdf5_handles.py",
            "fs_index/tests/test_lifecycle.py", "fs_index/tests/test_hdf5_handles.py",
            "fs_index/CMakeLists.txt", "fs_index/src/native.cpp", "fs_index/src/maintenance.inc"]}
    try:
        report["smoke"] = smoke(out)
        rows, report["collection"] = analyze(out)
        if args.plots:
            report["plot"] = plot(rows, out)
        if args.mode == "native":
            report["native"] = native(args, out)
        report["status"] = "passed"
        print("PASS: requested checks only; full paper reproduction remains incomplete.")
        print("FS-index speedup: %.3fx; managed: %.3fx (archived warm medians)." %
              (report["collection"]["scan_over_index"], report["collection"]["scan_over_managed"]))
        return 0
    except Exception as error:
        report.update(status="failed", error=str(error))
        print("FAIL: " + str(error), file=sys.stderr)
        return 1
    finally:
        save(out / "report.json", report)
        print("Report: " + str(out / "report.json"))


if __name__ == "__main__":
    raise SystemExit(main())
