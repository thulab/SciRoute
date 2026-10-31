#!/usr/bin/env python3
"""Run one frozen D1 view group and retain process/GPU cost samples.

Invoke this once per backend, J, mode, and repeat. Each view runs in its own
process and uses the same previously frozen trace across all backends/J values.
The output directory must be new, so no prior result is silently overwritten.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import subprocess
import sys
import time


VIEWS = ("V0", "V1", "V2", "V3")


def _clock() -> str:
    return datetime.now(timezone.utc).isoformat()


def _memory(pid: int) -> dict[str, int | None]:
    result: dict[str, int | None] = {"rss_bytes": None, "pss_bytes": None}
    try:
        for line in Path(f"/proc/{pid}/status").read_text().splitlines():
            if line.startswith("VmRSS:"):
                result["rss_bytes"] = int(line.split()[1]) * 1024
                break
    except (FileNotFoundError, PermissionError, ProcessLookupError):
        return result
    try:
        for line in Path(f"/proc/{pid}/smaps_rollup").read_text().splitlines():
            if line.startswith("Pss:"):
                result["pss_bytes"] = int(line.split()[1]) * 1024
                break
    except (FileNotFoundError, PermissionError, ProcessLookupError):
        pass
    return result


def _io(pid: int) -> dict[str, int | None]:
    result: dict[str, int | None] = {
        "rchar": None, "read_bytes": None,
        "wchar": None, "write_bytes": None,
    }
    try:
        for line in Path(f"/proc/{pid}/io").read_text().splitlines():
            name, value = line.split(":", 1)
            if name in result:
                result[name] = int(value.strip())
    except (FileNotFoundError, PermissionError, ProcessLookupError):
        pass
    return result


def _gpu() -> list[dict]:
    try:
        result = subprocess.run([
            "nvidia-smi", "--query-gpu=index,utilization.gpu,memory.used",
            "--format=csv,noheader,nounits",
        ], capture_output=True, text=True, timeout=5, check=True)
    except (OSError, subprocess.SubprocessError):
        return []
    rows = []
    for line in result.stdout.splitlines():
        parts = [part.strip() for part in line.split(",")]
        if len(parts) == 3:
            try:
                rows.append({"gpu": int(parts[0]), "utilization_percent": int(parts[1]),
                             "memory_used_mib": int(parts[2])})
            except ValueError:
                pass
    return rows


def _args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base", type=Path, required=True)
    parser.add_argument("--backend", choices=("f1", "q2", "z1"), required=True)
    parser.add_argument("--mode", choices=("replay", "train"), required=True)
    parser.add_argument("--views", nargs="+", choices=VIEWS, required=True)
    parser.add_argument("--gpus", nargs="+", type=int, required=True)
    parser.add_argument("--steps", type=int, default=50)
    parser.add_argument("--batch-size", type=int, default=2)
    parser.add_argument("--cache-total-bytes", type=int, default=4 << 30)
    parser.add_argument("--stats-dir", type=Path, required=True)
    parser.add_argument("--trace-dir", type=Path, required=True)
    parser.add_argument("--bindings", type=Path)
    parser.add_argument("--socket", type=Path)
    parser.add_argument("--zarr-store", type=Path)
    parser.add_argument("--observe-pid", type=int, action="append", default=[])
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    if (len(set(args.views)) != len(args.views) or len(args.gpus) != len(args.views)
            or len(set(args.gpus)) != len(args.gpus)):
        parser.error("each view and GPU must occur exactly once")
    if args.steps <= 0 or args.batch_size <= 0 or args.cache_total_bytes <= 0:
        parser.error("steps, batch size, and cache budget must be positive")
    if args.backend == "q2" and (args.bindings is None or args.socket is None):
        parser.error("Q2 requires --bindings and --socket")
    if args.backend == "z1" and args.zarr_store is None:
        parser.error("Z1 requires --zarr-store")
    if args.output_dir.exists():
        parser.error("output directory already exists")
    return args


def main() -> None:
    args = _args()
    base = args.base
    script = base / "runtime/grib_training/train.py"
    python = base / "venv/bin/python"
    manifests = (base / "manifests/d1-single-levels-validated.json",
                 base / "manifests/d1-pressure-levels-validated.json")
    contract = base / "manifests/d1-sample-contract.json"
    required = [script, python, *manifests, contract]
    required.extend(args.trace_dir /
                    f"d1-{view}-{args.steps}steps-b{args.batch_size}-trace.json"
                    for view in args.views)
    required.extend((args.stats_dir / ("east_asia" if view != "V1" else
                                       "north_america") / "stats.json")
                    for view in args.views)
    if any(not path.is_file() for path in required):
        raise FileNotFoundError("missing runtime, manifest, stats, or frozen trace: "
                                + ", ".join(str(path) for path in required
                                            if not path.is_file()))
    if args.backend == "z1" and not args.zarr_store.is_dir():
        raise FileNotFoundError(args.zarr_store)

    args.output_dir.mkdir(parents=True)
    handles = []
    children: dict[str, subprocess.Popen] = {}
    started = time.perf_counter_ns()
    started_utc = _clock()
    try:
        for view, gpu in zip(args.views, args.gpus):
            directory = args.output_dir / view
            command = [str(python), str(script), "--mode", args.mode,
                       "--backend", args.backend, "--single-manifest",
                       str(manifests[0]), "--pressure-manifest", str(manifests[1]),
                       "--contract", str(contract), "--view", view,
                       "--trace", str(args.trace_dir /
                                      f"d1-{view}-{args.steps}steps-b{args.batch_size}-trace.json"),
                       "--stats", str(args.stats_dir /
                                      ("east_asia" if view != "V1" else
                                       "north_america") / "stats.json"),
                       "--steps", str(args.steps), "--batch-size",
                       str(args.batch_size), "--device",
                       "cuda:0" if args.mode == "train" else "cpu",
                       "--cache-bytes", str(args.cache_total_bytes // len(args.views)),
                       "--output-dir", str(directory)]
            if args.backend == "q2":
                command.extend(["--bindings", str(args.bindings),
                                "--socket", str(args.socket)])
            if args.backend == "z1":
                command.extend(["--zarr-store", str(args.zarr_store)])
            stdout = (args.output_dir / f"{view}.stdout.log").open("w")
            stderr = (args.output_dir / f"{view}.stderr.log").open("w")
            handles.extend((stdout, stderr))
            environment = os.environ.copy()
            environment["CUDA_VISIBLE_DEVICES"] = str(gpu)
            environment["OMP_NUM_THREADS"] = "1"
            environment["MKL_NUM_THREADS"] = "1"
            children[view] = subprocess.Popen(command, stdout=stdout,
                                               stderr=stderr, env=environment)

        peaks: dict[str, dict[str, int]] = {}
        with (args.output_dir / "resources.jsonl").open("w") as resource_file:
            while any(child.poll() is None for child in children.values()):
                sample = {"elapsed_ns": time.perf_counter_ns() - started,
                          "utc": _clock(), "processes": {},
                          "observed": {}, "gpus": _gpu()}
                for view, child in children.items():
                    memory = _memory(child.pid)
                    sample["processes"][view] = {"pid": child.pid,
                                                  "exit_code": child.poll(),
                                                  **memory, **_io(child.pid)}
                    peak = peaks.setdefault(view, {"rss_bytes": 0,
                                                   "pss_bytes": 0})
                    for key in peak:
                        peak[key] = max(peak[key], memory[key] or 0)
                for pid in args.observe_pid:
                    sample["observed"][str(pid)] = {
                        **_memory(pid), **_io(pid)}
                resource_file.write(json.dumps(sample, separators=(",", ":"))
                                    + "\n")
                resource_file.flush()
                time.sleep(1)

        exits = {view: child.wait() for view, child in children.items()}
        summaries = {}
        for view in args.views:
            summary = args.output_dir / view / "summary.json"
            if summary.is_file():
                summaries[view] = json.loads(summary.read_text())
        wall_ns = time.perf_counter_ns() - started
        result = {
            "protocol": "iotdbfs-era5-matrix/1",
            "started_utc": started_utc, "ended_utc": _clock(),
            "backend": args.backend, "mode": args.mode, "views": args.views,
            "physical_gpus": dict(zip(args.views, args.gpus)),
            "steps": args.steps, "batch_size": args.batch_size,
            "cache_total_bytes": args.cache_total_bytes,
            "cache_bytes_per_f1_process": (args.cache_total_bytes // len(args.views)
                                           if args.backend == "f1" else None),
            "wall_ns": wall_ns, "exit_codes": exits,
            "peak_process_memory_bytes": peaks,
            "aggregate_samples_per_s": (
                len(summaries) * args.steps * args.batch_size * 1e9 / wall_ns),
            "summaries": summaries,
        }
        (args.output_dir / "matrix-summary.json").write_text(
            json.dumps(result, indent=2, sort_keys=True) + "\n")
        print(json.dumps({key: value for key, value in result.items()
                          if key != "summaries"}, sort_keys=True))
        if any(code != 0 for code in exits.values()):
            raise SystemExit("one or more D1 view runs failed; partial logs retained")
    except BaseException:
        for child in children.values():
            if child.poll() is None:
                child.terminate()
        for child in children.values():
            try:
                child.wait(timeout=10)
            except subprocess.TimeoutExpired:
                child.kill()
        raise
    finally:
        for handle in handles:
            handle.close()


if __name__ == "__main__":
    main()
