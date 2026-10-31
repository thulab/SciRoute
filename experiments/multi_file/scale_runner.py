#!/usr/bin/env python3
"""Publish all fixed-K N/D points before timing them in one database state."""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import time

POINTS = ((8, 4), (32, 4), (128, 4), (8, 16), (8, 64))
ROOT = Path(__file__).resolve().parent


def save(path, data):
    path.write_text(json.dumps(data, indent=2, sort_keys=True, allow_nan=False) + "\n")


def main(args):
    args.output.mkdir(parents=True, exist_ok=False)
    args.work.mkdir(parents=True, exist_ok=False)
    run = {"protocol": "iotdbfs-multifile-scale-campaign/1", "complete": False,
           "controls": {"K": 2, "depth": 2, "campaigns": 3, "repeats": 5},
           "points": {}, "stage": "prepare", "publication_during_query_campaign": False,
           "source_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
           "started_unix_ns": time.time_ns()}
    save(args.output / "scale-report.json", run)
    common = [sys.executable, str(ROOT / "small_runner.py"), "--native", str(args.native),
              "--environment", str(args.environment), "--writer-lock", str(args.writer_lock),
              "--campaigns", "3", "--repeats", "5"]

    def execute(extra):
        started = time.monotonic()
        # Inherit clean Python environment; child selects native library paths.
        subprocess.run(common + extra, check=True, timeout=1800)
        return time.monotonic() - started

    for n, d in POINTS:
        name = f"n{n}-d{d}"
        suffix = "small" if (n, d) == (8, 4) else name
        specs = ["--hdf5-spec", str(ROOT / "specs" / f"runner-hdf5-{suffix}.json"),
                 "--grib-spec", str(ROOT / "specs" / f"runner-grib-{suffix}.json")]
        if (n, d) == (8, 4):
            prepared, data = args.baseline_report, args.baseline_work
        else:
            output, data = args.output / ("prepare-" + name), args.work / ("prepare-" + name)
            elapsed = execute(specs + ["--output", str(output), "--work", str(data), "--prepare-only"])
            prepared = output / "report.json"
            print(json.dumps({"stage": "prepared", "point": name, "process_seconds": elapsed}), flush=True)
        run["points"][name] = {"N": n, "D": d, "spec_args": specs,
            "preparation_report": str(prepared), "data_work": str(data)}
        save(args.output / "scale-report.json", run)
    run["stage"] = "query"
    run["query_started_unix_ns"] = time.time_ns()
    save(args.output / "scale-report.json", run)
    for name, point in run["points"].items():
        output, work = args.output / name, args.work / name
        point["query_process_seconds"] = execute(point["spec_args"] + ["--output", str(output), "--work", str(work),
            "--reuse-preparation", point["preparation_report"], "--reuse-work", point["data_work"]])
        point["report"] = str(output / "report.json")
        subprocess.run([sys.executable, str(ROOT / "analyze_small.py"), point["report"],
                        "--output", str(output / "analysis.json")], check=True, timeout=30)
        print(json.dumps({"stage": "queried", "point": name}), flush=True)
        save(args.output / "scale-report.json", run)
    run.update(complete=True, stage="complete", finished_unix_ns=time.time_ns())
    save(args.output / "scale-report.json", run)
    print(json.dumps({"complete": True, "points": len(POINTS), "observations": 180 * len(POINTS)}), flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("native", "environment", "writer-lock", "output", "work", "baseline-report", "baseline-work"):
        parser.add_argument("--" + name, type=Path, required=True)
    main(parser.parse_args())
