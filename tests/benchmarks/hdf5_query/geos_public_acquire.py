#!/usr/bin/env python3
"""Fetch distinct public GEOS-5.7.2 time files for the scale experiment."""

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import hashlib
import json
from pathlib import Path
import subprocess
import time


BASE = ("https://gmao.gsfc.nasa.gov/media/gmaoftp/ops/"
        "GEOS572_Sample_Data/inst3_3d_asm_Np/")
TIMES = [f"201107{day:02d}_{hour:02d}00"
         for day in (1, 2) for hour in range(0, 24, 3)]
MAGIC = b"\x89HDF\r\n\x1a\n"


def digest(path):
    value = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1 << 20), b""):
            value.update(block)
    return value.hexdigest()


def fetch(timestamp, output):
    name = f"DAS.ops.asm.inst3_3d_asm_Np.GEOS572.{timestamp}.V01.nc4"
    path = output / name
    if not path.exists() or path.stat().st_size < 400_000_000:
        temporary = path.with_suffix(".part")
        command = ["curl", "--fail", "--location", "--silent", "--show-error",
                   "--retry", "2", "--retry-all-errors", "--connect-timeout", "30",
                   "--max-time", "3600", "--output", str(temporary), BASE + name]
        subprocess.run(command, check=True)
        if temporary.stat().st_size < 400_000_000:
            raise ValueError(f"truncated GEOS download: {temporary}")
        temporary.replace(path)
    with path.open("rb") as stream:
        if stream.read(8) != MAGIC:
            raise ValueError(f"invalid HDF5 magic: {path}")
    return {"timestamp": timestamp, "url": BASE + name, "path": str(path),
            "bytes": path.stat().st_size, "sha256": digest(path)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--exclude", nargs="*", default=[])
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    timestamps = [value for value in TIMES if value not in args.exclude]
    start = time.perf_counter_ns()
    entries = []
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {pool.submit(fetch, timestamp, args.output): timestamp
                   for timestamp in timestamps}
        for future in as_completed(futures):
            entry = future.result()
            entries.append(entry)
            print(json.dumps(entry), flush=True)
    report = {"protocol": "iotdbfs-geos-public-acquire/1",
              "elapsed_ns": time.perf_counter_ns() - start,
              "sources": sorted(entries, key=lambda entry: entry["timestamp"])}
    (args.output / "acquisition.json").write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n")


if __name__ == "__main__":
    main()
