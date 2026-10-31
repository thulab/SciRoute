"""Read-only owner object-reader mechanism comparison on sealed FM1 corpora.

This does not run end-to-end format queries or alter database/service state.
Credentials are loaded in memory from the trusted daemon, never serialized.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import statistics
import subprocess
import time

from experiments.single_file.environment import connection_environment

CORPORA = {
    "grib": "vd_fm1_6f4bcb67a5d7405d",
    "hdf5": "vd_fm1_c6f03d32cb744156",
}
ORDERS = ((1, 2, 4), (2, 4, 1), (4, 1, 2))


def dump(path: Path, value):
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--probe", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--trusted-pid", required=True, type=int)
    parser.add_argument("--database", required=True)
    parser.add_argument("--library-path", required=True)
    parser.add_argument("--smoke", action="store_true")
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    probe = args.probe.resolve(strict=True)
    env = {**os.environ, **connection_environment(args.trusted_pid, args.database)}
    env["LD_LIBRARY_PATH"] = args.library_path
    cpus = {2, 3}
    if not cpus <= os.sched_getaffinity(0):
        raise RuntimeError("required client CPU affinity unavailable")
    os.sched_setaffinity(0, cpus)
    plan = dict(
        kind="readonly-object-reader-mechanism", end_to_end_query=False,
        orders=ORDERS, rounds=3, samples_per_cell=5, operations=32,
        clients=4, batches=[1, 8], corpora=CORPORA,
        cache="retained server/OS; per-process reader warmup; not cold disk",
        client_cpus=sorted(cpus), database_resources="unchanged shared original instance",
        binary_sha256=hashlib.sha256(probe.read_bytes()).hexdigest(),
        start_utc=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        kernel=platform.release(), machine=platform.machine(),
        cpu_count=os.cpu_count(), initial_load=os.getloadavg(),
    )
    dump(args.output / "plan.json", plan)
    rows = []
    identities = {}
    try:
        for block, order in enumerate(ORDERS[:1] if args.smoke else ORDERS):
            for format_name, prefix in CORPORA.items():
                for batch in ([1] if args.smoke else [1, 8]):
                    for sample in range(1 if args.smoke else 5):
                        for size in order:
                            command = [str(probe), "--prefix", prefix, "--pool-size", str(size),
                                       "--clients", "4", "--operations", "32", "--batch", str(batch)]
                            before = time.monotonic_ns()
                            result = subprocess.run(command, env=env, capture_output=True,
                                                    text=True, timeout=90, check=False)
                            index = len(rows)
                            (args.output / f"{index:03d}.stdout").write_text(result.stdout)
                            # Probe errors are deliberately sanitized and never include SDK exceptions.
                            (args.output / f"{index:03d}.stderr").write_text(result.stderr)
                            if result.returncode:
                                raise RuntimeError(f"probe failed for {format_name}, pool {size}: exit {result.returncode}")
                            row = json.loads(result.stdout)
                            if not all(row.get(k) is True for k in ("complete", "correctness", "head_unchanged")):
                                raise RuntimeError("probe verification incomplete")
                            identity = tuple(row[k] for k in ("namespace", "incarnation", "head_commit", "head_sequence", "oracle_identity"))
                            if format_name in identities and identities[format_name] != identity:
                                raise RuntimeError("namespace head changed between measurements")
                            identities[format_name] = identity
                            row.update(format=format_name, prefix=prefix, block=block, sample=sample,
                                       envelope_ns=time.monotonic_ns() - before, load=os.getloadavg())
                            rows.append(row)
                            with (args.output / "measurements.jsonl").open("a") as output:
                                output.write(json.dumps(row, sort_keys=True) + "\n")
                            print(f"{len(rows)} completed: {format_name}, batch={batch}, readers={size}", flush=True)
        summary = []
        for format_name in CORPORA:
            for batch in sorted({row["batch"] for row in rows}):
                selected = [r for r in rows if r["format"] == format_name and r["batch"] == batch]
                amounts = {r["bytes"] for r in selected}
                if len(amounts) != 1:
                    raise RuntimeError("reader configurations performed different byte workloads")
                baseline = statistics.median(r["wall_ns"] for r in selected if r["pool_size"] == 1)
                for size in (1, 2, 4):
                    values = [r["wall_ns"] for r in selected if r["pool_size"] == size]
                    median = statistics.median(values)
                    summary.append(dict(format=format_name, batch=batch, pool_size=size,
                                        samples=len(values), bytes=next(iter(amounts)),
                                        median_ms=median / 1e6, min_ms=min(values) / 1e6,
                                        max_ms=max(values) / 1e6, speedup=baseline / median,
                                        mib_per_second=next(iter(amounts)) / (1024**2) / (median / 1e9)))
        dump(args.output / "summary.json", dict(complete=True, rows=len(rows), summary=summary,
                                               identities=identities, final_load=os.getloadavg(),
                                               finish_utc=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())))
    except BaseException as error:
        # Error strings above contain only public stage/exit information.
        dump(args.output / "failure.json", dict(complete=False, rows=len(rows),
                                               exception=type(error).__name__))
        raise


if __name__ == "__main__":
    main()
