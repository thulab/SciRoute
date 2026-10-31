#!/usr/bin/env python3
"""Single-client physical Arrow A–E benchmark on the dedicated public mount.

No training, predicates, transforms or synthetic cross-file windows. Restarts
only the specified experiment daemon, restores it in finally, never drops OS or
database caches. See docs/deliverables/arrow-query-paths-experiment.md for timing boundaries.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import statistics
import subprocess
import time

import numpy as np
import pyarrow as pa
import pyarrow.ipc as ipc

from public_iotdbfs import control
from run_cache_parallel_ablation import Mount
from summarize_query_profile import read_server_log


DATASETS = {
    "ETTm1": (7, 1790670158165, "cbd4833ce525c32dd883bf6e1b174f2d2b2ab7241889358c1a3325883c2b179b"),
    "UCI_Electricity_370": (370, 1790670177863, "536c3c72afb04f09daadf860ef021719bb4f90515c10a6710bd35d78de18fa0f"),
}


def save(path, value):
    path.write_text(json.dumps(value, indent=2) + "\n")


def cases(channels):
    result = []
    for count in sorted(set((1, min(7, channels), channels))):
        # Deliberately non-sorted projection verifies requested column ordering.
        children = list(reversed(range(count)))
        for label, begin, length in (("small", 37, 128), ("boundary", 4032, 128),
                                     ("tile", 0, 4096), ("full-row", 0, 8192)):
            result.append(dict(id=f"c{count}-{label}", batch=0, row=0, field="target",
                               children=children, begin=begin, length=length))
    return result


def owned(array):
    if isinstance(array, pa.ChunkedArray):
        array = array.combine_chunks()
    if array.type != pa.float32():
        raise ValueError(f"expected float32, got {array.type}")
    valid = np.array(array.is_valid().to_numpy(zero_copy_only=False), dtype=np.uint8, copy=True)
    values = np.array(array.fill_null(0).to_numpy(zero_copy_only=False), dtype="<f4", copy=True)
    return values, valid


def digest(columns, index):
    h = hashlib.sha256()
    for values, valid in columns:
        h.update(values.tobytes())
        h.update(valid.tobytes())
    return {"digest": h.hexdigest(),
            "index_digest": hashlib.sha256(np.asarray(index, dtype="<u8").tobytes()).hexdigest()}


def oracle(source, requests):
    result = {}
    with pa.memory_map(str(source), "r") as mapped:
        reader = ipc.open_stream(mapped)
        batches = list(reader)
        for case in requests:
            outer = batches[case["batch"]].column(case["field"])[case["row"]].values
            columns = [owned(outer[child].values.slice(case["begin"], case["length"]))
                       for child in case["children"]]
            index = np.arange(case["begin"], case["begin"] + case["length"], dtype="<u8")
            result[case["id"]] = digest(columns, index)
    return result


def declaration(logical, version, case):
    def bound(value, kind):
        return {"kind": kind, "value": {"type": "uint64", "value": value}}
    return {"kind": "typed", "request": {
        "sources": {"paths": [{"kind": "exact", "path": logical}],
                    "versions": {"kind": "exact", "exact": version},
                    "formats": {"adapters": "listed", "adapter_ids": ["arrow"],
                                "include_byte_passthrough": False}},
        "subset": {"projection": ["index"] + [f"value.{c}" for c in case["children"]],
            "physical_array": {"field_id": case["field"], "record_batch_index": case["batch"],
                "row_index": case["row"], "values": [{"child_indices": [c]} for c in case["children"]]},
            "slices": [{"dimension": "index", "lower": bound(case["begin"], "inclusive"),
                        "upper": bound(case["begin"] + case["length"], "exclusive")}]}}}


def execute_e(socket, logical, version, case):
    request = declaration(logical, version, case)
    timing = {}
    start = time.perf_counter_ns()
    response = control(socket, request, timing)
    registered = time.perf_counter_ns()
    path = response["path"]
    with pa.memory_map(path, "r") as mapped:
        encoded_bytes = mapped.size()
        table = ipc.open_stream(mapped).read_all()
        decoded = time.perf_counter_ns()
        columns = [owned(table.column(f"value.{c}")) for c in case["children"]]
        index = np.array(table.column("index").to_numpy(), dtype="<u8", copy=True)
        consumed = time.perf_counter_ns()
        del table
    end = time.perf_counter_ns()
    if not np.array_equal(index, np.arange(case["begin"], case["begin"] + case["length"], dtype="<u8")):
        raise ValueError("E index mismatch")
    return dict(path=path, case=case["id"], wall_ns=end-start,
                registration_ns=registered-start, ipc_read_ns=decoded-registered,
                consume_ns=consumed-decoded, close_ns=end-consumed,
                encoded_bytes=encoded_bytes, **timing, **digest(columns, index))


def trimmed(values):
    ordered = sorted(values)
    return statistics.mean(ordered[1:-1] if len(ordered) > 2 else ordered)


def single_client_records(parsed):
    """Attach wave counters only when a wave contains exactly one query."""
    counters = {(r["case"], r["repeat"]): r for r in parsed if r["type"] == "wave"}
    for row in parsed:
        if row["type"] != "query":
            continue
        wave = counters[(row["case"], row["repeat"])]
        if wave["clients"] != 1 or row["clients"] != 1:
            raise ValueError("single-client runner cannot attribute concurrent wave counters")
        yield dict(wave, **row)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("work-dir", "mount", "socket", "binary", "fallback", "probe", "output"):
        parser.add_argument(f"--{name}", type=Path, required=True)
    parser.add_argument("--daemon-pid", type=int, required=True)
    parser.add_argument("--repeats", type=int, default=10)
    parser.add_argument("--campaigns", type=int, default=2)
    parser.add_argument("--dataset", choices=list(DATASETS), action="append")
    parser.add_argument("--case", action="append")
    args = parser.parse_args()
    if args.repeats < 1 or args.campaigns < 1:
        parser.error("positive repetitions required")
    args.output.mkdir(parents=True, exist_ok=False)
    mount = Mount(args)
    settings = {"query.cache.max_entries": 16384, "query.cache.max_inflight": 256,
                "query.cache.worker_threads": 4, "query.cache.memory_budget_bytes": 1073741824}
    original = Path(mount.original_config).read_text()
    config = args.output / "benchmark-private.properties"
    config.write_text("\n".join(line for line in original.splitlines()
        if line.split("=", 1)[0].strip() not in settings) + "\n" +
        "\n".join(f"{k}={v}" for k, v in settings.items()) + "\n")
    config.chmod(0o600)
    records = []
    try:
        for dataset in args.dataset or DATASETS:
            channels, version, sha = DATASETS[dataset]
            filename = f"{dataset}-none-8192.arrows"
            source = args.work_dir / "arrow" / filename
            with source.open("rb") as stream:
                if hashlib.file_digest(stream, "sha256").hexdigest() != sha:
                    raise ValueError("local source does not match frozen ingestion hash")
            requests = [c for c in cases(channels) if not args.case or c["id"] in args.case]
            if not requests:
                raise ValueError("empty case selection")
            expected = oracle(source, requests)
            save(args.output / f"{dataset}-oracle.json", expected)
            for campaign in range(1, args.campaigns + 1):
                current = requests if campaign % 2 else list(reversed(requests))
                casefile = args.output / f"r{campaign}-{dataset}-cases.json"
                save(casefile, current)
                arms = [("A", "mmap"), ("A-read", "read"), ("B", "read"), ("C", "read"), ("D", "read")]
                if not campaign % 2:
                    arms.reverse()
                # D starts with a fresh daemon. This still leaves OS/IoTDB caches intact.
                mount.start(config, None, f"r{campaign}-{dataset}-direct")
                for arm, native_io in arms:
                    tag = f"r{campaign}-{dataset}-{arm}"
                    output = args.output / f"{tag}.jsonl"
                    command = [str(args.probe), "--path", arm[0], "--native-io", native_io,
                        "--config", str(config), "--source", str(args.mount / filename if arm == "D" else source),
                        "--logical", "/" + filename, "--version", str(version), "--cases", str(casefile),
                        "--output", str(output), "--repeats", str(args.repeats)]
                    print("START", tag, flush=True)
                    with (args.output / f"{tag}.log").open("w") as log:
                        subprocess.run(command, env=mount.env, check=True, stdout=log,
                                       stderr=subprocess.STDOUT, timeout=1800)
                    parsed = [json.loads(line) for line in output.read_text().splitlines()]
                    for row in single_client_records(parsed):
                        for key, value in expected[row["case"]].items():
                            if row[key] != value:
                                raise ValueError(f"{tag} {row['case']} {key} mismatch")
                        records.append(dict(row, arm=arm, dataset=dataset, campaign=campaign))
                    print("PASS", tag, flush=True)
                for case in current:
                    tag = f"r{campaign}-{dataset}-E-{case['id']}"
                    profile = args.output / f"{tag}-profile.jsonl"
                    mount.start(config, profile, tag)
                    rows = []
                    for iteration in range(args.repeats + 1):
                        row = execute_e(args.socket, "/" + filename, version, case)
                        for key, value in expected[case["id"]].items():
                            if row[key] != value:
                                raise ValueError(f"{tag} {key} mismatch")
                        rows.append(dict(row, arm="E", dataset=dataset, campaign=campaign,
                                         iteration=iteration, temperature="cold" if iteration == 0 else "warm"))
                    # stop flushes the profile and bounds route lifetimes.
                    mount.stop()
                    profiles = read_server_log(profile)
                    by_path = {p["path"]: p for p in profiles}
                    if len(profiles) != len(rows) or len(by_path) != len(rows):
                        raise ValueError("E profile count/route identity mismatch")
                    for row in rows:
                        server = by_path[row["path"]]
                        if not server["stages_ns"].get("typed_prepare") or not server["stages_ns"].get("typed_encode"):
                            raise ValueError("E bypassed typed execution")
                        row["server"] = server
                    records.extend(rows)
                    save(args.output / f"{tag}.json", rows)
                    print("PASS", tag, flush=True)
                save(args.output / "measurements.json", records)
    finally:
        try:
            mount.start(mount.original_config, None, "restored")
        except Exception:
            mount.start(mount.original_config, None, "fallback", args.fallback)
        save(args.output / "restored.json", {"pid": mount.pid, "profile_enabled": False})
        print(f"RESTORED daemon pid={mount.pid}", flush=True)


if __name__ == "__main__":
    main()
